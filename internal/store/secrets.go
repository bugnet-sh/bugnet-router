package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Secret states, as the secrets table names them
const (
	SecretApplying = "applying"
	SecretActive   = "active"
	SecretFailed   = "failed"
)

// Secret sources, as the secrets table names them
const (
	SourceLink    = "link"
	SourceBridge  = "bridge"
	SourceGateway = "gateway"
)

// Secret is one row of secrets, its value sealed to the egress proxy
type Secret struct {
	ID                uuid.UUID
	PlacementID       uuid.UUID
	Name              string
	Hosts             []string
	HeaderNames       []string
	FetchOnly         bool
	PlaceholderSHA256 []byte
	Sealed            []byte
	KeyID             string
	Source            string
	State             string
	FailureReason     string
	// IdempotencyKey is the put's key, link:<link id> for a link's submit
	IdempotencyKey string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Replaced says whether the row's last put replaced an earlier secret of its name
func (s Secret) Replaced() bool { return !s.CreatedAt.Equal(s.UpdatedAt) }

const secretColumns = `id, placement_id, name, hosts, header_names, fetch_only,
	placeholder_sha256, sealed, key_id, source, state, coalesce(failure_reason, ''),
	coalesce(idempotency_key, ''), created_at, updated_at`

func scanSecret(row pgx.Row, extra ...any) (Secret, error) {
	var x Secret
	dest := append([]any{&x.ID, &x.PlacementID, &x.Name, &x.Hosts, &x.HeaderNames,
		&x.FetchOnly, &x.PlaceholderSHA256, &x.Sealed, &x.KeyID, &x.Source, &x.State,
		&x.FailureReason, &x.IdempotencyKey, &x.CreatedAt, &x.UpdatedAt}, extra...)
	if err := row.Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Secret{}, ErrNotFound
		}
		return Secret{}, err
	}
	x.CreatedAt, x.UpdatedAt = utc(x.CreatedAt), utc(x.UpdatedAt)
	return x, nil
}

func (s *Store) secretWhere(ctx context.Context, where string, args ...any) (Secret, error) {
	x, err := scanSecret(s.q.QueryRow(ctx, `select `+secretColumns+` from secrets where `+where,
		args...))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Secret{}, fmt.Errorf("select secret: %w", err)
	}
	return x, err
}

// Secrets answers the placement's secrets by name
func (s *Store) Secrets(ctx context.Context, placement uuid.UUID) ([]Secret, error) {
	rows, err := s.q.Query(ctx, `select `+secretColumns+
		` from secrets where placement_id = $1 order by name`, placement)
	if err != nil {
		return nil, fmt.Errorf("select secrets: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Secret, error) {
		return scanSecret(r)
	})
	if err != nil {
		return nil, fmt.Errorf("select secrets: %w", err)
	}
	return out, nil
}

// SecretByID answers one secret or ErrNotFound
func (s *Store) SecretByID(ctx context.Context, id uuid.UUID) (Secret, error) {
	return s.secretWhere(ctx, "id = $1", id)
}

// SecretByKey answers the placement's secret whose last put carried key, or ErrNotFound
func (s *Store) SecretByKey(ctx context.Context, placement uuid.UUID, key string) (Secret, error) {
	return s.secretWhere(ctx, "placement_id = $1 and idempotency_key = $2", placement, key)
}

// PutSecret stores row as the placement's secret of its name, answering whether it replaced one
func (s *Store) PutSecret(ctx context.Context, row Secret) (Secret, bool, error) {
	var replaced bool
	// xmax is set only on a row the conflict updated
	got, err := scanSecret(s.q.QueryRow(ctx, `
		insert into secrets (id, placement_id, name, hosts, header_names, fetch_only,
			placeholder_sha256, sealed, key_id, source, state, idempotency_key)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		on conflict (placement_id, name) do update set
			hosts = excluded.hosts, header_names = excluded.header_names,
			fetch_only = excluded.fetch_only, placeholder_sha256 = excluded.placeholder_sha256,
			sealed = excluded.sealed, key_id = excluded.key_id, source = excluded.source,
			state = excluded.state, failure_reason = null,
			idempotency_key = excluded.idempotency_key, updated_at = now()
		returning `+secretColumns+`, xmax::text <> '0'`,
		row.ID, row.PlacementID, row.Name, row.Hosts, emptyIfNil(row.HeaderNames),
		row.FetchOnly, row.PlaceholderSHA256, row.Sealed, row.KeyID, row.Source, row.State,
		nullIfEmpty(row.IdempotencyKey)), &replaced)
	if err != nil {
		return Secret{}, false, fmt.Errorf("put secret: %w", err)
	}
	return got, replaced, nil
}

// DeleteSecret removes the placement's secret of name, answering it or ErrNotFound
func (s *Store) DeleteSecret(
	ctx context.Context, placement uuid.UUID, name string,
) (Secret, error) {
	x, err := scanSecret(s.q.QueryRow(ctx, `delete from secrets
		where placement_id = $1 and name = $2 returning `+secretColumns, placement, name))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Secret{}, fmt.Errorf("delete secret: %w", err)
	}
	return x, err
}

// MarkSecrets moves each loaded secret still at its loaded updated_at to state, with reason
func (s *Store) MarkSecrets(
	ctx context.Context, placement uuid.UUID, loaded []Secret, state, reason string,
) (int64, error) {
	ids := make([]uuid.UUID, len(loaded))
	ats := make([]time.Time, len(loaded))
	for i, x := range loaded {
		ids[i], ats[i] = x.ID, x.UpdatedAt
	}
	tag, err := s.q.Exec(ctx, `
		update secrets s set state = $2, failure_reason = $3
		from unnest($4::uuid[], $5::timestamptz[]) as l (id, at)
		where s.placement_id = $1 and s.id = l.id and s.updated_at = l.at`,
		placement, state, nullIfEmpty(reason), ids, ats)
	if err != nil {
		return 0, fmt.Errorf("mark secrets: %w", err)
	}
	return tag.RowsAffected(), nil
}

// SecretsPending says whether apply_secrets has work, a secret applying or a change since a run
func (s *Store) SecretsPending(ctx context.Context, placement uuid.UUID) (bool, error) {
	var pending bool
	err := s.q.QueryRow(ctx, `
		select secrets_version <> secrets_applied_version or exists (
			select 1 from secrets
			where placement_id = $1 and not fetch_only and state = $2)
		from placements where id = $1`, placement, SecretApplying).Scan(&pending)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, fmt.Errorf("secrets pending: %w", err)
	}
	return pending, nil
}

// BumpSecretsVersion adds one to secrets_version, pending leaving the change for apply_secrets
func (s *Store) BumpSecretsVersion(ctx context.Context, placement uuid.UUID, pending bool) error {
	tag, err := s.q.Exec(ctx, `
		update placements set secrets_version = secrets_version + 1,
			secrets_applied_version = case
				when not $2 and secrets_applied_version = secrets_version
				then secrets_version + 1 else secrets_applied_version end
		where id = $1`, placement, pending)
	return rowChanged(tag.RowsAffected(), err, "bump secrets version", placement)
}

// FinishSecretsApply adds one to secrets_version and records it as applied
func (s *Store) FinishSecretsApply(ctx context.Context, placement uuid.UUID) error {
	tag, err := s.q.Exec(ctx, `
		update placements set secrets_version = secrets_version + 1,
			secrets_applied_version = secrets_version + 1
		where id = $1`, placement)
	return rowChanged(tag.RowsAffected(), err, "finish secrets apply", placement)
}

// MintEgressToken stores the digest of a new tunnel token, false when the placement has one
func (s *Store) MintEgressToken(
	ctx context.Context, placement uuid.UUID, sha []byte,
) (bool, error) {
	tag, err := s.q.Exec(ctx, `update placements set egress_token_sha256 = $2
		where id = $1 and egress_token_sha256 is null`, placement, sha)
	if err != nil {
		return false, fmt.Errorf("mint egress token: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ClearEgressToken forgets the tunnel token of this digest, so the next run mints another
func (s *Store) ClearEgressToken(ctx context.Context, placement uuid.UUID, sha []byte) error {
	_, err := s.q.Exec(ctx, `update placements set egress_token_sha256 = null
		where id = $1 and egress_token_sha256 = $2`, placement, sha)
	if err != nil {
		return fmt.Errorf("clear egress token: %w", err)
	}
	return nil
}

/* Links */

// SecretLink is one row of secret_links
type SecretLink struct {
	ID               uuid.UUID
	PlacementID      uuid.UUID
	TokenSHA256      []byte
	PairingCode      string
	SuggestedName    string
	SuggestedHosts   []string
	SuggestedHeaders []string
	CreatedAt        time.Time
	ExpiresAt        time.Time
	UsedAt           *time.Time
	SubmissionID     string
	FailedAttempts   int
	// Expired is the database's own reading of expires_at
	Expired bool
}

const linkColumns = `id, placement_id, token_sha256, pairing_code, coalesce(suggested_name, ''),
	suggested_hosts, suggested_header_names, created_at, expires_at, used_at,
	coalesce(submission_id, ''), failed_attempts, expires_at <= now()`

func scanLink(row pgx.Row) (SecretLink, error) {
	var l SecretLink
	err := row.Scan(&l.ID, &l.PlacementID, &l.TokenSHA256, &l.PairingCode, &l.SuggestedName,
		&l.SuggestedHosts, &l.SuggestedHeaders, &l.CreatedAt, &l.ExpiresAt, &l.UsedAt,
		&l.SubmissionID, &l.FailedAttempts, &l.Expired)
	if errors.Is(err, pgx.ErrNoRows) {
		return SecretLink{}, ErrNotFound
	}
	if err != nil {
		return SecretLink{}, fmt.Errorf("select secret link: %w", err)
	}
	l.CreatedAt, l.ExpiresAt, l.UsedAt = utc(l.CreatedAt), utc(l.ExpiresAt), utcPtr(l.UsedAt)
	return l, nil
}

// InsertSecretLink stores a link living ttl, answering the stored one when its token exists
func (s *Store) InsertSecretLink(
	ctx context.Context, l SecretLink, ttl time.Duration,
) (SecretLink, error) {
	_, err := s.q.Exec(ctx, `
		insert into secret_links (id, placement_id, token_sha256, pairing_code, suggested_name,
			suggested_hosts, suggested_header_names, expires_at)
		values ($1, $2, $3, $4, $5, $6, $7, now() + make_interval(secs => $8))
		on conflict (token_sha256) do nothing`,
		l.ID, l.PlacementID, l.TokenSHA256, l.PairingCode, nullIfEmpty(l.SuggestedName),
		emptyIfNil(l.SuggestedHosts), emptyIfNil(l.SuggestedHeaders), ttl.Seconds())
	if err != nil {
		return SecretLink{}, fmt.Errorf("insert secret link: %w", err)
	}
	return s.SecretLinkByToken(ctx, l.TokenSHA256)
}

// SecretLinkByToken answers the link of a token digest, or ErrNotFound
func (s *Store) SecretLinkByToken(ctx context.Context, sha []byte) (SecretLink, error) {
	return scanLink(s.q.QueryRow(ctx, `select `+linkColumns+
		` from secret_links where token_sha256 = $1`, sha))
}

// ConsumeSecretLink uses a live, unused link for submission, false when it is used or expired
func (s *Store) ConsumeSecretLink(
	ctx context.Context, sha []byte, submission string,
) (bool, error) {
	tag, err := s.q.Exec(ctx, `
		update secret_links set used_at = now(), submission_id = $2
		where token_sha256 = $1 and used_at is null and expires_at > now()`, sha, submission)
	if err != nil {
		return false, fmt.Errorf("consume secret link: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// FailSecretLink counts one refused submit against a link
func (s *Store) FailSecretLink(ctx context.Context, sha []byte) error {
	_, err := s.q.Exec(ctx, `update secret_links set failed_attempts = failed_attempts + 1
		where token_sha256 = $1`, sha)
	if err != nil {
		return fmt.Errorf("fail secret link: %w", err)
	}
	return nil
}

/* Locks */

// TryLock runs fn holding the advisory lock name in a transaction of its own, false when held
func (s *Store) TryLock(ctx context.Context, name string, fn func() error) (bool, error) {
	tx, err := s.q.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var held bool
	if err := tx.QueryRow(ctx, `select pg_try_advisory_xact_lock(hashtextextended($1, 0))`,
		"bugnet:"+name).Scan(&held); err != nil {
		return false, fmt.Errorf("lock %s: %w", name, err)
	}
	if !held {
		return false, nil
	}
	return true, fn()
}

func emptyIfNil(xs []string) []string {
	if xs == nil {
		return []string{}
	}
	return xs
}

func rowChanged(n int64, err error, what string, id uuid.UUID) error {
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if n == 0 {
		return fmt.Errorf("%s of %s: %w", what, id, ErrNotFound)
	}
	return nil
}
