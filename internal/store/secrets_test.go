package store_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/migrations"
)

// appliedVersion is the goose version of the migration that adds secrets_applied_version
const appliedVersion = 9

func TestMigrationSecretsAppliedUpDownUp(t *testing.T) {
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(testenv.DatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = db.Close() })
	gp, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	columns := func() int {
		var n int
		if err := db.QueryRowContext(ctx, `select count(*) from information_schema.columns
			where table_schema = current_schema() and table_name = 'placements'
				and column_name = 'secrets_applied_version'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, step := range []struct {
		name string
		to   int64
		down bool
		want int
	}{{"up", appliedVersion, false, 1}, {"down", appliedVersion - 1, true, 0},
		{"up again", appliedVersion, false, 1}} {
		if step.down {
			_, err = gp.DownTo(ctx, step.to)
		} else {
			_, err = gp.UpTo(ctx, step.to)
		}
		if err != nil || columns() != step.want {
			t.Fatalf("%s: %v, %d columns, want %d", step.name, err, columns(), step.want)
		}
	}
}

// secretRow is a secret of the placement named name for huggingface.co, applying
func secretRow(placement uuid.UUID, name string) store.Secret {
	sum := sha256.Sum256([]byte(placement.String() + name))
	return store.Secret{
		ID: uuid.New(), PlacementID: placement, Name: name, Hosts: []string{"huggingface.co"},
		HeaderNames: []string{}, PlaceholderSHA256: sum[:], Sealed: []byte("box"), KeyID: "k1",
		Source: store.SourceGateway, State: store.SecretApplying, IdempotencyKey: "key-1",
	}
}

func mustPut(t *testing.T, s *store.Store, row store.Secret) (store.Secret, bool) {
	t.Helper()
	got, replaced, err := s.PutSecret(context.Background(), row)
	if err != nil {
		t.Fatal(err)
	}
	return got, replaced
}

func TestPutSecretReplacesByName(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-secret-put")
	row := secretRow(p.ID, "HF_TOKEN")
	first, replaced := mustPut(t, s, row)
	if replaced || first.ID != row.ID || first.State != store.SecretApplying ||
		!first.CreatedAt.Equal(first.UpdatedAt) || first.IdempotencyKey != "key-1" {
		t.Fatalf("first put %+v replaced %v", first, replaced)
	}
	if _, err := s.MarkSecrets(ctx, p.ID, []store.Secret{first}, store.SecretFailed,
		"PORT_443_TAKEN"); err != nil {
		t.Fatal(err)
	}
	row.ID, row.Sealed, row.IdempotencyKey = uuid.New(), []byte("box2"), "key-2"
	second, replaced := mustPut(t, s, row)
	if !replaced || second.ID != first.ID || !second.CreatedAt.Equal(first.CreatedAt) ||
		!second.UpdatedAt.After(first.UpdatedAt) || string(second.Sealed) != "box2" ||
		second.State != store.SecretApplying || second.FailureReason != "" {
		t.Fatalf("second put %+v replaced %v, first %+v", second, replaced, first)
	}
	byKey, err := s.SecretByKey(ctx, p.ID, "key-2")
	if err != nil || byKey.ID != first.ID {
		t.Fatalf("by key %+v %v", byKey, err)
	}
	if _, err := s.SecretByKey(ctx, p.ID, "key-1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the replaced key still answers: %v", err)
	}
	other := newPlacement(t, s, "o-secret-put-2")
	mustPut(t, s, secretRow(other.ID, "HF_TOKEN"))
	all, err := s.Secrets(ctx, p.ID)
	if err != nil || len(all) != 1 {
		t.Fatalf("secrets %+v %v", all, err)
	}
}

func TestMarkSecretsMatchesTheLoadedVersion(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-secret-mark")
	a, _ := mustPut(t, s, secretRow(p.ID, "A"))
	b, _ := mustPut(t, s, secretRow(p.ID, "B"))
	mustPut(t, s, secretRow(p.ID, "B"))
	n, err := s.MarkSecrets(ctx, p.ID, []store.Secret{a, b}, store.SecretActive, "")
	if err != nil || n != 1 {
		t.Fatalf("marked %d, %v", n, err)
	}
	got, err := s.Secrets(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].State != store.SecretActive || got[1].State != store.SecretApplying {
		t.Fatalf("states %s and %s", got[0].State, got[1].State)
	}
	if !got[0].UpdatedAt.Equal(a.UpdatedAt) {
		t.Fatal("marking moved updated_at")
	}
}

func TestSecretsPendingAndVersions(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-secret-pending")
	// pending checks the answer and returns the lag a run would read
	pending := func(want bool) int64 {
		t.Helper()
		got, lag, err := s.SecretsPending(ctx, p.ID)
		if err != nil || got != want {
			t.Fatalf("pending %v %v, want %v", got, err, want)
		}
		return lag
	}
	version := func(want int64) {
		t.Helper()
		got, err := s.PlacementByID(ctx, p.ID)
		if err != nil || got.SecretsVersion != want {
			t.Fatalf("version %d %v, want %d", got.SecretsVersion, err, want)
		}
	}
	bump := func(pending bool) {
		t.Helper()
		if err := s.BumpSecretsVersion(ctx, p.ID, pending); err != nil {
			t.Fatal(err)
		}
	}
	finish := func(lag int64) {
		t.Helper()
		if err := s.FinishSecretsApply(ctx, p.ID, lag); err != nil {
			t.Fatal(err)
		}
	}
	pending(false)
	row, _ := mustPut(t, s, secretRow(p.ID, "A"))
	lag := pending(true)
	if _, err := s.MarkSecrets(ctx, p.ID, []store.Secret{row}, store.SecretActive, ""); err != nil {
		t.Fatal(err)
	}
	finish(lag)
	pending(false)
	version(1)
	bump(false)
	pending(false)
	version(2)
	deleted, err := s.DeleteSecret(ctx, p.ID, "A")
	if err != nil || deleted.ID != row.ID {
		t.Fatalf("delete %+v %v", deleted, err)
	}
	if _, err := s.DeleteSecret(ctx, p.ID, "A"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second delete %v", err)
	}
	bump(true)
	version(3)
	bump(false)
	if lag = pending(true); lag != 1 {
		t.Fatalf("lag %d after a pending change, want 1", lag)
	}
	finish(lag)
	pending(false)
	version(5)

	// A pending change during a run outlives the run, a change needing no apply does not
	b, _ := mustPut(t, s, secretRow(p.ID, "B"))
	lag = pending(true)
	bump(true)
	if _, err := s.MarkSecrets(ctx, p.ID, []store.Secret{b}, store.SecretActive, ""); err != nil {
		t.Fatal(err)
	}
	finish(lag)
	lag = pending(true)
	bump(false)
	finish(lag)
	pending(false)
	version(9)

	fetch := secretRow(p.ID, "F")
	fetch.FetchOnly, fetch.State = true, store.SecretActive
	mustPut(t, s, fetch)
	pending(false)
}

func TestEgressTokenMintedOnce(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-secret-token")
	one, two := sha256.Sum256([]byte("one")), sha256.Sum256([]byte("two"))
	for _, c := range []struct {
		sha  []byte
		want bool
	}{{one[:], true}, {two[:], false}} {
		got, err := s.MintEgressToken(ctx, p.ID, c.sha)
		if err != nil || got != c.want {
			t.Fatalf("mint %v %v, want %v", got, err, c.want)
		}
	}
	if has, err := s.HasEgressToken(ctx, p.ID); err != nil || !has {
		t.Fatalf("has %v %v", has, err)
	}
	if err := s.ClearEgressToken(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	if has, err := s.HasEgressToken(ctx, p.ID); err != nil || has {
		t.Fatalf("has after clear %v %v", has, err)
	}
	if got, err := s.MintEgressToken(ctx, p.ID, two[:]); err != nil || !got {
		t.Fatalf("mint after clear %v %v", got, err)
	}
}

func TestSecretLinkLifecycle(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-secret-link")
	sum := sha256.Sum256([]byte("bnsl_a"))
	link := store.SecretLink{
		ID: uuid.New(), PlacementID: p.ID, TokenSHA256: sum[:], PairingCode: "K3F9X2",
		SuggestedName: "HF_TOKEN", SuggestedHosts: []string{"huggingface.co"},
		SuggestedHeaders: []string{},
	}
	got, err := s.InsertSecretLink(ctx, link, 10*time.Minute)
	if err != nil || got.ID != link.ID || got.Expired || got.UsedAt != nil ||
		got.ExpiresAt.Sub(got.CreatedAt) != 10*time.Minute {
		t.Fatalf("insert %+v %v", got, err)
	}
	again := link
	again.ID = uuid.New()
	if got, err := s.InsertSecretLink(ctx, again, time.Minute); err != nil || got.ID != link.ID {
		t.Fatalf("a second insert of the token answered %+v %v", got, err)
	}
	for range 2 {
		if err := s.FailSecretLink(ctx, sum[:]); err != nil {
			t.Fatal(err)
		}
	}
	if ok, _ := s.ConsumeSecretLink(ctx, sum[:], "sub-0", 2); ok {
		t.Fatal("a link at its failure limit was consumed")
	}
	ok, err := s.ConsumeSecretLink(ctx, sum[:], "sub-1", 3)
	if err != nil || !ok {
		t.Fatalf("consume %v %v", ok, err)
	}
	if ok, _ := s.ConsumeSecretLink(ctx, sum[:], "sub-2", 3); ok {
		t.Fatal("a used link was consumed again")
	}
	got, err = s.SecretLinkByToken(ctx, sum[:])
	if err != nil || got.UsedAt == nil || got.SubmissionID != "sub-1" || got.FailedAttempts != 2 ||
		got.SuggestedHosts[0] != "huggingface.co" {
		t.Fatalf("by token %+v %v", got, err)
	}
	old := sha256.Sum256([]byte("bnsl_old"))
	link.ID, link.TokenSHA256 = uuid.New(), old[:]
	if _, err := s.InsertSecretLink(ctx, link, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if ok, _ := s.ConsumeSecretLink(ctx, old[:], "sub", 3); ok {
		t.Fatal("an expired link was consumed")
	}
	if got, _ := s.SecretLinkByToken(ctx, old[:]); !got.Expired {
		t.Fatal("an expired link reads as live")
	}
	none := sha256.Sum256([]byte("none"))
	if _, err := s.SecretLinkByToken(ctx, none[:]); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown token %v", err)
	}
}

func TestTryLockLetsOneHolderRun(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	inner := make(chan bool, 1)
	held, err := s.TryLock(ctx, "apply:x", func() error {
		got, err := s.TryLock(ctx, "apply:x", func() error { return nil })
		if err != nil {
			return err
		}
		inner <- got
		return nil
	})
	if err != nil || !held || <-inner {
		t.Fatalf("outer %v %v, the inner run got the lock too", held, err)
	}
	want := errors.New("boom")
	if _, err := s.TryLock(ctx, "apply:x", func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("fn's error %v", err)
	}
}
