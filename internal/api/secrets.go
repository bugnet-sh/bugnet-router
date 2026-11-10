package api

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// defaultApplyWait is secrets.apply_wait's default
	defaultApplyWait = 15 * time.Second
	// maxLinkFailures locks a link after this many refused submits
	maxLinkFailures = 5
	// defaultMaxSecrets, defaultLinksPerHour and defaultPutsPerHour are [secrets] defaults
	defaultMaxSecrets   = 20
	defaultLinksPerHour = 10
	defaultPutsPerHour  = 30
	defaultLinkTTL      = 10 * time.Minute
	defaultPutGrantTTL  = 10 * time.Minute
	// linkKeyPrefix keys a link's secret, so a repeated submit finds what it stored
	linkKeyPrefix = "link:"
)

// applyPoll is how often a write rereads its secret while it waits for the apply
var applyPoll = 100 * time.Millisecond

// Secret reasons, named as DP 11.3 names them
const (
	reasonLinkUnknown     = "LINK_UNKNOWN"
	reasonLinkLocked      = "LINK_LOCKED"
	reasonPutGrantInvalid = "PUT_GRANT_INVALID"
)

var (
	errNoSecrets       = errors.New("api: this router serves no secrets")
	errNoSecretFeature = errors.New("api: the placement's provider holds no secrets")
	errLinkUnknown     = errors.New("api: the link is unknown, used or expired")
	errLinkLocked      = errors.New("api: the link is locked after too many refused values")
	errLinkUsed        = errors.New("api: that request id's link was used")
	errLinkExpired     = errors.New("api: that request id's link expired")
	errPutGrant        = errors.New("api: the put grant is expired, forged or for no placement")
	// errLinkGone means a link was used, expired or locked after it was read, inside the write
	errLinkGone = errors.New("api: the link was used meanwhile")
)

/* Gateway */

// CreateSecretLink answers the page a human enters one secret on, the same link per request id
func (s *Server) CreateSecretLink(
	ctx context.Context, req *connect.Request[routerv1.CreateSecretLinkRequest],
) (*connect.Response[routerv1.CreateSecretLinkResponse], error) {
	m := req.Msg
	ctx, p, err := s.placement(ctx, m.GetHandle())
	if err == nil {
		err = s.secretsFor(p)
	}
	if err == nil && s.d.SecretsHost == "" {
		err = errNoSecrets
	}
	if err != nil {
		return nil, s.status(err)
	}
	if !secrets.ValidKey(m.GetRequestId()) {
		return nil, invalid("request_id must be 1 to 128 printable ASCII characters")
	}
	if m.GetName() != "" {
		if err := secrets.CheckName(m.GetName()); err != nil {
			return nil, s.status(err)
		}
	}
	keyID := s.keyID(p)
	token, err := s.d.Keys.LinkToken(keyID, p.ID, m.GetRequestId())
	if err != nil {
		return nil, s.status(err)
	}
	sum := sha256.Sum256([]byte(token))
	link, err := s.d.Store.SecretLinkByToken(ctx, sum[:])
	if errors.Is(err, store.ErrNotFound) {
		link, err = s.newLink(ctx, p, keyID, sum[:], secrets.Suggest(m.GetName(), m.GetHosts()))
	}
	switch {
	case err != nil:
		return nil, s.status(err)
	case link.UsedAt != nil:
		return nil, s.status(errLinkUsed)
	case link.Expired:
		return nil, s.status(errLinkExpired)
	case link.FailedAttempts >= maxLinkFailures:
		return nil, s.status(errLinkLocked)
	}
	return connect.NewResponse(&routerv1.CreateSecretLinkResponse{
		Url: secrets.LinkURL(s.d.SecretsHost, token), PairingCode: link.PairingCode,
		ExpiresAt: timestamppb.New(link.ExpiresAt),
	}), nil
}

// newLink stores a link under the placement's row lock, spending an operation and a link token
func (s *Server) newLink(
	ctx context.Context, p store.Placement, keyID string, sum []byte,
	suggested *routerv1.SecretSpec,
) (store.SecretLink, error) {
	code, err := s.d.Keys.PairingCode(keyID, sum)
	if err != nil {
		return store.SecretLink{}, err
	}
	var link store.SecretLink
	err = s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		if _, err := s.lockReady(ctx, ts, p.ID); err != nil {
			return err
		}
		// A call with the same request id may have stored the link while this one waited
		link, err = ts.SecretLinkByToken(ctx, sum)
		if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		live, err := ts.LiveX402Hosts(ctx, p.ID)
		if err != nil {
			return err
		}
		if err := secrets.CheckInUse(suggested, live); err != nil {
			return err
		}
		if err := s.spend(ctx, p); err != nil {
			return err
		}
		n := cmp.Or(s.d.Secrets.LinksPerHour, defaultLinksPerHour)
		if err := s.takeKey(ctx, hourly("rl:links:", p.ID, n)); err != nil {
			return err
		}
		link, err = ts.InsertSecretLink(ctx, store.SecretLink{
			ID: uuid.New(), PlacementID: p.ID, TokenSHA256: sum, PairingCode: code,
			SuggestedName: suggested.GetName(), SuggestedHosts: suggested.GetHosts(),
			SuggestedHeaders: suggested.GetHeaderNames(),
		}, cmp.Or(s.d.Secrets.LinkTTL, defaultLinkTTL))
		return err
	})
	return link, err
}

// CreateSecretPutGrant answers a bnsp_ grant the bridge sends to the secrets route
func (s *Server) CreateSecretPutGrant(
	ctx context.Context, req *connect.Request[routerv1.CreateSecretPutGrantRequest],
) (*connect.Response[routerv1.CreateSecretPutGrantResponse], error) {
	ctx, p, err := s.placement(ctx, req.Msg.GetHandle())
	if err == nil {
		err = s.secretsFor(p)
	}
	if err == nil {
		err = s.spend(ctx, p)
	}
	if err != nil {
		return nil, s.status(err)
	}
	expiry := time.Now().Add(cmp.Or(s.d.Secrets.PutGrantTTL, defaultPutGrantTTL)).
		Truncate(time.Second)
	return connect.NewResponse(&routerv1.CreateSecretPutGrantResponse{
		PutGrant: s.d.Keys.PutGrant(p.ID, expiry), ExpiresAt: timestamppb.New(expiry),
	}), nil
}

// GetSecretSealingKey answers every current sealing key with the placement's binding
func (s *Server) GetSecretSealingKey(
	ctx context.Context, req *connect.Request[routerv1.GetSecretSealingKeyRequest],
) (*connect.Response[routerv1.GetSecretSealingKeyResponse], error) {
	_, p, err := s.placement(ctx, req.Msg.GetHandle())
	if err == nil {
		err = s.secretsFor(p)
	}
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.GetSecretSealingKeyResponse{
		SealingKeys: s.sealingKeys(p.ID),
	}), nil
}

// PutSecret stores a sealed secret the gateway sends, keyed by its idempotency key
func (s *Server) PutSecret(
	ctx context.Context, req *connect.Request[routerv1.PutSecretRequest],
) (*connect.Response[routerv1.PutSecretResponse], error) {
	m := req.Msg
	ctx, p, err := s.placement(ctx, m.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	row, err := s.put(ctx, p, m.GetSpec(), m.GetValue(), store.SourceGateway,
		m.GetIdempotencyKey())
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.PutSecretResponse{
		Secret: secrets.Info(row), Replaced: row.Replaced(),
	}), nil
}

// ListSecrets answers the placement's secrets by name, never a value or placeholder
func (s *Server) ListSecrets(
	ctx context.Context, req *connect.Request[routerv1.ListSecretsRequest],
) (*connect.Response[routerv1.ListSecretsResponse], error) {
	ctx, p, err := s.placement(ctx, req.Msg.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	rows, err := s.d.Store.Secrets(ctx, p.ID)
	if err != nil {
		return nil, s.status(err)
	}
	out := make([]*routerv1.SecretInfo, 0, len(rows))
	for _, r := range rows {
		out = append(out, secrets.Info(r))
	}
	return connect.NewResponse(&routerv1.ListSecretsResponse{Secrets: out}), nil
}

// DeleteSecret removes a secret for the apply to drop, a name the placement lacks changing nothing
func (s *Server) DeleteSecret(
	ctx context.Context, req *connect.Request[routerv1.DeleteSecretRequest],
) (*connect.Response[routerv1.DeleteSecretResponse], error) {
	ctx, p, err := s.placement(ctx, req.Msg.GetHandle())
	if err == nil {
		err = s.secretsFor(p)
	}
	if err != nil {
		return nil, s.status(err)
	}
	deleted := false
	err = s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		if _, err := s.lockReady(ctx, ts, p.ID); err != nil {
			return err
		}
		gone, err := ts.DeleteSecret(ctx, p.ID, req.Msg.GetName())
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		deleted = true
		if err := ts.BumpSecretsVersion(ctx, p.ID, !gone.FetchOnly); err != nil {
			return err
		}
		if gone.FetchOnly {
			return nil
		}
		return s.insertApply(ctx, tx, p.ID)
	})
	if err != nil {
		return nil, s.status(err)
	}
	if deleted {
		s.secretsChanged(ctx, p.ID)
	}
	return connect.NewResponse(&routerv1.DeleteSecretResponse{}), nil
}

/* The shared write */

// put is the one implementation behind PutSecret and PutBridgeSecret
func (s *Server) put(
	ctx context.Context, p store.Placement, spec *routerv1.SecretSpec,
	value *routerv1.SealedValue, source, key string,
) (store.Secret, error) {
	if key != "" && !secrets.ValidKey(key) {
		return store.Secret{}, invalid("idempotency_key must be 1 to 128 printable ASCII")
	}
	row, err := s.writeSecret(ctx, p, secretWrite{
		spec: spec, value: value, source: source, key: key, spend: true,
	})
	if err != nil {
		return store.Secret{}, err
	}
	return s.awaitApplied(ctx, row), nil
}

// secretWrite is one secret write, from the gateway, the bridge or a link's page
type secretWrite struct {
	spec   *routerv1.SecretSpec
	value  *routerv1.SealedValue
	source string
	// key is the idempotency key, or linkKeyPrefix and the link id for a link's submit
	key string
	// spend takes an operation token and a put token, as both put paths do
	spend bool
	// link is consumed in the write's transaction, with submission
	link       *store.SecretLink
	submission string
}

// writeSecret checks a write, then stores it with its apply job under the placement's row lock
func (s *Server) writeSecret(
	ctx context.Context, p store.Placement, w secretWrite,
) (store.Secret, error) {
	if err := s.secretsFor(p); err != nil {
		return store.Secret{}, err
	}
	row, changed, err := s.storeSecret(ctx, p, w)
	var r *secrets.Refusal
	if w.link != nil && errors.As(err, &r) {
		// A refused value leaves the link usable, and five refusals lock it
		if ferr := s.d.Store.FailSecretLink(ctx, w.link.TokenSHA256); ferr != nil {
			s.d.Log.Warn("api: count a refused secret link submit", "err", ferr)
		}
	}
	if err != nil {
		return store.Secret{}, err
	}
	if changed {
		s.secretsChanged(ctx, p.ID)
	}
	return row, nil
}

// storeSecret is writeSecret's transaction, reporting whether it moved the secrets version
func (s *Server) storeSecret(
	ctx context.Context, p store.Placement, w secretWrite,
) (store.Secret, bool, error) {
	if err := s.checkSecret(p, w); err != nil {
		return store.Secret{}, false, err
	}
	sha, err := s.placeholderSHA(p, w.spec.GetName())
	if err != nil {
		return store.Secret{}, false, err
	}
	var row store.Secret
	changed := false
	err = s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		cur, err := s.lockReady(ctx, ts, p.ID)
		if err != nil {
			return err
		}
		held, err := ts.Secrets(ctx, p.ID)
		if err != nil {
			return err
		}
		live, err := ts.LiveX402Hosts(ctx, p.ID)
		if err != nil {
			return err
		}
		prev, had := named(held, w.spec.GetName())
		if had && w.key != "" && prev.IdempotencyKey == w.key &&
			secrets.Matches(prev, w.spec, w.value) {
			row = prev
			return nil
		}
		if err := secrets.CheckInUse(w.spec, live); err != nil {
			return err
		}
		most := cmp.Or(s.d.Secrets.MaxPerPlacement, defaultMaxSecrets)
		if err := secrets.Fit(w.spec, heldOf(held), live, most); err != nil {
			return err
		}
		if err := s.claim(ctx, ts, cur, w); err != nil {
			return err
		}
		state := store.SecretApplying
		if w.spec.GetFetchOnly() {
			state = store.SecretActive
		}
		row, _, err = ts.PutSecret(ctx, store.Secret{
			ID: uuid.New(), PlacementID: p.ID, Name: w.spec.GetName(), Hosts: w.spec.GetHosts(),
			HeaderNames: w.spec.GetHeaderNames(), FetchOnly: w.spec.GetFetchOnly(),
			PlaceholderSHA256: sha, Sealed: w.value.GetBox(), KeyID: w.value.GetKeyId(),
			Source: w.source, State: state, IdempotencyKey: w.key,
		})
		if err != nil {
			return err
		}
		if !w.spec.GetFetchOnly() {
			return s.insertApply(ctx, tx, p.ID)
		}
		// Nothing enters the sandbox unless the put took a placeholder away
		pending := had && !prev.FetchOnly
		changed = true
		if err := ts.BumpSecretsVersion(ctx, p.ID, pending); err != nil || !pending {
			return err
		}
		return s.insertApply(ctx, tx, p.ID)
	})
	if err != nil {
		return store.Secret{}, false, err
	}
	return row, changed, nil
}

// claim consumes the write's link, or spends its operation and put tokens
func (s *Server) claim(
	ctx context.Context, ts *store.Store, p store.Placement, w secretWrite,
) error {
	if w.link != nil {
		ok, err := ts.ConsumeSecretLink(ctx, w.link.TokenSHA256, w.submission, maxLinkFailures)
		if err == nil && !ok {
			err = errLinkGone
		}
		return err
	}
	if !w.spend {
		return nil
	}
	// A link or the secret page may be held by anyone, so it never spends the owner's wallet
	spend := s.spend
	if w.link != nil || w.source == store.SourceBridge {
		spend = s.spendPlacement
	}
	if err := spend(ctx, p); err != nil {
		return err
	}
	return s.takeKey(ctx, hourly("rl:puts:", p.ID, cmp.Or(s.d.Secrets.PutsPerHour,
		defaultPutsPerHour)))
}

// checkSecret runs the rules that need no lock, the spec's and the sealed value's
func (s *Server) checkSecret(p store.Placement, w secretWrite) error {
	c := secrets.Context{Hosts: s.d.HostRules.ForSecrets(), CustomHosts: s.d.Secrets.CustomHosts}
	if pkg, ok := s.d.Catalog.Get(p.PackageID); ok {
		c.EgressAllow = pkg.EgressAllow
	}
	if err := secrets.Validate(w.spec, c); err != nil {
		return err
	}
	return secrets.CheckValue(w.value, s.d.Secrets.BoxPublicKeys)
}

// awaitApplied rereads an applying secret until it leaves applying or apply_wait passes
func (s *Server) awaitApplied(ctx context.Context, row store.Secret) store.Secret {
	if row.State != store.SecretApplying {
		return row
	}
	timer := time.NewTimer(cmp.Or(s.d.Secrets.ApplyWait, defaultApplyWait))
	defer timer.Stop()
	tick := time.NewTicker(applyPoll)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
		case <-timer.C:
			return row
		case <-ctx.Done():
			return row
		case <-s.ctx.Done():
			return row
		}
		cur, err := s.d.Store.SecretByID(ctx, row.ID)
		switch {
		case err != nil:
			return row
		case cur.State != store.SecretApplying:
			return cur
		}
		row = cur
	}
}

/* Helpers */

// secretsFor refuses a placement that is not ready or whose provider or router holds no secrets
func (s *Server) secretsFor(p store.Placement) error {
	prov, err := s.home(p)
	switch {
	case err != nil:
		return err
	case !s.d.Registry.Capabilities(prov).Secrets:
		return errNoSecretFeature
	case len(s.d.Keys.Roots()) == 0 || len(s.d.Secrets.BoxPublicKeys) == 0 ||
		s.d.EgressHost == "" || len(s.d.Secrets.EgressIPs) == 0 || s.d.River == nil:
		return errNoSecrets
	}
	return nil
}

// lockReady takes the placement's row lock, refusing one that left ready
func (s *Server) lockReady(
	ctx context.Context, ts *store.Store, id uuid.UUID,
) (store.Placement, error) {
	cur, err := ts.PlacementForUpdate(ctx, id)
	if err != nil {
		return store.Placement{}, err
	}
	if cur.State != store.PlacementReady {
		return store.Placement{}, fmt.Errorf("%w: placement is %s", errNotReady, cur.State)
	}
	return cur, nil
}

// insertApply inserts one apply_secrets job in the write's transaction
func (s *Server) insertApply(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	if _, err := s.d.River.InsertTx(ctx, tx, jobargs.ApplySecrets{PlacementID: id},
		nil); err != nil {
		return fmt.Errorf("insert apply_secrets job: %w", err)
	}
	return nil
}

// secretsChanged tells the egress proxy to drop its cached set at once
func (s *Server) secretsChanged(ctx context.Context, id uuid.UUID) {
	s.events.Publish(ctx, &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_SecretsChanged{
		SecretsChanged: &routerv1.SecretsChanged{PlacementId: id.String()},
	}})
}

// sealingKeys lists every current box key, newest first, with the placement's binding
func (s *Server) sealingKeys(id uuid.UUID) []*routerv1.SealingKey {
	binding := dpkeys.Binding(id)
	out := make([]*routerv1.SealingKey, 0, len(s.d.Secrets.BoxPublicKeys))
	for _, k := range s.d.Secrets.BoxPublicKeys {
		out = append(out, &routerv1.SealingKey{KeyId: k.ID, PublicKey: k.Key, Binding: binding})
	}
	return out
}

// keyID is the data-plane root of the placement, the newest for one made before roots existed
func (s *Server) keyID(p store.Placement) string {
	return cmp.Or(p.KeyID, s.d.Keys.Current().ID)
}

// placeholderSHA is the digest of the placeholder of the placement's secret name
func (s *Server) placeholderSHA(p store.Placement, name string) ([]byte, error) {
	ph, err := s.d.Keys.Placeholder(s.keyID(p), p.ID, name)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(ph))
	return sum[:], nil
}

// takeKey spends one token of a keyed bucket, letting the call through when the bucket is unread
func (s *Server) takeKey(ctx context.Context, b ratelimit.Bucket) error {
	tctx, cancel := context.WithTimeout(ctx, opsTimeout)
	defer cancel()
	ok, wait, err := s.ops.TakeKey(tctx, b)
	switch {
	case err != nil && gaveUp(ctx):
		return cmp.Or(ctx.Err(), context.DeadlineExceeded)
	case err != nil:
		s.d.Log.Warn("api: secrets limit unread, letting the write through", "err", err)
	case !ok:
		return rateLimited(wait)
	}
	return nil
}

// hourly is a placement's bucket under prefix that refills n tokens an hour
func hourly(prefix string, id uuid.UUID, n int) ratelimit.Bucket {
	return ratelimit.Bucket{Key: prefix + id.String(), PerSecond: ratelimit.PerHour(n), Burst: n}
}

func named(rows []store.Secret, name string) (store.Secret, bool) {
	for _, r := range rows {
		if r.Name == name {
			return r, true
		}
	}
	return store.Secret{}, false
}

func heldOf(rows []store.Secret) []secrets.Held {
	out := make([]secrets.Held, len(rows))
	for i, r := range rows {
		out[i] = secrets.Held{Name: r.Name, Hosts: r.Hosts}
	}
	return out
}
