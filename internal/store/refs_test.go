package store_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"log/slog"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// credentials are the provider ref fields the store seals, each with a plaintext value
var credentials = map[string]string{
	"envd_token": "envd-plain", "traffic_token": "traffic-plain", "secret": "koyeb-plain",
	"_token": "boat-plain",
}

// credentialRef is a ref carrying every sealed field and one plain field
func credentialRef() provider.Ref {
	meta := maps.Clone(credentials)
	meta["domain"] = "e2b.app"
	return provider.Ref{Provider: "e2b", ID: "sb-1", Meta: meta}
}

func mustKeys(t *testing.T, env string) dpkeys.Keys {
	t.Helper()
	k, err := dpkeys.Parse(env)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// readyPlacement creates a placement and marks it ready with ref
func readyPlacement(t *testing.T, s *store.Store, order string, ref provider.Ref) uuid.UUID {
	t.Helper()
	p := newPlacement(t, s, order)
	now := time.Now()
	if err := s.MarkPlacementReady(context.Background(), p.ID, ref, "small", now,
		now.Add(time.Minute), nil); err != nil {
		t.Fatalf("mark ready: %v", err)
	}
	return p.ID
}

// rawRef reads a placement's provider_ref as Postgres stores it
func rawRef(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) string {
	t.Helper()
	var raw string
	err := pool.QueryRow(context.Background(),
		`select provider_ref::text from placements where id = $1`, id).Scan(&raw)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRefSealing(t *testing.T) {
	env, keys := testenv.DataPlaneKeys(t)
	s, pool := newStoreWithPool(t, store.WithRefSealer(keys))
	ctx := context.Background()
	ref := credentialRef()
	id := readyPlacement(t, s, "o-seal", ref)
	if !maps.Equal(ref.Meta, credentialRef().Meta) {
		t.Fatalf("writing changed the caller's meta to %v", ref.Meta)
	}

	raw := rawRef(t, pool, id)
	for name, plain := range credentials {
		if strings.Contains(raw, plain) {
			t.Errorf("provider_ref holds %s in plaintext: %s", name, raw)
		}
	}
	var refID, envd, domain string
	err := pool.QueryRow(ctx, `
		select provider_ref->>'id', provider_ref->'meta'->>'envd_token',
			provider_ref->'meta'->>'domain'
		from placements where id = $1`, id).Scan(&refID, &envd, &domain)
	if err != nil {
		t.Fatal(err)
	}
	if refID != "sb-1" || domain != "e2b.app" || !strings.HasPrefix(envd, "v1.k1.") {
		t.Fatalf("stored id %q, domain %q, envd_token %q, want the id and domain plain and "+
			"envd_token sealed as v1.k1.", refID, domain, envd)
	}

	got, err := s.PlacementByID(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got.ProviderRef.Meta, credentialRef().Meta) || got.ProviderRef.ID != "sb-1" {
		t.Fatalf("read back %+v, want the ref as written", got.ProviderRef)
	}
	err = s.WithTx(ctx, func(tx pgx.Tx) error {
		inTx, err := s.Tx(tx).PlacementForUpdate(ctx, id)
		if err == nil && inTx.ProviderRef.Meta["secret"] != "koyeb-plain" {
			t.Errorf("a transaction's store read secret %q", inTx.ProviderRef.Meta["secret"])
		}
		return err
	})
	if err != nil {
		t.Fatalf("read in a transaction: %v", err)
	}

	rotated := mustKeys(t, "k2:"+base64.StdEncoding.EncodeToString(make([]byte, 32))+","+env)
	got, err = store.New(pool, store.WithRefSealer(rotated)).PlacementByID(ctx, id)
	if err != nil || got.ProviderRef.Meta["_token"] != "boat-plain" {
		t.Fatalf("after rotation: %+v, %v", got.ProviderRef, err)
	}
	next := readyPlacement(t, store.New(pool, store.WithRefSealer(rotated)), "o-seal-2", ref)
	if !strings.Contains(rawRef(t, pool, next), `"v1.k2.`) {
		t.Errorf("a ref written after rotation is not sealed under k2: %s", rawRef(t, pool, next))
	}
}

func TestRefWrittenBeforeSealingReadsAsPlaintext(t *testing.T) {
	_, keys := testenv.DataPlaneKeys(t)
	s, pool := newStoreWithPool(t, store.WithRefSealer(keys))
	ctx := context.Background()
	p := newPlacement(t, s, "o-old")
	_, err := pool.Exec(ctx, `
		update placements set state = 'ready', provider = 'e2b', provider_ref = $2
		where id = $1`, p.ID, `{"provider":"e2b","id":"sb-1","meta":{"envd_token":"envd-plain"}}`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.PlacementByID(ctx, p.ID)
	if err != nil || got.ProviderRef.Meta["envd_token"] != "envd-plain" {
		t.Fatalf("an unsealed row read %+v, %v, want its plaintext", got.ProviderRef, err)
	}
}

func TestStoreWithoutSealerRefusesToWriteCredentials(t *testing.T) {
	plain, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, plain, "o-plain")
	now := time.Now()
	err := plain.MarkPlacementReady(ctx, p.ID, credentialRef(), "small", now,
		now.Add(time.Minute), nil)
	if err == nil {
		t.Fatal("a store without a sealer wrote provider credentials")
	}
	for _, v := range credentials {
		if strings.Contains(err.Error(), v) {
			t.Fatalf("error quotes a credential: %v", err)
		}
	}
	got, err := plain.PlacementByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != store.PlacementPlacing || got.ProviderRef != nil {
		t.Fatalf("a refused write left %s with ref %+v", got.State, got.ProviderRef)
	}

	_, keys := testenv.DataPlaneKeys(t)
	id := readyPlacement(t, store.New(pool, store.WithRefSealer(keys)), "o-sealed",
		credentialRef())
	got, err = plain.PlacementByID(ctx, id)
	if err != nil {
		t.Fatalf("a store without a sealer read a sealed ref: %v", err)
	}
	for field := range credentials {
		if v, ok := got.ProviderRef.Meta[field]; ok {
			t.Errorf("a store without a sealer read %s as %q, want it absent", field, v)
		}
	}
}

func TestUnopenableRefReadsAsAbsentInEveryState(t *testing.T) {
	_, keys := testenv.DataPlaneKeys(t)
	s, pool := newStoreWithPool(t, store.WithRefSealer(keys))
	ctx := context.Background()
	live := readyPlacement(t, s, "o-live", credentialRef())
	stopping := readyPlacement(t, s, "o-stopping", credentialRef())
	ended := readyPlacement(t, s, "o-ended", credentialRef())
	for _, id := range []uuid.UUID{stopping, ended} {
		if _, err := s.MarkPlacementTerminating(ctx, id, "stop", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.MarkPlacementTerminated(ctx, ended, time.Now()); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	other := store.New(pool, store.WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
		store.WithRefSealer(mustKeys(t, "k9:"+base64.StdEncoding.EncodeToString(make([]byte, 32)))))
	states := map[string]uuid.UUID{"ready": live, "terminating": stopping, "terminated": ended}
	for name, id := range states {
		got, err := other.PlacementByID(ctx, id)
		if err != nil {
			t.Fatalf("a %s placement: %v", name, err)
		}
		if got.ProviderRef.ID != "sb-1" || got.ProviderRef.Meta["domain"] != "e2b.app" {
			t.Errorf("a %s placement lost its plain fields: %+v", name, got.ProviderRef)
		}
		for field := range credentials {
			if v, ok := got.ProviderRef.Meta[field]; ok {
				t.Errorf("a %s placement kept %s as %q, want it absent", name, field, v)
			}
		}
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != len(states)*len(credentials) {
		t.Fatalf("%d log lines, want one per placement and field:\n%s", len(lines), logs.String())
	}
	for name, id := range states {
		for field := range credentials {
			want := "placement=" + id.String() + " field=" + field
			if !strings.Contains(logs.String(), want) {
				t.Errorf("no warning names the %s placement's %s:\n%s", name, field, logs.String())
			}
		}
	}
	for _, line := range lines {
		if !strings.Contains(line, "level=WARN") || strings.Contains(line, "v1.") {
			t.Errorf("want a warning without the sealed value, got %s", line)
		}
	}

	_, err := pool.Exec(ctx, `
		update placements set provider_ref = (select provider_ref from placements where id = $2)
		where id = $1`, live, ended)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.PlacementByID(ctx, live)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := got.ProviderRef.Meta["envd_token"]; ok {
		t.Errorf("credentials moved from another placement's row opened as %q", v)
	}
}
