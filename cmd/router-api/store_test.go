package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// migratedPool returns a pool on a fresh migrated database
func migratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(t.Context(), url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestStoreSealsProviderCredentials(t *testing.T) {
	pool := migratedPool(t)
	_, keys := testenv.DataPlaneKeys(t)
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	st := openStore(pool, config.Config{DataPlaneKeys: keys}, log)
	p, _, err := st.CreatePlacement(t.Context(), store.NewPlacement{
		OrderID: "o", HandleSHA256: []byte("h"), PackageID: "p",
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := provider.Ref{Provider: "e2b", ID: "sb", Meta: map[string]string{"envd_token": "tok"}}
	now := time.Now()
	err = st.MarkPlacementReady(t.Context(), p.ID, ref, "s", now, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	var raw string
	err = pool.QueryRow(t.Context(),
		`select provider_ref->'meta'->>'envd_token' from placements`).Scan(&raw)
	if err != nil || !strings.HasPrefix(raw, "v1.k1.") {
		t.Fatalf("stored envd_token %q, %v, want it sealed under k1", raw, err)
	}
	got, err := st.PlacementByID(t.Context(), p.ID)
	if err != nil || got.ProviderRef.Meta["envd_token"] != "tok" {
		t.Fatalf("read back %+v, %v", got.ProviderRef, err)
	}

	_, err = pool.Exec(t.Context(), `update placements set provider_ref = jsonb_set(provider_ref,
		'{meta,envd_token}', '"v1.k9.AAAA"')`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PlacementByID(t.Context(), p.ID); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "field=envd_token") {
		t.Fatalf("an unopenable credential was not reported to the binary's log: %q", logs.String())
	}
}
