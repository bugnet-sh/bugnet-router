// Package testenv gives tests an isolated Postgres database and Valkey key prefix.
package testenv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"
)

const (
	defaultPGURL     = "postgres://localhost:5432/postgres?sslmode=disable"
	defaultValkeyURL = "localhost:6379"
	cleanupTimeout   = 10 * time.Second
)

func randomHex(t *testing.T) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random: %v", err)
	}
	return hex.EncodeToString(b)
}

func adminURL() string {
	if v := os.Getenv("BUGNET_TEST_PG_URL"); v != "" {
		return v
	}
	return defaultPGURL
}

// DatabaseURL creates a fresh database and returns its URL, dropping it when the test ends
func DatabaseURL(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	admin := adminURL()

	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	name := "bn_test_" + randomHex(t)
	if _, err := conn.Exec(ctx, "create database "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		c, err := pgx.Connect(ctx, admin)
		if err != nil {
			t.Errorf("connect to postgres for drop: %v", err)
			return
		}
		defer func() { _ = c.Close(ctx) }()
		drop := "drop database if exists " + pgx.Identifier{name}.Sanitize() + " with (force)"
		if _, err := c.Exec(ctx, drop); err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
	})

	u, err := url.Parse(admin)
	if err != nil {
		t.Fatalf("parse postgres url: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

// Postgres returns a pool on a fresh database that is dropped when the test ends
func Postgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), DatabaseURL(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	// Registered after the drop cleanup so it runs first and no connection blocks the drop
	t.Cleanup(pool.Close)
	return pool
}

// Valkey returns a client and a unique key prefix whose keys are deleted when the test ends
func Valkey(t *testing.T) (valkey.Client, string) {
	t.Helper()
	addr := os.Getenv("BUGNET_TEST_VALKEY_ADDR")
	if addr == "" {
		addr = defaultValkeyURL
	}
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	if err != nil {
		t.Fatalf("connect to valkey: %v", err)
	}
	prefix := "bn:test:" + randomHex(t) + ":"
	t.Cleanup(func() {
		defer client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		var cursor uint64
		for {
			cmd := client.B().Scan().Cursor(cursor).Match(prefix + "*").Count(500).Build()
			entry, err := client.Do(ctx, cmd).AsScanEntry()
			if err != nil {
				t.Errorf("scan valkey keys: %v", err)
				return
			}
			if len(entry.Elements) > 0 {
				del := client.B().Del().Key(entry.Elements...).Build()
				if err := client.Do(ctx, del).Error(); err != nil {
					t.Errorf("delete valkey keys: %v", err)
					return
				}
			}
			cursor = entry.Cursor
			if cursor == 0 {
				return
			}
		}
	})
	return client, prefix
}
