// Package testenv gives tests an isolated Postgres database and Valkey key prefix.
package testenv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
)

const (
	defaultPGURL     = "postgres://localhost:5432/postgres?sslmode=disable"
	defaultValkeyURL = "localhost:6379"
	cleanupTimeout   = 60 * time.Second
	dropRetryWait    = 2 * time.Second
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
		err := retryOnTimeout(dropRetryWait, func() error { return dropDatabase(admin, name) })
		if err != nil {
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

// dropDatabase drops name and every connection to it, giving up after cleanupTimeout
func dropDatabase(admin, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	c, err := pgx.Connect(ctx, admin)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = c.Close(ctx) }()
	drop := "drop database if exists " + pgx.Identifier{name}.Sanitize() + " with (force)"
	_, err = c.Exec(ctx, drop)
	return err
}

// retryOnTimeout runs try once more after wait when its first run timed out
func retryOnTimeout(wait time.Duration, try func() error) error {
	err := try()
	if !errors.Is(err, context.DeadlineExceeded) && !pgconn.Timeout(err) {
		return err
	}
	time.Sleep(wait)
	return try()
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

// DataPlaneKeys returns a fresh BUGNET_DATA_PLANE_KEYS value, as bugnet-dev keys prints it, parsed
func DataPlaneKeys(t *testing.T) (string, dpkeys.Keys) {
	t.Helper()
	env := dpkeys.NewEnv()
	keys, err := dpkeys.Parse(env)
	if err != nil {
		t.Fatalf("parse data-plane keys: %v", err)
	}
	return env, keys
}

// ValkeyAddr returns the address of the Valkey tests use
func ValkeyAddr() string {
	if addr := os.Getenv("BUGNET_TEST_VALKEY_ADDR"); addr != "" {
		return addr
	}
	return defaultValkeyURL
}

// Valkey returns a client and a unique key prefix whose keys are deleted when the test ends
func Valkey(t *testing.T) (valkey.Client, string) {
	t.Helper()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{ValkeyAddr()}})
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

// ValkeySocket relays a fresh Unix socket to the test Valkey and counts the connections it takes
func ValkeySocket(t *testing.T) (path string, accepted *atomic.Int32) {
	t.Helper()
	// A short directory keeps the socket path under the 104 bytes macOS allows
	dir, err := os.MkdirTemp("", "vk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path = filepath.Join(dir, "valkey.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepted = &atomic.Int32{}
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			out, err := net.Dial("tcp", ValkeyAddr())
			if err != nil {
				_ = in.Close()
				continue
			}
			go func() { _, _ = io.Copy(out, in); _ = out.Close() }()
			go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		}
	}()
	return path, accepted
}
