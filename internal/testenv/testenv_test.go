package testenv_test

import (
	"context"
	"testing"

	"github.com/oynozan/bugnet-router/internal/testenv"
)

func TestPostgresIsolated(t *testing.T) {
	ctx := context.Background()
	a := testenv.Postgres(t)
	b := testenv.Postgres(t)

	var nameA, nameB string
	if err := a.QueryRow(ctx, "select current_database()").Scan(&nameA); err != nil {
		t.Fatal(err)
	}
	if err := b.QueryRow(ctx, "select current_database()").Scan(&nameB); err != nil {
		t.Fatal(err)
	}
	if nameA == nameB {
		t.Fatalf("both pools use database %q", nameA)
	}

	var one int
	if err := a.QueryRow(ctx, "select 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("select 1 on first pool: %v, %d", err, one)
	}
	if err := b.QueryRow(ctx, "select 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("select 1 on second pool: %v, %d", err, one)
	}
}

func TestPostgresDropsDatabaseOnCleanup(t *testing.T) {
	ctx := context.Background()
	var name string
	t.Run("inner", func(t *testing.T) {
		pool := testenv.Postgres(t)
		if err := pool.QueryRow(ctx, "select current_database()").Scan(&name); err != nil {
			t.Fatal(err)
		}
	})

	other := testenv.Postgres(t)
	var n int
	err := other.QueryRow(ctx,
		"select count(*) from pg_database where datname = $1", name).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("database %q still exists after cleanup", name)
	}
}

func TestValkeyPrefixCleanup(t *testing.T) {
	ctx := context.Background()
	var prefix string
	t.Run("inner", func(t *testing.T) {
		c, p := testenv.Valkey(t)
		prefix = p
		for _, k := range []string{"a", "b"} {
			if err := c.Do(ctx, c.B().Set().Key(p+k).Value("v").Build()).Error(); err != nil {
				t.Fatal(err)
			}
		}
	})

	c, other := testenv.Valkey(t)
	if other == prefix {
		t.Fatalf("prefix %q reused", prefix)
	}
	keys, err := c.Do(ctx, c.B().Keys().Pattern(prefix+"*").Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("keys left after cleanup: %v", keys)
	}
}
