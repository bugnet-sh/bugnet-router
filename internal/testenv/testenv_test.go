package testenv_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/testenv"
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

	if name == "" {
		t.Fatal("inner test recorded no database name")
	}
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

func TestDataPlaneKeysAreFreshAndParse(t *testing.T) {
	envA, keysA := testenv.DataPlaneKeys(t)
	envB, _ := testenv.DataPlaneKeys(t)
	if envA == envB {
		t.Fatal("two calls gave the same root")
	}
	if keysA.Current().ID != "k1" || len(keysA.Roots()) != 1 {
		t.Fatalf("keys hold %d roots, current %q, want only k1", len(keysA.Roots()),
			keysA.Current().ID)
	}
}

func TestLocalInternetAnswersEveryHost(t *testing.T) {
	prev := http.DefaultTransport
	t.Run("swapped", func(t *testing.T) {
		testenv.LocalInternet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, r.Host+r.URL.Path)
		}))
		for _, host := range []string{"huggingface.co", "lium.io"} {
			res, err := http.Get("https://" + host + "/api/pods")
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()
			if string(body) != host+"/api/pods" {
				t.Errorf("%s answered %q, want the local server's echo", host, body)
			}
		}
		// Plain HTTP reaches the same TLS server, which refuses it
		res, err := http.Get("http://lium.io/api/pods")
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("plain http answered %d, want the local server's 400", res.StatusCode)
		}
	})
	if http.DefaultTransport != prev {
		t.Fatal("the default transport was not put back")
	}
}

func TestGoroutinesGone(t *testing.T) {
	const fn = "github.com/bugnet-sh/bugnet-router/internal/testenv_test.TestGoroutinesGone"
	stop := make(chan struct{})
	go func() { <-stop }()
	if testenv.GoroutinesGone(fn) {
		t.Fatal("a goroutine still waiting counted as gone")
	}
	close(stop)
	if !testenv.GoroutinesGone(fn) {
		t.Fatal("a goroutine that ended counted as running")
	}
}
