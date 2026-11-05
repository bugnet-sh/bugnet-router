package ratelimit_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/ratelimit"
	"github.com/oynozan/bugnet-router/internal/testenv"
)

// take spends one token of id's bucket and returns whether it was there and the wait otherwise
func take(t *testing.T, l *ratelimit.Limiter, id uuid.UUID) (bool, time.Duration) {
	t.Helper()
	ok, wait, err := l.Take(t.Context(), id)
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	if ok && wait != 0 {
		t.Fatalf("an allowed operation came with a wait of %v", wait)
	}
	return ok, wait
}

func pttl(t *testing.T, vk valkey.Client, key string) time.Duration {
	t.Helper()
	ms, err := vk.Do(t.Context(), vk.B().Pttl().Key(key).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return time.Duration(ms) * time.Millisecond
}

func TestBurstThenRefusedWithAWait(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	l := ratelimit.New(vk, prefix, 10, 3)
	id := uuid.New()
	for i := range 3 {
		if ok, _ := take(t, l, id); !ok {
			t.Fatalf("operation %d of a burst of 3 was refused", i+1)
		}
	}
	ok, wait := take(t, l, id)
	if ok {
		t.Fatal("the operation past the burst was allowed")
	}
	// A token comes every 100 ms at 10 a second, and three operations took a little of that
	if wait <= 0 || wait > 100*time.Millisecond {
		t.Fatalf("refused with a wait of %v, want more than 0 and at most 100ms", wait)
	}
}

func TestWaitIsTheTimeToTheNextToken(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	l := ratelimit.New(vk, prefix, 4, 1)
	id := uuid.New()
	if ok, _ := take(t, l, id); !ok {
		t.Fatal("the first operation was refused")
	}
	_, wait := take(t, l, id)
	if wait <= 200*time.Millisecond || wait > 250*time.Millisecond {
		t.Fatalf("refused with a wait of %v, want just under 250ms at 4 a second", wait)
	}
}

func TestRefillsAfterTheWait(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	l := ratelimit.New(vk, prefix, 10, 2)
	id := uuid.New()
	take(t, l, id)
	take(t, l, id)
	ok, wait := take(t, l, id)
	if ok {
		t.Fatal("the operation past the burst was allowed")
	}
	time.Sleep(wait)
	if ok, _ := take(t, l, id); !ok {
		t.Fatalf("refused again after waiting the %v it asked for", wait)
	}
	if ok, _ := take(t, l, id); ok {
		t.Fatal("one wait refilled more than one token")
	}
}

func TestPlacementsHaveSeparateBuckets(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	l := ratelimit.New(vk, prefix, 1, 2)
	busy, other := uuid.New(), uuid.New()
	take(t, l, busy)
	take(t, l, busy)
	if ok, _ := take(t, l, busy); ok {
		t.Fatal("the operation past the burst was allowed")
	}
	for i := range 2 {
		if ok, _ := take(t, l, other); !ok {
			t.Fatalf("operation %d on another placement was refused", i+1)
		}
	}
	shared := ratelimit.New(vk, prefix, 1, 2)
	if ok, _ := take(t, shared, busy); ok {
		t.Fatal("a second limiter on the same prefix gave the busy placement a fresh bucket")
	}
	if ok, _ := take(t, ratelimit.New(vk, prefix+"x:", 1, 2), busy); !ok {
		t.Fatal("a limiter on another prefix shared the busy placement's bucket")
	}
}

func TestBucketExpiresOnceFull(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	l := ratelimit.New(vk, prefix, 10, 3)
	id := uuid.New()
	key := prefix + "ops:" + id.String()
	take(t, l, id)
	if ttl := pttl(t, vk, key); ttl <= 0 || ttl > 100*time.Millisecond {
		t.Fatalf("%s expires in %v after one operation, want at most the 100ms to refill", key, ttl)
	}
	take(t, l, id)
	take(t, l, id)
	if ttl := pttl(t, vk, key); ttl <= 200*time.Millisecond || ttl > 300*time.Millisecond {
		t.Fatalf("%s expires in %v after three operations, want just under 300ms", key, ttl)
	}

	fast := ratelimit.New(vk, prefix, 100, 1)
	gone := uuid.New()
	take(t, fast, gone)
	time.Sleep(50 * time.Millisecond)
	n, err := vk.Do(t.Context(), vk.B().Exists().Key(prefix+"ops:"+gone.String()).Build()).
		AsInt64()
	if err != nil || n != 0 {
		t.Fatalf("a full bucket's key still exists (%d, %v)", n, err)
	}
}

func TestDefaultsWhenZero(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	l := ratelimit.New(vk, prefix, 0, 0)
	id := uuid.New()
	for i := range ratelimit.DefaultBurst {
		if ok, _ := take(t, l, id); !ok {
			t.Fatalf("operation %d of the default burst was refused", i+1)
		}
	}
	ok, wait := take(t, l, id)
	if ok || wait <= 0 || wait > time.Second/ratelimit.DefaultPerSecond {
		t.Fatalf("past the default burst: allowed %v with a wait of %v", ok, wait)
	}
}

func TestValkeyErrorIsReturned(t *testing.T) {
	gone, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{testenv.ValkeyAddr()}})
	if err != nil {
		t.Fatal(err)
	}
	gone.Close()
	ok, _, err := ratelimit.New(gone, "bn:test:closed:", 10, 20).Take(t.Context(), uuid.New())
	if err == nil || ok {
		t.Fatalf("take on a closed client gave %v, %v, want an error", ok, err)
	}
}
