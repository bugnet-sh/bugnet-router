package boat

import (
	"net/http"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

const (
	meActive    = `{"ok":true,"type":"user.info","user":{"login":"a","accountStatus":"active"}}`
	meSuspended = `{"ok":true,"user":{"login":"a","accountStatus":"suspended","suspended":true,` +
		`"suspendedReason":"abuse"}}`
	meClosed      = `{"ok":true,"user":{"login":"a","accountStatus":"closed","closed":true}}`
	limitsCounts  = `"activeSandboxes":3,"maxActiveSandboxes":100`
	limitsFree    = `{"canStart":true,` + limitsCounts + `,"nextStartDollars":0,`
	limitsPlenty  = limitsFree + `"wallet":{"balanceDollars":5}}`
	limitsOwing   = limitsFree + `"wallet":{"balanceDollars":-0.5}}`
	limitsBlocked = `{"canStart":false,` + limitsCounts + `,"blockedReason":"payment failed"}`
)

func TestBoatAccountMappings(t *testing.T) {
	for _, tc := range []struct {
		name         string
		meStatus     int
		me           string
		limitsStatus int
		limits       string
		want         provider.Block
	}{
		{"active", 0, meActive, 0, limitsPlenty, provider.BlockNone},
		{"suspended", 0, meSuspended, 0, limitsPlenty, provider.BlockBanned},
		{"closed", 0, meClosed, 0, limitsPlenty, provider.BlockBanned},
		{"wallet below zero", 0, meActive, 0, limitsOwing, provider.BlockUnfunded},
		{"billing reason blocks starts", 0, meActive, 0, limitsBlocked, provider.BlockUnfunded},
		{"401 on me", http.StatusUnauthorized, "", 0, limitsPlenty, provider.BlockAuth},
		{"401 on limits", 0, meActive, http.StatusUnauthorized, "", provider.BlockAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.setMe(tc.meStatus, tc.me)
			f.setLimits(tc.limitsStatus, tc.limits)
			acc, err := f.provider(t, fakeKey).CheckAccount(t.Context())
			if err != nil || acc.Block != tc.want {
				t.Fatalf("account = %+v, %v, want block %q", acc, err, tc.want)
			}
			if tc.want != provider.BlockNone && acc.Detail == "" {
				t.Fatal("a block carries no detail")
			}
		})
	}
}

func TestBoat403OnMeIsUnreadable(t *testing.T) {
	f := newFake(t)
	f.setMe(http.StatusForbidden, "")
	f.setLimits(http.StatusForbidden, "")
	p := f.provider(t, fakeKey)
	// The key lacks account.read, so nothing is known and nothing is blocked
	acc, err := p.CheckAccount(t.Context())
	if err != nil || acc.Block != provider.BlockNone {
		t.Fatalf("account = %+v, %v, want none", acc, err)
	}
	// A 403 from the sandbox list, which the runtime key must be able to make, reads as auth
	f.inject("GET /api/v1/sandboxes", fault{status: http.StatusForbidden, code: "forbidden"})
	acc, err = p.CheckAccount(t.Context())
	if err != nil || acc.Block != provider.BlockAuth {
		t.Fatalf("account = %+v, %v, want auth", acc, err)
	}
}

func TestBoatAccountReadsAPlainFailureAsAnError(t *testing.T) {
	f := newFake(t)
	f.setMe(0, meActive)
	f.setLimits(0, limitsPlenty)
	f.inject("GET /api/v1/me", fault{status: http.StatusBadGateway, code: "bad_gateway"})
	acc, err := f.provider(t, fakeKey).CheckAccount(t.Context())
	if err == nil || acc.Block != provider.BlockNone {
		t.Fatalf("account = %+v, %v, want an error and no block", acc, err)
	}
}

func TestBoatReadLimits(t *testing.T) {
	f := newFake(t)
	f.setLimits(0, limitsPlenty)
	got, err := f.provider(t, fakeKey).ReadLimits(t.Context())
	eq(t, err, nil)
	eq(t, got, provider.Limits{MaxConcurrent: 100, Active: 3})

	f.setLimits(0, `{"nextStartDollars":0}`)
	if _, err := f.provider(t, fakeKey).ReadLimits(t.Context()); err == nil {
		t.Fatal("a reply without counts was read as limits")
	}
}

func TestBoatLimitsShared(t *testing.T) {
	f := newFake(t)
	f.setLimits(0, limitsPlenty)
	vk, prefix := testenv.Valkey(t)
	replicas := []*Provider{f.sharedProvider(t, vk, prefix), f.sharedProvider(t, vk, prefix)}
	for i, p := range replicas {
		got, err := p.ReadLimits(t.Context())
		if err != nil || got.Active != 3 {
			t.Fatalf("replica %d read %+v, %v", i, got, err)
		}
	}
	// A fee check reads the same reply and calls Boat itself only when it is over a minute old
	replicas[1].refreshLimits()
	f.mu.Lock()
	calls := f.limitsCalls
	f.mu.Unlock()
	if calls != 1 {
		t.Fatalf("Boat answered %d /limits calls for two replicas and a fee check, want 1", calls)
	}
	ttl, err := vk.Do(t.Context(), vk.B().Pttl().Key(prefix+"limits:boat").Build()).AsInt64()
	if err != nil || ttl <= 0 {
		t.Fatalf("limits:boat ttl = %d, %v", ttl, err)
	}
	// A reply older than the poll interval is read again
	shared := &sharedLimits{c: vk, key: prefix + sharedLimitsKey}
	e, err := shared.get(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	e.At = time.Now().Add(-pollEvery - time.Second).UnixMilli()
	if err := shared.put(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if _, err := replicas[0].ReadLimits(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	calls = f.limitsCalls
	f.mu.Unlock()
	if calls != 2 {
		t.Fatalf("Boat answered %d calls after the reply grew old, want 2", calls)
	}
}

func TestBoatFeeCheckUsesTheSharedReplyUnderAMinute(t *testing.T) {
	f := newFake(t)
	f.setLimits(0, limitsPlenty)
	vk, prefix := testenv.Valkey(t)
	e := limitsEntry{
		At: time.Now().Add(-40 * time.Second).UnixMilli(), Body: []byte(limitsPlenty), RateLeft: -1,
	}
	if err := (&sharedLimits{c: vk, key: prefix + sharedLimitsKey}).put(t.Context(), e); err !=
		nil {
		t.Fatal(err)
	}
	p := f.sharedProvider(t, vk, prefix)
	p.refreshLimits()
	f.mu.Lock()
	calls := f.limitsCalls
	f.mu.Unlock()
	if calls != 0 {
		t.Fatalf("a 40 s old shared reply caused %d calls to Boat", calls)
	}
	if p.fees.fetched.IsZero() || time.Since(p.fees.fetched) < 39*time.Second {
		t.Fatalf("the fee cache is dated %v, want the reply's own time", p.fees.fetched)
	}
}

func TestBoatRateLimitHeaderIsKept(t *testing.T) {
	f := newFake(t)
	f.setLimits(0, limitsPlenty)
	f.mu.Lock()
	f.rateHeader = `"ip";r=1900;t=60`
	f.mu.Unlock()
	vk, prefix := testenv.Valkey(t)
	if _, ok, err := ReadRateLeft(t.Context(), vk, prefix); ok || err != nil {
		t.Fatalf("a missing entry read as %v, %v", ok, err)
	}
	if _, err := f.sharedProvider(t, vk, prefix).ReadLimits(t.Context()); err != nil {
		t.Fatal(err)
	}
	left, ok, err := ReadRateLeft(t.Context(), vk, prefix)
	eq(t, err, nil)
	eq(t, ok, true)
	eq(t, left, RateLeft{Remaining: 1900, Limit: 10000})
}
