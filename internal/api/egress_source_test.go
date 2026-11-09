package api_test

import (
	"context"
	"log/slog"
	"net/netip"
	"sync"
	"testing"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// sourceFake wraps the test provider with an egress source the test sets
type sourceFake struct {
	provider.Provider
	mu  sync.Mutex
	src provider.EgressSource
	ok  bool
	n   int
}

func (f *sourceFake) EgressSource(
	context.Context, provider.Ref,
) (provider.EgressSource, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return f.src, f.ok, nil
}

func (f *sourceFake) reports(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := netip.MustParseAddr(addr)
	f.src, f.ok = provider.EgressSource{Prefix: netip.PrefixFrom(a, a.BitLen()), Bind: a}, true
}

func (f *sourceFake) reads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

func newSources(
	t *testing.T, o options, reg *provider.Registry, st *store.Store, vk valkey.Client,
	prefix string, rc *river.Client[pgx.Tx],
) *secrets.Sources {
	t.Helper()
	s := secrets.SourceSettings{
		Modes:           map[string]secrets.Mode{"local": secrets.ModeEnforce},
		RefreshInterval: time.Minute, ReadTimeout: time.Second,
		EgressIPs: []netip.Addr{
			netip.MustParseAddr("203.0.113.7"), netip.MustParseAddr("2001:db8::7"),
		},
	}
	if o.sourceSettings != nil {
		o.sourceSettings(&s)
	}
	out, err := secrets.NewSources(secrets.SourcesDeps{
		Settings: s, Registry: reg, Store: st, Valkey: vk, Prefix: prefix, River: rc,
		Events: edgeevents.New(vk, prefix, discard), Log: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func withSource(addr string) *routerv1.ResolveTunnelRequest {
	r := byToken(sha(tunnelToken))
	r.SourceIp = addr
	return r
}

func (e *env) storedSource(t *testing.T, id uuid.UUID, addr string) {
	t.Helper()
	a := netip.MustParseAddr(addr)
	_, err := e.store.SetEgressSource(t.Context(), id, provider.EgressSource{
		Prefix: netip.PrefixFrom(a, a.BitLen()), Bind: a,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestResolveTunnelAnswersTheSourceCheck(t *testing.T) {
	enforce := routerv1.SourceCheckMode_SOURCE_CHECK_MODE_ENFORCE
	fake := &sourceFake{}
	e := newEnv(t, options{secrets: true, sourcer: fake})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	fake.reports("2001:db8::5")

	// No stored prefix with allow: no check, and no read
	r := e.mustTunnel(t, withSource("198.51.100.1"))
	eq(t, r.GetSourceCheck() == nil, true)
	eq(t, fake.reads(), 0)

	// A source inside the stored prefix reads nothing
	e.storedSource(t, id, "2001:db8::5")
	r = e.mustTunnel(t, withSource("2001:db8::5"))
	eq(t, r.GetSourceCheck().GetMode(), enforce)
	eq(t, r.GetSourceCheck().GetAllowedPrefixes(), []string{"2001:db8::5/128"})
	r = e.mustTunnel(t, withSource(""))
	eq(t, r.GetSourceCheck().GetMode(), enforce)
	r = e.mustTunnel(t, withSource("not-an-ip"))
	eq(t, r.GetSourceCheck().GetMode(), enforce)
	eq(t, fake.reads(), 0)

	// A source outside it reads once, the provider still saying the same address
	r = e.mustTunnel(t, withSource("198.51.100.66"))
	eq(t, r.GetSourceCheck().GetAllowedPrefixes(), []string{"2001:db8::5/128"})
	_ = e.mustTunnel(t, withSource("198.51.100.67"))
	eq(t, fake.reads(), 1)
	eq(t, e.placementOfID(t, id).EgressSource.Prefix.String(), "2001:db8::5/128")

	// The fetcher is never checked
	f := e.mustTunnel(t, byFetch(id.String()))
	eq(t, f.GetSourceCheck() == nil, true)
}

func TestResolveTunnelMovedAddressIsServedAndStored(t *testing.T) {
	fake := &sourceFake{}
	e := newEnv(t, options{secrets: true, sourcer: fake})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	e.storedSource(t, id, "2001:db8::5")
	before := e.placementOfID(t, id).SecretsVersion
	fake.reports("2001:db8::6")

	r := e.mustTunnel(t, withSource("2001:db8::6"))
	eq(t, r.GetSourceCheck().GetAllowedPrefixes(), []string{"2001:db8::6/128"})
	got := e.placementOfID(t, id)
	eq(t, got.EgressSource.Bind.String(), "2001:db8::6")
	eq(t, got.SecretsVersion, before+1)
}

func TestResolveTunnelRefuseWithoutASourceAnswersNoPrefixes(t *testing.T) {
	fake := &sourceFake{}
	e := newEnv(t, options{secrets: true, sourcer: fake,
		sourceSettings: func(s *secrets.SourceSettings) { s.RefuseUnknown = true }})
	_, p := e.readyPlacement(t)
	e.armed(t, p, 1)
	r := e.mustTunnel(t, withSource("198.51.100.1"))
	eq(t, r.GetSourceCheck().GetMode(), routerv1.SourceCheckMode_SOURCE_CHECK_MODE_ENFORCE)
	eq(t, len(r.GetSourceCheck().GetAllowedPrefixes()), 0)
	eq(t, fake.reads(), 1)
}

func TestResolveTunnelWithoutSourcesAnswersNoCheck(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	_, p := e.readyPlacement(t)
	e.armed(t, p, 1)
	eq(t, e.mustTunnel(t, withSource("2001:db8::5")).GetSourceCheck() == nil, true)
}

func (e *env) placementOfID(t *testing.T, id uuid.UUID) store.Placement {
	t.Helper()
	got, err := e.store.PlacementByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
