package worker_test

import (
	"context"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
)

// sourcedFake is the egress fake with an egress source the test sets
type sourcedFake struct {
	*egressFake
	smu sync.Mutex
	src provider.EgressSource
	ok  bool
	n   int
}

func (f *sourcedFake) EgressSource(
	context.Context, provider.Ref,
) (provider.EgressSource, bool, error) {
	f.smu.Lock()
	defer f.smu.Unlock()
	f.n++
	return f.src, f.ok, nil
}

func (f *sourcedFake) reports(addr string, ok bool) {
	f.smu.Lock()
	defer f.smu.Unlock()
	a := netip.MustParseAddr(addr)
	f.src, f.ok = provider.EgressSource{Prefix: netip.PrefixFrom(a, a.BitLen()), Bind: a}, ok
}

func (f *sourcedFake) reads() int {
	f.smu.Lock()
	defer f.smu.Unlock()
	return f.n
}

// sourceEnv is a secrets env whose local provider reports an egress source
func sourceEnv(
	t *testing.T, tune func(*secrets.SourceSettings),
) (*env, *sourcedFake, *clock) {
	t.Helper()
	f := &sourcedFake{egressFake: &egressFake{Provider: newLocal(t, "local")}}
	s := secrets.SourceSettings{
		Modes:           map[string]secrets.Mode{"local": secrets.ModeEnforce},
		RefreshInterval: time.Minute, ReadTimeout: time.Second, EgressIPs: egressTarget.IPs,
	}
	if tune != nil {
		tune(&s)
	}
	clk := &clock{}
	e := newEnv(t, options{
		providers: []provider.Provider{f},
		features:  provider.Features{Secrets: []string{"local"}},
		egress:    egressTarget, sources: &s, sourcesClock: clk,
	})
	return e, f, clk
}

func TestApplySecretsWritesSourceOnlyWhenStored(t *testing.T) {
	e, f, _ := sourceEnv(t, nil)
	e.start(t)
	p, _ := e.placeReady(t, "o-src-spec")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.apply(t, p)
	specs := f.sent()
	eq(t, len(specs), 1)
	eq(t, specs[0].Source == nil, true)
	eq(t, strings.Contains(string(f.raw[0]), "source"), false)
	eq(t, e.reload(t, p.ID).EgressSource.Prefix.IsValid(), false)

	f.reports("2001:db8::5", true)
	e.putSecret(t, p, "GH_TOKEN", false, "github.com")
	e.apply(t, p)
	specs = f.sent()
	eq(t, len(specs), 2)
	eq(t, specs[1].Source.Family, "ipv6")
	eq(t, specs[1].Source.Bind, "2001:db8::5")
	got := e.reload(t, p.ID)
	eq(t, got.EgressSource.Prefix.String(), "2001:db8::5/128")
}

func TestApplySecretsWritesNoSourceWhenOff(t *testing.T) {
	e, f, _ := sourceEnv(t, func(s *secrets.SourceSettings) {
		s.Modes = map[string]secrets.Mode{"local": secrets.ModeOff}
	})
	e.start(t)
	f.reports("2001:db8::5", true)
	p, _ := e.placeReady(t, "o-src-off")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.apply(t, p)
	eq(t, f.sent()[0].Source == nil, true)
	eq(t, f.reads(), 0)
}

func TestApplySecretsReadsOnlyWhenStale(t *testing.T) {
	e, f, clk := sourceEnv(t, nil)
	e.start(t)
	f.reports("2001:db8::5", true)
	p, _ := e.placeReady(t, "o-src-stale")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.apply(t, p)
	eq(t, f.reads(), 1)
	e.putSecret(t, p, "GH_TOKEN", false, "github.com")
	e.apply(t, p)
	eq(t, f.reads(), 1)
	clk.Advance(2 * time.Minute)
	e.putSecret(t, p, "NPM_TOKEN", false, "registry.npmjs.org")
	e.apply(t, p)
	eq(t, f.reads(), 2)
}

func TestSourceChangeInsideARunLeavesNoLag(t *testing.T) {
	e, f, _ := sourceEnv(t, nil)
	e.start(t)
	f.reports("2001:db8::5", true)
	p, _ := e.placeReady(t, "o-src-lag")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	before := e.secretsChanged(t, p)
	e.apply(t, p)
	pending, lag, err := e.store.SecretsPending(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, pending, false)
	eq(t, lag, int64(0))
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "active"})
	// The source change and the run's own finish each write the event
	eq(t, e.secretsChanged(t, p) > before, true)
	eq(t, len(f.sent()), 1)
}

func TestRefuseWithoutSourceFailsSecrets(t *testing.T) {
	e, f, _ := sourceEnv(t, func(s *secrets.SourceSettings) { s.RefuseUnknown = true })
	e.start(t)
	f.reports("2001:db8::5", false)
	p, _ := e.placeReady(t, "o-src-refuse")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.putSecret(t, p, "GITHUB_TOKEN", true, "api.github.com")
	e.apply(t, p)
	eq(t, len(f.sent()), 0)
	eq(t, e.states(t, p), map[string]string{
		"HF_TOKEN": "failed SOURCE_UNKNOWN", "GITHUB_TOKEN": "active",
	})
	has, err := e.store.HasEgressToken(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, has, false)
}
