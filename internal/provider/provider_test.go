package provider_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

func TestAsErrorFindsWrappedError(t *testing.T) {
	cause := errors.New("no machine ready")
	perr := &provider.Error{
		Code:        provider.CodeCapacity,
		RetryAfter:  time.Second,
		CanFailOver: true,
		Err:         cause,
	}
	wrapped := fmt.Errorf("create on boat: %w", perr)

	got, ok := provider.AsError(wrapped)
	if !ok || got != perr {
		t.Fatalf("AsError gave %v, %v, want the wrapped *Error", got, ok)
	}
	if !errors.Is(wrapped, cause) {
		t.Error("errors.Is does not reach the cause")
	}
	msg := perr.Error()
	if !strings.Contains(msg, "capacity") || !strings.Contains(msg, cause.Error()) {
		t.Errorf("Error() gave %q, want the code and the cause", msg)
	}
	if _, ok := provider.AsError(cause); ok {
		t.Error("AsError found an *Error in a plain error")
	}
	if _, ok := provider.AsError(nil); ok {
		t.Error("AsError found an *Error in nil")
	}
}

func TestRegistryKeepsConfiguredOrder(t *testing.T) {
	b := local.New(local.Options{Name: "b", Root: t.TempDir()})
	a := local.New(local.Options{Name: "a", Root: t.TempDir()})
	reg, err := provider.NewRegistry([]provider.Provider{b, a}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}

	ordered := reg.Ordered()
	if len(ordered) != 2 || ordered[0].Name() != "b" || ordered[1].Name() != "a" {
		t.Fatalf("Ordered gave %v", ordered)
	}
	ordered[0] = a
	if reg.Ordered()[0].Name() != "b" {
		t.Fatal("changing the slice Ordered returned changed the registry")
	}
	if got, ok := reg.Get("a"); !ok || got.Name() != "a" {
		t.Fatalf("Get(a) gave %v, %v", got, ok)
	}
	if _, ok := reg.Get("missing"); ok {
		t.Fatal("Get found a provider that was never registered")
	}
}

func TestRegistryRejectsDuplicateNames(t *testing.T) {
	one := local.New(local.Options{Name: "same", Root: t.TempDir()})
	two := local.New(local.Options{Name: "same", Root: t.TempDir()})
	_, err := provider.NewRegistry([]provider.Provider{one, two}, provider.Features{})
	if err == nil || !strings.Contains(err.Error(), `"same"`) {
		t.Fatalf("two providers named same gave %v, want an error naming it", err)
	}
}

func TestListingStopsAtItsLimits(t *testing.T) {
	l := provider.NewListing(2)
	if !l.Visit() || !l.Add(provider.FileEntry{Path: "/a"}) ||
		!l.Add(provider.FileEntry{Path: "/b"}) {
		t.Fatal("a listing refused entries within its limit")
	}
	if l.Truncated || !l.Full() || l.Room() != 0 {
		t.Fatalf("a listing at its limit is truncated %v full %v room %d", l.Truncated, l.Full(),
			l.Room())
	}
	if l.Add(provider.FileEntry{Path: "/c"}) || !l.Truncated || len(l.Entries) != 2 {
		t.Fatalf("a full listing took another entry, %+v", l)
	}

	dirs := provider.NewListing(1 << 20)
	for i := range provider.MaxListDirs {
		if !dirs.Visit() {
			t.Fatalf("visit %d refused below the directory limit", i)
		}
	}
	if dirs.Visit() || !dirs.Truncated {
		t.Fatal("a walk read more directories than the limit")
	}
}

func TestDefaultGatePortIsTheSharedWireValue(t *testing.T) {
	if provider.DefaultGatePort != 29417 {
		t.Fatalf("gate port %d, want 29417 as bugnet-run and the edges use",
			provider.DefaultGatePort)
	}
}

func TestCheckGatePort(t *testing.T) {
	for _, port := range []int{1, 29417, 65535} {
		if err := provider.CheckGatePort(port); err != nil {
			t.Errorf("port %d gave %v", port, err)
		}
	}
	for _, port := range []int{-1, 0, 65536} {
		e, ok := provider.AsError(provider.CheckGatePort(port))
		if !ok || e.Code != provider.CodeInvalid {
			t.Errorf("port %d gave %v, want invalid", port, e)
		}
	}
	err := provider.GateUnprepared("boat", "bx_1", "gate_host")
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeInvalid || !errors.Is(err, provider.ErrGateUnprepared) ||
		!strings.Contains(err.Error(), "gate_host") {
		t.Fatalf("unprepared gave %v, want invalid wrapping ErrGateUnprepared", err)
	}
}

func TestNoSpaceIsItsOwnCode(t *testing.T) {
	if provider.CodeNoSpace != "no_space" {
		t.Fatalf("no space code %q, want no_space", provider.CodeNoSpace)
	}
}

// reporting is a provider without a gate route whose adapter claims every data-plane feature
type reporting struct{ provider.Provider }

func (r reporting) Capabilities() provider.Capabilities {
	c := r.Provider.Capabilities()
	c.SSH, c.Preview, c.Secrets, c.ExecUser = true, true, true, "user"
	return c
}

// gated is a local provider with a gate route
type gated struct{ reporting }

func (gated) PrepareGate(context.Context, provider.Ref, int) (map[string]string, error) {
	return nil, nil
}

func (gated) GateRoute(provider.Ref, int) (provider.GateRoute, error) {
	return provider.GateRoute{}, nil
}

// native is a provider with an SSH server of its own and no gate route
type native struct{ provider.Provider }

func (native) AuthorizeNativeSSH(
	context.Context, provider.Ref, string,
) (provider.NativeSSH, error) {
	return provider.NativeSSH{}, nil
}

func newNamed(t *testing.T, name string) *local.Provider {
	t.Helper()
	return local.New(local.Options{Name: name, Root: t.TempDir()})
}

func TestRegistryComputesFeatures(t *testing.T) {
	listed := gated{reporting{newNamed(t, "listed")}}
	unlisted := gated{reporting{newNamed(t, "unlisted")}}
	plain := reporting{newNamed(t, "plain")}
	f := provider.Features{
		Preview: []string{"listed", "absent"}, SSH: []string{"listed"},
		Secrets: []string{"plain"},
	}
	reg, err := provider.NewRegistry([]provider.Provider{listed, unlisted, plain}, f)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		p                     provider.Provider
		preview, ssh, secrets bool
	}{
		{listed, true, true, false},
		{unlisted, false, false, false},
		{plain, false, false, true},
	} {
		got := reg.Capabilities(c.p)
		if got.Preview != c.preview || got.SSH != c.ssh || got.Secrets != c.secrets {
			t.Errorf("%s: preview %v, ssh %v, secrets %v, want %v, %v, %v", c.p.Name(),
				got.Preview, got.SSH, got.Secrets, c.preview, c.ssh, c.secrets)
		}
		if got.ExecUser != "user" || got.MaxLifetime != 24*time.Hour {
			t.Errorf("%s: %+v, want the adapter's other capabilities kept", c.p.Name(), got)
		}
	}
	if got := reg.Capabilities(reporting{newNamed(t, "stranger")}); got.SSH || got.Preview ||
		got.Secrets {
		t.Errorf("a provider the registry lacks got %+v, want no data-plane feature", got)
	}

	boat := native{newNamed(t, "boat")}
	reg, err = provider.NewRegistry([]provider.Provider{boat},
		provider.Features{SSH: []string{"boat"}, BoatNativeSSH: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := reg.Capabilities(boat); !got.SSH || got.Preview {
		t.Errorf("boat on its native path got %+v, want ssh alone", got)
	}
	if !reg.NativeSSH(boat) {
		t.Error("boat on its native path is not reached natively")
	}
	gatedBoat := gated{reporting{native{newNamed(t, "boat")}}}
	reg, err = provider.NewRegistry([]provider.Provider{gatedBoat},
		provider.Features{SSH: []string{"boat"}})
	if err != nil {
		t.Fatal(err)
	}
	if reg.NativeSSH(gatedBoat) || reg.NativeSSH(listed) {
		t.Error("a provider on the gate path is reached natively")
	}
}

func TestRegistryRefusesListedProviderWithoutGater(t *testing.T) {
	for _, c := range []struct {
		name, want string
		p          provider.Provider
		f          provider.Features
	}{
		{"preview without a gate", "plain is listed for previews but has no gate route",
			reporting{newNamed(t, "plain")}, provider.Features{Preview: []string{"plain"}}},
		{"ssh without a gate", "plain is listed for ssh but has no gate route",
			reporting{newNamed(t, "plain")}, provider.Features{SSH: []string{"plain"}}},
		{"native server on the gate path", "boat is listed for ssh but has no gate route",
			native{newNamed(t, "boat")}, provider.Features{SSH: []string{"boat"}}},
		{"native path without a native server",
			"boat is listed for ssh on its native path but has no SSH server of its own",
			gated{reporting{newNamed(t, "boat")}},
			provider.Features{SSH: []string{"boat"}, BoatNativeSSH: true}},
	} {
		reg, err := provider.NewRegistry([]provider.Provider{c.p}, c.f)
		if reg != nil || err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, %v, want an error with %q", c.name, reg, err, c.want)
		}
	}
	reg, err := provider.NewRegistry([]provider.Provider{reporting{newNamed(t, "plain")}},
		provider.Features{Secrets: []string{"plain"}})
	if err != nil || !reg.Capabilities(reg.Ordered()[0]).Secrets {
		t.Fatalf("secrets need no interface, got %v", err)
	}
}
