package provider_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/local"
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

func TestRegistryKeepsPreferenceOrder(t *testing.T) {
	b := local.New(local.Options{Name: "b", Root: t.TempDir()})
	a := local.New(local.Options{Name: "a", Root: t.TempDir()})
	reg := provider.NewRegistry([]provider.Provider{b, a})

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
	defer func() {
		if recover() == nil {
			t.Fatal("NewRegistry accepted two providers with one name")
		}
	}()
	one := local.New(local.Options{Name: "same", Root: t.TempDir()})
	two := local.New(local.Options{Name: "same", Root: t.TempDir()})
	provider.NewRegistry([]provider.Provider{one, two})
}
