package boxd

import (
	"reflect"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// newTestProvider serves boxd from memory and returns the adapter talking to it
func newTestProvider(t *testing.T) (*fakeBoxd, *Provider) {
	t.Helper()
	f := newFake(t)
	return f, f.provider(t, fakeKey)
}

// mustCreate makes a 2x8 machine under key and fails the test when Create errs
func mustCreate(t *testing.T, p *Provider, key string) provider.Ref {
	t.Helper()
	ref, err := create2x8(t, p, key)
	if err != nil {
		t.Fatalf("create under %s: %v", key, err)
	}
	return ref
}
