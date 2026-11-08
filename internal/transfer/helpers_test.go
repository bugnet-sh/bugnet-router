package transfer_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/transfer/transfertest"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// is fails the test unless err wraps want
func is(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error %v, want %v", err, want)
	}
}

var small = provider.Size{Name: "small", Resources: provider.Resources{VCPU: 1}}

// sandbox is one ready local sandbox, seen as user's when user is set
type sandbox struct {
	prov provider.Provider
	ref  provider.Ref
	l    *local.Provider
	root string
	fake *transfertest.Sandbox // nil for the plain local provider
}

// host maps a sandbox path to its host path
func (s sandbox) host(p string) string {
	return filepath.Join(s.root, s.ref.ID, "fs", filepath.FromSlash(p))
}

func newSandbox(t *testing.T, user string) sandbox {
	t.Helper()
	root := t.TempDir()
	l := local.New(local.Options{Root: root, Sizes: []provider.Size{small}})
	ctx := context.Background()
	ref, err := l.Create(ctx, provider.CreateRequest{
		IdempotencyKey: t.Name(), Size: small, Lifetime: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Destroy(context.Background(), ref) })
	sb := sandbox{prov: l, ref: ref, l: l, root: root}
	if user != "" {
		sb.fake = transfertest.New(l, root, user)
		if err := sb.fake.Prepare(ref); err != nil {
			t.Fatal(err)
		}
		sb.prov = sb.fake
	}
	return sb
}

func (s sandbox) caps() provider.Capabilities { return s.prov.Capabilities() }

// write puts data at the sandbox path p with mode
func (s sandbox) write(t *testing.T, p, data string) {
	t.Helper()
	if err := s.l.WriteFile(t.Context(), s.ref, p, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

// read is the bytes at the sandbox path p
func (s sandbox) read(t *testing.T, p string) string {
	t.Helper()
	b, _, err := s.l.ReadFile(t.Context(), s.ref, p, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
