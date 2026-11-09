package secrets

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/hostrules"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// mustCode fails the test unless err is a Refusal with code, and reason too when it is set
func mustCode(t *testing.T, err error, code connect.Code, reason string) {
	t.Helper()
	var r *Refusal
	if !errors.As(err, &r) {
		t.Fatalf("got %v, want a refusal %s", err, reason)
	}
	if r.Code != code || (reason != "" && r.Reason != reason) {
		t.Fatalf("got %v %s, want %v %s", r.Code, r.Reason, code, reason)
	}
}

// option changes the spec or context of one validate call
type option func(*routerv1.SecretSpec, *Context)

func name(n string) option {
	return func(s *routerv1.SecretSpec, _ *Context) { s.Name = n }
}

func hosts(h ...string) option {
	return func(s *routerv1.SecretSpec, _ *Context) { s.Hosts = h }
}

func headers(h ...string) option {
	return func(s *routerv1.SecretSpec, _ *Context) { s.HeaderNames = h }
}

func customHostsOff(_ *routerv1.SecretSpec, c *Context) { c.CustomHosts = false }

func allow(h ...string) option {
	return func(_ *routerv1.SecretSpec, c *Context) { c.EgressAllow = h }
}

// validContext is the router's default context, custom hosts on and no egress allowlist
func validContext() Context {
	r := hostrules.New(config.X402{SharedHostingSuffixes: []string{"workers.dev"}},
		config.Names{PreviewDomain: "preview.bugnet.sh", SecretsHost: "secrets.bugnet.sh"}, "")
	return Context{Hosts: r.ForSecrets(), CustomHosts: true}
}

// validate runs Validate on an HF_TOKEN spec for huggingface.co changed by opts
func validate(t *testing.T, opts ...option) error {
	t.Helper()
	spec := &routerv1.SecretSpec{Name: "HF_TOKEN", Hosts: []string{"huggingface.co"}}
	c := validContext()
	for _, o := range opts {
		o(spec, &c)
	}
	err := Validate(spec, c)
	if err != nil {
		for _, h := range spec.GetHosts() {
			if h != "" && strings.Contains(err.Error(), h) {
				t.Fatalf("the refusal quotes host %q: %v", h, err)
			}
		}
	}
	return err
}
