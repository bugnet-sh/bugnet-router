package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"golang.org/x/crypto/nacl/box"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// secretRouter answers GetSecretSealingKey with one key pair it holds and records PutSecret
type secretRouter struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	pub, priv *[32]byte
	put       chan *connect.Request[routerv1.PutSecretRequest]
}

func (r *secretRouter) GetSecretSealingKey(
	_ context.Context, _ *connect.Request[routerv1.GetSecretSealingKeyRequest],
) (*connect.Response[routerv1.GetSecretSealingKeyResponse], error) {
	return connect.NewResponse(&routerv1.GetSecretSealingKeyResponse{
		SealingKeys: []*routerv1.SealingKey{
			{KeyId: "k2", PublicKey: r.pub[:], Binding: "BINDING"},
			{KeyId: "k1", PublicKey: make([]byte, 32), Binding: "BINDING"},
		},
	}), nil
}

func (r *secretRouter) PutSecret(
	_ context.Context, req *connect.Request[routerv1.PutSecretRequest],
) (*connect.Response[routerv1.PutSecretResponse], error) {
	r.put <- req
	return connect.NewResponse(&routerv1.PutSecretResponse{
		Secret: &routerv1.SecretInfo{
			Spec: req.Msg.GetSpec(), State: routerv1.SecretState_SECRET_STATE_ACTIVE,
			UpdatedAt: timestamppb.New(time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)),
		},
	}), nil
}

func TestSecretSealsTheValueLocally(t *testing.T) {
	pub, priv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	r := &secretRouter{pub: pub, priv: priv,
		put: make(chan *connect.Request[routerv1.PutSecretRequest], 1)}
	settings := serveSecrets(t, r)
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": strings.Repeat("i", 32)})
	setStdin(t, "hf_value_from_stdin\n")
	out, stderr, code := runCLI(t, vars, "--config", settings, "secret", "--handle", "sbx_x",
		"--name", "HF_TOKEN", "--host", "huggingface.co", "--host", "hf.co",
		"--header", "x-api-key")
	want := "name=HF_TOKEN\nstate=active\nupdated_at=2026-11-08T10:00:00Z\nreplaced=false\n"
	if code != 0 || out != want {
		t.Fatalf("secret exited %d with %q and %q, want %q", code, out, stderr, want)
	}
	req := <-r.put
	m := req.Msg
	eq(t, m.GetHandle(), "sbx_x")
	deepEq(t, m.GetSpec().GetHosts(), []string{"huggingface.co", "hf.co"})
	deepEq(t, m.GetSpec().GetHeaderNames(), []string{"x-api-key"})
	eq(t, m.GetValue().GetKeyId(), "k2")
	if m.GetIdempotencyKey() == "" {
		t.Fatal("the put carried no idempotency key")
	}
	plain, ok := box.OpenAnonymous(nil, m.GetValue().GetBox(), pub, priv)
	if !ok {
		t.Fatal("the box does not open with the newest key")
	}
	var payload map[string]any
	if err := json.Unmarshal(plain, &payload); err != nil {
		t.Fatal(err)
	}
	deepEq(t, payload, map[string]any{
		"v": float64(1), "binding": "BINDING", "name": "HF_TOKEN",
		"hosts": []any{"hf.co", "huggingface.co"}, "headers": []any{"x-api-key"},
		"fetchOnly": false, "value": "hf_value_from_stdin",
	})
	if strings.Contains(out+stderr, "hf_value") {
		t.Fatal("the value was printed")
	}
}

func TestSecretNeedsAValueAndFlags(t *testing.T) {
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": strings.Repeat("i", 32)})
	setStdin(t, "")
	for _, args := range [][]string{
		{"secret", "--name", "A", "--host", "a.example"},
		{"secret", "--handle", "h", "--host", "a.example"},
		{"secret", "--handle", "h", "--name", "A"},
		{"secret", "--handle", "h", "--name", "A", "--host", "a.example"},
	} {
		if _, _, code := runCLI(t, vars, args...); code == 0 {
			t.Errorf("%v exited 0", args)
		}
	}
}

// deepEq fails the test when got and want differ, for values == cannot compare
func deepEq(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// serveSecrets serves r over h2c and returns a settings file pointing at it
func serveSecrets(t *testing.T, r *secretRouter) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(routerv1connect.NewSandboxRouterServiceHandler(r))
	srv := httptest.NewUnstartedServer(mux)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)
	return writeSettings(t, "[api]\nlisten_addr = \""+srv.Listener.Addr().String()+"\"\n")
}

// setStdin makes the commands read in from stdin until the test ends
func setStdin(t *testing.T, in string) {
	t.Helper()
	old := stdin
	stdin = strings.NewReader(in)
	t.Cleanup(func() { stdin = old })
}
