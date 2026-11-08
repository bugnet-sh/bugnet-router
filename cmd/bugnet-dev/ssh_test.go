package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// sshRouter answers AddSshKey and records each request
type sshRouter struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	got chan *connect.Request[routerv1.AddSshKeyRequest]
}

func (r *sshRouter) AddSshKey(
	_ context.Context, req *connect.Request[routerv1.AddSshKeyRequest],
) (*connect.Response[routerv1.AddSshKeyResponse], error) {
	r.got <- req
	return connect.NewResponse(&routerv1.AddSshKeyResponse{
		Alias: "bn-k3fz2q7max", Host: "ssh.test.example", Port: 2222,
		Fingerprint: "SHA256:abc", KnownHosts: []string{"[ssh.test.example]:2222 ssh-ed25519 A"},
		Command:   "ssh -p 2222 bn-k3fz2q7max@ssh.test.example",
		Config:    "Host bugnet-bn-k3fz2q7max\n  Port 2222\n",
		ExpiresAt: timestamppb.New(time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)),
	}), nil
}

func serveSSH(t *testing.T, r *sshRouter) string {
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

func TestSSHKey(t *testing.T) {
	r := &sshRouter{got: make(chan *connect.Request[routerv1.AddSshKeyRequest], 1)}
	settings := serveSSH(t, r)
	dir := t.TempDir()
	pub := filepath.Join(dir, "id.pub")
	if err := os.WriteFile(pub, []byte("ssh-ed25519 AAAA me@laptop\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	internal := strings.Repeat("i", 32)
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": internal})
	out, stderr, code := runCLI(t, vars, "--config", settings, "ssh-key", "--handle", "sbx_x",
		"--public-key", pub, "--identity-file", "$TMPDIR/bugnet/k")
	want := "alias=bn-k3fz2q7max\nhost=ssh.test.example\nport=2222\nfingerprint=SHA256:abc\n" +
		"known_hosts=[ssh.test.example]:2222 ssh-ed25519 A\n" +
		"command=ssh -p 2222 bn-k3fz2q7max@ssh.test.example\nexpires_at=2026-11-08T10:00:00Z\n" +
		"config=Host bugnet-bn-k3fz2q7max\nconfig=  Port 2222\n"
	if code != 0 || out != want {
		t.Fatalf("ssh-key exited %d with %q and %q, want %q", code, out, stderr, want)
	}
	req := <-r.got
	eq(t, req.Msg.GetHandle(), "sbx_x")
	eq(t, req.Msg.GetPublicKey(), "ssh-ed25519 AAAA me@laptop")
	eq(t, req.Msg.GetIdentityFile(), "$TMPDIR/bugnet/k")
	eq(t, req.Header().Get("Authorization"), "Bearer "+internal)

	private := filepath.Join(dir, "id")
	err := os.WriteFile(private, []byte("-----BEGIN OPENSSH PRIVATE KEY-----\nx\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"no handle":   {"--public-key", pub},
		"no key":      {"--handle", "h"},
		"private key": {"--handle", "h", "--public-key", private},
	} {
		args := append([]string{"--config", settings, "ssh-key"}, args...)
		if _, _, code := runCLI(t, vars, args...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
	select {
	case req := <-r.got:
		t.Fatalf("a refused call reached the router: %v", req.Msg)
	default:
	}
}
