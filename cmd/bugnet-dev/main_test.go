package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/handle"
	"github.com/oynozan/bugnet-router/internal/ordertoken"
)

const catalogJSON = `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 1, "memory_mib": 512}, "minutes": 10,
		"price_micros": 50000},
	{"id": "small-10m-more", "resources": {"vcpu": 1, "memory_mib": 512}, "minutes": 10,
		"price_micros": 45000, "extension_of": "small-10m"}
]}`

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func runCLI(t *testing.T, getenv func(string) string, args ...string) (string, string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), args, &stdout, &stderr, getenv)
	return stdout.String(), stderr.String(), code
}

func newKey(t *testing.T) (ed25519.PublicKey, string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, base64.StdEncoding.EncodeToString(priv.Seed())
}

// writeSettings writes a settings file and returns its path
func writeSettings(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "settings.toml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestKeysMakeAValidConfig(t *testing.T) {
	out, stderr, code := runCLI(t, env(nil), "keys")
	if code != 0 {
		t.Fatalf("keys exited %d: %s", code, stderr)
	}
	vars := map[string]string{}
	var names []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok || v == "" {
			t.Fatalf("line %q is not KEY=value", line)
		}
		vars[k] = v
		names = append(names, k)
	}
	want := []string{
		"BUGNET_INTERNAL_TOKEN", "BUGNET_ORDER_PRIVATE_KEY", "BUGNET_HANDLE_KEY",
		"BUGNET_EVENT_SIGNING_KEY", "BUGNET_DATA_PLANE_KEYS", "BUGNET_EDGE_TOKEN",
		"BUGNET_SECRETS_TOKEN", "BUGNET_SSH_TOKEN", "BUGNET_EGRESS_TOKEN",
		"BUGNET_EGRESS_FETCH_TOKEN", "BUGNET_HOOK_PATH_SECRET",
	}
	if !slices.Equal(names, want) {
		t.Fatalf("keys printed %v, want only the secrets %v", names, want)
	}
	path, err := base64.RawURLEncoding.DecodeString(vars["BUGNET_HOOK_PATH_SECRET"])
	if err != nil || len(path) != 32 {
		t.Fatalf("hook path secret is not base64url of 32 bytes: %v", err)
	}
	pub, stderr, code := runCLI(t, env(vars), "pubkey")
	if code != 0 {
		t.Fatalf("pubkey exited %d: %s", code, stderr)
	}
	vars["BUGNET_DATABASE_URL"] = "postgres://localhost/unused"
	settings := writeSettings(t, `order_token_public_key = "`+strings.TrimSpace(pub)+`"
packages_file = "packages.json"
`)
	cfg, err := config.Load(settings, func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	})
	if err != nil {
		t.Fatalf("config from the printed keys: %v", err)
	}
	if cfg.EventSigningKey == nil {
		t.Fatal("no event signing key")
	}
	if roots := cfg.DataPlaneKeys.Roots(); len(roots) != 1 || roots[0].ID != "k1" {
		t.Fatalf("data-plane keys hold %d roots, want only k1", len(roots))
	}
	tokens := []string{cfg.InternalToken, cfg.EdgeToken, cfg.SecretsToken, cfg.SSHToken,
		cfg.EgressToken, cfg.EgressFetchToken}
	for i, tok := range tokens {
		if len(tok) < 32 || slices.Contains(tokens[:i], tok) {
			t.Fatalf("caller token %d is short or repeated", i)
		}
	}
	token, _, code := runCLI(t, env(nil), "token", "--priv", vars["BUGNET_ORDER_PRIVATE_KEY"],
		"--package", "small-10m", "--amount", "1")
	if code != 0 {
		t.Fatalf("token exited %d", code)
	}
	_, err = ordertoken.Verify(cfg.OrderTokenPublicKey, strings.TrimSpace(token), time.Now())
	if err != nil {
		t.Errorf("token signed with BUGNET_ORDER_PRIVATE_KEY: %v", err)
	}
}

func TestPubkey(t *testing.T) {
	pub, seed := newKey(t)
	want := base64.StdEncoding.EncodeToString(pub) + "\n"
	raw, _ := base64.StdEncoding.DecodeString(seed)
	full := base64.StdEncoding.EncodeToString(ed25519.NewKeyFromSeed(raw))
	for name, args := range map[string][]string{
		"seed flag": {"pubkey", "--priv", seed},
		"full key":  {"pubkey", "--priv", full},
	} {
		if out, stderr, code := runCLI(t, env(nil), args...); code != 0 || out != want {
			t.Errorf("%s: printed %q and %q, exit %d, want %q", name, out, stderr, code, want)
		}
	}
	out, _, code := runCLI(t, env(map[string]string{"BUGNET_ORDER_PRIVATE_KEY": seed}), "pubkey")
	if code != 0 || out != want {
		t.Errorf("pubkey from the environment printed %q, exit %d, want %q", out, code, want)
	}
	if _, _, code := runCLI(t, env(nil), "pubkey"); code != 2 {
		t.Errorf("pubkey without a key exited %d, want 2", code)
	}
	if _, _, code := runCLI(t, env(nil), "pubkey", "--priv", seed, "more"); code != 2 {
		t.Errorf("pubkey with an extra argument exited %d, want 2", code)
	}
	if _, stderr, code := runCLI(t, env(nil), "pubkey", "--priv", "c2hvcnQ="); code != 1 ||
		strings.Contains(stderr, "c2hvcnQ=") {
		t.Errorf("pubkey with a bad key exited %d with %q, want 1 without the key", code, stderr)
	}
}

func TestAPIURL(t *testing.T) {
	for addr, want := range map[string]string{
		":8080":          "http://127.0.0.1:8080",
		"0.0.0.0:9000":   "http://127.0.0.1:9000",
		"[::]:8080":      "http://127.0.0.1:8080",
		"127.0.0.1:8080": "http://127.0.0.1:8080",
		"localhost:8081": "http://localhost:8081",
		"[::1]:8080":     "http://[::1]:8080",
		"10.0.0.5:80":    "http://10.0.0.5:80",
	} {
		if got, err := apiURL(addr); err != nil || got != want {
			t.Errorf("%s gave %q, %v, want %q", addr, got, err, want)
		}
	}
	for _, addr := range []string{"8080", "", "host"} {
		if got, err := apiURL(addr); err == nil || !strings.Contains(err.Error(), "listen_addr") {
			t.Errorf("%q gave %q, %v, want an error naming api.listen_addr", addr, got, err)
		}
	}
}

func TestSettingsReadOnlyWhenNeeded(t *testing.T) {
	_, priv := newKey(t)
	missing := filepath.Join(t.TempDir(), "absent.toml")
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": strings.Repeat("t", 32)})
	for name, args := range map[string][]string{
		"keys":         {"keys"},
		"pubkey":       {"pubkey", "--priv", priv},
		"priced token": {"token", "--priv", priv, "--package", "p", "--amount", "1"},
		"help":         {"help"},
	} {
		args := append([]string{"--config", missing}, args...)
		if _, stderr, code := runCLI(t, vars, args...); code != 0 {
			t.Errorf("%s with a missing settings file exited %d: %s", name, code, stderr)
		}
	}
	for name, args := range map[string][]string{
		"get":            {"get", "--handle", "sbx_x"},
		"stop":           {"stop", "--handle", "sbx_x"},
		"watch":          {"watch", "--handle", "sbx_x", "--job", "j"},
		"exec":           {"exec", "--handle", "sbx_x", "--", "true"},
		"check-extend":   {"check-extend", "--handle", "sbx_x", "--package", "p"},
		"extensions":     {"extensions", "--handle", "sbx_x"},
		"unpriced token": {"token", "--priv", priv, "--package", "p"},
		"signed create":  {"create", "--package", "p"},
	} {
		args := append([]string{"--config", missing}, args...)
		withKeys := env(map[string]string{
			"BUGNET_INTERNAL_TOKEN": strings.Repeat("t", 32), "BUGNET_ORDER_PRIVATE_KEY": priv,
		})
		if _, stderr, code := runCLI(t, withKeys, args...); code != 1 ||
			!strings.Contains(stderr, missing) {
			t.Errorf("%s with a missing settings file exited %d with %q, want 1 naming it",
				name, code, stderr)
		}
	}
	bad := writeSettings(t, "[api]\nlisten_addr = \"8080\"\n")
	_, stderr, code := runCLI(t, vars, "--config", bad, "get", "--handle", "sbx_x")
	if code != 1 || !strings.Contains(stderr, "api.listen_addr") {
		t.Errorf("get with a listen address without a port exited %d with %q", code, stderr)
	}
}

func TestTokenStartWithFlags(t *testing.T) {
	pub, priv := newKey(t)
	out, stderr, code := runCLI(t, env(nil), "token", "--priv", priv, "--kind", "start",
		"--package", "small-10m", "--order", "order-1", "--amount", "123", "--ttl", "2m")
	if code != 0 {
		t.Fatalf("token exited %d: %s", code, stderr)
	}
	c, err := ordertoken.Verify(pub, strings.TrimSpace(out), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if c.OrderID != "order-1" || c.Kind != ordertoken.KindStart ||
		c.PackageID != "small-10m" || c.AmountMicros != 123 || c.HandleSHA256 != "" {
		t.Errorf("claims %+v", c)
	}
	if left := time.Until(c.ExpiresAt); left < time.Minute || left > 2*time.Minute {
		t.Errorf("token expires in %s, want about 2m", left)
	}
}

func TestTokenExtendPricedFromSettings(t *testing.T) {
	pub, priv := newKey(t)
	packages := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(packages, []byte(catalogJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	settings := writeSettings(t, `packages_file = "`+packages+`"`)
	h, sum := handle.New()
	out, stderr, code := runCLI(t, env(map[string]string{"BUGNET_ORDER_PRIVATE_KEY": priv}),
		"--config", settings, "token", "--kind", "extend", "--package", "small-10m-more",
		"--handle", h)
	if code != 0 {
		t.Fatalf("token exited %d: %s", code, stderr)
	}
	c, err := ordertoken.Verify(pub, strings.TrimSpace(out), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if c.Kind != ordertoken.KindExtend || c.AmountMicros != 45000 ||
		c.HandleSHA256 != hex.EncodeToString(sum) || c.OrderID == "" {
		t.Errorf("claims %+v, want the extension price, the handle hash and an order id", c)
	}
	if left := time.Until(c.ExpiresAt); left < 9*time.Minute || left > 10*time.Minute {
		t.Errorf("token expires in %s, want about 10m", left)
	}
}

func TestTokenRefusesBadArguments(t *testing.T) {
	_, priv := newKey(t)
	h, _ := handle.New()
	noCatalog := writeSettings(t, "")
	for name, args := range map[string][]string{
		"no package":            {"--amount", "1"},
		"unknown kind":          {"--kind", "renew", "--package", "p", "--amount", "1"},
		"extend without handle": {"--kind", "extend", "--package", "p", "--amount", "1"},
		"handle on a start":     {"--package", "p", "--amount", "1", "--handle", h},
		"no amount or catalog":  {"--package", "p"},
		"extra argument":        {"--package", "p", "--amount", "1", "more"},
	} {
		args := append([]string{"--config", noCatalog, "token", "--priv", priv}, args...)
		if _, _, code := runCLI(t, env(nil), args...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
	if _, _, code := runCLI(t, env(nil), "token", "--package", "p", "--amount", "1"); code != 2 {
		t.Errorf("no private key: exited %d, want 2", code)
	}
}

func TestUsage(t *testing.T) {
	if _, stderr, code := runCLI(t, env(nil), "launch"); code != 2 ||
		!strings.Contains(stderr, "usage") {
		t.Errorf("unknown command exited %d with %q, want 2 and the usage", code, stderr)
	}
	if _, _, code := runCLI(t, env(nil)); code != 2 {
		t.Errorf("no command exited %d, want 2", code)
	}
	for _, args := range [][]string{{"help"}, {"--help"}, {"-h"}, {"--config", "x.toml", "-h"}} {
		out, _, code := runCLI(t, env(nil), args...)
		if code != 0 || !strings.Contains(out, "usage: bugnet-dev [--config FILE]") {
			t.Errorf("%q exited %d with %q, want 0 and the usage", args, code, out)
		}
	}
	for _, args := range [][]string{{"--config"}, {"--bogus", "keys"}, {"--config", "x.toml"}} {
		if _, stderr, code := runCLI(t, env(nil), args...); code != 2 ||
			!strings.Contains(stderr, "usage") {
			t.Errorf("%q exited %d with %q, want 2 and the usage", args, code, stderr)
		}
	}
}

func TestResumeHintNamesTheSettingsFile(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "my settings")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "other.toml")
	if err := os.WriteFile(path, []byte("[api]\nlisten_addr = \""+addr+"\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": strings.Repeat("t", 32)})
	_, stderr, code := runCLI(t, vars, "--config", path, "watch", "--handle", "sbx_x", "--job", "j",
		"--cursor", "c1")
	want := "resume with: bugnet-dev --config '" + path + "' watch --handle sbx_x --job j " +
		"--cursor c1"
	if code != 1 || !strings.Contains(stderr, want) {
		t.Errorf("watch on a closed API exited %d with %q, want 1 and %q", code, stderr, want)
	}

	broken := connect.NewError(connect.CodeUnavailable, errors.New("stream broke"))
	for configPath, want := range map[string]string{
		defaultConfig:      "resume with: bugnet-dev watch --handle sbx_x --job j\n",
		"bin/other.toml":   "resume with: bugnet-dev --config bin/other.toml watch --handle sbx_x",
		"it's here.toml":   `resume with: bugnet-dev --config 'it'\''s here.toml' watch`,
		"/srv/bn/a$b.toml": `resume with: bugnet-dev --config '/srv/bn/a$b.toml' watch`,
	} {
		c := &cli{configPath: configPath}
		got := c.resumable(broken, "sbx_x", "j", "").Error() + "\n"
		if !strings.Contains(got, want) {
			t.Errorf("settings file %q gave %q, want %q", configPath, got, want)
		}
	}
	ended := connect.NewError(connect.CodeAborted, errors.New("job lost"))
	if got := (&cli{configPath: "x.toml"}).resumable(ended, "sbx_x", "j", "c"); got != ended {
		t.Errorf("an ended job gave %v, want the error alone", got)
	}
}

// checkRouter answers CheckExtendPlacement with answer, or err when set, and records each call
type checkRouter struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	answer *routerv1.CheckExtendPlacementResponse
	err    error
	got    chan *connect.Request[routerv1.CheckExtendPlacementRequest]
}

func (r *checkRouter) CheckExtendPlacement(
	_ context.Context, req *connect.Request[routerv1.CheckExtendPlacementRequest],
) (*connect.Response[routerv1.CheckExtendPlacementResponse], error) {
	r.got <- req
	if r.err != nil {
		return nil, r.err
	}
	return connect.NewResponse(r.answer), nil
}

// serve runs h over h2c and returns a settings file pointing api.listen_addr at it
func serve(t *testing.T, h routerv1connect.SandboxRouterServiceHandler) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(routerv1connect.NewSandboxRouterServiceHandler(h))
	srv := httptest.NewUnstartedServer(mux)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)
	return writeSettings(t, "[api]\nlisten_addr = \""+srv.Listener.Addr().String()+"\"\n")
}

func TestCheckExtend(t *testing.T) {
	created := time.Date(2026, 11, 5, 1, 25, 34, 268894000, time.UTC)
	ready := created.Add(8925 * time.Microsecond)
	r := &checkRouter{
		answer: &routerv1.CheckExtendPlacementResponse{
			Placement: &routerv1.Placement{
				PlacementId: "5bef8fc5", State: routerv1.PlacementState_PLACEMENT_STATE_READY,
				PackageId: "small-10m", CreatedAt: timestamppb.New(created),
				ReadyAt:  timestamppb.New(ready),
				Deadline: timestamppb.New(ready.Add(10 * time.Minute)),
			},
			Deadline: timestamppb.New(ready.Add(20 * time.Minute)),
			Package: &routerv1.PackageOption{
				PackageId: "small-10m-more", Minutes: 10, PriceMicros: 45000, Available: true,
			},
		},
		got: make(chan *connect.Request[routerv1.CheckExtendPlacementRequest], 1),
	}
	settings := serve(t, r)
	token := strings.Repeat("t", 32)
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": token})
	out, stderr, code := runCLI(t, vars, "--config", settings, "check-extend", "--handle", "sbx_x",
		"--package", "small-10m-more")
	want := "placement_id=5bef8fc5\nstate=ready\npackage_id=small-10m\n" +
		"created_at=2026-11-05T01:25:34.268894Z\nready_at=2026-11-05T01:25:34.277819Z\n" +
		"deadline=2026-11-05T01:35:34.277819Z\ndeadline_after=2026-11-05T01:45:34.277819Z\n" +
		"price_micros=45000\n"
	if code != 0 || out != want {
		t.Fatalf("check-extend exited %d with %q and %q, want 0 and %q", code, out, stderr, want)
	}
	req := <-r.got
	if req.Msg.GetHandle() != "sbx_x" || req.Msg.GetPackageId() != "small-10m-more" ||
		req.Header().Get("Authorization") != "Bearer "+token {
		t.Errorf("router got %v with authorization %q", req.Msg, req.Header().Get("Authorization"))
	}

	refusing := serve(t, &checkRouter{
		err: connect.NewError(connect.CodeFailedPrecondition, errors.New("placement is placing")),
		got: make(chan *connect.Request[routerv1.CheckExtendPlacementRequest], 1),
	})
	out, stderr, code = runCLI(t, vars, "--config", refusing, "check-extend", "--handle", "sbx_x",
		"--package", "small-10m-more")
	if code != 1 || out != "" || !strings.Contains(stderr, "placement is placing") {
		t.Errorf("refused check-extend exited %d with %q and %q, want 1 and the refusal", code,
			out, stderr)
	}

	for name, args := range map[string][]string{
		"no handle":      {"--package", "small-10m-more"},
		"no package":     {"--handle", "sbx_x"},
		"extra argument": {"--handle", "sbx_x", "--package", "p", "more"},
	} {
		args := append([]string{"--config", settings, "check-extend"}, args...)
		if _, _, code := runCLI(t, vars, args...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
	line := "\n  check-extend --handle H --package P    print a placement and an extension's " +
		"deadline and price\n"
	if help, _, code := runCLI(t, env(nil), "help"); code != 0 || !strings.Contains(help, line) {
		t.Errorf("help exited %d with %q, want the line %q", code, help, line)
	}
}

// listRouter answers ListExtensions with answer, or err when set, and records each call
type listRouter struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	answer *routerv1.ListExtensionsResponse
	err    error
	got    chan *connect.Request[routerv1.ListExtensionsRequest]
}

func (r *listRouter) ListExtensions(
	_ context.Context, req *connect.Request[routerv1.ListExtensionsRequest],
) (*connect.Response[routerv1.ListExtensionsResponse], error) {
	r.got <- req
	if r.err != nil {
		return nil, r.err
	}
	return connect.NewResponse(r.answer), nil
}

func TestExtensions(t *testing.T) {
	r := &listRouter{
		answer: &routerv1.ListExtensionsResponse{Options: []*routerv1.PackageOption{
			{PackageId: "small-10m-more", Minutes: 10, PriceMicros: 45000, Available: true},
			{PackageId: "small-30m-more", Minutes: 30, PriceMicros: 120000,
				UnavailableReason: "exceeds_lifetime"},
		}},
		got: make(chan *connect.Request[routerv1.ListExtensionsRequest], 1),
	}
	settings := serve(t, r)
	token := strings.Repeat("t", 32)
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": token})
	out, stderr, code := runCLI(t, vars, "--config", settings, "extensions", "--handle", "sbx_x")
	want := "small-10m-more minutes=10 price_micros=45000 available=true reason=\n" +
		"small-30m-more minutes=30 price_micros=120000 available=false reason=exceeds_lifetime\n"
	if code != 0 || out != want {
		t.Fatalf("extensions exited %d with %q and %q, want 0 and %q", code, out, stderr, want)
	}
	req := <-r.got
	if req.Msg.GetHandle() != "sbx_x" || req.Header().Get("Authorization") != "Bearer "+token {
		t.Errorf("router got %v with authorization %q", req.Msg, req.Header().Get("Authorization"))
	}

	refusing := serve(t, &listRouter{
		err: connect.NewError(connect.CodeNotFound, errors.New("placement not found")),
		got: make(chan *connect.Request[routerv1.ListExtensionsRequest], 1),
	})
	out, stderr, code = runCLI(t, vars, "--config", refusing, "extensions", "--handle", "sbx_x")
	if code != 1 || out != "" || !strings.Contains(stderr, "placement not found") {
		t.Errorf("refused extensions exited %d with %q and %q, want 1 and the refusal", code, out,
			stderr)
	}

	for name, args := range map[string][]string{
		"no handle":      {},
		"extra argument": {"--handle", "sbx_x", "more"},
	} {
		args := append([]string{"--config", settings, "extensions"}, args...)
		if _, _, code := runCLI(t, vars, args...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
	if help, _, code := runCLI(t, env(nil), "help"); code != 0 ||
		!strings.Contains(help, "\n  extensions --handle H ") {
		t.Errorf("help exited %d with %q, want extensions listed", code, help)
	}
}
