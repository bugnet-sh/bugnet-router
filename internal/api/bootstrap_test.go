package api_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/bootstrap"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/netguard"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	cloneSHA   = "0123456789abcdef0123456789abcdef01234567"
	fetchToken = "f00d0123456789abcdef0123456789abcdef"
)

// gitRepo makes a real repository with one commit and returns its directory and commit
func gitRepo(t *testing.T) (dir, sha string) {
	t.Helper()
	dir = filepath.Join(t.TempDir(), "acme", "app.git")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Skipf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hi\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-q", "-m", "first")
	return dir, run("rev-parse", "HEAD")
}

// routeGitHub points the sandbox's git at dir for https://github.com/acme/app.git
func (e *env) routeGitHub(t *testing.T, h, dir string) {
	t.Helper()
	conf := "[url \"file://" + dir + "\"]\n\tinsteadOf = https://github.com/acme/app.git\n"
	_, err := e.client.WriteFile(t.Context(), connect.NewRequest(&routerv1.WriteFileRequest{
		Handle: h, Path: "/root/.gitconfig", Data: []byte(conf)}))
	eq(t, err, nil)
}

// setSource gives the placement a GitHub source as an estimate would
func (e *env) setSource(t *testing.T, p *routerv1.Placement, rev string, read bool, unread string) {
	t.Helper()
	e.setRepo(t, p, "app", rev, read, unread)
}

func (e *env) setRepo(
	t *testing.T, p *routerv1.Placement, repo, rev string, read bool, unread string,
) {
	t.Helper()
	_, err := e.pool.Exec(t.Context(), `
		update placements set source_kind = 'github', source_canonical = 'github.com/acme/' || $5,
			source_ref = 'main', source_revision = $2, source_read = $3, source_unread = $4
		where id = $1`, uuid.MustParse(p.GetPlacementId()), nullable(rev), read, unread, repo)
	if err != nil {
		t.Fatal(err)
	}
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (e *env) fetchSource(t *testing.T, h string) (*routerv1.Bootstrap, error) {
	t.Helper()
	resp, err := e.client.FetchSource(t.Context(), connect.NewRequest(
		&routerv1.FetchSourceRequest{Handle: h}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetBootstrap(), nil
}

func (e *env) mustFetch(t *testing.T, h string) *routerv1.Bootstrap {
	t.Helper()
	b, err := e.fetchSource(t, h)
	if err != nil {
		t.Fatalf("FetchSource: %v", err)
	}
	return b
}

// bootstrapIs waits until GetPlacement shows the state
func (e *env) bootstrapIs(
	t *testing.T, h string, want routerv1.BootstrapState,
) *routerv1.Bootstrap {
	t.Helper()
	var got *routerv1.Bootstrap
	for range 200 {
		got = e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED, 0).GetBootstrap()
		if got.GetState() == want {
			return got
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("bootstrap is %v (%s), want %v", got.GetState(), got.GetFailureReason(), want)
	return nil
}

func (e *env) jobKey(t *testing.T, id string) string {
	t.Helper()
	var key string
	err := e.pool.QueryRow(t.Context(), `select coalesce(idempotency_key, '') from jobs
		where id = $1`, id).Scan(&key)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestFetchSourcePublic(t *testing.T) {
	dir, sha := gitRepo(t)
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.routeGitHub(t, h, dir)
	e.setSource(t, p, sha, true, "")
	b := e.mustFetch(t, h)
	eq(t, b.GetState(), routerv1.BootstrapState_BOOTSTRAP_STATE_RUNNING)
	eq(t, b.GetPath(), "~/src/app")
	eq(t, e.jobKey(t, b.GetJobId()), "_bootstrap:1")
	done := e.bootstrapIs(t, h, routerv1.BootstrapState_BOOTSTRAP_STATE_DONE)
	eq(t, done.GetRevision(), sha)
	out := e.exec(t, sh(h, "cat ~/src/app/hello.txt"))
	eq(t, string(out.GetStdoutTail()), "hi\n")
}

func TestFailedCloneRunsAgainUnderANewKey(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.setSource(t, p, "", true, "")
	first := e.mustFetch(t, h)
	failed := e.bootstrapIs(t, h, routerv1.BootstrapState_BOOTSTRAP_STATE_FAILED)
	eq(t, failed.GetFailureReason(), "CLONE_FAILED")
	again := e.mustFetch(t, h)
	eq(t, e.jobKey(t, first.GetJobId()), "_bootstrap:1")
	eq(t, e.jobKey(t, again.GetJobId()), "_bootstrap:2")
}

func TestExecRefusesReservedKeys(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	req := sh(h, "true")
	req.IdempotencyKey = "_x"
	_, err := e.client.Exec(t.Context(), connect.NewRequest(req))
	mustCode(t, err, connect.CodeInvalidArgument, "IDEMPOTENCY_KEY_RESERVED")
}

func TestNoSourceAndNeedsSecret(t *testing.T) {
	e := newEnv(t, options{fetchToken: fetchToken})
	h, p := e.readyPlacement(t)
	_, err := e.fetchSource(t, h)
	mustCode(t, err, connect.CodeFailedPrecondition, "NO_SOURCE")
	e.setSource(t, p, "", false, "not_found")
	_, err = e.fetchSource(t, h)
	mustCode(t, err, connect.CodeFailedPrecondition, "SECRET_REQUIRED")
	got := e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED, 0)
	eq(t, got.GetBootstrap().GetState(), routerv1.BootstrapState_BOOTSTRAP_STATE_NEEDS_SECRET)
}

func TestToolMissing(t *testing.T) {
	e := newEnv(t, options{images: map[string]config.ImageTools{"local": {Git: "none"}}})
	h, p := e.readyPlacement(t)
	e.setSource(t, p, "", true, "")
	_, err := e.fetchSource(t, h)
	mustCode(t, err, connect.CodeFailedPrecondition, "TOOL_MISSING")
}

func TestDotDotRepositoryRefusedBeforeAnyRequest(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.setSource(t, p, "", true, "")
	if _, err := e.pool.Exec(t.Context(), `update placements set
		source_canonical = 'github.com/acme/..'`); err != nil {
		t.Fatal(err)
	}
	b := e.mustFetch(t, h)
	eq(t, []any{b.GetState(), b.GetFailureReason(), b.GetJobId()},
		[]any{routerv1.BootstrapState_BOOTSTRAP_STATE_FAILED, "FETCH_FAILED", ""})
}

func TestStaleHeartbeatReadsInterrupted(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.setSource(t, p, "", false, "not_found")
	if _, err := e.pool.Exec(t.Context(), `update placements set bootstrap_state = 'private',
		bootstrap_heartbeat = now() - interval '5 minutes',
		bootstrap_private_attempts = 1`); err != nil {
		t.Fatal(err)
	}
	got := e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED, 0).GetBootstrap()
	eq(t, []any{got.GetState(), got.GetFailureReason()},
		[]any{routerv1.BootstrapState_BOOTSTRAP_STATE_FAILED, "INTERRUPTED"})
}

/* The private fetch, through a fake of the egress proxy and of GitHub */

// certPair makes a certificate for host signed by ca, or a self-signed one when ca is nil
func certPair(t *testing.T, host string, ca *tls.Certificate) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: host},
		DNSNames: []string{host}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true, IsCA: ca == nil,
	}
	if ca == nil {
		tmpl.KeyUsage |= x509.KeyUsageCertSign
	}
	parent, signer := tmpl, any(key)
	if ca != nil {
		parent, signer = ca.Leaf, ca.PrivateKey
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, signer)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func pemFile(t *testing.T, c tls.Certificate) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "cert.pem")
	b := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Certificate[0]})
	if err := os.WriteFile(f, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

// tarball is a GitHub archive of one file, its commit in the pax header and the top directory
func tarball(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	body := bytes.Repeat([]byte("private\n"), 1000)
	for _, h := range []*tar.Header{
		{Typeflag: tar.TypeXGlobalHeader, Name: "pax_global_header",
			PAXRecords: map[string]string{"comment": cloneSHA}},
		{Typeflag: tar.TypeDir, Name: "acme-priv-0123456/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "acme-priv-0123456/secret.txt", Mode: 0o644,
			Size: int64(len(body))},
	} {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
	}
	_, _ = tw.Write(body)
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// quiet drops the fake servers' own log lines, which a refused handshake fills
var quiet = log.New(io.Discard, "", 0)

// oneConn serves a single connection to an http.Server
type oneConn struct {
	c    chan net.Conn
	done chan struct{}
	once sync.Once
}

func newOneConn(c net.Conn) *oneConn {
	l := &oneConn{c: make(chan net.Conn, 1), done: make(chan struct{})}
	l.c <- c
	return l
}

func (l *oneConn) Accept() (net.Conn, error) {
	select {
	case c := <-l.c:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *oneConn) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *oneConn) Addr() net.Addr { return &net.TCPAddr{} }

// proxyFake speaks the fetcher's side of DP 10.5 and 10.7: PROXY v2, outer TLS, the tunnel
// upgrade, inner TLS for api.github.com and one GitHub tarball endpoint
type proxyFake struct {
	addr        string
	outerRoots  string
	caFile      string
	placeholder func() string
	location    func(codeload string) string
	hold        chan struct{}
	started     chan struct{}
	headers     chan http.Header
	up          bootstrap.Upstream
}

func newProxyFake(t *testing.T, tb []byte, placementID func() string) *proxyFake {
	t.Helper()
	outer := certPair(t, egressHost, nil)
	ca := certPair(t, "Bugnet test CA", nil)
	inner := certPair(t, "api.github.com", &ca)
	serveTar := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(tb) }
	codeload := httptest.NewTLSServer(http.HandlerFunc(serveTar))
	t.Cleanup(codeload.Close)
	f := &proxyFake{
		outerRoots: pemFile(t, outer), caFile: pemFile(t, ca),
		headers: make(chan http.Header, 8), started: make(chan struct{}, 8),
	}
	pool := x509.NewCertPool()
	pool.AddCert(codeload.Certificate())
	f.up = bootstrap.Upstream{
		CodeloadURL: codeload.URL + "/", CodeloadRoots: pool,
		Guard: netguard.Dialer{Refuses: func(netip.Addr) bool { return false }},
	}
	f.location = func(c string) string { return c + "acme/priv/legacy.tar.gz/main" }
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.headers <- r.Header.Clone()
		if r.Header.Get("Authorization") != "Bearer "+f.placeholder() ||
			!strings.HasPrefix(r.URL.Path, "/repos/acme/priv/tarball") {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, f.location(codeload.URL+"/"), http.StatusFound)
	})
	tunnel := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer bnf_"+fetchToken ||
			r.Header.Get("Bugnet-Placement") != placementID() {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{"bugnet-tunnel.v1"}})
		if err != nil {
			return
		}
		f.started <- struct{}{}
		if f.hold != nil {
			<-f.hold
		}
		nc := websocket.NetConn(r.Context(), ws, websocket.MessageBinary)
		tc := tls.Server(nc, &tls.Config{Certificates: []tls.Certificate{inner}})
		srv := &http.Server{Handler: api, ErrorLog: quiet}
		l := newOneConn(tc)
		go func() { time.Sleep(10 * time.Second); _ = l.Close() }()
		_ = srv.Serve(l)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	f.addr = ln.Addr().String()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				var hdr [16]byte
				if _, err := io.ReadFull(c, hdr[:]); err != nil {
					return
				}
				rest := make([]byte, binary.BigEndian.Uint16(hdr[14:]))
				if _, err := io.ReadFull(c, rest); err != nil {
					return
				}
				tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{outer}})
				srv := &http.Server{Handler: http.HandlerFunc(tunnel), ErrorLog: quiet}
				_ = srv.Serve(newOneConn(tc))
			}()
		}
	}()
	return f
}

// privateEnv is an environment with a private source, a GitHub secret and the fake proxy
type privateEnv struct {
	*env
	h     string
	p     *routerv1.Placement
	id    string
	proxy *proxyFake
}

func newPrivate(t *testing.T, tune func(*options)) *privateEnv {
	t.Helper()
	pe := &privateEnv{}
	pe.proxy = newProxyFake(t, tarball(t), func() string { return pe.id })
	opts := options{
		fetchToken:     fetchToken,
		fetch:          config.Fetch{MaxBytes: 1 << 20, EgressRootsFile: pe.proxy.outerRoots},
		secretSettings: func(c *config.Secrets) { c.CACertFile = pe.proxy.caFile },
		upstream:       pe.proxy.up,
	}
	if tune != nil {
		tune(&opts)
	}
	opts.fetch.EgressAddr = pe.proxy.addr
	pe.env = newEnv(t, opts)
	pe.h, pe.p = pe.readyPlacement(t)
	pe.id = pe.p.GetPlacementId()
	pe.setRepo(t, pe.p, "priv", "", false, "not_found")
	row, err := pe.store.PlacementByID(t.Context(), uuid.MustParse(pe.id))
	if err != nil {
		t.Fatal(err)
	}
	keyID := cmpKey(row.KeyID, pe.dpKeys.Current().ID)
	ph, err := pe.dpKeys.Placeholder(keyID, row.ID, "GITHUB_TOKEN")
	if err != nil {
		t.Fatal(err)
	}
	pe.proxy.placeholder = func() string { return ph }
	sum := sha256.Sum256([]byte(ph))
	if _, _, err := pe.store.PutSecret(t.Context(), store.Secret{
		ID: uuid.New(), PlacementID: row.ID, Name: "GITHUB_TOKEN", FetchOnly: true,
		Hosts: []string{"api.github.com"}, PlaceholderSHA256: sum[:], Sealed: []byte("box"),
		KeyID: "k2", Source: store.SourceGateway, State: store.SecretActive,
	}); err != nil {
		t.Fatal(err)
	}
	return pe
}

func cmpKey(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (pe *privateEnv) egressUsed(t *testing.T) int64 {
	t.Helper()
	n, err := pe.vk.Do(t.Context(), pe.vk.B().Get().Key(pe.prefix+"egress:used:"+pe.id).Build()).
		AsInt64()
	if err != nil {
		t.Fatalf("read the egress counter: %v", err)
	}
	return n
}

func TestFetchSourcePrivate(t *testing.T) {
	pe := newPrivate(t, nil)
	b := pe.mustFetch(t, pe.h)
	eq(t, b.GetState(), routerv1.BootstrapState_BOOTSTRAP_STATE_RUNNING)
	eq(t, b.GetPath(), "~/src/priv")
	done := pe.bootstrapIs(t, pe.h, routerv1.BootstrapState_BOOTSTRAP_STATE_DONE)
	eq(t, done.GetRevision(), "0123456")
	out := pe.exec(t, sh(pe.h, "wc -c < ~/src/priv/secret.txt; ls ~/.bugnet"))
	// BSD wc pads the count and GNU wc does not
	eq(t, strings.TrimLeft(string(out.GetStdoutTail()), " "), "8000\n")
	// Every streamed byte is in the counter
	if got := pe.egressUsed(t); got < 100 {
		t.Fatalf("egress counter is %d", got)
	}
}

func TestFetchOuterTrustSetting(t *testing.T) {
	pe := newPrivate(t, func(o *options) { o.fetch.EgressRootsFile = "" })
	pe.mustFetch(t, pe.h)
	failed := pe.bootstrapIs(t, pe.h, routerv1.BootstrapState_BOOTSTRAP_STATE_FAILED)
	eq(t, failed.GetFailureReason(), "FETCH_FAILED")
}

func TestRedirectElsewhereFails(t *testing.T) {
	pe := newPrivate(t, nil)
	pe.proxy.location = func(string) string { return "https://evil.example/x.tar.gz" }
	pe.mustFetch(t, pe.h)
	failed := pe.bootstrapIs(t, pe.h, routerv1.BootstrapState_BOOTSTRAP_STATE_FAILED)
	eq(t, failed.GetFailureReason(), "FETCH_FAILED")
}

func TestThirdPrivateFetchRefused(t *testing.T) {
	pe := newPrivate(t, nil)
	if _, err := pe.pool.Exec(t.Context(), `update placements set bootstrap_state = 'failed',
		bootstrap_reason = 'FETCH_FAILED', bootstrap_private_attempts = 2`); err != nil {
		t.Fatal(err)
	}
	b := pe.mustFetch(t, pe.h)
	eq(t, []any{b.GetState(), b.GetFailureReason()},
		[]any{routerv1.BootstrapState_BOOTSTRAP_STATE_FAILED, "ATTEMPTS_SPENT"})
}

func TestSpentEgressBudget(t *testing.T) {
	pe := newPrivate(t, nil)
	err := pe.vk.Do(t.Context(), pe.vk.B().Set().Key(pe.prefix+"egress:spent:"+pe.id).
		Value("1").Build()).Error()
	eq(t, err, nil)
	b := pe.mustFetch(t, pe.h)
	eq(t, []any{b.GetState(), b.GetFailureReason(), b.GetJobId()},
		[]any{routerv1.BootstrapState_BOOTSTRAP_STATE_FAILED, "BUDGET_SPENT", ""})
}

func TestFifthConcurrentPrivateFetch(t *testing.T) {
	pe := newPrivate(t, func(o *options) { o.bootstrap = config.Bootstrap{MaxConcurrent: 1} })
	pe.proxy.hold = make(chan struct{})
	defer close(pe.proxy.hold)
	pe.mustFetch(t, pe.h)
	<-pe.proxy.started
	// A second placement of this replica asks while the first holds its slot
	h2, p2 := pe.readyPlacement(t)
	pe.setRepo(t, p2, "priv", "", false, "not_found")
	pid2 := uuid.MustParse(p2.GetPlacementId())
	if _, _, err := pe.store.PutSecret(t.Context(), store.Secret{
		ID: uuid.New(), PlacementID: pid2, Name: "GITHUB_TOKEN", FetchOnly: true,
		Hosts: []string{"api.github.com"}, PlaceholderSHA256: make([]byte, 32),
		Sealed: []byte("box"), KeyID: "k2", Source: store.SourceGateway,
		State: store.SecretActive,
	}); err != nil {
		t.Fatal(err)
	}
	_, err := pe.fetchSource(t, h2)
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
	if d := wantRetry(t, err); d != 10*time.Second {
		t.Fatalf("retry after %v", d)
	}
}
