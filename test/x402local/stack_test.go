// Package x402local runs the x402 local run, XS 11.2, on this machine.
//
// make x402-local builds router-api, router-worker and bugnet-egress with -tags localrun, then
// runs this package with BUGNET_TEST_X402_LOCAL=1. A stack is the real router-api and
// router-worker on a local-provider sandbox, the real bugnet-egress, the real Payments service on
// a fake chain, the fake seller and the sandbox's bugnet-run forwarder. Nothing is mocked.
package x402local

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"

	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

const (
	enableVar = "BUGNET_TEST_X402_LOCAL"
	binVar    = "BUGNET_X402_LOCAL_BIN"
	readyWait = 60 * time.Second
	stopGrace = 8 * time.Second
	// Seller names sit under x402.test, the egress host under a name of its own so that no
	// seller is a Bugnet name
	sellerHost = "seller.x402.test"
	otherHost  = "other.x402.test"
	egressHost = "egress.bugnet-local.test"
	// packageID is the dev catalog's 10 minute package, which costs $0.05
	packageID     = "dev-small-10m"
	packageMicros = 50_000
	feeMicros     = 3_000
)

// repo is the bugnet-router checkout, found from this file's own path
func repo(t testing.TB) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// paymentsDir is the bugnet-payments checkout, BUGNET_PAYMENTS_DIR or the one beside the router
func paymentsDir(t testing.TB) string {
	t.Helper()
	if d := os.Getenv("BUGNET_PAYMENTS_DIR"); d != "" {
		return d
	}
	for _, rel := range []string{"..", filepath.Join("..", "..", "..")} {
		d := filepath.Join(repo(t), rel, "bugnet-payments")
		if _, err := os.Stat(filepath.Join(d, "package.json")); err == nil {
			return d
		}
	}
	t.Fatal("x402local: no bugnet-payments checkout, set BUGNET_PAYMENTS_DIR")
	return ""
}

// stack is one running x402 local run
type stack struct {
	t        *testing.T
	root     string // the router checkout
	bin      string
	dir      string
	keys     map[string]string
	env      []string
	settings string
	apiURL   string
	pki      string // the dev mTLS files, which are egress' credential directory too
	// sellerCA signs the sellers' certificate and is egress' upstream_roots_file
	sellerCA     string
	sellerAddr   string
	sellerPayTo  string
	sellerATA    string
	sellerToken  string
	payments     paymentsControl
	egressCAFile string // the CA the sandbox's code trusts, Bugnet's own
	outerCert    string // egress' self-signed outer certificate, which the forwarder trusts
	egressAddr   string
	fwdListen    string
	procs        []*proc
	stopOnce     sync.Once
	n            int
	taken        map[string]bool
	// egressListen is where the proxy listens and egressAddr the front forwarders dial
	egressListen string
	fronts       []net.Listener
	metrics      string
}

// paymentsControl is the harness' control address and what it printed on start
type paymentsControl struct {
	URL              string `json:"control"`
	Signer           int    `json:"signer"`
	OrderPublicKey   string `json:"orderPublicKey"`
	Chain            string `json:"chain"`
	FeePayer         string `json:"feePayer"`
	Payer            string `json:"payer"`
	Internal         string `json:"internal"`
	paymentsToken    string
	paymentsTokenKey string
}

// proc is one running binary of the stack
type proc struct {
	name string
	cmd  *exec.Cmd
	log  string
	done chan struct{}
	err  error
}

// gate skips the run unless make x402-local asked for it
func gate(t *testing.T) {
	t.Helper()
	if os.Getenv(enableVar) != "1" {
		t.Skipf("set %s=1, make x402-local does", enableVar)
	}
}

// startStack builds nothing: make x402-local made the binaries
func startStack(t *testing.T) *stack {
	t.Helper()
	bin := os.Getenv(binVar)
	if bin == "" {
		t.Fatalf("x402local: %s names the directory make x402-local built into", binVar)
	}
	dir, err := os.MkdirTemp("", "bugnet-x402local-")
	if err != nil {
		t.Fatal(err)
	}
	s := &stack{t: t, root: repo(t), bin: bin, dir: dir}
	t.Cleanup(func() { s.stop() })
	s.keys = s.devKeys()
	s.pki = filepath.Join(dir, "pki")
	s.run(nil, "bugnet-dev", "mtls", "init", "--dev", "--out", s.pki)
	s.makeSellerCerts()
	s.makeOuterCert()
	s.startPayments()
	s.startSeller()
	s.startRouter()
	s.startEgress()
	return s
}

/* Files */

// write puts content in the stack's directory
func (s *stack) write(name string, content []byte) string {
	s.t.Helper()
	path := filepath.Join(s.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		s.t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		s.t.Fatal(err)
	}
	return path
}

func freeAddr(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// hostEnv is the test's environment without any BUGNET_ setting
func hostEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "BUGNET_") {
			env = append(env, kv)
		}
	}
	return env
}

// devKeys runs bugnet-dev keys
func (s *stack) devKeys() map[string]string {
	s.t.Helper()
	keys := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(s.run(nil, "bugnet-dev", "keys")), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			keys[k] = v
		}
	}
	return keys
}

// run runs a built binary to its end and answers its stdout
func (s *stack) run(env []string, name string, args ...string) string {
	s.t.Helper()
	cmd := exec.Command(filepath.Join(s.bin, name), args...)
	cmd.Dir, cmd.Env = s.root, append(hostEnv(), env...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		s.t.Fatalf("x402local: %s %s: %v\n%s", name, args[0], err, stderr.String())
	}
	return string(out)
}

// selfSigned writes a certificate for names, signed by itself, and answers its PEM and key PEM
func selfSigned(t testing.TB, names ...string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: names[0]},
		DNSNames: names, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// makeSellerCerts makes the pair the fake seller serves for both seller names
func (s *stack) makeSellerCerts() {
	certPEM, keyPEM := selfSigned(s.t, sellerHost, otherHost)
	s.sellerCA = s.write("seller/cert.pem", certPEM)
	s.write("seller/key.pem", keyPEM)
}

// makeOuterCert makes the pair egress serves its tunnel on, which the forwarder is told to trust
func (s *stack) makeOuterCert() {
	certPEM, keyPEM := selfSigned(s.t, egressHost)
	s.outerCert = s.write("outer/fullchain.pem", certPEM)
	s.write("outer/key.pem", keyPEM)
}

const b58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// base58 encodes b as Solana prints keys
func base58(b []byte) string {
	n := new(big.Int).SetBytes(b)
	radix, zero, mod := big.NewInt(58), big.NewInt(0), new(big.Int)
	var out []byte
	for n.Cmp(zero) > 0 {
		n.DivMod(n, radix, mod)
		out = append(out, b58Alphabet[mod.Int64()])
	}
	for _, c := range b {
		if c != 0 {
			break
		}
		out = append(out, b58Alphabet[0])
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

func newWallet(t testing.TB) (address, secret string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return base58(pub), base58(priv)
}

/* Processes */

func (s *stack) startProc(env []string, name, bin string, args ...string) *proc {
	s.t.Helper()
	s.n++
	log, err := os.Create(filepath.Join(s.dir, fmt.Sprintf("%02d-%s.log", s.n, name)))
	if err != nil {
		s.t.Fatal(err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = s.root, env, log, log
	if err := cmd.Start(); err != nil {
		s.t.Fatalf("x402local: start %s: %v", name, err)
	}
	p := &proc{name: name, cmd: cmd, log: log.Name(), done: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		_ = log.Close()
		close(p.done)
	}()
	s.procs = append(s.procs, p)
	return p
}

// waitReady polls url until it answers 200, and fails when a process of the stack ends first
func (s *stack) waitReady(url string) {
	s.t.Helper()
	deadline := time.Now().Add(readyWait)
	for {
		for _, p := range s.procs {
			select {
			case <-p.done:
				s.fail("%s exited before the stack was ready: %v", p.name, p.err)
			default:
			}
		}
		if resp, err := http.Get(url); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			s.fail("%s was not ready within %s", url, readyWait)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *stack) fail(format string, args ...any) {
	s.t.Helper()
	s.t.Errorf("x402local: "+format, args...)
	s.stop()
	s.t.FailNow()
}

// stop ends every process with SIGTERM, then SIGKILL, and logs their tails when the test failed
func (s *stack) stop() {
	s.stopOnce.Do(func() {
		s.paymentsStop()
		for _, ln := range s.fronts {
			_ = ln.Close()
		}
		var wg sync.WaitGroup
		for _, p := range s.procs {
			wg.Go(func() {
				_ = p.cmd.Process.Signal(syscall.SIGTERM)
				select {
				case <-p.done:
				case <-time.After(stopGrace):
					_ = p.cmd.Process.Kill()
					<-p.done
				}
			})
		}
		wg.Wait()
		lp := local.New(local.Options{Name: "local", Root: filepath.Join(s.dir, "local")})
		owned, _ := lp.ListOwned(context.Background(), nil)
		for _, o := range owned {
			if os.Getenv("BUGNET_X402_LOCAL_KEEP") != "" {
				break
			}
			_ = lp.Destroy(context.Background(), o.Ref)
		}
		if s.t.Failed() {
			for _, p := range s.procs {
				raw, _ := os.ReadFile(p.log)
				lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
				s.t.Logf("last lines of %s:\n%s", p.name,
					strings.Join(lines[max(len(lines)-30, 0):], "\n"))
			}
		}
		if os.Getenv("BUGNET_X402_LOCAL_KEEP") != "" {
			s.t.Logf("x402local: kept %s", s.dir)
			return
		}
		_ = os.RemoveAll(s.dir)
	})
}

/* Payments */

// startPayments starts the real Payments service on a fake chain through the harness
func (s *stack) startPayments() {
	s.t.Helper()
	keyB64 := ""
	envAdd, err := os.ReadFile(filepath.Join(s.pki, "payments.env.add"))
	if err != nil {
		s.t.Fatal(err)
	}
	for _, line := range strings.Split(string(envAdd), "\n") {
		if v, ok := strings.CutPrefix(line, "BUGNET_X402_TLS_KEY="); ok {
			keyB64 = v
		}
	}
	s.apiURL = "http://" + freeAddr(s.t)
	in := map[string]any{
		"paymentsDir": paymentsDir(s.t), "workDir": s.dir, "routerURL": s.apiURL,
		"routerToken":    s.keys["BUGNET_PAYMENTS_ROUTER_TOKEN"],
		"eventPublicKey": s.eventPublicKey(), "orderSeed": s.keys["BUGNET_ORDER_PRIVATE_KEY"],
		"claimKey":      base64.StdEncoding.EncodeToString(randomBytes(32)),
		"paymentsToken": strings.Repeat("p", 40),
		"pki": map[string]string{
			"ca":         filepath.Join(s.pki, "internal-ca.pem"),
			"serverCert": filepath.Join(s.pki, "x402-server.pem"), "serverKeyB64": keyB64,
		},
	}
	raw, _ := json.Marshal(in)
	input := s.write("payments-input.json", raw)
	// The harness runs from Payments' own directory, with its node_modules linked beside it
	hdir := filepath.Join(s.dir, "harness")
	if err := os.MkdirAll(hdir, 0o700); err != nil {
		s.t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(paymentsDir(s.t), "node_modules"),
		filepath.Join(hdir, "node_modules")); err != nil {
		s.t.Fatal(err)
	}
	src, err := os.ReadFile(filepath.Join(s.root, "test", "x402local", "payments-harness.mts"))
	if err != nil {
		s.t.Fatal(err)
	}
	harness := filepath.Join(hdir, "harness.mts")
	if err := os.WriteFile(harness, src, 0o600); err != nil {
		s.t.Fatal(err)
	}
	tsx, err := exec.LookPath("tsx")
	if err != nil {
		s.t.Fatal("x402local: tsx is not installed, npm install -g tsx")
	}
	log, err := os.Create(filepath.Join(s.dir, "payments.log"))
	if err != nil {
		s.t.Fatal(err)
	}
	cmd := exec.Command(tsx, harness, input)
	cmd.Dir = paymentsDir(s.t)
	cmd.Env = os.Environ()
	out, err := cmd.StdoutPipe()
	if err != nil {
		s.t.Fatal(err)
	}
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		s.t.Fatal(err)
	}
	p := &proc{name: "payments", cmd: cmd, log: log.Name(), done: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	s.procs = append(s.procs, p)
	found := make(chan paymentsControl, 1)
	go func() {
		sc := bufio.NewScanner(io.TeeReader(out, log))
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var c paymentsControl
			if strings.HasPrefix(sc.Text(), `{"control"`) && json.Unmarshal(sc.Bytes(), &c) == nil {
				found <- c
			}
		}
	}()
	select {
	case s.payments = <-found:
	case <-p.done:
		s.fail("payments exited before it was ready: %v", p.err)
	case <-time.After(readyWait):
		s.fail("payments was not ready within %s", readyWait)
	}
	s.payments.paymentsToken = in["paymentsToken"].(string)
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// eventPublicKey is the public half of the router's event signing key
func (s *stack) eventPublicKey() string {
	s.t.Helper()
	out := s.run(nil, "bugnet-dev", "pubkey", "--priv", s.keys["BUGNET_EVENT_SIGNING_KEY"])
	return strings.TrimSpace(out)
}

// control calls the harness and decodes its answer into out
func (s *stack) control(path string, body, out any) {
	s.t.Helper()
	raw, _ := json.Marshal(body)
	resp, err := http.Post(s.payments.URL+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		s.t.Fatalf("x402local: %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("x402local: %s answered %d: %s", path, resp.StatusCode, b)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			s.t.Fatalf("x402local: %s: %v: %s", path, err, b)
		}
	}
}

func (s *stack) paymentsStop() {
	if s.payments.URL == "" {
		return
	}
	resp, err := http.Post(s.payments.URL+"/stop", "application/json", strings.NewReader("{}"))
	if err == nil {
		_ = resp.Body.Close()
	}
	s.payments.URL = ""
}

/* The seller */

// startSeller starts the fake seller, settling on the fake chain through its own fee payer
func (s *stack) startSeller() {
	s.t.Helper()
	payTo, _ := newWallet(s.t)
	feePayer, feeSecret := newWallet(s.t)
	var ata struct {
		ATA string `json:"ata"`
	}
	s.control("/account", map[string]string{"owner": payTo}, &ata)
	s.sellerPayTo, s.sellerATA = payTo, ata.ATA
	s.sellerAddr = freeAddr(s.t)
	cfg := fmt.Sprintf(`listen = %q
tls_cert_file = %q
tls_key_file = %q
pay_to = %q
pay_to_token_account = %q
fee_payer = %q
bugnet_fee_payer = %q
settle = "self"
chain_url = %q
late = "4s"
slow = "20s"
stall = "6s"
`, s.sellerAddr, filepath.Join(s.dir, "seller", "cert.pem"),
		filepath.Join(s.dir, "seller", "key.pem"), payTo, ata.ATA, feePayer, s.payments.FeePayer,
		s.payments.Chain)
	path := s.write("seller/seller.toml", []byte(cfg))
	s.sellerToken = rand.Text()
	env := append(hostEnv(), "TESTSELLER_FEE_PAYER_KEY="+feeSecret,
		"TESTSELLER_TEST_TOKEN="+s.sellerToken)
	s.startProc(env, "x402-fake-seller", filepath.Join(s.bin, "x402-fake-seller"), "-config", path)
	s.waitTLS(s.sellerAddr)
}

// waitTLS waits until addr accepts a connection
func (s *stack) waitTLS(addr string) {
	s.t.Helper()
	for deadline := time.Now().Add(readyWait); time.Now().Before(deadline); {
		if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.fail("%s did not accept within %s", addr, readyWait)
}

/* The router */

func table(m map[string]any, name string) map[string]any {
	if t, ok := m[name].(map[string]any); ok {
		return t
	}
	t := map[string]any{}
	m[name] = t
	return t
}

// startRouter migrates, then starts router-api and router-worker on config/dev.toml with x402 on
func (s *stack) startRouter() {
	s.t.Helper()
	// Forwarders dial the front, which hands the proxy a PROXY v2 header as HAProxy does
	s.egressListen = freeAddr(s.t)
	s.egressAddr = s.proxyFront(s.egressListen)
	s.fwdListen = "127.0.0.1:0"
	dbURL := testenv.DatabaseURL(s.t)
	_, prefix := testenv.Valkey(s.t)
	s.env = append(hostEnv(), "BUGNET_DATABASE_URL="+dbURL)
	for k, v := range s.keys {
		s.env = append(s.env, k+"="+v)
	}
	caDir := filepath.Join(s.dir, "egress-ca")
	if err := os.MkdirAll(caDir, 0o700); err != nil {
		s.t.Fatal(err)
	}
	s.run(nil, "bugnet-egress", "ca", "init", "--out", caDir)
	s.egressCAFile = filepath.Join(caDir, "egress-ca.pem")
	credDir := filepath.Join(s.pki)
	if err := os.Symlink(filepath.Join(caDir, "egress-ca.key"),
		filepath.Join(credDir, "egress_ca_key")); err != nil {
		s.t.Fatal(err)
	}
	boxPub := ""
	for _, line := range strings.Split(s.run(nil, "bugnet-egress", "box-key", "new", "--id", "k1",
		"--out", credDir), "\n") {
		if pub, ok := strings.CutPrefix(line, "public_key: "); ok {
			boxPub = pub
		}
	}
	if boxPub == "" {
		s.t.Fatal("x402local: bugnet-egress box-key new printed no public key")
	}
	migrate := exec.Command(filepath.Join(s.bin, "router-api"), "migrate")
	migrate.Env = s.env
	if out, err := migrate.CombinedOutput(); err != nil {
		s.t.Fatalf("x402local: router-api migrate: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(filepath.Join(s.root, "config", "dev.toml"))
	if err != nil {
		s.t.Fatal(err)
	}
	m := map[string]any{}
	if err := toml.Unmarshal(raw, &m); err != nil {
		s.t.Fatal(err)
	}
	m["order_token_public_key"] = s.payments.OrderPublicKey
	m["providers"] = []string{"local"}
	m["deployment_id"] = "x402local-" + strings.ToLower(rand.Text()[:8])
	apiAddr, workerAddr := strings.TrimPrefix(s.apiURL, "http://"), freeAddr(s.t)
	table(m, "api")["listen_addr"] = apiAddr
	table(m, "worker")["listen_addr"] = workerAddr
	table(m, "valkey")["addr"] = testenv.ValkeyAddr()
	table(m, "valkey")["prefix"] = prefix
	runner := filepath.Join(s.root, "..", "bugnet-run", "dist", "bugnet-run-linux-amd64")
	table(m, "runner")["linux_amd64"] = runner
	local := table(m, "local")
	local["enabled"], local["root"] = true, filepath.Join(s.dir, "local")
	local["runner"] = filepath.Join(s.bin, "bugnet-run")
	table(m, "names")["egress_host"] = egressHost
	secrets := table(m, "secrets")
	secrets["egress_ips"] = []string{"127.0.0.1"}
	secrets["ca_cert_file"], secrets["ca_version"] = s.egressCAFile, 1
	secrets["box_public_keys"] = []map[string]any{{"id": "k1", "key": boxPub}}
	secrets["providers"] = []string{"local"}
	table(m, "fetch")["egress_addr"] = s.egressAddr
	table(m, "fetch")["egress_roots_file"] = s.outerCert
	table(m, "x402")["enabled"] = true
	table(m, "x402")["max_services"] = 32
	_, port, _ := net.SplitHostPort(s.egressAddr)
	p, _ := strconv.Atoi(port)
	m["localrun"] = map[string]any{"forward_listen": s.fwdListen, "egress_port": p}
	out, err := toml.Marshal(m)
	if err != nil {
		s.t.Fatal(err)
	}
	s.settings = s.write("router.toml", out)
	s.startProc(s.env, "router-api", filepath.Join(s.bin, "router-api"), "-config", s.settings)
	s.startProc(s.env, "router-worker", filepath.Join(s.bin, "router-worker"), "-config",
		s.settings)
	s.waitReady(s.apiURL + "/readyz")
	s.waitReady("http://" + workerAddr + "/readyz")
}

/* Egress */

// startEgress starts bugnet-egress with Payments' signer, the fake seller and the seller roots
func (s *stack) startEgress() {
	s.t.Helper()
	metrics := freeAddr(s.t)
	s.metrics = metrics
	content := fmt.Sprintf(`router_url = %q
listen_addr = %q
metrics_listen_addr = %q
proxy_protocol_from = ["127.0.0.1/32"]
egress_host = %q
ca_cert_file = %q
ca_version = 1
box_keys = [{ id = "k1", credential = "egress_box_k1" }]
acme_enabled = false
static_dir = %q
[x402]
payments_url = %q
payments_ca_file = %q
payments_client_cert_file = %q
first_header_timeout = "3s"
[localrun]
upstream_roots_file = %q
[localrun.upstream_addrs]
%q = %q
%q = %q
`, s.apiURL, s.egressListen, metrics, egressHost, s.egressCAFile, filepath.Join(s.dir, "outer"),
		fmt.Sprintf("https://127.0.0.1:%d", s.payments.Signer),
		filepath.Join(s.pki, "internal-ca.pem"), filepath.Join(s.pki, "egress-x402-client.pem"),
		s.sellerCA, sellerHost, s.sellerAddr, otherHost, s.sellerAddr)
	path := s.write("egress.toml", []byte(content))
	env := append(append([]string{}, s.env...), "CREDENTIALS_DIRECTORY="+s.pki)
	s.startProc(env, "bugnet-egress", filepath.Join(s.bin, "bugnet-egress"), "serve", "-config",
		path)
	s.waitReady("http://" + metrics + "/readyz")
}

// outerTLS is the TLS config of a client that trusts the proxy's tunnel certificate
func outerTLS(t testing.TB, file string) *tls.Config {
	t.Helper()
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(b)
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

// proxyFront passes each loopback connection to the proxy behind a PROXY v2 header
func (s *stack) proxyFront(to string) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.t.Fatal(err)
	}
	s.fronts = append(s.fronts, ln)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go relay(c, to)
		}
	}()
	return ln.Addr().String()
}

func relay(c net.Conn, to string) {
	defer func() { _ = c.Close() }()
	up, err := net.Dial("tcp", to)
	if err != nil {
		return
	}
	defer func() { _ = up.Close() }()
	if _, err := up.Write(proxyHeader(c.RemoteAddr(), c.LocalAddr())); err != nil {
		return
	}
	go func() {
		_, _ = io.Copy(up, c)
		_ = up.(*net.TCPConn).CloseWrite()
	}()
	_, _ = io.Copy(c, up)
}

// proxyHeader is a PROXY v2 header for a TCP connection over IPv4 from src to dst
func proxyHeader(src, dst net.Addr) []byte {
	from, to := src.(*net.TCPAddr), dst.(*net.TCPAddr)
	h := append([]byte("\r\n\r\n\x00\r\nQUIT\n"), 0x21, 0x11, 0, 12)
	h = append(append(h, from.IP.To4()...), to.IP.To4()...)
	h = binary.BigEndian.AppendUint16(h, uint16(from.Port))
	return binary.BigEndian.AppendUint16(h, uint16(to.Port))
}

// egressCounters logs the proxy's refusal and x402 counters, which say why a call failed
func (s *stack) egressCounters() {
	resp, err := http.Get("http://" + s.metrics + "/metrics")
	if err != nil {
		return
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "egress_") && !strings.Contains(line, "_bucket") &&
			!strings.HasSuffix(line, " 0") {
			s.t.Log(line)
		}
	}
}
