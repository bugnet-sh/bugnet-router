package x402local

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

const (
	allowanceMicros = 100_000
	priceMicros     = 1_000
	paidPath        = "/ok/x"
	freePath        = "/free/x"
	paidWait        = 30 * time.Second
)

// sandbox is one placement of the stack with its x402 services enabled
type sandbox struct {
	s       *stack
	handle  string
	orderID string
	fs      string // the sandbox's filesystem root on this machine
	fwd     string // where its forwarder listens
	client  *http.Client
}

// dev runs bugnet-dev against the stack's router
func (s *stack) dev(args ...string) string {
	s.t.Helper()
	return s.run(s.env, "bugnet-dev",
		append([]string{"--config", s.settings}, args...)...)
}

// accepted is the first payment option of the seller's 402, read the way a client reads it
func (s *stack) accepted() json.RawMessage {
	s.t.Helper()
	b, err := os.ReadFile(s.sellerCA)
	if err != nil {
		s.t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(b)
	c := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: sellerHost},
	}}
	resp, err := c.Get("https://" + s.sellerAddr + paidPath)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := base64.StdEncoding.DecodeString(resp.Header.Get("PAYMENT-REQUIRED"))
	if resp.StatusCode != http.StatusPaymentRequired || err != nil {
		s.t.Fatalf("x402local: the seller answered %d with no payment header", resp.StatusCode)
	}
	var req struct {
		Accepts []json.RawMessage `json:"accepts"`
	}
	if err := json.Unmarshal(raw, &req); err != nil || len(req.Accepts) == 0 {
		s.t.Fatalf("x402local: the seller offered nothing: %v", err)
	}
	return req.Accepts[0]
}

// newSandbox pays a start order with an allowance, places it and enables the two services
func (s *stack) newSandbox() *sandbox {
	s.t.Helper()
	var order struct {
		OrderID    string `json:"orderId"`
		OrderToken string `json:"orderToken"`
	}
	s.control("/order", map[string]any{
		"packageId": packageID, "packageMicros": packageMicros, "allowanceMicros": allowanceMicros,
		"feeMicros": feeMicros, "method": "GET", "url": "https://" + sellerHost + paidPath,
		"maxPriceMicros": priceMicros, "payTo": s.sellerPayTo, "accepted": s.accepted(),
	}, &order)
	out := s.dev("create", "--package", packageID, "--token", order.OrderToken)
	sb := &sandbox{s: s, orderID: order.OrderID}
	for _, line := range strings.Split(out, "\n") {
		if h, ok := strings.CutPrefix(line, "handle="); ok {
			sb.handle = strings.TrimSpace(h)
		}
	}
	if sb.handle == "" {
		s.t.Fatalf("x402local: bugnet-dev create printed no handle:\n%s", out)
	}
	line := func(prefix, path string) string {
		return strings.Join([]string{"GET", sellerHost, prefix, strconv.Itoa(priceMicros),
			s.sellerPayTo, path}, ",")
	}
	s.dev("x402", "enable", "--handle", sb.handle, "--wait",
		"--service", line("/ok/", paidPath), "--service", line("/free/", freePath))
	sb.findFS()
	sb.trustProxy()
	return sb
}

// findFS finds the sandbox no earlier sandbox of the stack took
func (sb *sandbox) findFS() {
	t, s := sb.s.t, sb.s
	lp := local.New(local.Options{Name: "local", Root: filepath.Join(s.dir, "local")})
	owned, err := lp.ListOwned(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.taken == nil {
		s.taken = map[string]bool{}
	}
	for _, o := range owned {
		if !s.taken[o.Ref.ID] {
			s.taken[o.Ref.ID] = true
			sb.fs = filepath.Join(s.dir, "local", o.Ref.ID, "fs")
			return
		}
	}
	t.Fatal("x402local: no new local sandbox")
}

// trustProxy gives the sandbox the roots a real one gets from its system store and a forwarder
func (sb *sandbox) trustProxy() {
	t, s := sb.s.t, sb.s
	cfgPath := filepath.Join(sb.fs, "etc", "bugnet", "egress.json")
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := map[string]any{}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	cfg["roots_file"] = "/etc/bugnet/proxy-roots.pem"
	pemBytes, err := os.ReadFile(s.outerCert)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := json.Marshal(cfg)
	if err := os.WriteFile(filepath.Join(sb.fs, "etc", "bugnet", "proxy-roots.pem"), pemBytes,
		0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgPath, out, 0o644); err != nil {
		t.Fatal(err)
	}
	// The forwarder apply started has no log and trusts the system roots, so the stack replaces
	// it with one of its own under the same root
	pidFile := filepath.Join(sb.fs, "run", "bugnet", "forward.pid")
	pidRaw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidRaw)))
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(readyWait); ; time.Sleep(50 * time.Millisecond) {
		if _, err := os.Stat(pidFile); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("x402local: the sandbox's forwarder did not stop")
		}
	}
	s.startProc(s.env, "forward", filepath.Join(s.bin, "bugnet-run"), "forward", "--root", sb.fs,
		"--config", cfgPath)
	var addr []byte
	for deadline := time.Now().Add(readyWait); ; time.Sleep(50 * time.Millisecond) {
		addr, err = os.ReadFile(filepath.Join(sb.fs, "run", "bugnet", "forward.addr"))
		if err == nil && len(addr) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("x402local: the stack's forwarder did not listen")
		}
	}
	sb.fwd = strings.TrimSpace(string(addr))
	caPEM, err := os.ReadFile(s.egressCAFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	sb.client = &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, sb.fwd)
		},
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}}
}

// call is what the sandbox's code does, a GET of the seller through its forwarder
type call struct {
	status int
	header http.Header
	body   string
}

func (sb *sandbox) get(path string) call {
	sb.s.t.Helper()
	resp, err := sb.client.Get("https://" + sellerHost + path)
	if err != nil {
		sb.s.egressCounters()
		sb.s.t.Fatalf("x402local: GET %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return call{status: resp.StatusCode, header: resp.Header, body: string(b)}
}

// ledger is what Payments holds, which the fake chain confirms
type ledger struct {
	Payments []struct {
		Ref, State   string
		AmountMicros string `json:"amount_micros"`
		TxSignature  string `json:"tx_signature"`
	} `json:"payments"`
	AllowanceUSDC string `json:"allowanceUsdc"`
	Tail          []map[string]any
	Incidents     []map[string]any
}

func (s *stack) ledger() ledger {
	s.t.Helper()
	var l ledger
	s.control("/ledger", map[string]any{}, &l)
	return l
}

// settled waits until Payments holds n confirmed payments
func (s *stack) settled(n int) ledger {
	s.t.Helper()
	deadline := time.Now().Add(paidWait)
	for {
		l := s.ledger()
		done := 0
		for _, p := range l.Payments {
			if p.State == "confirmed" {
				done++
			}
		}
		if done >= n {
			return l
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("x402local: %d payments confirmed within %s, want %d: %+v", done,
				paidWait, n, l)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestX402Local(t *testing.T) {
	gate(t)
	s := startStack(t)
	t.Run("ok", func(t *testing.T) { s.t = t; modeOK(t, s) })
	t.Run("free", func(t *testing.T) { s.t = t; modeFree(t, s) })
}

// modeOK pays one call of $0.001 from a $0.10 allowance and sees it settle on the fake chain
func modeOK(t *testing.T, s *stack) {
	sb := s.newSandbox()
	c := sb.get(paidPath)
	eq(t, c.status, http.StatusOK)
	eq(t, c.header.Get("Bugnet-X402-Status"), "paid")
	eq(t, c.header.Get("Bugnet-X402-Paid-Usd"), "0.001")
	eq(t, c.header.Get("Bugnet-X402-Left-Usd"), "0.099")
	l := s.settled(1)
	eq(t, len(l.Payments), 1)
	eq(t, l.Payments[0].AmountMicros, strconv.Itoa(priceMicros))
	if l.Payments[0].TxSignature == "" {
		t.Fatal("the confirmed payment has no transaction")
	}
	// The fake chain holds exactly what the ledger holds
	var bal struct {
		Micros string `json:"micros"`
	}
	s.control("/balance", map[string]string{"owner": s.sellerPayTo}, &bal)
	eq(t, bal.Micros, strconv.Itoa(priceMicros))
	eq(t, l.AllowanceUSDC, strconv.Itoa(allowanceMicros+packageMicros+feeMicros-priceMicros))
	// The resolved event reaches the router from Payments' outbox a moment after the chain
	var out string
	for deadline := time.Now().Add(paidWait); ; time.Sleep(500 * time.Millisecond) {
		if out = s.dev("x402", "list", "--handle", sb.handle); strings.Contains(out, "paid=1") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ListX402 shows no paid call within %s:\n%s", paidWait, out)
		}
	}
}

// modeFree makes 300 free calls, then the 301st is refused as the unpaid cap
func modeFree(t *testing.T, s *stack) {
	sb := s.newSandbox()
	for i := range 300 {
		if c := sb.get(freePath); c.status != http.StatusOK {
			t.Fatalf("free call %d answered %d: %s", i+1, c.status, c.body)
		}
	}
	c := sb.get(freePath)
	eq(t, c.status, http.StatusForbidden)
	eq(t, c.header.Get("Bugnet-X402-Error"), "unpaid_cap")
}

func eq[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
