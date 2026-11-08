package x402local

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/store"
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

// service is one x402 service a sandbox enables
type service struct {
	method, prefix, path string
	maxMicros            int64
}

// mode is the service of a fake seller mode, priced at the seller's $0.01 default
func mode(name string) service {
	return service{"GET", "/" + name + "/", "/" + name + "/x", 10_000}
}

var (
	okService   = service{"GET", "/ok/", paidPath, priceMicros}
	freeService = service{"GET", "/free/", freePath, priceMicros}
)

// newSandbox is a sandbox with the ok and free services on the default allowance
func (s *stack) newSandbox() *sandbox {
	s.t.Helper()
	return s.newSandboxFor(allowanceMicros, freeService)
}

// newSandboxFor pays a start order with an allowance, places it and enables the ok service and
// the given ones
func (s *stack) newSandboxFor(allowance int64, services ...service) *sandbox {
	s.t.Helper()
	var order struct {
		OrderID    string `json:"orderId"`
		OrderToken string `json:"orderToken"`
	}
	accepted := s.accepted()
	pins := []map[string]any{}
	for _, v := range append([]service{okService}, services...) {
		pins = append(pins, map[string]any{
			"method": v.method, "url": "https://" + sellerHost + v.path,
			"maxPriceMicros": v.maxMicros, "payTo": s.sellerPayTo, "accepted": accepted,
		})
	}
	s.control("/order", map[string]any{
		"packageId": packageID, "packageMicros": packageMicros, "allowanceMicros": allowance,
		"feeMicros": feeMicros, "services": pins,
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
	args := []string{"x402", "enable", "--handle", sb.handle, "--wait"}
	for _, v := range append([]service{okService}, services...) {
		args = append(args, "--service", strings.Join([]string{v.method, sellerHost, v.prefix,
			strconv.FormatInt(v.maxMicros, 10), s.sellerPayTo, v.path}, ","))
	}
	s.dev(args...)
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
	}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// call is what the sandbox's code does, a GET of the seller through its forwarder
type call struct {
	status int
	header http.Header
	body   string
}

func (sb *sandbox) get(path string) call {
	sb.s.t.Helper()
	return sb.do(http.MethodGet, path, "", nil)
}

// do sends one request of the sandbox's code to the seller through its forwarder
func (sb *sandbox) do(method, path, body string, hdr map[string]string) call {
	sb.s.t.Helper()
	req, err := http.NewRequest(method, "https://"+sellerHost+path, strings.NewReader(body))
	if err != nil {
		sb.s.t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := sb.client.Do(req)
	if err != nil {
		sb.s.egressCounters()
		sb.s.t.Fatalf("x402local: %s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return call{status: resp.StatusCode, header: resp.Header, body: string(b)}
}

// ledger is what Payments holds, which the fake chain confirms
// payment is one row of Payments' ledger
type payment struct {
	Ref, State, Path string
	AmountMicros     string `json:"amount_micros"`
	TxSignature      string `json:"tx_signature"`
}

type ledger struct {
	Payments      []payment `json:"payments"`
	AllowanceUSDC string    `json:"allowanceUsdc"`
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
	run := func(name string, f func(*testing.T, *stack)) {
		t.Run(name, func(t *testing.T) { s.t = t; f(t, s) })
	}
	run("ok", modeOK)
	run("free", modeFree)
	runMore(s, run)
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

/* The other modes of XS 11.2 */

const (
	settleWait  = 90 * time.Second
	modePayment = 10_000
)

// rows are the ledger rows of a path
func (s *stack) rows(path string) []payment {
	s.t.Helper()
	var out []payment
	for _, p := range s.ledger().Payments {
		if p.Path == path {
			out = append(out, p)
		}
	}
	return out
}

// rowState waits until the only ledger row of a path is in a state, and returns it
func (s *stack) rowState(path, state string) payment {
	s.t.Helper()
	var rows []payment
	for deadline := time.Now().Add(settleWait); time.Now().Before(deadline); {
		if rows = s.rows(path); len(rows) == 1 && rows[0].State == state {
			return rows[0]
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.t.Fatalf("x402local: %s has no single row in state %s within %s: %+v", path, state,
		settleWait, rows)
	return payment{}
}

// sellerMicros is what the seller holds on the fake chain
func (s *stack) sellerMicros() int64 {
	s.t.Helper()
	var bal struct {
		Micros string `json:"micros"`
	}
	s.control("/balance", map[string]string{"owner": s.sellerPayTo}, &bal)
	n, err := strconv.ParseInt(bal.Micros, 10, 64)
	if err != nil {
		s.t.Fatal(err)
	}
	return n
}

// record is the x402 rows the session page serves for a path, as the router builds them
func (s *stack) record(path string) []map[string]any {
	s.t.Helper()
	url := ""
	for _, kv := range s.env {
		if v, ok := strings.CutPrefix(kv, "BUGNET_DATABASE_URL="); ok {
			url = v
		}
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		s.t.Fatal(err)
	}
	defer pool.Close()
	var placement uuid.UUID
	err = pool.QueryRow(ctx, `select r.placement_id from session_records r
		join session_events e on e.record_id = r.id
		where e.kind = 'x402' and e.data->>'path' = $1 limit 1`, path).Scan(&placement)
	if err != nil {
		return nil
	}
	raw, err := sessions.BuildRecord(ctx, store.New(pool), placement)
	if err != nil {
		s.t.Fatal(err)
	}
	var rec struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		s.t.Fatal(err)
	}
	var out []map[string]any
	for _, e := range rec.Events {
		if e["kind"] == "x402" && e["path"] == path {
			out = append(out, e)
		}
	}
	return out
}

// served waits until the record of a path holds n rows that all read as resolved, and returns them
func (s *stack) served(path string, n int, resolved string) []map[string]any {
	s.t.Helper()
	var rows []map[string]any
	for deadline := time.Now().Add(settleWait); time.Now().Before(deadline); {
		rows = s.record(path)
		ok := len(rows) == n
		for _, r := range rows {
			ok = ok && r["resolved"] == resolved
		}
		if ok {
			return rows
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.t.Fatalf("x402local: the record of %s has no %d rows resolved %q within %s: %v", path, n,
		resolved, settleWait, rows)
	return nil
}

// refused checks a refusal: the status, the code in header and body, and no new ledger row
func refused(t *testing.T, s *stack, c call, status int, code string, before int) {
	t.Helper()
	eq(t, c.status, status)
	eq(t, c.header.Get("Bugnet-X402-Error"), code)
	if !strings.Contains(c.body, `"code":"`+code+`"`) {
		t.Fatalf("the body does not carry %s: %s", code, c.body)
	}
	eq(t, len(s.ledger().Payments), before)
}

func runMore(s *stack, run func(string, func(*testing.T, *stack))) {
	var shared *sandbox
	refusals := []struct{ mode, code string }{
		{"bait", "price_above_max"}, {"switch", "payee_changed"},
		{"splits", "splits_unsupported"}, {"upto", "flow_unsupported"},
		{"escrow", "flow_unsupported"}, {"ata", "ata_creation_unsupported"},
		{"feepayer-bugnet", "fee_payer_refused"}, {"v1only", "version_unsupported"},
		{"base", "network_unsupported"},
	}
	// One sandbox holds every mode that is refused or paid without a failure, since a refusal
	// costs nothing
	setup := func() *sandbox {
		if shared == nil {
			var svcs []service
			for _, r := range refusals {
				svcs = append(svcs, mode(r.mode))
			}
			for _, m := range []string{"upfront", "memo", "hints"} {
				svcs = append(svcs, mode(m))
			}
			svcs = append(svcs, service{"GET", "/memo-long/33/", "/memo-long/33/x", modePayment})
			shared = s.newSandboxFor(allowanceMicros, svcs...)
		}
		return shared
	}
	for _, r := range refusals {
		run(r.mode, func(t *testing.T, s *stack) {
			sb := setup()
			before := len(s.ledger().Payments)
			refused(t, s, sb.get(mode(r.mode).path), http.StatusPaymentRequired, r.code, before)
		})
	}
	run("memo-long/33", func(t *testing.T, s *stack) {
		sb := setup()
		before := len(s.ledger().Payments)
		refused(t, s, sb.get("/memo-long/33/x"), http.StatusPaymentRequired, "flow_unsupported",
			before)
	})
	for _, m := range []string{"upfront", "memo", "hints"} {
		run(m, func(t *testing.T, s *stack) {
			c := setup().get(mode(m).path)
			eq(t, c.status, http.StatusOK)
			eq(t, c.header.Get("Bugnet-X402-Status"), "paid")
			s.rowState(mode(m).path, "confirmed")
		})
	}
	run("cent", modeCent)
	run("late", modeLate)
	run("lie", modeLie)
	run("silent", modeSilent)
	run("reject", modeReject)
	run("settle500", func(t *testing.T, s *stack) { modeSettle(t, s, "settle500", 500) })
	run("settle429", func(t *testing.T, s *stack) { modeSettle(t, s, "settle429", 429) })
	run("slow", modeSlow)
	run("stall", modeStall)
	run("pid", modePid)
	run("echo", modeEcho)
	run("redirect", modeRedirect)
	run("post400", modePost400)
}

// modeCent spends $0.05 in 32 calls, 8 at a time (egress' open-payment cap), first with different
// bodies, then with one body
func modeCent(t *testing.T, s *stack) {
	for _, same := range []bool{false, true} {
		sb := s.newSandboxFor(50_000, service{"POST", "/cent/", "/cent/x", modePayment})
		var mu sync.Mutex
		tally := map[string]int{}
		var wg sync.WaitGroup
		slots := make(chan struct{}, 8)
		for i := range 32 {
			wg.Add(1)
			slots <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				body := "same"
				if !same {
					body = fmt.Sprintf("body %d", i)
				}
				c := sb.do(http.MethodPost, "/cent/x", body, nil)
				mu.Lock()
				defer mu.Unlock()
				h := c.header
				tally[cmp.Or(h.Get("Bugnet-X402-Status"), h.Get("Bugnet-X402-Error"))]++
			}()
		}
		wg.Wait()
		eq(t, tally["paid"], 5)
		eq(t, tally["allowance_spent"], 27)
	}
}

// modeLate answers success false, then the seller submits and the payment lands
func modeLate(t *testing.T, s *stack) {
	sb := s.newSandboxFor(allowanceMicros, mode("late"))
	before := s.sellerMicros()
	c := sb.get("/late/x")
	eq(t, c.header.Get("Bugnet-X402-Status"), "uncertain")
	eq(t, s.rows("/late/x")[0].State, "signed")
	eq(t, s.sellerMicros(), before)
	s.rowState("/late/x", "confirmed")
	eq(t, s.sellerMicros(), before+modePayment)
}

// modeLie answers success with a transaction that never exists, and the room is released
func modeLie(t *testing.T, s *stack) {
	sb := s.newSandboxFor(allowanceMicros, mode("lie"))
	before := s.sellerMicros()
	sb.get("/lie/x")
	eq(t, s.rowState("/lie/x", "expired_unlanded").TxSignature, "")
	eq(t, s.sellerMicros(), before)
	for _, r := range s.served("/lie/x", 1, "released") {
		if r["tx"] != nil && r["tx"] != "" {
			t.Fatalf("a released payment serves the transaction %v", r["tx"])
		}
	}
}

// modeSilent sends no PAYMENT-RESPONSE and submits
func modeSilent(t *testing.T, s *stack) {
	sb := s.newSandboxFor(allowanceMicros, mode("silent"))
	before := s.sellerMicros()
	c := sb.get("/silent/x")
	eq(t, c.status, http.StatusOK)
	eq(t, c.header.Get("Bugnet-X402-Status"), "uncertain")
	s.rowState("/silent/x", "confirmed")
	eq(t, s.sellerMicros(), before+modePayment)
}

// modeReject answers 402 to the paid retry
func modeReject(t *testing.T, s *stack) {
	sb := s.newSandboxFor(allowanceMicros, mode("reject"))
	before := s.sellerMicros()
	c := sb.get("/reject/x")
	eq(t, c.status, http.StatusPaymentRequired)
	eq(t, c.header.Get("Bugnet-X402-Error"), "payment_rejected")
	s.rowState("/reject/x", "expired_unlanded")
	eq(t, s.sellerMicros(), before)
	s.served("/reject/x", 1, "released")
}

// modeSettle sends one request three times; the seller takes the payment once, answers its
// failure to the first send and refuses the payload it has seen on the other two
func modeSettle(t *testing.T, s *stack, name string, status int) {
	sb := s.newSandboxFor(allowanceMicros, mode(name))
	path := mode(name).path
	before := s.sellerMicros()
	eq(t, sb.get(path).status, status)
	// The seller refuses the payload it has seen, so the two resends meet a 402
	for range 2 {
		eq(t, sb.get(path).header.Get("Bugnet-X402-Error"), "payment_rejected")
	}
	s.rowState(path, "confirmed")
	eq(t, s.sellerMicros(), before+modePayment)
	var paying, resent int
	for _, r := range s.served(path, 3, "paid") {
		if r["resent"] == true {
			resent++
			eq(t, fmt.Sprint(r["amountMicros"]), "0")
		} else {
			paying++
			eq(t, fmt.Sprint(r["amountMicros"]), strconv.Itoa(modePayment))
		}
	}
	eq(t, paying, 1)
	eq(t, resent, 2)
}

// modeSlow settles after the blockhash is gone, so nothing lands and the room is released
func modeSlow(t *testing.T, s *stack) {
	sb := s.newSandboxFor(allowanceMicros, mode("slow"))
	before := s.sellerMicros()
	c := sb.get("/slow/x")
	eq(t, c.header.Get("Bugnet-X402-Status"), "uncertain")
	s.rowState("/slow/x", "expired_unlanded")
	eq(t, s.sellerMicros(), before)
	s.served("/slow/x", 1, "released")
}

// modeStall holds the first answer past egress' first_header_timeout
func modeStall(t *testing.T, s *stack) {
	sb := s.newSandboxFor(allowanceMicros, mode("stall"))
	before := len(s.ledger().Payments)
	refused(t, s, sb.get("/stall/x"), http.StatusPaymentRequired, "first_answer_slow", before)
}

// modePid pays and sends the payment identifier the seller offered
func modePid(t *testing.T, s *stack) {
	sb := s.newSandboxFor(allowanceMicros, mode("pid"))
	c := sb.get("/pid/x")
	eq(t, c.status, http.StatusOK)
	eq(t, c.header.Get("Bugnet-X402-Status"), "paid")
	req, err := http.NewRequest(http.MethodGet, "https://"+s.sellerAddr+"/__test/payloads", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+s.sellerToken)
	pool := x509.NewCertPool()
	pem, _ := os.ReadFile(s.sellerCA)
	pool.AppendCertsFromPEM(pem)
	api := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: sellerHost},
	}}
	resp, err := api.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var payloads []string
	if err := json.NewDecoder(resp.Body).Decode(&payloads); err != nil {
		t.Fatal(err)
	}
	for _, p := range payloads {
		raw, err := base64.StdEncoding.DecodeString(p)
		if err == nil && strings.Contains(string(raw), "payment-identifier") {
			return
		}
	}
	t.Fatalf("none of the %d payloads the seller took carries a payment identifier", len(payloads))
}

// modeEcho sees the seller echo the request: the payment is scrubbed and no Bugnet header went up
func modeEcho(t *testing.T, s *stack) {
	sb := s.newSandboxFor(allowanceMicros, mode("echo"))
	c := sb.do(http.MethodGet, "/echo/x", "", map[string]string{"Bugnet-Probe": "up"})
	eq(t, c.status, http.StatusOK)
	eq(t, c.header.Get("Echo-Payment-Signature"), "[bugnet-payment]")
	for k := range c.header {
		if strings.HasPrefix(k, "Echo-Bugnet") {
			t.Fatalf("the seller received %s", k)
		}
	}
	if strings.Contains(strings.ToLower(c.body), "bugnet-probe") {
		t.Fatalf("the seller's body shows a Bugnet header: %s", c.body)
	}
}

// modeRedirect pays once and leaves the seller's 302 to the code
func modeRedirect(t *testing.T, s *stack) {
	sb := s.newSandboxFor(allowanceMicros, mode("redirect"))
	c := sb.get("/redirect/x")
	eq(t, c.status, http.StatusFound)
	if c.header.Get("Location") == "" {
		t.Fatal("the 302 lost its Location")
	}
	s.rowState("/redirect/x", "confirmed")
}

// modePost400 answers an empty POST unpaid and pays a POST with a body
func modePost400(t *testing.T, s *stack) {
	sb := s.newSandboxFor(allowanceMicros, service{"POST", "/post400/", "/post400/x", modePayment})
	c := sb.do(http.MethodPost, "/post400/x", "", nil)
	eq(t, c.status, http.StatusBadRequest)
	eq(t, len(s.rows("/post400/x")), 0)
	c = sb.do(http.MethodPost, "/post400/x", "hello", nil)
	eq(t, c.status, http.StatusOK)
	eq(t, c.header.Get("Bugnet-X402-Status"), "paid")
	s.rowState("/post400/x", "confirmed")
}
