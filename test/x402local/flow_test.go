package x402local

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// The gateway's MCP protocol version and how long one tool call may take
const (
	mcpVersion  = "2025-06-18"
	mcpCallWait = 90 * time.Second
	gatewayWait = 60 * time.Second
	linksDomain = "dev.bugnet.sh"
	// briefMax caps how much of an answer a failure message prints
	briefMax = 600
)

// secretShapes are the parts of an answer a failure message never prints
var secretShapes = []struct {
	re   *regexp.Regexp
	with string
}{
	{regexp.MustCompile(`\S*` + regexp.QuoteMeta(linksDomain) + `\S*`), "[session link]"},
	{regexp.MustCompile(`#k=\S*`), "#k=[key]"},
	{regexp.MustCompile(`bnk_[A-Za-z0-9_-]+`), "[key]"},
	{regexp.MustCompile(`(?i)("?claim"?\W{0,4})[A-Za-z0-9_.~+/=-]{8,}`), "${1}[claim]"},
}

// gatewayDir is the bugnet-gateway checkout, BUGNET_GATEWAY_DIR or the one beside the router
func gatewayDir(t testing.TB) string {
	t.Helper()
	if d := os.Getenv("BUGNET_GATEWAY_DIR"); d != "" {
		return d
	}
	for _, rel := range []string{"..", filepath.Join("..", "..", "..")} {
		d := filepath.Join(repo(t), rel, "bugnet-gateway")
		if _, err := os.Stat(filepath.Join(d, "packages", "gateway", "package.json")); err == nil {
			return d
		}
	}
	t.Fatal("x402local: no bugnet-gateway checkout, set BUGNET_GATEWAY_DIR")
	return ""
}

// gateway is the real MCP gateway running against the stack
type gateway struct {
	t   *testing.T
	url string
	p   *proc
	id  atomic.Int64
	// claims are the order claims the flow saw, kept out of every failure message
	claims []string
}

// redact drops session links, record keys and claims from text a failure message prints
func (g *gateway) redact(text string) string {
	for _, c := range g.claims {
		text = strings.ReplaceAll(text, c, "[claim]")
	}
	for _, sh := range secretShapes {
		text = sh.re.ReplaceAllString(text, sh.with)
	}
	if len(text) > briefMax {
		text = text[:briefMax] + " [cut]"
	}
	return text
}

// brief is the redacted and capped text of an answer, for failure messages
func (g *gateway) brief(r toolResult) string {
	return g.redact(r.text())
}

// gatewayEnv is the few host settings tsx needs, with no real secret of this machine
func gatewayEnv() []string {
	var env []string
	for _, k := range []string{"PATH", "HOME", "TMPDIR"} {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	return env
}

// startGateway runs the gateway on a free port against the stack until the test ends
func (s *stack) startGateway(t *testing.T) *gateway {
	t.Helper()
	addr := freeAddr(t)
	cfg := fmt.Sprintf(`listen_addr = %q
public_url = %q
[router]
url = %q
links_domain = %q
provider_names = ["local"]
[payments]
mode = "http"
url = %q
[x402]
enabled = true
`, addr, "http://"+addr, s.apiURL, linksDomain, s.payments.Internal)
	in, _ := json.Marshal(map[string]string{
		"gatewayDir": gatewayDir(t), "config": s.write("gateway/gateway.toml", []byte(cfg)),
		"sellerHost": sellerHost, "sellerAddr": s.sellerAddr, "sellerCA": s.sellerCA,
	})
	input := s.write("gateway/input.json", in)
	src, err := os.ReadFile(filepath.Join(s.root, "test", "x402local", "gateway-harness.mts"))
	if err != nil {
		t.Fatal(err)
	}
	harness := s.write("gateway/gateway.mts", src)
	tsx, err := exec.LookPath("tsx")
	if err != nil {
		t.Fatal("x402local: tsx is not installed, npm install -g tsx")
	}
	logFile, err := os.Create(filepath.Join(s.dir, "gateway.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(tsx, harness, input)
	cmd.Dir = filepath.Join(gatewayDir(t), "packages", "gateway")
	cmd.Env = append(gatewayEnv(),
		"BUGNET_INTERNAL_TOKEN="+s.keys["BUGNET_INTERNAL_TOKEN"],
		"BUGNET_QUOTE_KEY="+base64.StdEncoding.EncodeToString(randomBytes(32)),
		"BUGNET_PAYMENTS_TOKEN="+s.payments.paymentsToken)
	cmd.Stdout = logFile
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{name: "gateway", cmd: cmd, log: logFile.Name(), done: make(chan struct{})}
	listening := make(chan struct{}, 1)
	go func() {
		sc := bufio.NewScanner(io.TeeReader(stderr, logFile))
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			if strings.Contains(sc.Text(), `"msg":"listening"`) {
				listening <- struct{}{}
			}
		}
		p.err = cmd.Wait()
		_ = logFile.Close()
		close(p.done)
	}()
	s.procs = append(s.procs, p)
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(stopGrace):
			_ = cmd.Process.Kill()
			<-p.done
		}
	})
	select {
	case <-listening:
	case <-p.done:
		s.fail("the gateway exited before it listened: %v", p.err)
	case <-time.After(gatewayWait):
		s.fail("the gateway did not listen within %s", gatewayWait)
	}
	return &gateway{t: t, url: "http://" + addr + "/mcp", p: p}
}

// toolResult is the part of a tools/call answer the flow reads
type toolResult struct {
	IsError    bool           `json:"isError"`
	Structured map[string]any `json:"structuredContent"`
	Content    []struct {
		Text string `json:"text"`
	} `json:"content"`
}

func (r toolResult) text() string {
	var parts []string
	for _, c := range r.Content {
		parts = append(parts, c.Text)
	}
	return strings.Join(parts, "\n")
}

// str is a string field of the structured answer
func (r toolResult) str(key string) string {
	v, _ := r.Structured[key].(string)
	return v
}

// call sends one tools/call over MCP Streamable HTTP and reads a JSON or SSE answer
func (g *gateway) call(name string, args map[string]any) toolResult {
	g.t.Helper()
	id := g.id.Add(1)
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": "tools/call",
		"params": map[string]any{"name": name, "arguments": args},
	})
	req, err := http.NewRequest(http.MethodPost, g.url, bytes.NewReader(body))
	if err != nil {
		g.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Protocol-Version", mcpVersion)
	resp, err := (&http.Client{Timeout: mcpCallWait}).Do(req)
	if err != nil {
		g.t.Fatalf("x402local: %s: %v", name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		g.t.Fatalf("x402local: %s answered %d: %s", name, resp.StatusCode, g.redact(string(raw)))
	}
	msgs := [][]byte{raw}
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		msgs = nil
		for _, line := range strings.Split(string(raw), "\n") {
			if data, ok := strings.CutPrefix(line, "data:"); ok {
				msgs = append(msgs, []byte(strings.TrimSpace(data)))
			}
		}
	}
	for _, m := range msgs {
		var rpc struct {
			ID     int64           `json:"id"`
			Result *toolResult     `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if json.Unmarshal(m, &rpc) != nil || rpc.ID != id {
			continue
		}
		if rpc.Result == nil {
			g.t.Fatalf("x402local: %s failed: %s", name, g.redact(string(rpc.Error)))
		}
		return *rpc.Result
	}
	g.t.Fatalf("x402local: %s gave no answer: %s", name, g.redact(string(raw)))
	return toolResult{}
}

// payStep checks a tool answered a payment step and pays it from the harness' funded payer
func (g *gateway) payStep(s *stack, r toolResult) (orderID, claim string) {
	g.t.Helper()
	orderID, claim = r.str("orderId"), r.str("claim")
	if claim != "" {
		g.claims = append(g.claims, claim)
	}
	accepts, _ := r.Structured["accepts"].([]any)
	if !r.IsError || orderID == "" || claim == "" || r.str("payUrl") == "" || len(accepts) == 0 {
		g.t.Fatalf("x402local: no payment step: %s", g.brief(r))
	}
	s.control("/pay", map[string]any{"payUrl": r.str("payUrl"), "accepted": accepts[0]}, nil)
	return orderID, claim
}

// sessionFee is the one-time fee of a sandbox's first allowance, as Payments' terms say
func (s *stack) sessionFee(t *testing.T) int64 {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, s.payments.Internal+"/v1/x402/terms", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+s.payments.paymentsToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var terms struct {
		SessionFeeMicros int64 `json:"sessionFeeMicros"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&terms); err != nil {
		t.Fatalf("x402local: Payments' terms: %v", err)
	}
	return terms.SessionFeeMicros
}

// usdText writes micros as the dollar text the tools take
func usdText(micros int64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%d.%06d", micros/1e6, micros%1e6),
		"0"), ".")
}

// usdMicros reads the dollar text the tools answer as micros
func usdMicros(t *testing.T, usd string) int64 {
	t.Helper()
	whole, frac, _ := strings.Cut(strings.TrimPrefix(usd, "$"), ".")
	if len(frac) > 6 {
		t.Fatalf("x402local: %q has more than 6 decimals", usd)
	}
	n, err := strconv.ParseInt(whole+frac+strings.Repeat("0", 6-len(frac)), 10, 64)
	if err != nil {
		t.Fatalf("x402local: %q is no dollar amount: %v", usd, err)
	}
	return n
}

// payerMicros is the USDC the harness' funded payer holds on the fake chain
func (s *stack) payerMicros() int64 {
	s.t.Helper()
	var bal struct {
		Micros string `json:"micros"`
	}
	s.control("/balance", map[string]string{"owner": s.payments.Payer}, &bal)
	n, err := strconv.ParseInt(bal.Micros, 10, 64)
	if err != nil {
		s.t.Fatal(err)
	}
	return n
}

// lived is the state and deadline that stay the same while a sandbox runs on
func lived(r toolResult) string {
	return r.str("state") + " until " + r.str("deadline")
}

// exec runs one shell command in the sandbox through the gateway and answers its stdout lines
func (g *gateway) exec(handle, cmd string) []string {
	g.t.Helper()
	r := g.call("bugnet_exec", map[string]any{"handle": handle, "cmd": cmd, "waitSeconds": 30})
	if code, done := r.Structured["exitCode"].(float64); r.IsError || !done || code != 0 {
		g.t.Fatalf("x402local: the command failed: %s", g.brief(r))
	}
	return strings.Split(strings.ReplaceAll(r.str("stdout"), "\r", ""), "\n")
}

// field is the value of the first stdout line that starts with key=
func field(lines []string, key string) string {
	for _, l := range lines {
		if v, ok := strings.CutPrefix(l, key+"="); ok {
			return v
		}
	}
	return ""
}

// flow pays a gateway start and an x402 budget by wallet, then a call with no sandbox restart
func flow(t *testing.T, s *stack) {
	g := s.startGateway(t)
	paidFrom := s.payerMicros()
	fee := s.sessionFee(t)

	q := g.call("bugnet_quote", map[string]any{"workload": "script", "needs": []string{"secrets"}})
	quoteID := q.str("quoteId")
	price := ""
	pkgs, _ := q.Structured["packages"].([]any)
	for _, p := range pkgs {
		if m, _ := p.(map[string]any); m["id"] == packageID {
			price, _ = m["priceUsd"].(string)
		}
	}
	if q.IsError || quoteID == "" || price == "" {
		t.Fatalf("no quote for %s: %s", packageID, g.brief(q))
	}
	start := g.call("bugnet_start", map[string]any{
		"quoteId": quoteID, "packageId": packageID, "confirmPriceUsd": price,
	})
	startOrder, claim := g.payStep(s, start)
	var handle string
	var r toolResult
	for deadline := time.Now().Add(paidWait); handle == ""; time.Sleep(time.Second) {
		r = g.call("bugnet_redeem", map[string]any{
			"orderId": startOrder, "claim": claim, "quoteId": quoteID,
		})
		if handle = r.str("handle"); handle == "" && time.Now().After(deadline) {
			t.Fatalf("the paid start gave no sandbox within %s: %s", paidWait, g.brief(r))
		}
	}
	for deadline := time.Now().Add(readyWait); r.str("state") != "ready"; {
		r = g.call("bugnet_status", map[string]any{"handle": handle, "waitSeconds": 20})
		if time.Now().After(deadline) {
			t.Fatalf("the sandbox was not ready within %s: %s", readyWait, g.brief(r))
		}
	}

	// A process started before the budget must outlive it, which a restart would not allow
	pid, err := strconv.Atoi(field(g.exec(handle,
		"sleep 600 >/dev/null 2>&1 </dev/null & echo pid=$!"), "pid"))
	if err != nil || pid <= 1 {
		t.Fatalf("no background process in the sandbox: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	life := lived(r)
	if r.str("deadline") == "" {
		t.Fatalf("the ready sandbox has no deadline: %s", g.brief(r))
	}

	// The fake seller's ok route, unlisted, at its live price
	line := fmt.Sprintf("GET https://%s%s max=%s payTo=%s prefix=%s", sellerHost, paidPath,
		usdText(priceMicros), s.sellerPayTo, okService.prefix)
	budget := map[string]any{
		"handle": handle, "amountUsd": usdText(allowanceMicros),
		"confirmPriceUsd": usdText(allowanceMicros + fee),
		"services":        []string{line}, "allowUnlisted": true,
	}
	orderID, claim := g.payStep(s, g.call("bugnet_x402_budget", budget))
	budget["orderId"], budget["claim"] = orderID, claim
	var enabled toolResult
	for deadline := time.Now().Add(paidWait); ; {
		if enabled = g.call("bugnet_x402_budget", budget); !enabled.IsError {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the paid budget enabled nothing within %s: %s", paidWait, g.brief(enabled))
		}
		time.Sleep(time.Second)
	}
	x402, _ := enabled.Structured["x402"].(map[string]any)
	services, _ := x402["services"].([]any)
	if len(services) != 1 || !strings.Contains(enabled.text(), sellerHost) {
		t.Fatalf("the budget did not enable the seller: %s", g.brief(enabled))
	}
	// The wallet paid the start, the allowance and the session fee and nothing more
	eq(t, paidFrom-s.payerMicros(), usdMicros(t, price)+allowanceMicros+fee)

	// The sandbox runs on, the stack only swaps its forwarder for one that trusts the test proxy
	sb := &sandbox{s: s, handle: handle}
	sb.findFS()
	sb.trustProxy()
	before := s.sellerMicros()
	// A local job reads the host's /etc, so the budget's env file, hosts and CA are passed by hand
	curl := fmt.Sprintf("set -a; . %s; set +a; echo hosts=$BUGNET_X402_HOSTS; "+
		"curl -sS -o /dev/null -D - -w 'code=%%{http_code}\\n' --connect-to %s:443:%s "+
		"--cacert %s https://%s%s",
		filepath.Join(sb.fs, "etc", "bugnet", "secrets.env"), sellerHost, sb.fwd, s.egressCAFile,
		sellerHost, paidPath)
	out := g.exec(handle, curl)
	// The budget wrote the seller's host into the sandbox's env file
	if got := field(out, "hosts"); got != sellerHost {
		t.Fatalf("the env file names hosts %q, not %q", got, sellerHost)
	}
	status := ""
	for _, l := range out {
		if k, v, ok := strings.Cut(l, ":"); ok && strings.EqualFold(k, "bugnet-x402-status") {
			status = strings.TrimSpace(v)
		}
	}
	if code := field(out, "code"); code != "200" || status != "paid" {
		t.Fatalf("the paid call from the sandbox answered %q with status %q", code, status)
	}

	// The process from before the budget still runs and the sandbox kept its state and deadline
	if alive := field(g.exec(handle, fmt.Sprintf("kill -0 %d && echo alive=yes", pid)),
		"alive"); alive != "yes" {
		t.Fatalf("process %d died, so the sandbox restarted", pid)
	}
	after := g.call("bugnet_status", map[string]any{"handle": handle})
	eq(t, lived(after), life)

	// Exactly one seller payment for this sandbox, on the fake chain and in Payments' ledger
	var mine []payment
	for deadline := time.Now().Add(paidWait); ; time.Sleep(500 * time.Millisecond) {
		mine = nil
		for _, p := range s.rows(paidPath) {
			if p.AccountID == startOrder {
				mine = append(mine, p)
			}
		}
		if len(mine) == 1 && mine[0].State == "confirmed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no single confirmed payment for this sandbox within %s: %+v", paidWait, mine)
		}
	}
	eq(t, mine[0].AmountMicros, strconv.Itoa(priceMicros))
	if mine[0].TxSignature == "" {
		t.Fatal("the confirmed payment has no transaction")
	}
	eq(t, s.sellerMicros(), before+priceMicros)

	stop := g.call("bugnet_stop", map[string]any{"handle": handle})
	said := stop.text()
	if stop.IsError || !strings.Contains(said, "spent in 1 paid calls") ||
		!strings.Contains(said, "goes back to the paying wallet") {
		t.Fatalf("the stop answer is not the paid one: %s", g.redact(said))
	}
	// The answer's session link carries the record key, so only its money line is logged
	for _, l := range strings.Split(said, "\n") {
		if strings.HasPrefix(l, "x402:") {
			t.Logf("stop line: %s", g.redact(l))
		}
	}
}
