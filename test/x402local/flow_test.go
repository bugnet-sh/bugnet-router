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
)

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
}

// startGateway runs the gateway on a free port against the stack until the test ends
func (s *stack) startGateway(t *testing.T) *gateway {
	t.Helper()
	addr := freeAddr(t)
	cfg := fmt.Sprintf(`listen_addr = %q
public_url = %q
[router]
url = %q
links_domain = "dev.bugnet.sh"
provider_names = ["local"]
[payments]
mode = "http"
url = %q
[x402]
enabled = true
`, addr, "http://"+addr, s.apiURL, s.payments.Internal)
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
	cmd.Env = append(hostEnv(),
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
		g.t.Fatalf("x402local: %s answered %d: %s", name, resp.StatusCode, raw)
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
			g.t.Fatalf("x402local: %s failed: %s", name, rpc.Error)
		}
		return *rpc.Result
	}
	g.t.Fatalf("x402local: %s gave no answer: %s", name, raw)
	return toolResult{}
}

// payStep checks a tool answered a payment step and pays it from the harness' funded payer
func (s *stack) payStep(t *testing.T, r toolResult) (orderID, claim string) {
	t.Helper()
	orderID, claim = r.str("orderId"), r.str("claim")
	accepts, _ := r.Structured["accepts"].([]any)
	if !r.IsError || orderID == "" || claim == "" || r.str("payUrl") == "" || len(accepts) == 0 {
		t.Fatalf("x402local: no payment step: %s %v", r.text(), r.Structured)
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

// flow pays a gateway start and an x402 budget by wallet, then a call with no sandbox restart
func flow(t *testing.T, s *stack) {
	g := s.startGateway(t)

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
		t.Fatalf("no quote for %s: %s", packageID, q.text())
	}
	start := g.call("bugnet_start", map[string]any{
		"quoteId": quoteID, "packageId": packageID, "confirmPriceUsd": price,
	})
	orderID, claim := s.payStep(t, start)
	var handle string
	var r toolResult
	for deadline := time.Now().Add(paidWait); handle == ""; time.Sleep(time.Second) {
		r = g.call("bugnet_redeem", map[string]any{
			"orderId": orderID, "claim": claim, "quoteId": quoteID,
		})
		if handle = r.str("handle"); handle == "" && time.Now().After(deadline) {
			t.Fatalf("the paid start gave no sandbox within %s: %s", paidWait, r.text())
		}
	}
	for deadline := time.Now().Add(readyWait); r.str("state") != "ready"; {
		r = g.call("bugnet_status", map[string]any{"handle": handle, "waitSeconds": 20})
		if time.Now().After(deadline) {
			t.Fatalf("the sandbox was not ready within %s: %s", readyWait, r.text())
		}
	}

	// The fake seller's ok route, unlisted, at its live price
	line := fmt.Sprintf("GET https://%s%s max=%s payTo=%s prefix=%s", sellerHost, paidPath,
		usdText(priceMicros), s.sellerPayTo, okService.prefix)
	budget := map[string]any{
		"handle": handle, "amountUsd": usdText(allowanceMicros),
		"confirmPriceUsd": usdText(allowanceMicros + s.sessionFee(t)),
		"services":        []string{line}, "allowUnlisted": true,
	}
	orderID, claim = s.payStep(t, g.call("bugnet_x402_budget", budget))
	budget["orderId"], budget["claim"] = orderID, claim
	var enabled toolResult
	for deadline := time.Now().Add(paidWait); ; {
		if enabled = g.call("bugnet_x402_budget", budget); !enabled.IsError {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the paid budget enabled nothing within %s: %s", paidWait, enabled.text())
		}
		time.Sleep(time.Second)
	}
	x402, _ := enabled.Structured["x402"].(map[string]any)
	services, _ := x402["services"].([]any)
	if len(services) != 1 || !strings.Contains(enabled.text(), sellerHost) {
		t.Fatalf("the budget did not enable the seller: %s", enabled.text())
	}
	t.Logf("budget answer:\n%s", enabled.text())

	// The sandbox runs on, the stack only swaps its forwarder for one that trusts the test proxy
	sb := &sandbox{s: s, handle: handle}
	sb.findFS()
	sb.trustProxy()
	before, paidBefore := s.sellerMicros(), len(s.rows(paidPath))
	// A local job reads the host's /etc, so it gets the sandbox's env, hosts and CA by hand
	curl := fmt.Sprintf("set -a; . %s; set +a; echo hosts=$BUGNET_X402_HOSTS; "+
		"curl -sS -o /dev/null -D - --connect-to %s:443:%s --cacert %s https://%s%s",
		filepath.Join(sb.fs, "etc", "bugnet", "secrets.env"), sellerHost, sb.fwd, s.egressCAFile,
		sellerHost, paidPath)
	ex := g.call("bugnet_exec", map[string]any{"handle": handle, "cmd": curl, "waitSeconds": 30})
	head := strings.ToLower(ex.str("stdout"))
	// The answer's environment reaches a new command with no restart
	if ex.IsError || !strings.Contains(head, "hosts="+sellerHost+"\n") {
		t.Fatalf("the new command lacks BUGNET_X402_HOSTS: %s", ex.text())
	}
	if !strings.Contains(head, " 200") || !strings.Contains(head, "bugnet-x402-status: paid") {
		t.Fatalf("the paid call from the sandbox failed: %s", ex.text())
	}

	// Exactly one seller payment, on the fake chain and in Payments' ledger
	var rows []payment
	for deadline := time.Now().Add(paidWait); ; time.Sleep(500 * time.Millisecond) {
		rows = s.rows(paidPath)
		if len(rows) == paidBefore+1 && rows[paidBefore].State == "confirmed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no single confirmed payment within %s: %+v", paidWait, rows)
		}
	}
	eq(t, rows[paidBefore].AmountMicros, strconv.Itoa(priceMicros))
	if rows[paidBefore].TxSignature == "" {
		t.Fatal("the confirmed payment has no transaction")
	}
	eq(t, s.sellerMicros(), before+priceMicros)

	stop := g.call("bugnet_stop", map[string]any{"handle": handle})
	said := stop.text()
	if stop.IsError || !strings.Contains(said, "spent in 1 paid calls") ||
		!strings.Contains(said, "goes back to the paying wallet") {
		t.Fatalf("the stop answer is not the paid one: %s", said)
	}
	// The answer's session link carries the record key, so only its money line is logged
	for _, l := range strings.Split(said, "\n") {
		if strings.HasPrefix(l, "x402:") {
			t.Logf("stop line: %s", l)
		}
	}
}
