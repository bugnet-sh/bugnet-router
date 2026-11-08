package testseller

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// feePayerLabel is the label X34's golden derives its fee payer from
const feePayerLabel = "bugnet fakeseller test fee payer"

// chain is a fake Solana JSON-RPC that records what the facilitator asks
type chain struct {
	mu    sync.Mutex
	calls []rpcCall
}

type rpcCall struct {
	Method string
	Params []json.RawMessage
}

func (c *chain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	raw, _ := io.ReadAll(r.Body)
	if json.Unmarshal(raw, &req) != nil {
		http.Error(w, "bad", http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	c.calls = append(c.calls, rpcCall{Method: req.Method, Params: req.Params})
	c.mu.Unlock()
	var result any
	switch req.Method {
	case "simulateTransaction":
		result = map[string]any{
			"context": map[string]any{"slot": 100},
			"value":   map[string]any{"err": nil, "logs": []string{}, "unitsConsumed": 5000},
		}
	case "sendTransaction":
		var b64 string
		_ = json.Unmarshal(req.Params[0], &b64)
		tx, _ := base64.StdEncoding.DecodeString(b64)
		result = base58(tx[1:65])
	case "getSignatureStatuses":
		result = map[string]any{
			"context": map[string]any{"slot": 101},
			"value": []any{map[string]any{
				"slot": 101, "confirmations": nil, "err": nil, "confirmationStatus": "confirmed",
			}},
		}
	case "getLatestBlockhash":
		result = map[string]any{
			"context": map[string]any{"slot": 100},
			"value": map[string]any{
				"blockhash": "11111111111111111111111111111111", "lastValidBlockHeight": 1000,
			},
		}
	default:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0", "id": req.ID,
			"error": map[string]any{"code": -32601, "message": "method not found: " + req.Method},
		})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
}

func (c *chain) methods() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, call := range c.calls {
		out = append(out, call.Method)
	}
	return out
}

const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// base58 encodes b, with a 1 for each leading zero byte
func base58(b []byte) string {
	n := new(big.Int).SetBytes(b)
	var out []byte
	mod := new(big.Int)
	for n.Sign() > 0 {
		n.DivMod(n, big.NewInt(58), mod)
		out = append([]byte{alphabet[mod.Int64()]}, out...)
	}
	for _, x := range b {
		if x != 0 {
			break
		}
		out = append([]byte{'1'}, out...)
	}
	return string(out)
}

// startFacilitator runs server.mjs on 127.0.0.1:0 against rpc and returns its base URL
func startFacilitator(t *testing.T, rpc string) string {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	if _, err := os.Stat("facilitator/node_modules/@x402/svm"); err != nil {
		t.Skip("run pnpm install in facilitator first")
	}
	seed := sha256.Sum256([]byte(feePayerLabel))
	key := ed25519.NewKeyFromSeed(seed[:])
	ints := make([]int, len(key))
	for i, b := range key {
		ints[i] = int(b)
	}
	keyJSON, _ := json.Marshal(ints)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, node, "server.mjs")
	cmd.Dir = "facilitator"
	cmd.Env = []string{
		"TESTSELLER_FEE_PAYER_KEY=" + string(keyJSON), "FACILITATOR_ADDR=127.0.0.1:0",
		"SOLANA_RPC_URL=" + rpc, "PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
	}
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); _ = cmd.Wait() })
	line := make(chan string, 1)
	go func() {
		s, _ := bufio.NewReader(out).ReadString('\n')
		line <- s
	}()
	select {
	case s := <-line:
		addr, ok := strings.CutPrefix(strings.TrimSpace(s), "facilitator listening ")
		if !ok {
			t.Fatalf("facilitator said %q, stderr %q", s, stderr.String())
		}
		return "http://" + addr
	case <-time.After(30 * time.Second):
		t.Fatalf("facilitator did not start, stderr %q", stderr.String())
	}
	return ""
}

func post(t *testing.T, url string, body []byte) map[string]any {
	t.Helper()
	res, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// golden is the path of Task X34's verify or settle body
func golden(name string) string {
	return filepath.Join("facilitator", "testdata", "facilitator-"+name+".json")
}

func TestFacilitatorSettlesX34Transfer(t *testing.T) {
	rpc := &chain{}
	srv := httptest.NewServer(rpc)
	defer srv.Close()
	base := startFacilitator(t, srv.URL)
	verifyBody, err := os.ReadFile(golden("verify"))
	if err != nil {
		t.Fatal(err)
	}
	settleBody, err := os.ReadFile(golden("settle"))
	if err != nil {
		t.Fatal(err)
	}
	if got := post(t, base+"/verify", verifyBody); got["isValid"] != true {
		t.Fatalf("verify answered %v, rpc calls %v", got, rpc.methods())
	}
	got := post(t, base+"/settle", settleBody)
	if got["success"] != true {
		t.Fatalf("settle answered %v, rpc calls %v", got, rpc.methods())
	}
	var golden struct {
		Payload struct {
			Payload struct {
				Transaction string `json:"transaction"`
			} `json:"payload"`
		} `json:"paymentPayload"`
	}
	if err := json.Unmarshal(settleBody, &golden); err != nil {
		t.Fatal(err)
	}
	want, _ := base64.StdEncoding.DecodeString(golden.Payload.Payload.Transaction)
	var sent []byte
	n := 0
	rpc.mu.Lock()
	for _, c := range rpc.calls {
		if c.Method == "sendTransaction" {
			var b64 string
			_ = json.Unmarshal(c.Params[0], &b64)
			sent, _ = base64.StdEncoding.DecodeString(b64)
			n++
		}
	}
	rpc.mu.Unlock()
	if n != 1 {
		t.Fatalf("%d sendTransaction calls, want 1 (calls %v)", n, rpc.methods())
	}
	if len(sent) != len(want) || !bytes.Equal(sent[65:], want[65:]) {
		t.Fatal("the sent transaction is not the golden's message and second signature")
	}
	if bytes.Equal(sent[1:65], want[1:65]) {
		t.Fatal("the fee payer did not sign")
	}
	if got["transaction"] != base58(sent[1:65]) {
		t.Fatalf("settle answered transaction %v", got["transaction"])
	}
}
