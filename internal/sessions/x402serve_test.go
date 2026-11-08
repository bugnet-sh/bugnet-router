package sessions

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const sigTx = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8u" +
	"irBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW"

func x402Event(id int64, sec int, data string) store.SessionEvent {
	return store.SessionEvent{ID: id, At: t0.Add(time.Duration(sec) * time.Second),
		Kind: "x402", Data: json.RawMessage(data)}
}

func resolvedEvent(id int64, data string) store.SessionEvent {
	return store.SessionEvent{ID: id, At: t0.Add(time.Minute), Kind: KindX402Resolved,
		Data: json.RawMessage(data)}
}

func serveRows(t *testing.T, events ...store.SessionEvent) []map[string]any {
	t.Helper()
	raw, err := json.Marshal(served(events))
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func paying(ref, payRef string, amount int) string {
	return fmt.Sprintf(`{"ref":%q,"payment_ref":%q,"host":"api.example.com","method":"GET",`+
		`"path":"/q","status":"paid","amount_micros":%d,"asset":%q,"network":"solana:x",`+
		`"pay_to":"11111111111111111111111111111111","scheme":"exact","count":1}`,
		ref, payRef, amount, usdcMint)
}

func TestServeMoneyOnceAndJoinByPaymentRef(t *testing.T) {
	rows := serveRows(t,
		x402Event(1, 0, paying("aaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaa", 10000)),
		x402Event(2, 1, paying("bbbbbbbbbbbbbbbb", "aaaaaaaaaaaaaaaa", 10000)),
		x402Event(3, 2, paying("cccccccccccccccc", "aaaaaaaaaaaaaaaa", 10000)),
		resolvedEvent(4, `{"ref":"aaaaaaaaaaaaaaaa","status":"paid","amount_micros":10000,`+
			`"tx":"`+sigTx+`"}`))
	eq(t, len(rows), 3)
	eq(t, rows[0]["amountMicros"], any(float64(10000)))
	eq(t, rows[0]["tx"], any(sigTx))
	eq(t, rows[0]["resent"], any(false))
	for _, r := range rows[1:] {
		eq(t, r["amountMicros"], any(float64(0)))
		eq(t, r["resent"], any(true))
		eq(t, r["resolved"], any("paid"))
		if _, ok := r["tx"]; ok {
			t.Fatal("a resent row serves a tx")
		}
	}
	eq(t, rows[0]["asset"], any("USDC"))
	raw, _ := json.Marshal(rows)
	for _, k := range []string{"payTo", "paymentRef", `"ref"`, "scheme", "serviceId"} {
		if strings.Contains(string(raw), k) {
			t.Fatalf("served rows hold %s", k)
		}
	}
}

func TestServeResolvedFallbackAndAlone(t *testing.T) {
	// A row egress never paid joins by its own ref
	rows := serveRows(t,
		x402Event(1, 0, `{"ref":"bbbbbbbbbbbbbbbb","status":"not_paid",`+
			`"reason":"payments_unavailable","host":"h","method":"GET","path":"/","count":1}`),
		resolvedEvent(2, `{"ref":"bbbbbbbbbbbbbbbb","status":"released"}`))
	eq(t, len(rows), 1)
	eq(t, rows[0]["status"], any("not_paid"))
	eq(t, rows[0]["resolved"], any("released"))
	// A resolve with no row is served on its own until a row arrives
	rows = serveRows(t, resolvedEvent(1, `{"ref":"cccccccccccccccc","status":"paid",`+
		`"amount_micros":500,"tx":"`+sigTx+`","host":"h","method":"GET","path":"/"}`))
	eq(t, len(rows), 1)
	eq(t, rows[0]["status"], any("uncertain"))
	eq(t, rows[0]["tx"], any(sigTx))
	eq(t, rows[0]["amountMicros"], any(float64(500)))
	// A refusal row serves every key, with null resolved
	rows = serveRows(t, x402Event(1, 0,
		`{"ref":"dddddddddddddddd","status":"refused_host","count":3}`))
	eq(t, len(rows[0]), 18)
	eq(t, rows[0]["resolved"], any(nil))
}
