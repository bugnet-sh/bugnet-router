package lium

import (
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var _ provider.HookVerifier = (*Provider)(nil)

const (
	// hookWindow is how far X-Lium-Timestamp may be from now, as Lium's docs ask
	hookWindow = 5 * time.Minute
	// graceStop is how long pods run once the balance reached zero
	graceStop = 2 * time.Minute
	// msStamp is the least timestamp read as milliseconds, which seconds reach only in 5138
	msStamp = 100_000_000_000
	// signaturePrefix starts X-Lium-Signature, the hex HMAC following it
	signaturePrefix = "sha256="
)

// The events of Lium's low-balance webhook, every one about the account
const (
	eventLowBalance   = "low_balance"
	eventGraceStarted = "balance_grace_started"
	eventPodsStopped  = "pods_stopped_insufficient_balance"
)

// liumEvent holds the fields of a low-balance webhook the router reads, their types unverified
type liumEvent struct {
	ID        json.RawMessage `json:"id"`
	Type      json.RawMessage `json:"type"`
	Event     json.RawMessage `json:"event"`
	CreatedAt json.RawMessage `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

// VerifyHook checks X-Lium-Signature over the timestamp and body, each event asking for the check
func (p *Provider) VerifyHook(
	h http.Header, body []byte, secret []byte,
) (provider.HookEvent, error) {
	ts := h.Get("X-Lium-Timestamp")
	hexSig, ok := strings.CutPrefix(h.Get("X-Lium-Signature"), signaturePrefix)
	sig, err := hex.DecodeString(hexSig)
	at, known := hookTime(ts)
	if len(secret) == 0 || !ok || err != nil || !known || p.now().Sub(at).Abs() > hookWindow {
		return provider.HookEvent{}, provider.ErrBadSignature
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts + "."))
	mac.Write(body)
	if !hmac.Equal(mac.Sum(nil), sig) {
		return provider.HookEvent{}, provider.ErrBadSignature
	}
	var ev liumEvent
	if err := provider.DecodeStrict(body, &ev); err != nil {
		return provider.HookEvent{}, provider.ErrBadSignature
	}
	kind := cmp.Or(stringOf(ev.Type), stringOf(ev.Event))
	if kind == "" {
		return provider.HookEvent{}, provider.ErrBadSignature
	}
	out := provider.HookEvent{ID: idOf(ev.ID), Type: provider.HookAccount, At: at.UTC()}
	if t, ok := timeOf(ev.CreatedAt); ok {
		out.At = t
	}
	if out.ID == "" {
		// Lium sends an event once, so the delivery itself names it for the router's dedupe
		sum := sha256.Sum256(append([]byte(ts+"."), body...))
		out.ID = "lium-" + hex.EncodeToString(sum[:16])
	}
	switch kind {
	case eventGraceStarted:
		var data struct {
			PodsStopAt json.RawMessage `json:"pods_stop_at"`
		}
		out.StopAt = out.At.Add(graceStop)
		if json.Unmarshal(ev.Data, &data) == nil {
			if t, ok := timeOf(data.PodsStopAt); ok {
				out.StopAt = t
			}
		}
	case eventLowBalance, eventPodsStopped:
	default:
		// Every event of the low-balance webhook is about the account, so a new one checks it too
		p.log.Warn("lium: a webhook event the adapter does not know, read as an account event",
			"event", kind, "id", out.ID)
	}
	return out, nil
}

// hookTime reads X-Lium-Timestamp in Unix seconds or milliseconds or as RFC 3339
func hookTime(ts string) (time.Time, bool) {
	if n, err := strconv.ParseInt(ts, 10, 64); err == nil {
		if n >= msStamp {
			return time.UnixMilli(n), true
		}
		return time.Unix(n, 0), true
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	return t, err == nil
}

// timeOf reads a time Lium writes as a JSON string
func timeOf(raw json.RawMessage) (time.Time, bool) {
	s := stringOf(raw)
	return parseTime(&s)
}

// stringOf reads a JSON string, empty for anything else
func stringOf(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

// idOf reads an event id written as a string or a number
func idOf(raw json.RawMessage) string {
	if s := stringOf(raw); s != "" {
		return s
	}
	var n json.Number
	if json.Unmarshal(raw, &n) != nil {
		return ""
	}
	return n.String()
}
