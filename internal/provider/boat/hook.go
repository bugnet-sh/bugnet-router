package boat

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var _ provider.HookVerifier = (*Provider)(nil)

// defaultHookWindow is how far X-Ascii-Timestamp may be from now
const defaultHookWindow = 5 * time.Minute

// boatEvent is the part of a lifecycle webhook the router reads
type boatEvent struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"createdAt"`
	Data      struct {
		Sandbox struct {
			ID string `json:"id"`
		} `json:"sandbox"`
	} `json:"data"`
}

// VerifyHook checks X-Ascii-Signature over the delivery id, the timestamp and the exact body
func (p *Provider) VerifyHook(
	h http.Header, body []byte, secret []byte,
) (provider.HookEvent, error) {
	id, ts := h.Get("X-Ascii-Delivery"), h.Get("X-Ascii-Timestamp")
	sig, ok := strings.CutPrefix(h.Get("X-Ascii-Signature"), "v1=")
	sec, err := strconv.ParseInt(ts, 10, 64)
	if !ok || err != nil || p.clock().Sub(time.Unix(sec, 0)).Abs() > p.hookWindow {
		return provider.HookEvent{}, provider.ErrBadSignature
	}
	mac := hmac.New(sha256.New, secret) // the whole whsec_ string is the key
	mac.Write([]byte(id + "." + ts + "."))
	mac.Write(body)
	if !hmac.Equal([]byte(hex.EncodeToString(mac.Sum(nil))), []byte(sig)) {
		return provider.HookEvent{}, provider.ErrBadSignature
	}
	var ev boatEvent
	if err := provider.DecodeStrict(body, &ev); err != nil || ev.ID == "" ||
		ev.Data.Sandbox.ID == "" {
		return provider.HookEvent{}, provider.ErrBadSignature
	}
	return provider.HookEvent{
		ID: ev.ID, Type: ev.Type, SandboxID: ev.Data.Sandbox.ID, At: ev.CreatedAt,
	}, nil
}

// clock is the adapter's time source, which tests replace
func (p *Provider) clock() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}
