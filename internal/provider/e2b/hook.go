package e2b

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var _ provider.HookVerifier = (*Provider)(nil)

// e2bEvent is the part of a lifecycle webhook the router reads
type e2bEvent struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	SandboxID string    `json:"sandbox_id"`
	EventData struct {
		SandboxMetadata map[string]any `json:"sandbox_metadata"`
	} `json:"event_data"`
}

// VerifyHook checks e2b-signature, the unpadded base64 of SHA-256 over the secret and the body
func (p *Provider) VerifyHook(
	h http.Header, body []byte, secret []byte,
) (provider.HookEvent, error) {
	sum := sha256.New()
	sum.Write(secret)
	sum.Write(body)
	want := base64.RawStdEncoding.EncodeToString(sum.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(h.Get("e2b-signature")), []byte(want)) != 1 {
		return provider.HookEvent{}, provider.ErrBadSignature
	}
	// The hash is not an HMAC, so a forger extending a signed body is stopped by the strict parse
	var ev e2bEvent
	if err := provider.DecodeStrict(body, &ev); err != nil || ev.ID == "" || ev.SandboxID == "" {
		return provider.HookEvent{}, provider.ErrBadSignature
	}
	var labels map[string]string
	for k, v := range ev.EventData.SandboxMetadata {
		if s, ok := v.(string); ok {
			if labels == nil {
				labels = map[string]string{}
			}
			labels[k] = s
		}
	}
	return provider.HookEvent{
		ID: ev.ID, Type: ev.Type, SandboxID: ev.SandboxID, Labels: labels, At: ev.Timestamp,
	}, nil
}
