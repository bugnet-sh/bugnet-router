package e2b

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const hookBody = `{"id":"evt-1","type":"sandbox.lifecycle.killed",` +
	`"timestamp":"2026-11-08T10:00:00Z","sandbox_id":"isb1",` +
	`"event_data":{"sandbox_metadata":{"placement":"p1","n":3},` +
	`"kill_reason":"admin"}}`

func e2bSig(secret, body string) string {
	sum := sha256.Sum256([]byte(secret + body))
	return base64.RawStdEncoding.EncodeToString(sum[:])
}

func hookHeader(sig string) http.Header {
	h := http.Header{}
	h.Set("e2b-signature", sig)
	return h
}

func TestE2BVerifier(t *testing.T) {
	p := &Provider{}
	secret := "whsec-secret"
	ev, err := p.VerifyHook(hookHeader(e2bSig(secret, hookBody)), []byte(hookBody), []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, ev, provider.HookEvent{
		ID: "evt-1", Type: "sandbox.lifecycle.killed", SandboxID: "isb1",
		Labels: map[string]string{"placement": "p1"},
		At:     time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC),
	})

	good := e2bSig(secret, hookBody)
	extended := hookBody + "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x58" +
		`{"id":"evt-2"}`
	for name, c := range map[string]struct{ sig, body, secret string }{
		"wrong secret": {e2bSig("other", hookBody), hookBody, secret},
		"padded":       {good + "=", hookBody, secret},
		"spaced":       {" " + good, hookBody, secret},
		"empty":        {"", hookBody, secret},
		"body changed": {good, hookBody + " ", secret},
		"extended":     {e2bSig(secret, extended), extended, secret},
		"not json":     {e2bSig(secret, "x"), "x", secret},
		"no id":        {e2bSig(secret, `{"sandbox_id":"a"}`), `{"sandbox_id":"a"}`, secret},
	} {
		_, err := p.VerifyHook(hookHeader(c.sig), []byte(c.body), []byte(c.secret))
		if !errors.Is(err, provider.ErrBadSignature) {
			t.Errorf("%s: got %v, want ErrBadSignature", name, err)
		}
	}
}
