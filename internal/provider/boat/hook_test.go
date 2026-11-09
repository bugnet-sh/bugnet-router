package boat

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const boatBody = `{"id":"evt_0123456789abcdef0123456789abcdef","type":"sandbox.error",` +
	`"createdAt":"2026-11-08T10:00:00Z","data":{"sandbox":{"id":"bx_abcdefgh","name":"n"},` +
	`"state":"error"}}`

var hookNow = time.Date(2026, 11, 8, 10, 0, 5, 0, time.UTC)

func boatHeader(secret, id string, at time.Time, body string) http.Header {
	ts := strconv.FormatInt(at.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(id + "." + ts + "." + body))
	h := http.Header{}
	h.Set("X-Ascii-Delivery", id)
	h.Set("X-Ascii-Timestamp", ts)
	h.Set("X-Ascii-Signature", "v1="+hex.EncodeToString(mac.Sum(nil)))
	return h
}

func TestBoatVerifier(t *testing.T) {
	p := &Provider{hookWindow: 5 * time.Minute, now: func() time.Time { return hookNow }}
	const secret, previous = "whsec_current", "whsec_previous"
	const id = "evt_0123456789abcdef0123456789abcdef"

	ev, err := p.VerifyHook(boatHeader(secret, id, hookNow, boatBody), []byte(boatBody),
		[]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, ev, provider.HookEvent{
		ID: id, Type: "sandbox.error", SandboxID: "bx_abcdefgh",
		At: time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC),
	})
	// The previous secret passes when the caller tries it
	_, err = p.VerifyHook(boatHeader(previous, id, hookNow, boatBody), []byte(boatBody),
		[]byte(previous))
	eq(t, err, nil)

	noPrefix := boatHeader(secret, id, hookNow, boatBody)
	noPrefix.Set("X-Ascii-Signature", noPrefix.Get("X-Ascii-Signature")[3:])
	otherID := boatHeader(secret, id, hookNow, boatBody)
	otherID.Set("X-Ascii-Delivery", "evt_x")
	early := boatHeader(secret, id, hookNow.Add(301*time.Second), boatBody)
	late := boatHeader(secret, id, hookNow.Add(-301*time.Second), boatBody)
	badTS := boatHeader(secret, id, hookNow, boatBody)
	badTS.Set("X-Ascii-Timestamp", "soon")
	for name, c := range map[string]struct {
		h      http.Header
		body   string
		secret string
	}{
		"wrong key":   {boatHeader(secret, id, hookNow, boatBody), boatBody, previous},
		"301 s early": {early, boatBody, secret},
		"301 s late":  {late, boatBody, secret},
		"no v1=":      {noPrefix, boatBody, secret},
		"bad time":    {badTS, boatBody, secret},
		"other id":    {otherID, boatBody, secret},
		"body edited": {boatHeader(secret, id, hookNow, boatBody), boatBody + " ", secret},
		"trailing": {boatHeader(secret, id, hookNow, boatBody+"{}"), boatBody + "{}",
			secret},
	} {
		_, err := p.VerifyHook(c.h, []byte(c.body), []byte(c.secret))
		if !errors.Is(err, provider.ErrBadSignature) {
			t.Errorf("%s: got %v, want ErrBadSignature", name, err)
		}
	}
	// Just inside the window still passes
	_, err = p.VerifyHook(boatHeader(secret, id, hookNow.Add(-299*time.Second), boatBody),
		[]byte(boatBody), []byte(secret))
	eq(t, err, nil)
}
