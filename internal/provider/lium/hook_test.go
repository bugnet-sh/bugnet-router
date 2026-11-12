package lium

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// hookSecret and hookPrevious stand in for LIUM_WEBHOOK_SECRET and its _PREVIOUS
	hookSecret   = "lium-hook-secret-now"
	hookPrevious = "lium-hook-secret-old"
)

// liumSigned signs body as Lium does at the timestamp ts
func liumSigned(secret, ts, body string) http.Header {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts + "." + body))
	h := http.Header{}
	h.Set("X-Lium-Timestamp", ts)
	h.Set("X-Lium-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	return h
}

// unix writes t as Unix seconds
func unix(t time.Time) string { return strconv.FormatInt(t.Unix(), 10) }

// hookFake is an adapter whose clock stands at the test clock's time, its warnings in logs
func hookFake(t *testing.T) (*Provider, *clock, *bytes.Buffer) {
	t.Helper()
	f := newFake(t)
	clk := newClock()
	var logs bytes.Buffer
	opts := testOptions()
	opts.Log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return f.provider(t, opts, clk.now), clk, &logs
}

func TestLiumWebhook(t *testing.T) {
	p, clk, logs := hookFake(t)
	var _ provider.HookVerifier = p
	now := clk.now()
	ts := unix(now)
	verify := func(h http.Header, body, secret string) (provider.HookEvent, error) {
		return p.VerifyHook(h, []byte(body), []byte(secret))
	}
	bad := func(t *testing.T, h http.Header, body, secret string) {
		t.Helper()
		if ev, err := verify(h, body, secret); !errors.Is(err, provider.ErrBadSignature) {
			t.Fatalf("got %+v, %v, want a bad signature", ev, err)
		}
	}

	t.Run("each account event asks for the account check", func(t *testing.T) {
		at := now.Add(-30 * time.Second)
		for _, tc := range []struct {
			body string
			want provider.HookEvent
		}{
			{
				`{"id":"evt_low","type":"low_balance","created_at":"2026-11-11T11:59:30Z",` +
					`"data":{"balance":4.2,"threshold":10}}`,
				provider.HookEvent{ID: "evt_low", Type: provider.HookAccount, At: at},
			},
			{
				`{"id":"evt_grace","type":"balance_grace_started",` +
					`"created_at":"2026-11-11T11:59:30Z",` +
					`"data":{"balance":0,"pods_stop_at":"2026-11-11T12:01:30Z"}}`,
				provider.HookEvent{
					ID: "evt_grace", Type: provider.HookAccount, At: at,
					StopAt: at.Add(2 * time.Minute),
				},
			},
			{
				`{"id":"evt_stopped","type":"pods_stopped_insufficient_balance",` +
					`"created_at":"2026-11-11T11:59:30Z","data":{"pod_ids":["p-1","p-2"]}}`,
				provider.HookEvent{ID: "evt_stopped", Type: provider.HookAccount, At: at},
			},
		} {
			ev, err := verify(liumSigned(hookSecret, ts, tc.body), tc.body, hookSecret)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, ev, tc.want)
		}
		eq(t, logs.String(), "")
	})
	t.Run("a grace event without its stop time stops 2 minutes after it", func(t *testing.T) {
		for _, body := range []string{
			`{"id":"evt_g2","event":"balance_grace_started","data":{}}`,
			`{"id":"evt_g2","type":"balance_grace_started","data":{"pods_stop_at":1762862520}}`,
			`{"id":"evt_g2","type":"balance_grace_started","data":"soon","created_at":7}`,
			`{"id":"evt_g2","type":"balance_grace_started","created_at":"yesterday"}`,
		} {
			ev, err := verify(liumSigned(hookSecret, ts, body), body, hookSecret)
			if err != nil {
				t.Fatalf("%s: %v", body, err)
			}
			eq(t, ev, provider.HookEvent{
				ID: "evt_g2", Type: provider.HookAccount, At: now,
				StopAt: now.Add(2 * time.Minute),
			})
		}
		// An id written as a number is read as its digits
		body := `{"id":12345,"type":"low_balance"}`
		ev, err := verify(liumSigned(hookSecret, ts, body), body, hookSecret)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, ev.ID, "12345")
	})
	t.Run("an event with no id gets one of its delivery", func(t *testing.T) {
		body := `{"type":"low_balance","data":{"balance":4.2}}`
		first, err := verify(liumSigned(hookSecret, ts, body), body, hookSecret)
		if err != nil {
			t.Fatal(err)
		}
		again, _ := verify(liumSigned(hookSecret, ts, body), body, hookSecret)
		other := `{"type":"low_balance","data":{"balance":4.1}}`
		next, _ := verify(liumSigned(hookSecret, ts, other), other, hookSecret)
		if first.ID == "" || first.ID != again.ID || first.ID == next.ID {
			t.Fatalf("ids %q %q %q", first.ID, again.ID, next.ID)
		}
		eq(t, first.Type, provider.HookAccount)
	})
	t.Run("an event Lium adds later still asks for the check", func(t *testing.T) {
		body := `{"id":"evt_new","type":"balance_restored"}`
		ev, err := verify(liumSigned(hookSecret, ts, body), body, hookSecret)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, ev, provider.HookEvent{ID: "evt_new", Type: provider.HookAccount, At: now})
		if !strings.Contains(logs.String(), "balance_restored") {
			t.Fatalf("log %q", logs.String())
		}
	})

	body := `{"id":"evt_low","type":"low_balance","data":{"balance":4.2}}`
	t.Run("signature", func(t *testing.T) {
		h := liumSigned(hookSecret, ts, body)
		bad(t, h, body, "another secret")
		bad(t, h, body, "")
		bad(t, h, strings.Replace(body, "4.2", "42", 1), hookSecret)
		bad(t, h, body+" ", hookSecret)
		for _, sig := range []string{
			strings.TrimPrefix(h.Get("X-Lium-Signature"), "sha256="),
			"sha1=" + strings.TrimPrefix(h.Get("X-Lium-Signature"), "sha256="),
			"sha256=", "sha256=zz", "",
		} {
			forged := h.Clone()
			forged.Set("X-Lium-Signature", sig)
			bad(t, forged, body, hookSecret)
		}
		// The timestamp is signed too
		moved := h.Clone()
		moved.Set("X-Lium-Timestamp", unix(now.Add(-time.Second)))
		bad(t, moved, body, hookSecret)
		// An empty secret signs nothing, whatever the header says
		bad(t, liumSigned("", ts, body), body, "")
		// Hex in capitals is the same signature
		upper := h.Clone()
		upper.Set("X-Lium-Signature", "sha256="+strings.ToUpper(
			strings.TrimPrefix(h.Get("X-Lium-Signature"), "sha256=")))
		if _, err := verify(upper, body, hookSecret); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("the previous secret during rotation", func(t *testing.T) {
		h := liumSigned(hookPrevious, ts, body)
		// The router tries the current secret first and the previous one on a bad signature
		bad(t, h, body, hookSecret)
		ev, err := verify(h, body, hookPrevious)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, ev.Type, provider.HookAccount)
	})
	t.Run("timestamp window", func(t *testing.T) {
		for _, at := range []time.Time{now.Add(-5 * time.Minute), now.Add(4 * time.Minute)} {
			if _, err := verify(liumSigned(hookSecret, unix(at), body), body,
				hookSecret); err != nil {
				t.Fatalf("at %v: %v", at, err)
			}
		}
		for _, at := range []time.Time{
			now.Add(-5*time.Minute - time.Second), now.Add(5*time.Minute + time.Second),
			now.Add(-24 * time.Hour),
		} {
			bad(t, liumSigned(hookSecret, unix(at), body), body, hookSecret)
		}
		for _, stamp := range []string{"", "soon", "1.7e9", unix(now) + " "} {
			bad(t, liumSigned(hookSecret, stamp, body), body, hookSecret)
		}
		// Milliseconds and RFC 3339 are read too, Lium's format being unverified
		for _, stamp := range []string{
			strconv.FormatInt(now.UnixMilli(), 10), now.Format(time.RFC3339),
		} {
			if _, err := verify(liumSigned(hookSecret, stamp, body), body,
				hookSecret); err != nil {
				t.Fatalf("stamp %s: %v", stamp, err)
			}
		}
		clk.add(5*time.Minute + time.Second)
		bad(t, liumSigned(hookSecret, ts, body), body, hookSecret)
		clk.add(-5*time.Minute - time.Second)
	})
	t.Run("bodies that are no event", func(t *testing.T) {
		for _, b := range []string{
			"", "not json", `["low_balance"]`, `{"id":"e"}`, `{"type":""}`,
			body + body, `{"type":7}`,
		} {
			bad(t, liumSigned(hookSecret, ts, b), b, hookSecret)
		}
	})
}
