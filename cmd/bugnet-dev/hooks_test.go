package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
)

const (
	hookUUID = "0b3c4d5e-6f70-4812-9a3b-4c5d6e7f8091"
	boatID   = "wh_0123456789abcdef01234567"
)

var hookPath = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))

// hookAPI is a provider webhook API that records each call and answers the shapes the CLI reads
type hookAPI struct {
	mu    sync.Mutex
	calls []string
	auth  []string
	body  map[string]any
	url   string // the endpoint listed
}

func (a *hookAPI) log(r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, r.Method+" "+r.URL.Path)
	a.auth = append(a.auth, r.Header.Get("X-API-Key")+r.Header.Get("Authorization"))
	a.body = nil
	if raw, _ := io.ReadAll(r.Body); len(raw) > 0 {
		_ = json.Unmarshal(raw, &a.body)
	}
}

func jsonReply(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (a *hookAPI) e2b(t *testing.T) string {
	detail := func() map[string]any {
		return map[string]any{
			"id": hookUUID, "teamId": "team-1", "name": "bugnet", "url": a.url,
			"enabled": true, "createdAt": "2026-11-08T10:00:00Z",
			"events": []string{"sandbox.lifecycle.killed", "sandbox.lifecycle.paused"},
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events/webhooks", func(w http.ResponseWriter, r *http.Request) {
		a.log(r)
		jsonReply(w, 201, detail())
	})
	mux.HandleFunc("GET /events/webhooks", func(w http.ResponseWriter, r *http.Request) {
		a.log(r)
		jsonReply(w, 200, []any{detail()})
	})
	mux.HandleFunc("GET /events/webhooks/{id}/deliveries",
		func(w http.ResponseWriter, r *http.Request) {
			a.log(r)
			jsonReply(w, 200, map[string]any{"nextCursor": nil, "data": []any{map[string]any{
				"eventId": hookUUID, "eventType": "sandbox.lifecycle.killed", "sandboxId": "sb",
				"attempts": []any{map[string]any{
					"timestamp": "2026-11-08T10:01:00Z", "eventType": "sandbox.lifecycle.killed",
					"status": "success", "responseHttpStatusCode": 204,
				}},
			}}})
		})
	mux.HandleFunc("PATCH /events/webhooks/{id}", func(w http.ResponseWriter, r *http.Request) {
		a.log(r)
		jsonReply(w, 200, detail())
	})
	mux.HandleFunc("DELETE /events/webhooks/{id}", func(w http.ResponseWriter, r *http.Request) {
		a.log(r)
		w.WriteHeader(204)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

func (a *hookAPI) boat(t *testing.T) string {
	hook := func() map[string]any {
		return map[string]any{
			"id": boatID, "name": "bugnet", "url": a.url, "createdAt": "2026-11-08T10:00:00Z",
			"updatedAt": "2026-11-08T10:00:00Z",
			"events":    []string{"sandbox.error", "sandbox.archived"},
		}
	}
	secret := func(s string) map[string]any {
		return map[string]any{"ok": true, "type": "webhook.created", "secret": s, "webhook": hook()}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks", func(w http.ResponseWriter, r *http.Request) {
		a.log(r)
		jsonReply(w, 201, secret("whsec_new"))
	})
	mux.HandleFunc("GET /webhooks", func(w http.ResponseWriter, r *http.Request) {
		a.log(r)
		jsonReply(w, 200, map[string]any{"ok": true, "type": "webhook.list",
			"webhooks": []any{hook()}})
	})
	mux.HandleFunc("POST /webhooks/{id}/rotate", func(w http.ResponseWriter, r *http.Request) {
		a.log(r)
		jsonReply(w, 200, secret("whsec_rotated"))
	})
	mux.HandleFunc("DELETE /webhooks/{id}", func(w http.ResponseWriter, r *http.Request) {
		a.log(r)
		jsonReply(w, 200, map[string]any{"ok": true, "type": "webhook.deleted", "id": boatID})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// runHooks runs "hooks args" as a cli whose prompts answer from asks in order
func runHooks(
	t *testing.T, vars map[string]string, base map[string]string, asks []string, args ...string,
) (string, string, []string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	var labels []string
	c := &cli{
		ctx: t.Context(), stdout: &stdout, stderr: &stderr, getenv: env(vars),
		configPath: writeSettings(t, "[names]\nhooks_host = \"hooks.test.example\"\n"),
		hookBase:   base,
		ask: func(label string) (string, error) {
			labels = append(labels, label)
			v := asks[0]
			asks = asks[1:]
			return v, nil
		},
	}
	err := c.hooks(args)
	return stdout.String(), stderr.String(), labels, err
}

func TestHooksE2B(t *testing.T) {
	api := &hookAPI{url: "https://hooks.test.example/hooks/e2b/" + hookPath}
	base := map[string]string{"e2b": api.e2b(t)}
	vars := map[string]string{"E2B_API_KEY": "e2b_key", "BUGNET_HOOK_PATH_SECRET": hookPath}

	out, _, asked, err := runHooks(t, vars, base, nil, "register", "e2b")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(asked), 0)
	if !strings.Contains(out, "E2B_WEBHOOK_SECRET=") ||
		!strings.Contains(out, `team_id = "team-1"`) {
		t.Fatalf("register printed %q", out)
	}
	secret := strings.TrimPrefix(strings.SplitN(out, "\n", 2)[0], "E2B_WEBHOOK_SECRET=")
	raw, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil || len(raw) != 32 {
		t.Fatalf("the secret %q is not 32 random bytes", secret)
	}
	eq[any](t, api.body["signatureSecret"], secret)
	eq[any](t, api.body["url"], "https://hooks.test.example/hooks/e2b/"+hookPath)
	same(t, api.body["events"], []any{"sandbox.lifecycle.killed", "sandbox.lifecycle.paused"})
	eq(t, api.auth[0], "e2b_key")

	out, _, _, err = runHooks(t, vars, base, nil, "list", "e2b")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, hookPath) {
		t.Fatalf("list printed the path secret: %q", out)
	}
	for _, want := range []string{hookUUID, "https://hooks.test.example/hooks/e2b/<path secret>",
		"2026-11-08T10:01:00Z sandbox.lifecycle.killed success http 204"} {
		if !strings.Contains(out, want) {
			t.Errorf("list output lacks %q: %q", want, out)
		}
	}

	out, _, _, err = runHooks(t, vars, base, nil, "rotate", "e2b")
	if err != nil || !strings.HasPrefix(out, "E2B_WEBHOOK_SECRET=") {
		t.Fatalf("rotate printed %q, %v", out, err)
	}
	_, _, _, err = runHooks(t, vars, base, nil, "delete", "e2b", hookUUID)
	if err != nil {
		t.Fatal(err)
	}
	last := api.calls[len(api.calls)-1]
	eq(t, last, "DELETE /events/webhooks/"+hookUUID)
	_, _, _, err = runHooks(t, vars, base, nil, "delete", "e2b", "nope")
	if err == nil {
		t.Fatal("delete took a bad id")
	}
}

func TestHooksBoatAsksForTheKeyWithEchoOff(t *testing.T) {
	api := &hookAPI{url: "https://hooks.test.example/hooks/boat/" + hookPath}
	base := map[string]string{"boat": api.boat(t)}
	vars := map[string]string{}

	out, stderr, asked, err := runHooks(t, vars, base, []string{hookPath, "boat_admin"},
		"register", "boat")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(asked, []string{"BUGNET_HOOK_PATH_SECRET: ", "Boat admin API key: "}) {
		t.Fatalf("asked %v", asked)
	}
	eq(t, out, "BOAT_WEBHOOK_SECRET=whsec_new\n")
	eq(t, api.auth[0], "Bearer boat_admin")
	same(t, api.body["events"], []any{
		"sandbox.error", "sandbox.archived", "sandbox.degraded", "sandbox.recovered"})
	if strings.Contains(out+stderr, "boat_admin") || strings.Contains(out+stderr, hookPath) {
		t.Fatal("a typed secret was printed")
	}

	// The environment wins and nothing is asked
	vars = map[string]string{"BOAT_ADMIN_API_KEY": "boat_env", "BUGNET_HOOK_PATH_SECRET": hookPath}
	out, _, asked, err = runHooks(t, vars, base, nil, "list", "boat")
	if err != nil || len(asked) != 0 {
		t.Fatalf("list: %v, asked %v", err, asked)
	}
	if strings.Contains(out, hookPath) || !strings.Contains(out, boatID) {
		t.Fatalf("list printed %q", out)
	}
	out, _, _, err = runHooks(t, vars, base, nil, "rotate", "boat")
	eq(t, err, nil)
	eq(t, out, "BOAT_WEBHOOK_SECRET=whsec_rotated\n")
	eq(t, api.calls[len(api.calls)-1], "POST /webhooks/"+boatID+"/rotate")
	_, _, _, err = runHooks(t, vars, base, nil, "delete", "boat", boatID)
	eq(t, err, nil)
	eq(t, api.calls[len(api.calls)-1], "DELETE /webhooks/"+boatID)
}

func TestHooksRefusals(t *testing.T) {
	vars := map[string]string{"E2B_API_KEY": "k", "BUGNET_HOOK_PATH_SECRET": "c2hvcnQ"}
	for _, c := range []struct {
		args []string
		want string
	}{
		{nil, "register, list, rotate or delete"},
		{[]string{"enable", "e2b"}, "register, list, rotate or delete"},
		{[]string{"register", "koyeb"}, "e2b and boat"},
		{[]string{"delete", "e2b"}, "a provider and an endpoint id"},
		{[]string{"register", "e2b"}, "BUGNET_HOOK_PATH_SECRET must be 32 bytes"},
	} {
		_, _, _, err := runHooks(t, vars, nil, nil, c.args...)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: got %v, want %q", c.args, err, c.want)
		}
	}
}

// same fails the test unless got and want print alike
func same(t *testing.T, got, want any) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
