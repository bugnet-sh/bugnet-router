package api_test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/api"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/e2b"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

const (
	hookSecret   = "hook-secret-now"
	hookPrevious = "hook-secret-old"
)

// hooked is a provider that verifies webhooks with E2B's real verifier
type hooked struct {
	faulty
	verify e2b.Provider
}

func (h hooked) VerifyHook(
	hd http.Header, body, secret []byte,
) (provider.HookEvent, error) {
	return h.verify.VerifyHook(hd, body, secret)
}

func hookOptions() options {
	return options{
		providerName: "e2b",
		wrap: func(l *local.Provider, _ string) provider.Provider {
			return hooked{faulty: faulty{Provider: l}}
		},
		hooks: config.Hooks{Providers: []string{"e2b"}, HintUnique: 10 * time.Second},
		hookSecrets: map[string]config.HookSecret{
			"e2b": {Current: []byte(hookSecret), Previous: []byte(hookPrevious)},
		},
	}
}

func hookBody(event, sandbox string) []byte {
	return []byte(`{"id":"` + event + `","type":"sandbox.lifecycle.killed",` +
		`"timestamp":"2026-11-08T10:00:00Z","sandbox_id":"` + sandbox + `","event_data":` +
		`{"sandbox_metadata":{"placement":"p1","bugnet_deployment":"main"}}}`)
}

func hookRequest(name, secret string, body []byte) *routerv1.DeliverHookRequest {
	sum := sha256.Sum256(append([]byte(secret), body...))
	return &routerv1.DeliverHookRequest{
		Provider: name, Body: body, ClientIp: "203.0.113.9",
		Headers: []*routerv1.HeaderValue{
			{Name: "e2b-signature", Value: base64.RawStdEncoding.EncodeToString(sum[:])},
		},
	}
}

func (e *env) hookJobs(t *testing.T) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(),
		`select count(*) from river_job where kind = 'hook_hint'`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *env) deliver(
	t *testing.T, req *routerv1.DeliverHookRequest,
) (*routerv1.DeliverHookResponse, error) {
	t.Helper()
	resp, err := e.edge(t).DeliverHook(t.Context(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func TestDeliverHook(t *testing.T) {
	e := newEnv(t, hookOptions())

	// A provider outside hooks.providers has no verifier
	_, err := e.deliver(t, hookRequest("boat", hookSecret, hookBody("evt-1", "sb1")))
	mustCode(t, err, connect.CodeNotFound, "HOOK_UNKNOWN_PROVIDER")
	_, err = e.deliver(t, hookRequest("nope", hookSecret, hookBody("evt-1", "sb1")))
	mustCode(t, err, connect.CodeNotFound, "HOOK_UNKNOWN_PROVIDER")

	_, err = e.deliver(t, hookRequest("e2b", "wrong", hookBody("evt-1", "sb1")))
	mustCode(t, err, connect.CodePermissionDenied, "HOOK_BAD_SIGNATURE")
	eq(t, e.hookJobs(t), 0)

	resp, err := e.deliver(t, hookRequest("e2b", hookSecret, hookBody("evt-1", "sb1")))
	eq(t, err, nil)
	eq(t, resp.Status, uint32(204))
	eq(t, e.hookJobs(t), 1)

	// The same event again is a duplicate
	resp, err = e.deliver(t, hookRequest("e2b", hookSecret, hookBody("evt-1", "sb1")))
	eq(t, err, nil)
	eq(t, resp.Status, uint32(204))
	eq(t, e.hookJobs(t), 1)

	// Another event for the same sandbox inside the window folds into the waiting hint
	_, err = e.deliver(t, hookRequest("e2b", hookSecret, hookBody("evt-2", "sb1")))
	eq(t, err, nil)
	eq(t, e.hookJobs(t), 1)

	// The previous secret still passes, and another sandbox gets its own hint
	_, err = e.deliver(t, hookRequest("e2b", hookPrevious, hookBody("evt-3", "sb2")))
	eq(t, err, nil)
	eq(t, e.hookJobs(t), 2)

	var args string
	err = e.pool.QueryRow(t.Context(), `select args::text from river_job
		where kind = 'hook_hint' order by id limit 1`).Scan(&args)
	eq(t, err, nil)
	for _, want := range []string{`"sandbox_id": "sb1"`, `"placement_label": "p1"`,
		`"deployment_label": "main"`, `"event_id": "evt-1"`} {
		if !strings.Contains(args, want) {
			t.Errorf("args %s lack %s", args, want)
		}
	}
}

func TestDeliverHookLaterHintRunsAgain(t *testing.T) {
	opts := hookOptions()
	opts.hooks.HintUnique = time.Second
	e := newEnv(t, opts)
	_, err := e.deliver(t, hookRequest("e2b", hookSecret, hookBody("evt-1", "sb1")))
	eq(t, err, nil)
	// Once the first hint ran, a later event for the sandbox queues another
	if _, err := e.pool.Exec(t.Context(),
		`update river_job set state = 'completed', finalized_at = now() where kind = 'hook_hint'`,
	); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	_, err = e.deliver(t, hookRequest("e2b", hookSecret, hookBody("evt-2", "sb1")))
	eq(t, err, nil)
	eq(t, e.hookJobs(t), 2)
}

func TestDeliverHookPostgresDown(t *testing.T) {
	e := newEnv(t, hookOptions())
	e.pool.Close()
	_, err := e.deliver(t, hookRequest("e2b", hookSecret, hookBody("evt-9", "sb9")))
	mustCode(t, err, connect.CodeUnavailable, "")
	// The provider's retry of the same event must not be taken for a duplicate
	key := e.prefix + "hook:e2b:evt-9"
	err = e.vk.Do(t.Context(), e.vk.B().Get().Key(key).Build()).Error()
	if !valkey.IsValkeyNil(err) {
		t.Fatalf("the dedupe mark stayed after a failed insert: %v", err)
	}
}

func TestStartRefusesHookProviderWithoutSecret(t *testing.T) {
	e := newEnv(t, hookOptions())
	reg := e.registry
	secrets := map[string]config.HookSecret{"e2b": {Current: []byte("s")}}
	eq(t, api.CheckHooks(config.Hooks{Providers: []string{"e2b"}}, secrets, reg), nil)

	err := api.CheckHooks(config.Hooks{Providers: []string{"e2b"}}, nil, reg)
	if err == nil || !strings.Contains(err.Error(), "hooks.providers lists e2b") {
		t.Fatalf("no secret: %v", err)
	}
	err = api.CheckHooks(config.Hooks{Providers: []string{"boat"}},
		map[string]config.HookSecret{"boat": {Current: []byte("s")}}, reg)
	if err == nil || !strings.Contains(err.Error(), "not in providers") {
		t.Fatalf("not a provider: %v", err)
	}
	plain := newEnv(t, options{})
	err = api.CheckHooks(config.Hooks{Providers: []string{"local"}},
		map[string]config.HookSecret{"local": {Current: []byte("s")}}, plain.registry)
	if err == nil || !strings.Contains(err.Error(), "verifies no webhooks") {
		t.Fatalf("no verifier: %v", err)
	}
}

func TestStartWantsLiumWebhookSecretOnlyWhileListed(t *testing.T) {
	reg, err := provider.NewRegistry(nil, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, api.CheckHooks(config.Hooks{}, nil, reg), nil)
	err = api.CheckHooks(config.Hooks{Providers: []string{"lium"}}, nil, reg)
	if err == nil || !strings.Contains(err.Error(),
		"hooks.providers lists lium but its webhook secret is not set") {
		t.Fatalf("lium without LIUM_WEBHOOK_SECRET: %v", err)
	}
	// With the secret set, the check goes on to the adapter, which this registry lacks
	secrets := map[string]config.HookSecret{"lium": {Current: []byte("whsec")}}
	err = api.CheckHooks(config.Hooks{Providers: []string{"lium"}}, secrets, reg)
	if err == nil || !strings.Contains(err.Error(), "not in providers") {
		t.Fatalf("lium with its secret: %v", err)
	}
}

// accountHooked verifies a test webhook whose events are about the provider account alone
type accountHooked struct{ faulty }

func (accountHooked) VerifyHook(
	h http.Header, body, secret []byte,
) (provider.HookEvent, error) {
	if h.Get("X-Test-Signature") != string(secret) {
		return provider.HookEvent{}, provider.ErrBadSignature
	}
	var ev struct {
		ID     string    `json:"id"`
		Kind   string    `json:"kind"`
		StopAt time.Time `json:"pods_stop_at"`
	}
	if err := json.Unmarshal(body, &ev); err != nil {
		return provider.HookEvent{}, err
	}
	out := provider.HookEvent{ID: ev.ID, Type: provider.HookAccount}
	if ev.Kind == "balance_grace_started" {
		out.StopAt = ev.StopAt
	}
	return out, nil
}

func accountRequest(event, kind, stopAt string) *routerv1.DeliverHookRequest {
	body := `{"id":"` + event + `","kind":"` + kind + `"`
	if stopAt != "" {
		body += `,"pods_stop_at":"` + stopAt + `"`
	}
	return &routerv1.DeliverHookRequest{
		Provider: "lium", Body: []byte(body + "}"), ClientIp: "203.0.113.9",
		Headers: []*routerv1.HeaderValue{{Name: "X-Test-Signature", Value: hookSecret}},
	}
}

func (e *env) accountChecks(t *testing.T) []string {
	t.Helper()
	rows, err := e.pool.Query(t.Context(),
		`select args::text from river_job where kind = 'account_check' order by id`)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		var args string
		if err := rows.Scan(&args); err != nil {
			t.Fatal(err)
		}
		out = append(out, args)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDeliverHookAccountEvent(t *testing.T) {
	logs := &syncBuffer{}
	e := newEnv(t, options{
		providerName: "lium", log: slog.New(slog.NewJSONHandler(logs, nil)),
		wrap: func(l *local.Provider, _ string) provider.Provider {
			return accountHooked{faulty{Provider: l}}
		},
		hooks:       config.Hooks{Providers: []string{"lium"}, HintUnique: 10 * time.Second},
		hookSecrets: map[string]config.HookSecret{"lium": {Current: []byte(hookSecret)}},
	})

	resp, err := e.deliver(t, accountRequest("evt-low", "low_balance", ""))
	eq(t, err, nil)
	eq(t, resp.Status, uint32(204))
	eq(t, e.hookJobs(t), 0)
	eq(t, e.accountChecks(t), []string{`{"now": true, "provider": "lium"}`})

	// The same event id again is a duplicate, even once the check it asked for is gone
	if _, err := e.pool.Exec(t.Context(), `delete from river_job`); err != nil {
		t.Fatal(err)
	}
	_, err = e.deliver(t, accountRequest("evt-low", "low_balance", ""))
	eq(t, err, nil)
	eq(t, len(e.accountChecks(t)), 0)
	if strings.Contains(logs.String(), `"level":"ERROR"`) {
		t.Fatalf("a low balance event logged an error: %s", logs)
	}

	// The grace event asks for a check too and names when the pods stop
	_, err = e.deliver(t, accountRequest("evt-grace", "balance_grace_started",
		"2026-11-11T10:02:00Z"))
	eq(t, err, nil)
	eq(t, e.hookJobs(t), 0)
	eq(t, len(e.accountChecks(t)), 1)
	var line struct {
		Level, Provider, Event string
		StopAt                 time.Time `json:"stop_at"`
	}
	for raw := range strings.Lines(logs.String()) {
		if strings.Contains(raw, `"level":"ERROR"`) {
			eq(t, json.Unmarshal([]byte(raw), &line), nil)
		}
	}
	eq(t, line.Provider, "lium")
	eq(t, line.Event, "evt-grace")
	eq(t, line.StopAt.Equal(time.Date(2026, 11, 11, 10, 2, 0, 0, time.UTC)), true)
}
