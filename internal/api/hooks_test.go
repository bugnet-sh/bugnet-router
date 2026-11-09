package api_test

import (
	"crypto/sha256"
	"encoding/base64"
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
