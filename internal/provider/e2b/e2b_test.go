package e2b

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
	processpb "github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/process"
)

func TestE2BContract(t *testing.T) {
	f := newFake(t)
	contract.Run(t, func(t *testing.T) provider.Provider { return f.provider(t, fakeKey) })
}

func TestE2BSendsHardeningFlags(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	labels := map[string]string{"bugnet": "1", "bugnet_deployment": "main", "placement": "p-1"}
	size, _ := p.SizeFor(provider.Resources{VCPU: 3})
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "Order_1|0|E2B", Size: size, Labels: labels, Lifetime: 15 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	call := f.lastCreate()
	sum := sha256.Sum256([]byte("Order_1|0|E2B"))
	want := hex.EncodeToString(sum[:])[:32]
	if got := call.header.Get("Idempotency-Key"); got != want {
		t.Errorf("Idempotency-Key %q, want %q", got, want)
	}
	if call.header.Get("X-API-Key") != fakeKey {
		t.Errorf("X-API-Key %q", call.header.Get("X-API-Key"))
	}
	network, _ := call.body["network"].(map[string]any)
	if public, ok := network["allowPublicTraffic"]; !ok || public != false {
		t.Errorf("network %v, want allowPublicTraffic sent as false", call.body["network"])
	}
	if pause, ok := call.body["autoPause"]; !ok || pause != false {
		t.Errorf("autoPause %v, want sent as false", call.body["autoPause"])
	}
	if call.body["timeout"] != float64(900) || call.body["templateID"] != "bugnet-4x8:v1" {
		t.Errorf("timeout %v template %v, want 900 and bugnet-4x8:v1", call.body["timeout"],
			call.body["templateID"])
	}
	metadata := map[string]string{}
	for k, v := range call.body["metadata"].(map[string]any) {
		metadata[k], _ = v.(string)
	}
	if !maps.Equal(metadata, labels) {
		t.Errorf("metadata %v, want the labels %v", metadata, labels)
	}

	box, _ := f.box(ref.ID)
	created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
	if err != nil || created.After(box.started) || box.started.Sub(created) > 2*time.Second {
		t.Errorf("ref created_at %q, want the second the sandbox started at %v or just before",
			ref.Meta["created_at"], box.started)
	}
	meta := map[string]string{
		"arch": "amd64", "domain": fakeDomain, "envd_token": box.token,
		"created_at": ref.Meta["created_at"],
	}
	if ref.Provider != "e2b" || !maps.Equal(ref.Meta, meta) {
		t.Errorf("ref %+v, want meta %v", ref, meta)
	}

	ctx := t.Context()
	if _, _, err := runExec(ctx, p, ref, provider.ExecSpec{Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	start := f.lastStart()
	headers := map[string]string{
		"E2b-Sandbox-Id": ref.ID, "E2b-Sandbox-Port": "49983", "X-Access-Token": box.token,
		"Authorization": "Basic cm9vdDo=", "Keepalive-Ping-Interval": "50",
		"Connect-Timeout-Ms": "",
	}
	for k, v := range headers {
		if got := start.header.Get(k); got != v {
			t.Errorf("exec header %s is %q, want %q", k, got, v)
		}
	}
	if !strings.Contains(start.header.Get("Content-Type"), "json") {
		t.Errorf("exec content type %q, want the JSON codec", start.header.Get("Content-Type"))
	}
	if start.msg.Stdin == nil || start.msg.GetStdin() {
		t.Errorf("exec stdin %v, want false", start.msg.Stdin)
	}

	if err := p.WriteFile(ctx, ref, filepath.Join(t.TempDir(), "f"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if u := f.lastUpload(); u.Get("username") != "root" {
		t.Errorf("upload as %q, want root", u.Get("username"))
	}
}

func TestE2BBinaryCodec(t *testing.T) {
	f := newFake(t)
	p, err := New(Options{
		APIKey: fakeKey, Templates: fakeTemplates, BaseURL: f.rest.URL, EnvdURL: f.envd.URL,
		BinaryCodec: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := f.add(nil)
	out, code, err := runExec(t.Context(), p, ref, provider.ExecSpec{Argv: []string{"echo", "hi"}})
	if err != nil || string(out) != "hi\n" || code != 0 {
		t.Fatalf("exec gave %q exit %d, %v", out, code, err)
	}
	if ct := f.lastStart().header.Get("Content-Type"); ct != "application/connect+proto" {
		t.Errorf("content type %q, want the binary codec", ct)
	}
}

func TestE2BErrorMapping(t *testing.T) {
	cases := []struct {
		name       string
		route      string
		fault      fault
		op         func(ctx context.Context, p *Provider, ref provider.Ref) error
		code       provider.Code
		failOver   bool
		retryAfter time.Duration
		calls      int
	}{
		{
			name: "rate limit with retry after", route: "POST /v2/sandboxes",
			fault: fault{status: 429, message: "rate limit exceeded",
				header: map[string]string{"Retry-After": "7", "Should-Retry": "true"}},
			op: create, code: provider.CodeRateLimited, failOver: true,
			retryAfter: 7 * time.Second, calls: 1,
		},
		{
			name: "rate limit without retry after", route: "POST /v2/sandboxes",
			fault: fault{status: 429, message: "rate limit exceeded"},
			op:    create, code: provider.CodeRateLimited, failOver: true,
			retryAfter: time.Second, calls: 1,
		},
		{
			name: "concurrency limit", route: "POST /v2/sandboxes",
			fault: fault{status: 429, message: "you have reached the maximum number of " +
				"concurrent E2B sandboxes (20)"},
			op: create, code: provider.CodeQuota, failOver: true, calls: 1,
		},
		{
			name: "capacity", route: "POST /v2/sandboxes",
			fault: fault{status: 503, errorCode: "sandbox_capacity_unavailable",
				message: "no capacity", header: map[string]string{"Should-Retry": "true"}},
			op: create, code: provider.CodeCapacity, failOver: true, calls: maxAttempts,
		},
		{
			name: "placement timeout", route: "POST /v2/sandboxes",
			fault: fault{status: 504, errorCode: "sandbox_placement_timeout",
				message: "placing the sandbox timed out"},
			op: create, code: provider.CodeCapacity, failOver: true, calls: 1,
		},
		{
			name: "bad key", route: "POST /v2/sandboxes",
			fault: fault{status: 401, message: "Invalid API key"},
			op:    create, code: provider.CodeAuth, failOver: true, calls: 1,
		},
		{
			name: "forbidden", route: "POST /v2/sandboxes",
			fault: fault{status: 403, message: "access to the template is forbidden"},
			op:    create, code: provider.CodeAuth, failOver: true, calls: 1,
		},
		{
			name: "team blocked", route: "POST /v2/sandboxes",
			fault: fault{status: 403, message: "team is blocked, check your billing"},
			op:    create, code: provider.CodeUnfunded, failOver: true, calls: 1,
		},
		{
			name: "payment required", route: "POST /v2/sandboxes",
			fault: fault{status: 402, message: "add a payment method"},
			op:    create, code: provider.CodeUnfunded, failOver: true, calls: 1,
		},
		{
			name: "team banned", route: "POST /v2/sandboxes",
			fault: fault{status: 403, message: "team is banned"},
			op:    create, code: provider.CodeAbuseLock, failOver: true, calls: 1,
		},
		{
			name: "bad request", route: "POST /v2/sandboxes",
			fault: fault{status: 400, message: "timeout above the plan limit"},
			op:    create, code: provider.CodeInvalid, calls: 1,
		},
		{
			name: "key mismatch", route: "POST /v2/sandboxes",
			fault: fault{status: 409, errorCode: "idempotency_request_mismatch",
				message: "different parameters",
				header:  map[string]string{"Should-Retry": "false"}},
			op: create, code: provider.CodeInvalid, calls: 1,
		},
		{
			name: "outcome unknown", route: "POST /v2/sandboxes",
			fault: fault{status: 422, errorCode: "idempotency_outcome_unknown",
				message: "original outcome unknown",
				header:  map[string]string{"Should-Retry": "false"}},
			op: create, code: provider.CodeOutcomeUnknown, calls: 1,
		},
		{
			name: "still in progress", route: "POST /v2/sandboxes",
			fault: fault{status: 409, errorCode: "idempotency_in_progress",
				message: "original in progress", header: map[string]string{"Should-Retry": "true"}},
			op: create, code: provider.CodeOutcomeUnknown, calls: maxAttempts,
		},
		{
			name: "create refused", route: "POST /v2/sandboxes",
			fault: fault{status: 500, errorCode: "sandbox_create_refused",
				message: "vcpu count out of bounds"},
			op: create, code: provider.CodeInternal, failOver: true, calls: 1,
		},
		{
			name: "server error", route: "POST /v2/sandboxes",
			fault: fault{status: 500, message: "scheduler fell over"},
			op:    create, code: provider.CodeInternal, calls: 1,
		},
		{
			name: "bad gateway", route: "POST /v2/sandboxes",
			fault: fault{status: 502, message: "upstream reset"},
			op:    create, code: provider.CodeInternal, calls: 1,
		},
		{
			name: "connection dropped after sending", route: "POST /v2/sandboxes",
			fault: fault{drop: true, message: "EOF"},
			op:    create, code: provider.CodeOutcomeUnknown, calls: 1,
		},
		{
			name: "timeout of a missing sandbox", route: "POST /sandboxes/{id}/timeout",
			fault: fault{status: 404, message: "sandbox not found"},
			op:    setLifetime, code: provider.CodeNotFound, calls: 1,
		},
		{
			name: "list rate limited", route: "GET /v2/sandboxes",
			fault: fault{status: 429, message: "slow down", header: map[string]string{
				"Retry-After": "3"}},
			op: listOwned, code: provider.CodeRateLimited, failOver: true,
			retryAfter: 3 * time.Second, calls: 1,
		},
		{
			name: "kill failed", route: "DELETE /sandboxes/{id}",
			fault: fault{status: 500, message: "kill failed"},
			op:    destroy, code: provider.CodeInternal, calls: 1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			ref := f.add(map[string]string{"bugnet": "1"})
			f.inject(c.route, c.fault)
			err := c.op(t.Context(), p, ref)
			e, ok := provider.AsError(err)
			if !ok {
				t.Fatalf("got %v, want a provider error", err)
			}
			if e.Code != c.code || e.CanFailOver != c.failOver || e.RetryAfter != c.retryAfter {
				t.Fatalf("got code %s fail over %v retry after %v, want %s, %v and %v (%v)",
					e.Code, e.CanFailOver, e.RetryAfter, c.code, c.failOver, c.retryAfter, err)
			}
			if !strings.Contains(err.Error(), c.fault.message) {
				t.Errorf("error %q lacks the message %q", err, c.fault.message)
			}
			if n := f.callCount(c.route); n != c.calls {
				t.Errorf("%s called %d times, want %d", c.route, n, c.calls)
			}
		})
	}
}

func TestE2BShouldRetryKeepsKey(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	f.inject("POST /v2/sandboxes", fault{
		status: 409, errorCode: "idempotency_in_progress", message: "in progress", times: 2,
		header: map[string]string{"Should-Retry": "true"},
	})
	ref, err := createKey(t, p, "key-retry")
	if err != nil {
		t.Fatal(err)
	}
	if n := f.callCount("POST /v2/sandboxes"); n != 3 || f.count() != 1 {
		t.Fatalf("%d creates sent and %d sandboxes, want 3 sends and one sandbox", n, f.count())
	}
	want := idempotencyKey("key-retry")
	if keys := f.createKeys(); !slices.Equal(keys, []string{want, want, want}) {
		t.Fatalf("creates carried keys %v, want %s each time", keys, want)
	}
	if _, ok := f.box(ref.ID); !ok {
		t.Fatalf("ref %+v names no sandbox", ref)
	}
}

func TestE2BRetriesConnectionFailureBeforeSending(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	refuse := &refusingTransport{base: http.DefaultTransport, left: 2}
	p.rest.Transport = refuse
	if _, err := createKey(t, p, "key-dial"); err != nil {
		t.Fatalf("create after two refused connections: %v", err)
	}
	if refuse.tries.Load() != 3 {
		t.Fatalf("%d tries, want two refusals and a success", refuse.tries.Load())
	}

	refuse = &refusingTransport{base: http.DefaultTransport, left: 100}
	p.rest.Transport = refuse
	_, err := createKey(t, p, "key-down")
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeInternal || !e.CanFailOver {
		t.Fatalf("create while E2B is unreachable gave %v, want internal that can fail over", err)
	}
	if refuse.tries.Load() != maxAttempts {
		t.Fatalf("%d tries, want %d", refuse.tries.Load(), maxAttempts)
	}
}

func TestE2BCreateLostReplyReplaysUnderKey(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	f.inject("POST /v2/sandboxes", fault{drop: true, after: true})
	_, err := createKey(t, p, "key-lost")
	if !isCode(err, provider.CodeOutcomeUnknown) {
		t.Fatalf("create whose reply was lost gave %v, want outcome_unknown", err)
	}
	f.clearFaults()
	ref, err := createKey(t, p, "key-lost")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f.box(ref.ID); !ok || f.count() != 1 {
		t.Fatalf("replay gave %+v with %d sandboxes, want the one first created", ref, f.count())
	}
}

func TestE2BCreateRejectsBadInput(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	size, _ := p.SizeFor(provider.Resources{})
	reqs := map[string]provider.CreateRequest{
		"unknown size": {IdempotencyKey: "k", Size: provider.Size{Name: "3x9"},
			Lifetime: time.Minute},
		"no lifetime":           {IdempotencyKey: "k", Size: size},
		"lifetime over the cap": {IdempotencyKey: "k", Size: size, Lifetime: time.Hour + 1},
	}
	for name, req := range reqs {
		_, err := p.Create(t.Context(), req)
		if !isCode(err, provider.CodeInvalid) {
			t.Errorf("%s gave %v, want invalid", name, err)
		}
	}
	if n := f.callCount("POST /v2/sandboxes"); n != 0 {
		t.Fatalf("rejected creates reached E2B %d times", n)
	}
	if _, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "k", Size: size, Lifetime: time.Hour,
	}); err != nil {
		t.Fatalf("create at the lifetime cap: %v", err)
	}
	if md, ok := f.lastCreate().body["metadata"]; ok {
		t.Errorf("create without labels sent metadata %v", md)
	}
}

func TestE2BSetLifetime(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ctx := t.Context()
	ref, err := createKey(t, p, "key-lifetime")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetLifetime(ctx, ref, time.Now().Add(50*time.Minute)); err != nil {
		t.Fatal(err)
	}
	box, _ := f.box(ref.ID)
	left := time.Until(box.endAt)
	if left < 50*time.Minute-2*time.Second || left > 50*time.Minute+2*time.Second {
		t.Fatalf("sandbox ends in %v, want 50 minutes", left)
	}
	if err := p.SetLifetime(ctx, ref, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	box, _ = f.box(ref.ID)
	if left = time.Until(box.endAt); left <= 0 || left > 2*time.Second {
		t.Fatalf("a past lifetime left %v, want the one second minimum", left)
	}

	created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
	if err != nil {
		t.Fatalf("ref meta %v has no creation time: %v", ref.Meta, err)
	}
	err = p.SetLifetime(ctx, ref, created.Add(time.Hour+time.Second))
	if !isCode(err, provider.CodeInvalid) {
		t.Fatalf("lifetime past the cap gave %v, want invalid", err)
	}
	if n := f.callCount("POST /sandboxes/{id}/timeout"); n != 2 {
		t.Fatalf("%d timeout calls, want the refused one kept from E2B", n)
	}
	if n := f.callCount("GET /sandboxes/{id}"); n != 0 {
		t.Fatalf("%d sandbox lookups for a ref that carries its creation time", n)
	}

	old := f.add(nil)
	f.setBox(old.ID, func(b *fakeSandbox) { b.started = time.Now().Add(-50 * time.Minute) })
	err = p.SetLifetime(ctx, old, time.Now().Add(15*time.Minute))
	if !isCode(err, provider.CodeInvalid) {
		t.Fatalf("lifetime past the cap of a ref without its creation time gave %v", err)
	}
	if err := p.SetLifetime(ctx, old, time.Now().Add(5*time.Minute)); err != nil {
		t.Fatalf("lifetime within the cap of a ref without its creation time: %v", err)
	}
	if n := f.callCount("GET /sandboxes/{id}"); n != 2 {
		t.Fatalf("%d sandbox lookups, want one per call on a ref without its creation time", n)
	}
	if err := p.Destroy(ctx, old); err != nil {
		t.Fatal(err)
	}
	err = p.SetLifetime(ctx, old, time.Now().Add(time.Minute))
	if !isCode(err, provider.CodeNotFound) {
		t.Fatalf("lifetime of a gone sandbox gave %v, want not_found", err)
	}
}

func TestE2BWaitReady(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	f.setBox(ref.ID, func(b *fakeSandbox) { b.envdDown = 3 })
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatalf("wait through an envd that comes up: %v", err)
	}
	if box, _ := f.box(ref.ID); box.envdDown != 0 {
		t.Fatalf("WaitReady returned with envd still down %d more times", box.envdDown)
	}

	f.setBox(ref.ID, func(b *fakeSandbox) { b.envdDown = 1 << 20 })
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	err := p.WaitReady(ctx, ref)
	if !errors.Is(err, context.DeadlineExceeded) ||
		!strings.Contains(err.Error(), "port is not open") {
		t.Fatalf("wait on an envd that never comes up gave %v", err)
	}
}

func TestE2BEnvdChecksToken(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	ref.Meta["envd_token"] = "stolen"
	_, _, err := runExec(t.Context(), p, ref, provider.ExecSpec{Argv: []string{"true"}})
	if !isCode(err, provider.CodeAuth) {
		t.Errorf("exec with a wrong token gave %v, want auth", err)
	}
	_, _, err = p.ReadFile(t.Context(), ref, "/etc/hosts", 0, 10)
	if !isCode(err, provider.CodeAuth) {
		t.Errorf("read with a wrong token gave %v, want auth", err)
	}
}

func TestE2BExec(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	cwd := filepath.Join(t.TempDir(), "it's a dir")
	if err := os.Mkdir(cwd, 0o755); err != nil {
		t.Fatal(err)
	}
	greeting := "it's $HOME `id` \"q\" \\"
	spec := provider.ExecSpec{
		Argv: []string{"sh", "-c", `printf '%s|' "$GREETING" "$1" "$2" "$(pwd)"; echo err >&2`,
			"argv0", "a b", "it's;$(echo no)"},
		Cwd: cwd,
		Env: map[string]string{"GREETING": greeting},
	}
	out, code, err := runExec(t.Context(), p, ref, spec)
	if err != nil {
		t.Fatal(err)
	}
	want := greeting + "|a b|it's;$(echo no)|" + cwd + "|"
	if string(out) != want || code != 0 {
		t.Fatalf("exec gave %q exit %d, want %q without stderr", out, code, want)
	}
	if got := f.lastStart().msg.GetProcess(); got.GetCmd() != "sh" || len(got.GetArgs()) != 5 {
		t.Fatalf("process %v, want argv split into cmd and args", got)
	}

	big := provider.ExecSpec{Argv: []string{"sh", "-c", "head -c 300000 /dev/zero; exit 4"}}
	out, code, err = runExec(t.Context(), p, ref, big)
	if err != nil || len(out) != 300000 || code != 4 {
		t.Fatalf("large output gave %d bytes exit %d, %v", len(out), code, err)
	}

	if _, err := p.Exec(t.Context(), ref, provider.ExecSpec{}); !isCode(err, provider.CodeInvalid) {
		t.Errorf("exec without a command gave %v, want invalid", err)
	}
	_, err = p.Exec(t.Context(), ref, provider.ExecSpec{Argv: []string{"/no/such/command"}})
	if err == nil {
		t.Error("exec of a missing command succeeded")
	}
}

func TestE2BExecEvents(t *testing.T) {
	cases := []struct {
		name   string
		events []*processpb.ProcessEvent
		out    string
		code   int
		err    provider.Code
	}{
		{
			name: "keepalives and stderr between output",
			events: []*processpb.ProcessEvent{
				startEvent(7), {Event: &processpb.ProcessEvent_Keepalive{
					Keepalive: &processpb.ProcessEvent_KeepAlive{},
				}},
				stdoutEvent([]byte("o")), stderrEvent([]byte("e")), stdoutEvent([]byte("ut")),
				endEvent(3),
			},
			out: "out", code: 3,
		},
		{
			name: "killed by a signal",
			events: []*processpb.ProcessEvent{startEvent(7), {Event: &processpb.ProcessEvent_End{
				End: &processpb.ProcessEvent_EndEvent{ExitCode: -1, Status: "signal: killed"},
			}}},
			code: -1,
		},
		{
			name:   "stream ends without an end event",
			events: []*processpb.ProcessEvent{startEvent(7), stdoutEvent([]byte("out"))},
			out:    "out", code: -1, err: provider.CodeOutcomeUnknown,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			ref := f.add(nil)
			f.replyExec(c.events...)
			out, code, err := runExec(t.Context(), p, ref, provider.ExecSpec{Argv: []string{"x"}})
			if string(out) != c.out || code != c.code {
				t.Fatalf("exec gave %q exit %d, want %q exit %d", out, code, c.out, c.code)
			}
			if (c.err == "" && err != nil) || (c.err != "" && !isCode(err, c.err)) {
				t.Fatalf("exec gave error %v, want %q", err, c.err)
			}
		})
	}
}

func TestE2BExecRunsUntilItEnds(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	spec := provider.ExecSpec{
		Argv: []string{"sh", "-c", "echo early; sleep 0.5; echo late; exit 7"},
	}
	out, code, err := runExec(t.Context(), p, ref, spec)
	if err != nil || string(out) != "early\nlate\n" || code != 7 {
		t.Fatalf("long command gave %q exit %d, %v", out, code, err)
	}
}

func TestE2BExecCloseCancelsStream(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	stream, err := p.Exec(t.Context(), ref, provider.ExecSpec{Argv: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	if f.running.Load() != 1 {
		t.Fatalf("%d commands run once Exec returned, want 1", f.running.Load())
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for f.running.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := f.running.Load(); n != 0 {
		t.Fatalf("%d commands still run after Close", n)
	}
	if _, err := stream.Wait(); err == nil {
		t.Fatal("Wait after Close reported a clean exit")
	}
}

func TestE2BFiles(t *testing.T) {
	modes := []struct {
		name                    string
		noRange, noContentRange bool
	}{
		{name: "range"},
		{name: "range without content range", noContentRange: true},
		{name: "whole file", noRange: true},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			f := newFake(t)
			f.noRange, f.noContentRange = mode.noRange, mode.noContentRange
			p := f.provider(t, fakeKey)
			ref := f.add(nil)
			ctx := t.Context()
			dir := t.TempDir()

			data := make([]byte, 1<<20+5)
			_, _ = rand.Read(data)
			name := filepath.Join(dir, "deep", "er", "blob.bin")
			if err := p.WriteFile(ctx, ref, name, data, 0o644); err != nil {
				t.Fatal(err)
			}
			if n := f.startCount(); n != 0 {
				t.Fatalf("write with the default mode ran %d commands, want none", n)
			}
			reads := []struct {
				offset int64
				length int
				want   []byte
				eof    bool
			}{
				{0, len(data) + 10, data, true},
				{0, len(data), data, true},
				{int64(len(data) - 3), 2, data[len(data)-3 : len(data)-1], false},
				{int64(len(data) - 3), 3, data[len(data)-3:], true},
				{5, 0, nil, false},
				{int64(len(data) - 1), 0, nil, false},
				{int64(len(data)), 0, nil, true},
				{int64(len(data)), 10, nil, true},
				{int64(len(data) + 50), 10, nil, true},
			}
			for _, r := range reads {
				got, eof, err := p.ReadFile(ctx, ref, name, r.offset, r.length)
				if err != nil || !bytes.Equal(got, r.want) || eof != r.eof {
					t.Errorf("read at %d for %d gave %d bytes eof %v, %v, want %d bytes eof %v",
						r.offset, r.length, len(got), eof, err, len(r.want), r.eof)
				}
			}
			tail := fmt.Sprintf("bytes=%d-%d", len(data)-3, len(data)-1)
			if asked := f.rangesAsked(); !slices.Contains(asked, tail) ||
				!slices.Contains(asked, "bytes=5-5") {
				t.Errorf("reads asked for ranges %v, want %s and bytes=5-5 among them", asked, tail)
			}

			empty := filepath.Join(dir, "empty")
			if err := p.WriteFile(ctx, ref, empty, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			got, eof, err := p.ReadFile(ctx, ref, empty, 0, 10)
			if err != nil || len(got) != 0 || !eof {
				t.Fatalf("empty file read gave %q eof %v, %v", got, eof, err)
			}

			_, _, err = p.ReadFile(ctx, ref, filepath.Join(dir, "missing"), 0, 10)
			if !isCode(err, provider.CodeNotFound) {
				t.Errorf("read of a missing file gave %v, want not_found", err)
			}
			_, _, err = p.ReadFile(ctx, ref, dir, 0, 10)
			if !isCode(err, provider.CodeInvalid) {
				t.Errorf("read of a directory gave %v, want invalid", err)
			}
			_, _, err = p.ReadFile(ctx, ref, name, -1, 10)
			if !isCode(err, provider.CodeInvalid) {
				t.Errorf("read at a negative offset gave %v, want invalid", err)
			}
		})
	}
}

func TestE2BRefusesAgentRedirects(t *testing.T) {
	agent, reached := contract.Redirector(t)
	f := newFake(t)
	p, err := New(Options{
		APIKey: fakeKey, Templates: fakeTemplates, BaseURL: f.rest.URL, EnvdURL: agent,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := f.add(nil)
	ctx := t.Context()
	if data, _, err := p.ReadFile(ctx, ref, "/etc/hostname", 0, 64); err == nil {
		t.Errorf("read through a redirecting envd gave %q, want an error", data)
	}
	if err := p.WriteFile(ctx, ref, "/tmp/f", []byte("tenant bytes"), 0o644); err == nil {
		t.Error("write through a redirecting envd succeeded")
	}
	if _, err := listAll(ctx, p, ref, "/tmp", 1); err == nil {
		t.Error("list through a redirecting envd succeeded")
	}
	if reached.Load() {
		t.Fatal("the adapter followed an envd redirect")
	}
}

func TestE2BWriteFileMode(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	name := filepath.Join(t.TempDir(), "-run.sh")
	if err := p.WriteFile(t.Context(), ref, name, []byte("#!/bin/sh\n"), 0o750); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0o750 {
		t.Fatalf("written file %v, %v, want mode 0750", info, err)
	}
	if got := f.lastStart().msg.GetProcess(); got.GetCmd() != "chmod" ||
		!slices.Equal(got.GetArgs(), []string{"--", "750", name}) {
		t.Fatalf("mode set by %v, want chmod -- 750 %s", got, name)
	}

	f.replyExec(startEvent(1), endEvent(1))
	err = p.WriteFile(t.Context(), ref, name, []byte("x"), 0o700)
	if !isCode(err, provider.CodeInternal) || !strings.Contains(err.Error(), "chmod") {
		t.Fatalf("write whose chmod failed gave %v, want an internal chmod error", err)
	}
}

func TestE2BListFiles(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	ctx := t.Context()
	root := t.TempDir()
	for _, name := range []string{"a.txt", "sub/b.txt", "sub/deeper/c.txt", "z.txt"} {
		err := p.WriteFile(ctx, ref, filepath.Join(root, name), []byte(name), 0o644)
		if err != nil {
			t.Fatal(err)
		}
	}

	paths := func(depth int) []string {
		t.Helper()
		entries, err := listAll(ctx, p, ref, root, depth)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range entries {
			rel, _ := filepath.Rel(root, e.Path)
			if e.IsDir != (e.Mode&fs.ModeDir != 0) || e.Mode.Perm() == 0 {
				t.Errorf("entry %+v has a mode that disagrees with its type", e)
			}
			if e.IsDir {
				rel += "/"
			} else if e.Size != int64(len(rel)) || e.Modified.IsZero() {
				t.Errorf("entry %+v, want %d bytes and a modification time", e, len(rel))
			}
			out = append(out, rel)
		}
		slices.Sort(out)
		return out
	}
	if got, want := paths(1), []string{"a.txt", "sub/", "z.txt"}; !slices.Equal(got, want) {
		t.Errorf("depth 1 lists %v, want %v", got, want)
	}
	want := []string{"a.txt", "sub/", "sub/b.txt", "sub/deeper/", "z.txt"}
	if got := paths(2); !slices.Equal(got, want) {
		t.Errorf("depth 2 lists %v, want %v", got, want)
	}

	if _, err := listAll(ctx, p, ref, root, 0); !isCode(err, provider.CodeInvalid) {
		t.Errorf("depth 0 gave %v, want invalid", err)
	}
	_, err := listAll(ctx, p, ref, filepath.Join(root, "nope"), 1)
	if !isCode(err, provider.CodeNotFound) {
		t.Errorf("listing a missing directory gave %v, want not_found", err)
	}
}

func TestE2BListOwned(t *testing.T) {
	f := newFake(t)
	f.pageSize = 2
	p := f.provider(t, fakeKey)
	odd := map[string]string{"bugnet": "1", "placement": "a&b=c d%"}
	var want []string
	for i := range 5 {
		ref := f.add(maps.Clone(odd))
		if i == 3 {
			f.setBox(ref.ID, func(b *fakeSandbox) { b.state = "paused" })
		}
		want = append(want, ref.ID)
	}
	f.add(map[string]string{"bugnet": "1", "placement": "other"})
	f.add(nil)

	owned, err := p.ListOwned(t.Context(), odd)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, s := range owned {
		got = append(got, s.Ref.ID)
		if s.Ref.Provider != "e2b" || !maps.Equal(s.Labels, odd) || s.CreatedAt.IsZero() ||
			s.CreatedAt.Location() != time.UTC {
			t.Errorf("summary %+v, want an e2b ref with the labels and a UTC creation time", s)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("ListOwned gave %v, want the running and paused %v", got, want)
	}
	if n := f.callCount("GET /v2/sandboxes"); n != 3 {
		t.Fatalf("%d list pages fetched, want 3", n)
	}
}

func TestE2BWrongKeyIsAuth(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, "e2b_wrong")
	_, err := p.ListOwned(t.Context(), map[string]string{"bugnet": "1"})
	if !isCode(err, provider.CodeAuth) {
		t.Fatalf("list with a wrong key gave %v, want auth", err)
	}
}

func TestE2BForeignRefIsNotFound(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	ref.Provider = "boxd"
	if err := p.WaitReady(t.Context(), ref); !isCode(err, provider.CodeNotFound) {
		t.Errorf("WaitReady of another provider's ref gave %v", err)
	}
	if err := p.Destroy(t.Context(), ref); err != nil || f.count() != 1 {
		t.Errorf("Destroy of another provider's ref gave %v with %d sandboxes", err, f.count())
	}
}

func TestE2BSizesAndPrices(t *testing.T) {
	p, err := New(Options{APIKey: fakeKey, Templates: fakeTemplates})
	if err != nil {
		t.Fatal(err)
	}
	want := provider.Capabilities{
		EgressControl: true, NeedsRunnerUpload: true, MaxLifetime: time.Hour,
	}
	if caps := p.Capabilities(); caps != want {
		t.Errorf("capabilities %+v, want %+v", caps, want)
	}
	if p.Name() != "e2b" || p.BillingIncrement() != time.Second {
		t.Errorf("name %q increment %v", p.Name(), p.BillingIncrement())
	}
	cases := []struct {
		floor  provider.Resources
		size   string
		hourly int64
	}{
		{provider.Resources{}, "2x2", 133200},
		{provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10}, "2x2", 133200},
		{provider.Resources{VCPU: 3}, "4x8", 331200},
		{provider.Resources{MemoryMiB: 4096}, "4x8", 331200},
	}
	for _, c := range cases {
		s, ok := p.SizeFor(c.floor)
		if !ok || s.Name != c.size || p.HourlyCostMicros(s) != c.hourly {
			t.Errorf("floor %+v gave %+v %v at %d, want %s at %d", c.floor, s, ok,
				p.HourlyCostMicros(s), c.size, c.hourly)
		}
	}
	for _, floor := range []provider.Resources{
		{VCPU: 5}, {MemoryMiB: 8193}, {DiskGiB: 11}, {GPUCount: 1}, {GPUMemoryGiB: 1},
	} {
		if s, ok := p.SizeFor(floor); ok {
			t.Errorf("floor %+v gave %+v, want none", floor, s)
		}
	}

	bare, err := New(Options{APIKey: fakeKey, MaxLifetime: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := bare.SizeFor(provider.Resources{}); ok {
		t.Errorf("an adapter without templates offers %+v", s)
	}
	if got := bare.Capabilities().MaxLifetime; got != 24*time.Hour {
		t.Errorf("max lifetime %v, want the configured 24h", got)
	}
}

func TestE2BOptions(t *testing.T) {
	bad := map[string]Options{
		"no key":         {},
		"template no id": {APIKey: "k", Templates: []Template{{Name: "a", VCPU: 1, MemoryMiB: 1}}},
		"template no vcpu": {APIKey: "k", Templates: []Template{
			{Name: "a", TemplateID: "t", MemoryMiB: 1},
		}},
		"template no name": {APIKey: "k", Templates: []Template{
			{TemplateID: "t", VCPU: 1, MemoryMiB: 1},
		}},
		"negative lifetime": {APIKey: "k", MaxLifetime: -time.Hour},
		"two templates named alike": {APIKey: "k", Templates: []Template{
			{Name: "a", TemplateID: "t", VCPU: 1, MemoryMiB: 1, DiskGiB: 1},
			{Name: "a", TemplateID: "u", VCPU: 2, MemoryMiB: 2, DiskGiB: 1},
		}},
	}
	for name, opts := range bad {
		if _, err := New(opts); err == nil {
			t.Errorf("New with %s succeeded", name)
		}
	}

	cases := []struct {
		domain, meta, rest, envd string
	}{
		{"", "", "https://api.e2b.app", "https://sandbox.e2b.app"},
		{"e2b-juliett.dev", "", "https://api.e2b-juliett.dev", "https://sandbox.e2b-juliett.dev"},
		{"", "e2b-juliett.dev", "https://api.e2b.app", "https://sandbox.e2b-juliett.dev"},
		{"foxtrot-3.e2b.dev", "", "https://api.foxtrot-3.e2b.dev",
			"https://49983-isbx.foxtrot-3.e2b.dev"},
	}
	for _, c := range cases {
		p, err := New(Options{APIKey: "k", Domain: c.domain})
		if err != nil {
			t.Fatal(err)
		}
		ref := provider.Ref{Provider: "e2b", ID: "isbx", Meta: map[string]string{"domain": c.meta}}
		if p.restURL != c.rest || p.envdBase(ref) != c.envd {
			t.Errorf("domain %q meta %q gave %s and %s, want %s and %s", c.domain, c.meta,
				p.restURL, p.envdBase(ref), c.rest, c.envd)
		}
	}
}

func TestNewChecksTemplates(t *testing.T) {
	good := []Template{
		{Name: "2x2", TemplateID: "bugnet-2x2", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
		{Name: "4x8", TemplateID: "team/bugnet-4x8:v1", VCPU: 4, MemoryMiB: 8192, DiskGiB: 20},
	}
	p, err := New(Options{APIKey: "k", Templates: good})
	if err != nil {
		t.Fatal(err)
	}
	if size, ok := p.SizeFor(provider.Resources{VCPU: 3}); !ok || size.Name != "4x8" {
		t.Fatalf("size for 3 vCPU %+v, %v, want 4x8", size, ok)
	}
	if p, err := New(Options{APIKey: "k"}); err != nil || len(p.templates) != 0 {
		t.Fatalf("no templates gave %+v, %v", p, err)
	}
	bad := map[string][]Template{
		"no name":        {{TemplateID: "bugnet", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10}},
		"no template ID": {{Name: "2x2", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10}},
		"no vCPU":        {{Name: "2x2", TemplateID: "bugnet", MemoryMiB: 2048, DiskGiB: 10}},
		"no memory":      {{Name: "2x2", TemplateID: "bugnet", VCPU: 2, DiskGiB: 10}},
		"no disk":        {{Name: "2x2", TemplateID: "bugnet", VCPU: 2, MemoryMiB: 2048}},
		"negative disk": {
			{Name: "2x2", TemplateID: "bugnet", VCPU: 2, MemoryMiB: 2048, DiskGiB: -1},
		},
		"a size twice": {
			{Name: "2x2", TemplateID: "a", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
			{Name: "2x2", TemplateID: "b", VCPU: 4, MemoryMiB: 4096, DiskGiB: 10},
		},
	}
	for name, templates := range bad {
		if _, err := New(Options{APIKey: "k", Templates: templates}); err == nil {
			t.Errorf("New with %s succeeded", name)
		}
	}
}

/* Helpers */

// refusingTransport fails its first calls the way a refused dial does, before anything is sent
type refusingTransport struct {
	base  http.RoundTripper
	left  int32
	tries atomic.Int32
}

func (r *refusingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if r.tries.Add(1) <= r.left {
		return nil, errors.New("dial tcp: connect: connection refused")
	}
	return r.base.RoundTrip(req)
}

func create(ctx context.Context, p *Provider, _ provider.Ref) error {
	size, _ := p.SizeFor(provider.Resources{})
	_, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: "key-errors", Size: size, Labels: map[string]string{"bugnet": "1"},
		Lifetime: time.Minute,
	})
	return err
}

func setLifetime(ctx context.Context, p *Provider, ref provider.Ref) error {
	return p.SetLifetime(ctx, ref, time.Now().Add(30*time.Minute))
}

func listOwned(ctx context.Context, p *Provider, _ provider.Ref) error {
	_, err := p.ListOwned(ctx, map[string]string{"bugnet": "1"})
	return err
}

func destroy(ctx context.Context, p *Provider, ref provider.Ref) error {
	return p.Destroy(ctx, ref)
}

func createKey(t *testing.T, p *Provider, key string) (provider.Ref, error) {
	t.Helper()
	size, _ := p.SizeFor(provider.Resources{})
	return p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: key, Size: size, Labels: map[string]string{"bugnet": "1"},
		Lifetime: 15 * time.Minute,
	})
}

func runExec(
	ctx context.Context, p *Provider, ref provider.Ref, spec provider.ExecSpec,
) ([]byte, int, error) {
	stream, err := p.Exec(ctx, ref, spec)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = stream.Close() }()
	var out bytes.Buffer
	if _, err := io.Copy(&out, stream); err != nil {
		return out.Bytes(), 0, err
	}
	code, err := stream.Wait()
	return out.Bytes(), code, err
}

func isCode(err error, code provider.Code) bool {
	e, ok := provider.AsError(err)
	return ok && e.Code == code
}

// listAll lists through ListFiles with room for every entry a test makes
func listAll(
	ctx context.Context, p *Provider, ref provider.Ref, dir string, depth int,
) ([]provider.FileEntry, error) {
	l := provider.NewListing(10000)
	err := p.ListFiles(ctx, ref, dir, depth, l)
	return l.Entries, err
}

func TestE2BListFilesStopsAtTheLimit(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	contract.ListLimit(t, p, f.add(nil), t.TempDir())
}

func TestE2BBoundsEnvdMessages(t *testing.T) {
	f := newFake(t)
	p, err := New(Options{
		APIKey: fakeKey, Templates: fakeTemplates, BaseURL: f.rest.URL, EnvdURL: f.envd.URL,
		maxMessage: 512,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := f.add(nil)
	dir := t.TempDir()
	for i := range 20 {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("file-%02d", i)), nil,
			0o644); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := listAll(t.Context(), p, ref, dir, 1)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid || e.CanFailOver ||
		!strings.Contains(err.Error(), "listing is too large") {
		t.Fatalf("a listing over the message cap gave %d entries and %v, want invalid as too "+
			"large", len(entries), err)
	}
}

func TestE2BListFilesRateLimited(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	f.listLimited.Store(true)
	_, err := listAll(t.Context(), p, ref, t.TempDir(), 1)
	if !isCode(err, provider.CodeRateLimited) {
		t.Fatalf("envd refusing a listing as resource exhausted gave %v, want rate_limited", err)
	}
}

func TestCapabilitiesReportNoSSHOrPreview(t *testing.T) {
	c := newFake(t).provider(t, fakeKey).Capabilities()
	eq(t, c.SSH || c.Preview, false)
}
