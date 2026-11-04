package koyeb

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
)

func TestKoyebContract(t *testing.T) {
	f := newFake(t)
	contract.Run(t, func(t *testing.T) provider.Provider { return f.provider(t, fakeToken) })
}

func TestKoyebCreatesSandboxService(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	labels := map[string]string{"bugnet": "1", "bugnet_deployment": "main", "placement": "p-1"}
	size, _ := p.SizeFor(provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10})
	ref, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: "Order_1|0|koyeb", Size: size, Labels: labels, Lifetime: 15 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	sum := sha256.Sum256([]byte("Order_1|0|koyeb"))
	name := "bn-" + hex.EncodeToString(sum[:])[:20]
	f.mu.Lock()
	app := *f.liveApp(name)
	f.mu.Unlock()
	svc, dep, _ := f.box(app.id)
	if ref.Provider != "koyeb" || ref.ID != app.id || svc.name != name {
		t.Fatalf("ref %+v, app %s named %s, service %s, want the app named %s", ref, app.id,
			app.name, svc.name, name)
	}
	checks := map[string]string{
		"app_id":                    `"` + app.id + `"`,
		"name":                      `"` + name + `"`,
		"life_cycle":                `{"delete_after_create": 86400}`,
		"definition.type":           `"SANDBOX"`,
		"definition.docker":         `{"image": "koyeb/sandbox", "privileged": false}`,
		"definition.regions":        `["fra"]`,
		"definition.instance_types": `[{"type": "medium"}]`,
		"definition.scalings":       `[{"min": 1, "max": 1}]`,
		"definition.ports": `[{"port": 3030, "protocol": "http"},
			{"port": 3031, "protocol": "http"}]`,
		"definition.routes": `[{"port": 3030, "path": "/koyeb-sandbox/"},
			{"port": 3031, "path": "/"}]`,
		"definition.mesh":           `"DEPLOYMENT_MESH_AUTO"`,
		"definition.name":           `"` + name + `"`,
		"definition.env.0.key":      `"SANDBOX_SECRET"`,
		"definition.env.1.key":      `"BUGNET_LABELS"`,
		"definition.health_checks":  `null`,
		"definition.network_policy": `null`,
	}
	for path, want := range checks {
		if got := field(svc.body, path); !jsonEqual(got, want) {
			t.Errorf("service %s is %v, want %s", path, got, want)
		}
	}

	env := fakeEnv(&dep)
	var sent map[string]string
	if err := json.Unmarshal([]byte(env["BUGNET_LABELS"]), &sent); err != nil ||
		!maps.Equal(sent, labels) {
		t.Errorf("BUGNET_LABELS %q, want the labels %v as JSON", env["BUGNET_LABELS"], labels)
	}
	secret := env["SANDBOX_SECRET"]
	if len(secret) < 43 {
		t.Errorf("SANDBOX_SECRET %q is shorter than 32 random bytes", secret)
	}
	created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
	if err != nil || created.After(svc.created) || svc.created.Sub(created) > 2*time.Second {
		t.Errorf("ref created_at %q, want the second the service was made at %v or before",
			ref.Meta["created_at"], svc.created)
	}
	meta := map[string]string{
		"service_id": svc.id, "secret": secret, "arch": "amd64",
		"created_at": ref.Meta["created_at"],
	}
	if !maps.Equal(ref.Meta, meta) {
		t.Errorf("ref meta %v, want %v", ref.Meta, meta)
	}

	if err := p.WaitReady(ctx, ref); err != nil {
		t.Fatal(err)
	}
	meta["executor_url"] = "https://" + f.domain(app.id) + "/koyeb-sandbox"
	meta["routing_key"] = dep.routingKey
	if !maps.Equal(ref.Meta, meta) {
		t.Errorf("ref meta after WaitReady %v, want %v", ref.Meta, meta)
	}

	other, err := createKey(t, p, "Order_2|0|koyeb", labels)
	if err != nil {
		t.Fatal(err)
	}
	if other.ID == ref.ID || other.Meta["secret"] == secret {
		t.Errorf("a second sandbox %+v shares the app or secret of the first", other)
	}
}

func TestKoyebStreamsLogsFromStartProcess(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	ref := ready(t, p, "key-logs")
	dir := t.TempDir()
	gate := filepath.Join(dir, "go")
	script := `echo "early $GREETING $2"; echo err >&2; ` +
		`while [ ! -e "$1" ]; do sleep 0.01; done; echo late; exit 7`
	spec := provider.ExecSpec{
		Argv: []string{"sh", "-c", script, "argv0", gate, "it's; $(echo no)"},
		Cwd:  dir,
		Env:  map[string]string{"GREETING": "it's $HOME"},
	}
	stream, err := p.Exec(ctx, ref, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	lines := bufio.NewReader(stream)
	early, err := lines.ReadString('\n')
	if err != nil || early != "early it's $HOME it's; $(echo no)\n" {
		t.Fatalf("first line %q, %v, want it while the command still runs", early, err)
	}
	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(lines)
	if err != nil || string(rest) != "late\n" {
		t.Fatalf("rest of stdout %q, %v, want late without stderr", rest, err)
	}
	if code, err := stream.Wait(); err != nil || code != 7 {
		t.Fatalf("Wait gave %d, %v, want 7", code, err)
	}

	start := f.lastStart()
	wrapper := regexp.MustCompile(
		`^mkdir -p (\S+) && \{ sh -c 'exec "\$@"' sh (.*); echo \$\? > (\S+); \}$`)
	m := wrapper.FindStringSubmatch(start.cmd)
	if m == nil {
		t.Fatalf("start_process cmd %q is not the exit file wrapper", start.cmd)
	}
	if m[1] != p.exitDir || !regexp.MustCompile(`^[0-9a-f]{16}\.exit$`).MatchString(
		strings.TrimPrefix(m[3], p.exitDir+"/")) {
		t.Errorf("wrapper makes %s and writes %s, want a random .exit file in %s", m[1], m[3],
			p.exitDir)
	}
	words := `sh -c '` + strings.ReplaceAll(script, `'`, `'\''`) + `' argv0 ` + gate +
		` 'it'\''s; $(echo no)'`
	if m[2] != words {
		t.Errorf("wrapper passes argv as\n%s\nwant\n%s", m[2], words)
	}
	if start.cwd != dir || !maps.Equal(start.env, spec.Env) {
		t.Errorf("start_process cwd %q env %v, want %q and %v", start.cwd, start.env, dir,
			spec.Env)
	}
	_, dep, _ := f.box(ref.ID)
	if got := start.header.Get("Authorization"); got != "Bearer "+ref.Meta["secret"] {
		t.Errorf("executor Authorization %q, want the sandbox secret", got)
	}
	if got := start.header.Get("X-Routing-Key"); got != dep.routingKey {
		t.Errorf("X-Routing-Key %q, want %q", got, dep.routingKey)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	routes := f.routesCalled()
	want := []string{
		"GET /health", "POST /start_process", "GET /process_logs_streaming", "GET /files",
	}
	if !slices.Equal(routes, want) {
		t.Errorf("executor routes %v, want %v and never /run or /run_streaming", routes, want)
	}
}

func TestKoyebExecEvents(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		exit string
		out  string
		code int
		err  provider.Code
	}{
		{
			name: "keepalives, stderr and unknown events between lines",
			raw: ":keepalive\n\n" + logEvent("stdout", "one") + ": note\n\n" +
				logEvent("stderr", "bad") + "event: ping\ndata: {}\n\n" +
				"event: log\r\ndata:" + `{"stream":"stdout","data":"two"}` + "\r\n\r\n" +
				completeEvent,
			exit: "3\n", out: "one\ntwo\n", code: 3,
		},
		{
			name: "no exit file",
			raw:  logEvent("stdout", "one") + completeEvent,
			out:  "one\n", code: -1, err: provider.CodeOutcomeUnknown,
		},
		{
			name: "an exit file that is not a number",
			raw:  completeEvent, exit: "done\n", code: -1, err: provider.CodeOutcomeUnknown,
		},
		{
			name: "process not found",
			raw:  "event: error\ndata: {\"error\": \"process not found: x\"}\n\n",
			code: -1, err: provider.CodeInternal,
		},
		{
			name: "stream ends before complete",
			raw:  logEvent("stdout", "one"),
			out:  "one\n", code: -1, err: provider.CodeInternal,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeToken)
			ref := ready(t, p, "key-events")
			f.replyLogs(c.raw)
			stream, err := p.Exec(t.Context(), ref, provider.ExecSpec{Argv: []string{"x"}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stream.Close() }()
			if c.exit != "" {
				exitFile := regexp.MustCompile(`> (\S+); \}$`).FindStringSubmatch(f.lastStart().cmd)
				if err := os.MkdirAll(filepath.Dir(exitFile[1]), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(exitFile[1], []byte(c.exit), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			var out bytes.Buffer
			_, readErr := io.Copy(&out, stream)
			code, err := stream.Wait()
			if out.String() != c.out || code != c.code {
				t.Fatalf("exec gave %q exit %d, want %q exit %d", out.String(), code, c.out,
					c.code)
			}
			if (c.err == "" && err != nil) || (c.err != "" && !isCode(err, c.err)) {
				t.Fatalf("Wait gave %v, want %q", err, c.err)
			}
			if c.err == provider.CodeInternal && !isCode(readErr, provider.CodeInternal) {
				t.Fatalf("Read of a broken stream gave %v, want internal", readErr)
			}
		})
	}
}

func TestKoyebExecCloseStopsReading(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-close")
	stream, err := p.Exec(t.Context(), ref, provider.ExecSpec{
		Argv: []string{"sh", "-c", "echo up; sleep 30"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if line, err := bufio.NewReader(stream).ReadString('\n'); err != nil || line != "up\n" {
		t.Fatalf("first line %q, %v", line, err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, stream)
		done <- err
	}()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Read after Close ended without an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read still blocks after Close")
	}
	if _, err := stream.Wait(); err == nil {
		t.Fatal("Wait after Close reported a clean exit")
	}
}

func TestKoyebExecCloseKillsTheCommand(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-kill")
	pidFile := filepath.Join(t.TempDir(), "pid")
	stream, err := p.Exec(t.Context(), ref, provider.ExecSpec{Argv: []string{
		"sh", "-c", `echo $$ > "$0"; while :; do echo tick; sleep 0.05; done`, pidFile,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if line, err := bufio.NewReader(stream).ReadString('\n'); err != nil || line != "tick\n" {
		t.Fatalf("first line %q, %v", line, err)
	}
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if kills := f.killed(); len(kills) != 1 || !strings.HasPrefix(kills[0], "proc-") {
		t.Fatalf("kill_process calls %q, want one for the started process", kills)
	}
	deadline := time.Now().Add(5 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("command %d still runs 5 s after Close", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := stream.Close(); err != nil || len(f.killed()) != 1 {
		t.Fatalf("a second Close gave %v after %d kills, want nothing more", err,
			len(f.killed()))
	}
}

func TestKoyebErrorMapping(t *testing.T) {
	cases := []struct {
		name       string
		route      string
		fault      fault
		op         func(ctx context.Context, p *Provider, ref provider.Ref) error
		code       provider.Code
		failOver   bool
		retryAfter time.Duration
	}{
		{
			name: "rate limit with retry after", route: "POST /v1/apps",
			fault: fault{status: 429, code: "rate_limit_error", message: "Too many requests",
				header: map[string]string{"Retry-After": "7", "X-Ratelimit-Reset": "30"}},
			op: create, code: provider.CodeRateLimited, failOver: true, retryAfter: 7 * time.Second,
		},
		{
			name: "rate limit with a reset header", route: "POST /v1/apps",
			fault: fault{status: 429, code: "rate_limit_error", message: "Too many requests",
				header: map[string]string{"X-Ratelimit-Reset": "12"}},
			op: create, code: provider.CodeRateLimited, failOver: true,
			retryAfter: 12 * time.Second,
		},
		{
			name: "rate limit without headers", route: "POST /v1/services",
			fault: fault{status: 429, code: "rate_limit_error", message: "slow down"},
			op:    create, code: provider.CodeRateLimited, failOver: true, retryAfter: time.Second,
		},
		{
			name: "payment required", route: "POST /v1/services",
			fault: fault{status: 402, code: "api_error", message: "Request Failed"},
			op:    create, code: provider.CodeUnfunded, failOver: true,
		},
		{
			name: "no payment method", route: "POST /v1/apps",
			fault: fault{status: 403, code: "authorization_required",
				message: "add a payment method to your organization"},
			op: create, code: provider.CodeUnfunded, failOver: true,
		},
		{
			name: "service quota", route: "POST /v1/services",
			fault: fault{status: 400, code: "invalid_argument",
				message: "You have reached the limit of 100 services for your plan"},
			op: create, code: provider.CodeQuota, failOver: true,
		},
		{
			name: "suspended organization", route: "POST /v1/apps",
			fault: fault{status: 403, code: "authorization_required",
				message: "organization is suspended"},
			op: create, code: provider.CodeAbuseLock, failOver: true,
		},
		{
			name: "bad token", route: "POST /v1/apps",
			fault: fault{status: 401, code: "authentication_error",
				message: "Request unauthenticated with bearer"},
			op: create, code: provider.CodeAuth, failOver: true,
		},
		{
			name: "forbidden", route: "POST /v1/services",
			fault: fault{status: 403, code: "authorization_required",
				message: "You don't have the requested permission"},
			op: create, code: provider.CodeAuth, failOver: true,
		},
		{
			name: "instance type refused", route: "POST /v1/services",
			fault: fault{status: 400, code: "invalid_argument",
				message: "Instance type gpu-nvidia-rtx-4000-sff-ada is not available in fra"},
			op: create, code: provider.CodeCapacity, failOver: true,
		},
		{
			name: "no instance left", route: "POST /v1/services",
			fault: fault{status: 422, code: "invalid_argument",
				message: "no instance of type small can be allocated right now"},
			op: create, code: provider.CodeCapacity, failOver: true,
		},
		{
			name: "invalid definition", route: "POST /v1/services",
			fault: fault{status: 400, code: "invalid_argument",
				message: "definition.docker.image cannot be blank"},
			op: create, code: provider.CodeInvalid,
		},
		{
			name: "unprocessable", route: "POST /v1/services",
			fault: fault{status: 422, code: "invalid_argument", message: "unprocessable"},
			op:    create, code: provider.CodeInvalid,
		},
		{
			name: "service unavailable", route: "POST /v1/services",
			fault: fault{status: 503, code: "api_error", message: "try again later"},
			op:    create, code: provider.CodeCapacity, failOver: true,
		},
		{
			name: "no capacity", route: "POST /v1/apps",
			fault: fault{status: 500, code: "api_error", message: "insufficient capacity in fra"},
			op:    create, code: provider.CodeCapacity, failOver: true,
		},
		{
			name: "server error with nothing created", route: "POST /v1/apps",
			fault: fault{status: 500, code: "api_error", message: "database fell over"},
			op:    create, code: provider.CodeInternal, failOver: true,
		},
		{
			name: "app reply lost with nothing created", route: "POST /v1/apps",
			fault: fault{drop: true, message: "EOF"},
			op:    create, code: provider.CodeOutcomeUnknown,
		},
		{
			name: "service reply lost with nothing created", route: "POST /v1/services",
			fault: fault{drop: true, message: "EOF"},
			op:    create, code: provider.CodeOutcomeUnknown,
		},
		{
			name: "missing service", route: "GET /v1/services/{id}",
			fault: fault{status: 404, code: "unknown_resource", message: "Service not found"},
			op:    waitReady, code: provider.CodeNotFound,
		},
		{
			name: "list rate limited", route: "GET /v1/services",
			fault: fault{status: 429, code: "rate_limit_error", message: "slow down",
				header: map[string]string{"Retry-After": "3"}},
			op: listOwned, code: provider.CodeRateLimited, failOver: true,
			retryAfter: 3 * time.Second,
		},
		{
			name: "list bad gateway", route: "GET /v1/services",
			fault: fault{status: 502, message: "upstream reset"},
			op:    listOwned, code: provider.CodeInternal,
		},
		{
			name: "delete failed", route: "DELETE /v1/apps/{id}",
			fault: fault{status: 500, code: "api_error", message: "delete failed"},
			op:    destroy, code: provider.CodeInternal,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeToken)
			ref := ready(t, p, "key-ready")
			f.inject(c.route, c.fault)
			before := f.callCount(c.route)
			err := c.op(t.Context(), p, ref)
			e, ok := provider.AsError(err)
			if !ok {
				t.Fatalf("got %v, want a provider error", err)
			}
			if e.Code != c.code || e.CanFailOver != c.failOver || e.RetryAfter != c.retryAfter {
				t.Fatalf("got code %s fail over %v retry after %v, want %s, %v and %v (%v)",
					e.Code, e.CanFailOver, e.RetryAfter, c.code, c.failOver, c.retryAfter, err)
			}
			if !c.fault.drop && !strings.Contains(err.Error(), c.fault.message) {
				t.Errorf("error %q lacks the message %q", err, c.fault.message)
			}
			if n := f.callCount(c.route) - before; n != 1 {
				t.Errorf("%s called %d times, want once", c.route, n)
			}
		})
	}
}

func TestKoyebCreateSettlesLostAndRepeatedCreates(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(f *fakeKoyeb)
	}{
		{"repeated key", func(*fakeKoyeb) {}},
		{"repeated key refused with 400", func(f *fakeKoyeb) { f.conflictStatus = 400 }},
		{"app reply lost", func(f *fakeKoyeb) {
			f.inject("POST /v1/apps", fault{drop: true, after: true, times: 1})
		}},
		{"service reply lost", func(f *fakeKoyeb) {
			f.inject("POST /v1/services", fault{drop: true, after: true, times: 1})
		}},
		{"app server error after creating", func(f *fakeKoyeb) {
			f.inject("POST /v1/apps", fault{status: 500, message: "oops", after: true, times: 1})
		}},
		{"app left without its service", func(f *fakeKoyeb) { f.addApp(appName("key-settle")) }},
	}
	labels := map[string]string{"bugnet": "1", "placement": "p-9"}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeToken)
			c.prepare(f)
			first, err := createKey(t, p, "key-settle", labels)
			if err != nil {
				t.Fatal(err)
			}
			again, err := createKey(t, p, "key-settle", labels)
			if err != nil {
				t.Fatal(err)
			}
			if apps, services := f.counts(); apps != 1 || services != 1 {
				t.Fatalf("%d apps and %d services, want one of each", apps, services)
			}
			_, dep, ok := f.box(first.ID)
			if !ok || again.ID != first.ID ||
				again.Meta["secret"] != fakeEnv(&dep)["SANDBOX_SECRET"] ||
				first.Meta["secret"] != again.Meta["secret"] ||
				again.Meta["service_id"] != first.Meta["service_id"] {
				t.Fatalf("creates gave %+v and %+v, want the one sandbox with its secret",
					first, again)
			}
			if err := p.WaitReady(t.Context(), again); err != nil {
				t.Fatalf("adopted sandbox does not come up: %v", err)
			}
			out, code, err := runExec(t.Context(), p, again, provider.ExecSpec{
				Argv: []string{"echo", "adopted"},
			})
			if err != nil || string(out) != "adopted\n" || code != 0 {
				t.Fatalf("exec on the adopted sandbox gave %q exit %d, %v", out, code, err)
			}
		})
	}
}

func TestKoyebCreateRefusesAnotherRequestsApp(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	if _, err := createKey(t, p, "key-taken", map[string]string{"placement": "a"}); err != nil {
		t.Fatal(err)
	}
	_, err := createKey(t, p, "key-taken", map[string]string{"placement": "b"})
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeInternal || !e.CanFailOver {
		t.Fatalf("create under a name held by other labels gave %v, want internal that fails over",
			err)
	}
	if apps, services := f.counts(); apps != 1 || services != 1 {
		t.Fatalf("%d apps and %d services, want the first sandbox alone", apps, services)
	}
}

func TestKoyebCreateRemovesAppOfFailedService(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	f.inject("POST /v1/services", fault{status: 400, code: "invalid_argument", message: "bad"})
	if _, err := createKey(t, p, "key-failed", nil); !isCode(err, provider.CodeInvalid) {
		t.Fatalf("create whose service was refused gave %v, want invalid", err)
	}
	if apps, services := f.counts(); apps != 0 || services != 0 {
		t.Fatalf("%d apps and %d services left, want the empty app removed", apps, services)
	}
}

func TestKoyebCreateWhoseLookupFailsIsUnknown(t *testing.T) {
	limited := fault{status: 429, code: "rate_limit_error", message: "slow down", times: 1,
		header: map[string]string{"Retry-After": "0"}}
	cases := []struct {
		name    string
		earlier bool
		prepare func(f *fakeKoyeb, appID string)
	}{
		{"repeated key and the service list rate limited", true, func(f *fakeKoyeb, _ string) {
			f.inject("GET /v1/services", limited)
		}},
		{"repeated key and the deployment read failing", true, func(f *fakeKoyeb, _ string) {
			f.inject("GET /v1/deployments/{id}", fault{status: 500, message: "oops", times: 1})
		}},
		{"repeated key and the lookup connection dropped", true, func(f *fakeKoyeb, _ string) {
			// Twice, since the transport retries an idempotent call once on a reused connection
			f.inject("GET /v1/services", fault{drop: true, times: 2})
		}},
		{"repeated key refused with 400 and the app list unavailable", true,
			func(f *fakeKoyeb, _ string) {
				f.conflictStatus = 400
				f.inject("GET /v1/apps", fault{status: 503, message: "busy", times: 1})
			}},
		{"repeated key and a service with no deployment yet", true,
			func(f *fakeKoyeb, appID string) {
				f.setService(appID, func(s *fakeService) { s.deployment = "" })
			}},
		{"service reply lost and its lookup refused", false, func(f *fakeKoyeb, _ string) {
			f.inject("POST /v1/services", fault{drop: true, after: true, times: 1})
			f.inject("GET /v1/services", fault{status: 403, code: "authorization_required",
				message: "forbidden", times: 1})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeToken)
			labels := map[string]string{"bugnet": "1"}
			var first provider.Ref
			if c.earlier {
				var err error
				if first, err = createKey(t, p, "key-unsettled", labels); err != nil {
					t.Fatal(err)
				}
			}
			kept, _, _ := f.box(first.ID)
			c.prepare(f, first.ID)
			_, err := createKey(t, p, "key-unsettled", labels)
			e, ok := provider.AsError(err)
			if !ok || e.Code != provider.CodeOutcomeUnknown || e.CanFailOver {
				t.Fatalf("create whose lookup failed gave %v, want outcome_unknown, no fail over",
					err)
			}
			if apps, services := f.counts(); apps != 1 || services != 1 {
				t.Fatalf("%d apps and %d services, want the one sandbox kept", apps, services)
			}
			f.clearFaults()
			if c.earlier {
				f.setService(first.ID, func(s *fakeService) { s.deployment = kept.deployment })
			}
			again, err := createKey(t, p, "key-unsettled", labels)
			svc, _, ok := f.box(again.ID)
			if err != nil || !ok || again.Meta["service_id"] != svc.id ||
				(c.earlier && again.ID != first.ID) {
				t.Fatalf("retry once the lookup works gave %+v, %v, want the one sandbox", again,
					err)
			}
		})
	}
}

func TestKoyebCreateRejectsBadInput(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	size, _ := p.SizeFor(provider.Resources{})
	reqs := map[string]provider.CreateRequest{
		"unknown size": {IdempotencyKey: "k", Size: provider.Size{Name: "nano"},
			Lifetime: time.Minute},
		"no lifetime": {IdempotencyKey: "k", Size: size},
		"lifetime over the cap": {IdempotencyKey: "k", Size: size,
			Lifetime: 24*time.Hour - 10*time.Minute + time.Second},
	}
	for name, req := range reqs {
		if _, err := p.Create(t.Context(), req); !isCode(err, provider.CodeInvalid) {
			t.Errorf("%s gave %v, want invalid", name, err)
		}
	}
	if n := f.callCount("POST /v1/apps"); n != 0 {
		t.Fatalf("rejected creates reached Koyeb %d times", n)
	}
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "k", Size: size, Lifetime: 24*time.Hour - 10*time.Minute,
	})
	if err != nil {
		t.Fatalf("create at the cap: %v", err)
	}
	_, dep, _ := f.box(ref.ID)
	if got := fakeEnv(&dep)["BUGNET_LABELS"]; got != "{}" {
		t.Errorf("BUGNET_LABELS without labels is %q, want {}", got)
	}
	keyless := provider.CreateRequest{Size: size, Lifetime: time.Minute}
	if _, err := p.Create(t.Context(), keyless); err != nil {
		t.Fatalf("create without a key: %v", err)
	}
	if apps, _ := f.counts(); apps != 2 {
		t.Fatalf("%d apps, want a second one for the create without a key", apps)
	}
}

func TestKoyebWaitReady(t *testing.T) {
	f := newFake(t)
	f.startPolls = 3
	p := f.provider(t, fakeToken)
	ref, err := createKey(t, p, "key-wait", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.setHealthDown(2)
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatalf("wait through a starting deployment and executor: %v", err)
	}
	f.mu.Lock()
	down := f.healthDown
	f.mu.Unlock()
	if down != 0 || f.callCount("GET /v1/deployments/{id}") < 4 {
		t.Fatalf("WaitReady returned before the deployment and executor were up")
	}

	f.setHealthDown(1 << 20)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	err = p.WaitReady(ctx, ref)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "503") {
		t.Fatalf("wait on an executor that never answers gave %v", err)
	}

	f.setDeployment(ref.ID, func(d *fakeDeployment) { d.status = "ERROR" })
	err = p.WaitReady(t.Context(), ref)
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeCapacity || !e.CanFailOver ||
		!strings.Contains(err.Error(), "ERROR") {
		t.Fatalf("wait on a failed deployment gave %v, want capacity that fails over", err)
	}
}

func TestKoyebWaitReadyHonoursRateLimits(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref, err := createKey(t, p, "key-wait-limited", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.inject("GET /v1/deployments/{id}", fault{status: 429, code: "rate_limit_error",
		message: "slow down", header: map[string]string{"Retry-After": "1"}, times: 1})
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatalf("wait through a rate limit: %v", err)
	}
	reads := f.callTimes("GET /v1/deployments/{id}")
	if len(reads) < 2 || reads[1].Sub(reads[0]) < time.Second {
		t.Fatalf("deployment reads at %v, want the second a full Retry-After after the first",
			reads)
	}
}

func TestKoyebWaitReadyWaitsOnUnknownStatuses(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref, err := createKey(t, p, "key-wait-unknown", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.setDeployment(ref.ID, func(d *fakeDeployment) { d.status = "WARMING_UP_SOMEHOW" })
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	err = p.WaitReady(ctx, ref)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "WARMING") {
		t.Fatalf("wait on a status Koyeb added later gave %v, want it waited on", err)
	}
	f.setDeployment(ref.ID, func(d *fakeDeployment) { d.status = "HEALTHY" })
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatalf("wait once healthy: %v", err)
	}
}

func TestKoyebReachesExecutorByAppDomain(t *testing.T) {
	f := newFake(t)
	f.noMetadata = true
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-domain")
	want := "https://" + f.domain(ref.ID) + "/koyeb-sandbox"
	if got := ref.Meta["executor_url"]; got != want {
		t.Fatalf("executor_url %q, want the app domain %q", got, want)
	}
	if _, ok := ref.Meta["routing_key"]; ok {
		t.Errorf("ref meta %v has a routing key the deployment never gave", ref.Meta)
	}
	out, code, err := runExec(t.Context(), p, ref, provider.ExecSpec{Argv: []string{"echo", "hi"}})
	if err != nil || string(out) != "hi\n" || code != 0 {
		t.Fatalf("exec by the app domain gave %q exit %d, %v", out, code, err)
	}
}

func TestKoyebFindsExecutorForBareRef(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	full := ready(t, p, "key-bare")
	bare := provider.Ref{Provider: "koyeb", ID: full.ID}
	out, code, err := runExec(t.Context(), p, bare, provider.ExecSpec{Argv: []string{"echo", "hi"}})
	if err != nil || string(out) != "hi\n" || code != 0 {
		t.Fatalf("exec on a ref without meta gave %q exit %d, %v", out, code, err)
	}
	if err := p.SetLifetime(t.Context(), bare, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("lifetime of a ref without meta: %v", err)
	}
}

func TestKoyebSetLifetime(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	ref := ready(t, p, "key-lifetime")
	created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
	if err != nil {
		t.Fatal(err)
	}
	capped := p.Capabilities().MaxLifetime
	before := f.callCount("GET /v1/services/{id}")
	if err := p.SetLifetime(ctx, ref, created.Add(capped)); err != nil {
		t.Fatalf("lifetime at the cap: %v", err)
	}
	if n := f.callCount("GET /v1/services/{id}") - before; n != 1 {
		t.Fatalf("%d service reads, want one to check the sandbox exists", n)
	}
	err = p.SetLifetime(ctx, ref, created.Add(capped+time.Second))
	if !isCode(err, provider.CodeInvalid) {
		t.Fatalf("lifetime past the cap gave %v, want invalid", err)
	}
	if n := f.callCount("GET /v1/services/{id}") - before; n != 1 {
		t.Fatalf("a lifetime past the cap read the service, %d reads", n)
	}

	delete(ref.Meta, "created_at")
	f.setService(ref.ID, func(s *fakeService) { s.created = time.Now().UTC().Add(-23 * time.Hour) })
	if err := p.SetLifetime(ctx, ref, time.Now().Add(30*time.Minute)); err != nil {
		t.Fatalf("lifetime within a day of the service's creation: %v", err)
	}
	err = p.SetLifetime(ctx, ref, time.Now().Add(55*time.Minute))
	if !isCode(err, provider.CodeInvalid) {
		t.Fatalf("lifetime inside the backstop but past the cap gave %v, want invalid", err)
	}
}

func TestKoyebDestroy(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	ref := ready(t, p, "key-destroy")
	if err := p.Destroy(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if apps, services := f.counts(); apps != 0 || services != 0 {
		t.Fatalf("%d apps and %d services after Destroy", apps, services)
	}
	if err := p.Destroy(ctx, ref); err != nil {
		t.Fatalf("Destroy of a deleted app: %v", err)
	}

	ref = ready(t, p, "key-destroy-2")
	f.inject("DELETE /v1/apps/{id}", fault{status: 500, message: "late", after: true})
	if err := p.Destroy(ctx, ref); err != nil {
		t.Fatalf("Destroy that deleted the app but answered 500: %v", err)
	}

	foreign := provider.Ref{Provider: "e2b", ID: "x"}
	before := f.callCount("DELETE /v1/apps/{id}")
	if err := p.Destroy(ctx, foreign); err != nil || f.callCount("DELETE /v1/apps/{id}") != before {
		t.Fatalf("Destroy of another provider's ref gave %v", err)
	}
}

func TestKoyebListOwned(t *testing.T) {
	f := newFake(t)
	f.pageSize = 2
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	odd := map[string]string{"bugnet": "1", "placement": `a"b c`}
	var want []string
	for i := range 5 {
		ref, err := createKey(t, p, fmt.Sprintf("key-own-%d", i), odd)
		if err != nil {
			t.Fatal(err)
		}
		if i == 2 {
			if err := p.Destroy(ctx, ref); err != nil {
				t.Fatal(err)
			}
			continue
		}
		want = append(want, ref.ID)
	}
	if _, err := createKey(t, p, "key-other", map[string]string{"bugnet": "1"}); err != nil {
		t.Fatal(err)
	}
	f.addForeign("web-app", `{"bugnet":"1","placement":"a\"b c"}`)

	owned, err := p.ListOwned(ctx, odd)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, s := range owned {
		got = append(got, s.Ref.ID)
		_, dep, ok := f.box(s.Ref.ID)
		if s.Ref.Provider != "koyeb" || !maps.Equal(s.Labels, odd) || s.CreatedAt.IsZero() ||
			s.CreatedAt.Location() != time.UTC || !ok ||
			s.Ref.Meta["secret"] != fakeEnv(&dep)["SANDBOX_SECRET"] ||
			s.Ref.Meta["service_id"] != dep.serviceID {
			t.Errorf("summary %+v, want a koyeb ref with meta, the labels and a UTC time", s)
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("ListOwned gave %v, want the live bn- sandboxes %v", got, want)
	}
	if n := f.callCount("GET /v1/services"); n != 4 {
		t.Fatalf("%d list pages fetched, want 4", n)
	}
}

func TestKoyebListOwnedSkipsSandboxesItCannotRead(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	labels := map[string]string{"bugnet": "1"}
	var ids []string
	for i := range 4 {
		ref, err := createKey(t, p, fmt.Sprintf("key-sweep-%d", i), labels)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, ref.ID)
	}
	f.setService(ids[3], func(s *fakeService) { s.deployment = "" })
	f.inject("GET /v1/deployments/{id}", fault{status: 429, code: "rate_limit_error",
		message: "slow down", header: map[string]string{"Retry-After": "1"}, times: 1})
	var logged bytes.Buffer
	p.log = slog.New(slog.NewTextHandler(&logged, nil))
	before := len(f.callTimes("GET /v1/deployments/{id}"))
	owned, err := p.ListOwned(ctx, labels)
	if err != nil {
		t.Fatalf("one rate limited read and one sandbox still being created stopped the sweep: %v",
			err)
	}
	reads := f.callTimes("GET /v1/deployments/{id}")[before:]
	if len(owned) != 2 || len(reads) != 3 {
		t.Fatalf("sweep listed %d sandboxes after %d deployment reads, want 2 after 3", len(owned),
			len(reads))
	}
	for _, s := range owned {
		if s.Ref.ID == ids[3] {
			t.Fatalf("sweep listed %s, which has no deployment yet", s.Ref.ID)
		}
	}
	if reads[1].Sub(reads[0]) < time.Second {
		t.Fatalf("deployment reads at %v, want a full Retry-After after the rate limit", reads)
	}
	if n := strings.Count(logged.String(), "skips"); n != 1 ||
		!strings.Contains(logged.String(), "rate_limited") {
		t.Fatalf("sweep logged %q, want one warning for the rate limited read alone",
			logged.String())
	}
}

func TestKoyebFiles(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-files")
	ctx := t.Context()
	dir := t.TempDir()

	data := make([]byte, 1<<20+5)
	_, _ = rand.Read(data)
	name := filepath.Join(dir, "deep", "er", "blob.bin")
	if err := p.WriteFile(ctx, ref, name, data, 0o755); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("written file %v, %v, want mode 0755", info, err)
	}
	up := f.lastUpload()
	sum := sha256.Sum256(data)
	if q := up.URL.Query(); q.Get("mode") != "0755" || q.Get("parents") != "1" ||
		up.Header.Get("X-Content-SHA256") != hex.EncodeToString(sum[:]) {
		t.Errorf("upload %v with headers %v, want mode 0755, parents and the checksum",
			up.URL.Query(), up.Header)
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
		{int64(len(data)), 0, nil, true},
		{int64(len(data) + 50), 10, nil, true},
	}
	for _, r := range reads {
		got, eof, err := p.ReadFile(ctx, ref, name, r.offset, r.length)
		if err != nil || !bytes.Equal(got, r.want) || eof != r.eof {
			t.Errorf("read at %d for %d gave %d bytes eof %v, %v, want %d bytes eof %v",
				r.offset, r.length, len(got), eof, err, len(r.want), r.eof)
		}
	}

	empty := filepath.Join(dir, "empty")
	if err := p.WriteFile(ctx, ref, empty, nil, 0o600); err != nil {
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
	if _, _, err = p.ReadFile(ctx, ref, name, -1, 10); !isCode(err, provider.CodeInvalid) {
		t.Errorf("read at a negative offset gave %v, want invalid", err)
	}
	if err := p.WriteFile(ctx, ref, dir, []byte("x"), 0o644); !isCode(err, provider.CodeInvalid) {
		t.Errorf("write over a directory gave %v, want invalid", err)
	}
}

func TestKoyebListFiles(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-list")
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
			if e.IsDir != (e.Mode&fs.ModeDir != 0) || e.Mode.Perm() == 0 || e.Modified.IsZero() {
				t.Errorf("entry %+v has a mode or time that disagrees with its type", e)
			}
			if e.IsDir {
				rel += "/"
			} else if e.Size != int64(len(rel)) {
				t.Errorf("entry %+v, want %d bytes", e, len(rel))
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
	if got := paths(5); len(got) != 6 {
		t.Errorf("depth 5 lists %v, want everything", got)
	}
	if _, err := listAll(ctx, p, ref, root, 0); !isCode(err, provider.CodeInvalid) {
		t.Errorf("depth 0 gave %v, want invalid", err)
	}
	_, err := listAll(ctx, p, ref, filepath.Join(root, "nope"), 1)
	if !isCode(err, provider.CodeNotFound) {
		t.Errorf("listing a missing directory gave %v, want not_found", err)
	}
	_, err = listAll(ctx, p, ref, filepath.Join(root, "a.txt"), 1)
	if !isCode(err, provider.CodeInvalid) {
		t.Errorf("listing a file gave %v, want invalid", err)
	}
}

func TestKoyebExecutorErrors(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	ref := ready(t, p, "key-exec-errors")
	cases := map[string]struct {
		spec provider.ExecSpec
		code provider.Code
	}{
		"no command": {provider.ExecSpec{}, provider.CodeInvalid},
		"a NUL in a word": {
			provider.ExecSpec{Argv: []string{"echo", "a\x00b"}}, provider.CodeInvalid,
		},
		"a missing directory": {
			provider.ExecSpec{Argv: []string{"true"}, Cwd: "/no/such/dir"}, provider.CodeInvalid,
		},
	}
	for name, c := range cases {
		if _, err := p.Exec(ctx, ref, c.spec); !isCode(err, c.code) {
			t.Errorf("exec with %s gave %v, want %s", name, err, c.code)
		}
	}
	out, code, err := runExec(ctx, p, ref, provider.ExecSpec{Argv: []string{"/no/such/command"}})
	if err != nil || code != 127 || len(out) != 0 {
		t.Errorf("exec of a missing command gave %q exit %d, %v, want exit 127", out, code, err)
	}

	stolen := ref
	stolen.Meta = maps.Clone(ref.Meta)
	stolen.Meta["secret"] = "stolen"
	_, err = p.Exec(ctx, stolen, provider.ExecSpec{Argv: []string{"true"}})
	if !isCode(err, provider.CodeAuth) {
		t.Errorf("exec with a wrong secret gave %v, want auth", err)
	}
	if _, _, err := p.ReadFile(ctx, stolen, "/etc/hosts", 0, 10); !isCode(err, provider.CodeAuth) {
		t.Errorf("read with a wrong secret gave %v, want auth", err)
	}
}

func TestKoyebDeadExecutorOfGoneSandboxIsNotFound(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	ref := ready(t, p, "key-dead")
	dead := ref
	dead.Meta = maps.Clone(ref.Meta)
	dead.Meta["executor_url"] = "https://127.0.0.1:1/koyeb-sandbox"
	if _, _, err := p.ReadFile(ctx, dead, "/etc/hosts", 0, 1); !isCode(err, provider.CodeInternal) {
		t.Errorf("read through a dead host of a live sandbox gave %v, want internal", err)
	}

	f.mu.Lock()
	f.goneStatus = http.StatusBadGateway
	f.mu.Unlock()
	if err := p.Destroy(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.ReadFile(ctx, dead, "/etc/hosts", 0, 1); !isCode(err, provider.CodeNotFound) {
		t.Errorf("read through a dead host of a gone sandbox gave %v, want not_found", err)
	}
	_, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"true"}})
	if !isCode(err, provider.CodeNotFound) {
		t.Errorf("exec on a gone sandbox answered by a bad gateway gave %v, want not_found", err)
	}
}

func TestKoyebExecutorNeverNegotiatesHTTP2(t *testing.T) {
	f := newFake(t)
	resp, err := f.exec.Client().Get(f.exec.URL + "/s/none/koyeb-sandbox/health")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("the fake executor answered a willing client over %s, want HTTP/2", resp.Proto)
	}

	p := f.provider(t, fakeToken)
	tr, ok := p.exec.Transport.(*http.Transport)
	if !ok || tr.ForceAttemptHTTP2 || tr.TLSNextProto == nil || len(tr.TLSNextProto) != 0 {
		t.Fatalf("executor transport %T, want HTTP/2 off with an empty TLSNextProto",
			p.exec.Transport)
	}
	ref := ready(t, p, "key-http1")
	_, _, err = runExec(t.Context(), p, ref, provider.ExecSpec{Argv: []string{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	protos := slices.Clone(f.protos)
	f.mu.Unlock()
	notHTTP1 := func(p string) bool { return p != "HTTP/1.1" }
	if len(protos) < 4 || slices.ContainsFunc(protos, notHTTP1) {
		t.Fatalf("executor requests used %v, want HTTP/1.1 every time", protos)
	}
}

func TestKoyebWrongTokenIsAuth(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, "wrong")
	_, err := p.ListOwned(t.Context(), map[string]string{"bugnet": "1"})
	if !isCode(err, provider.CodeAuth) {
		t.Fatalf("list with a wrong token gave %v, want auth", err)
	}
}

func TestKoyebForeignRefIsNotFound(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := provider.Ref{Provider: "e2b", ID: "x"}
	if err := p.WaitReady(t.Context(), ref); !isCode(err, provider.CodeNotFound) {
		t.Errorf("WaitReady of another provider's ref gave %v", err)
	}
	_, err := p.Exec(t.Context(), ref, provider.ExecSpec{Argv: []string{"true"}})
	if !isCode(err, provider.CodeNotFound) {
		t.Errorf("Exec of another provider's ref gave %v", err)
	}
}

func TestKoyebSizesAndPrices(t *testing.T) {
	p, err := New(Options{APIToken: fakeToken})
	if err != nil {
		t.Fatal(err)
	}
	want := provider.Capabilities{
		Preview: true, GPU: true, EgressControl: true, NeedsRunnerUpload: true,
		MaxLifetime: 24*time.Hour - 10*time.Minute,
	}
	if caps := p.Capabilities(); caps != want {
		t.Errorf("capabilities %+v, want %+v", caps, want)
	}
	if p.Name() != "koyeb" || p.BillingIncrement() != time.Second {
		t.Errorf("name %q increment %v", p.Name(), p.BillingIncrement())
	}
	gpu := provider.Resources{VCPU: 4, MemoryMiB: 16384, DiskGiB: 50, GPUCount: 1, GPUMemoryGiB: 20}
	cases := []struct {
		floor  provider.Resources
		size   string
		hourly int64
	}{
		{provider.Resources{}, "small", 14400},
		{provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10}, "medium", 28800},
		{provider.Resources{VCPU: 2, MemoryMiB: 4096}, "large", 57600},
		{provider.Resources{VCPU: 4, MemoryMiB: 8192, DiskGiB: 20}, "xlarge", 115200},
		{provider.Resources{VCPU: 40, MemoryMiB: 131072, DiskGiB: 400}, "5xlarge", 1843200},
		{gpu, "gpu-nvidia-rtx-4000-sff-ada", 504000},
		{provider.Resources{GPUMemoryGiB: 1}, "gpu-nvidia-rtx-4000-sff-ada", 504000},
	}
	for _, c := range cases {
		s, ok := p.SizeFor(c.floor)
		if !ok || s.Name != c.size || p.HourlyCostMicros(s) != c.hourly {
			t.Errorf("floor %+v gave %+v %v at %d, want %s at %d", c.floor, s, ok,
				p.HourlyCostMicros(s), c.size, c.hourly)
		}
	}
	s, _ := p.SizeFor(gpu)
	if s.Resources != (provider.Resources{
		VCPU: 6, MemoryMiB: 45056, DiskGiB: 240, GPUCount: 1, GPUMemoryGiB: 20,
	}) {
		t.Errorf("GPU size %+v", s)
	}
	for _, floor := range []provider.Resources{
		{VCPU: 41}, {MemoryMiB: 131073}, {DiskGiB: 401}, {GPUCount: 2}, {GPUMemoryGiB: 21},
		{VCPU: 7, GPUCount: 1},
	} {
		if s, ok := p.SizeFor(floor); ok {
			t.Errorf("floor %+v gave %+v, want none", floor, s)
		}
	}

	f := newFake(t)
	named, err := New(Options{
		APIToken: fakeToken, BaseURL: f.api.URL,
		InstanceTypeBySize: map[string]string{"cpu": "large", "gpu": "gpu-nvidia-rtx-4000-sff-ada"},
	})
	if err != nil {
		t.Fatal(err)
	}
	s, ok := named.SizeFor(provider.Resources{})
	if !ok || s.Name != "cpu" || s.Resources.VCPU != 4 || named.HourlyCostMicros(s) != 57600 {
		t.Fatalf("configured sizes gave %+v %v", s, ok)
	}
	if _, ok := named.SizeFor(provider.Resources{VCPU: 5}); ok {
		t.Error("configured sizes offer more than the configured types")
	}
	ref, err := named.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "key-named", Size: s, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	svc, _, _ := f.box(ref.ID)
	if got := field(svc.body, "definition.instance_types.0.type"); got != "large" {
		t.Errorf("size cpu deployed as %v, want large", got)
	}
}

func TestKoyebOptions(t *testing.T) {
	bad := map[string]Options{
		"no token": {},
		"an eco type": {
			APIToken: "t", InstanceTypeBySize: map[string]string{"a": "eco-large"},
		},
		"a size without name": {APIToken: "t", InstanceTypeBySize: map[string]string{"": "small"}},
	}
	for name, opts := range bad {
		if _, err := New(opts); err == nil {
			t.Errorf("New with %s succeeded", name)
		}
	}
	p, err := New(Options{APIToken: "t", Region: "par"})
	if err != nil || p.region != "par" {
		t.Fatalf("region option gave %+v, %v", p, err)
	}
}

func TestParseInstanceTypes(t *testing.T) {
	got, err := ParseInstanceTypes(" cpu=medium , gpu=gpu-nvidia-rtx-4000-sff-ada")
	want := map[string]string{"cpu": "medium", "gpu": "gpu-nvidia-rtx-4000-sff-ada"}
	if err != nil || !maps.Equal(got, want) {
		t.Fatalf("parsed %v, %v, want %v", got, err, want)
	}
	if got, err := ParseInstanceTypes(""); err != nil || len(got) != 0 {
		t.Fatalf("empty list gave %v, %v", got, err)
	}
	for _, s := range []string{
		"cpu", "=medium", "cpu=", "cpu=medium,cpu=large", "cpu=medium,", "cpu=nano",
	} {
		if _, err := ParseInstanceTypes(s); err == nil {
			t.Errorf("ParseInstanceTypes(%q) succeeded", s)
		}
	}
}

/* Helpers */

const completeEvent = "event: complete\ndata: {\"message\": \"stream ended\"}\n\n"

func logEvent(stream, line string) string {
	data, _ := json.Marshal(map[string]string{
		"timestamp": "2026-11-04T12:00:00Z", "stream": stream, "data": line,
	})
	return "event: log\ndata: " + string(data) + "\n\n"
}

// field walks a decoded JSON value along a dotted path of keys and indexes
func field(v any, path string) any {
	for _, k := range strings.Split(path, ".") {
		switch x := v.(type) {
		case map[string]any:
			v = x[k]
		case []any:
			var i int
			if _, err := fmt.Sscan(k, &i); err != nil || i >= len(x) {
				return nil
			}
			v = x[i]
		default:
			return nil
		}
	}
	return v
}

func jsonEqual(got any, want string) bool {
	var w any
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		return false
	}
	return reflect.DeepEqual(got, w)
}

func createKey(
	t *testing.T, p *Provider, key string, labels map[string]string,
) (provider.Ref, error) {
	t.Helper()
	size, _ := p.SizeFor(provider.Resources{})
	return p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: key, Size: size, Labels: labels, Lifetime: 15 * time.Minute,
	})
}

// ready creates a sandbox and waits until its executor answers
func ready(t *testing.T, p *Provider, key string) provider.Ref {
	t.Helper()
	ref, err := createKey(t, p, key, map[string]string{"bugnet": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

func create(ctx context.Context, p *Provider, _ provider.Ref) error {
	size, _ := p.SizeFor(provider.Resources{})
	_, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: "key-errors", Size: size, Labels: map[string]string{"bugnet": "1"},
		Lifetime: time.Minute,
	})
	return err
}

func waitReady(ctx context.Context, p *Provider, ref provider.Ref) error {
	return p.WaitReady(ctx, ref)
}

func listOwned(ctx context.Context, p *Provider, _ provider.Ref) error {
	_, err := p.ListOwned(ctx, map[string]string{"bugnet": "1"})
	return err
}

func destroy(ctx context.Context, p *Provider, ref provider.Ref) error {
	return p.Destroy(ctx, ref)
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

func TestKoyebRefusesExecutorRedirects(t *testing.T) {
	agent, reached := contract.Redirector(t)
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := provider.Ref{Provider: "koyeb", ID: "app-1", Meta: map[string]string{
		metaExecutor: agent, metaSecret: "secret",
	}}
	ctx := t.Context()
	if data, _, err := p.ReadFile(ctx, ref, "/etc/hostname", 0, 64); err == nil {
		t.Errorf("read through a redirecting executor gave %q, want an error", data)
	}
	if err := p.WriteFile(ctx, ref, "/tmp/f", []byte("tenant bytes"), 0o644); err == nil {
		t.Error("write through a redirecting executor succeeded")
	}
	if reached.Load() {
		t.Fatal("the adapter followed an executor redirect")
	}
}

// listAll lists through ListFiles with room for every entry a test makes
func listAll(
	ctx context.Context, p *Provider, ref provider.Ref, dir string, depth int,
) ([]provider.FileEntry, error) {
	l := provider.NewListing(10000)
	err := p.ListFiles(ctx, ref, dir, depth, l)
	return l.Entries, err
}

func TestKoyebListFilesStopsAtTheLimit(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	contract.ListLimit(t, p, ready(t, p, "key-list-limit"), t.TempDir())
}

func TestKoyebRefusesAnEndlessEvent(t *testing.T) {
	line := "data: " + strings.Repeat("x", 1<<20) + "\n"
	r := strings.NewReader(strings.Repeat(line, 9))
	s := &execStream{lines: bufio.NewScanner(r)}
	s.lines.Buffer(make([]byte, 64<<10), maxEventLine)
	if _, data, err := s.event(); err == nil {
		t.Fatalf("an event of 9 MiB of data lines gave %d bytes, want an error", len(data))
	}
}
