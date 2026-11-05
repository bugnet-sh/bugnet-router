package boat

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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
	"github.com/oynozan/bugnet-router/internal/provider/shell"
	"github.com/oynozan/bugnet-router/internal/testenv"
)

const (
	routeCreate  = "POST /api/v1/sandboxes"
	routeGet     = "GET /api/v1/sandboxes/{id}"
	routeUpdate  = "PATCH /api/v1/sandboxes/{id}"
	routeDelete  = "DELETE /api/v1/sandboxes/{id}"
	routeCommand = "POST /api/v1/sandboxes/{id}/commands"
	routeAttach  = "GET /api/v1/sandboxes/{id}/commands/{pid}/stream"
	routeRead    = "GET /api/v1/sandboxes/{id}/files"
	routeWrite   = "PUT /api/v1/sandboxes/{id}/files"
	routeInfo    = "GET /api/v1/sandboxes/{id}/files/info"
)

func TestBoatContract(t *testing.T) {
	f := newFake(t)
	f.startPolls = 2
	contract.Run(t, func(t *testing.T) provider.Provider { return f.provider(t, fakeKey) })
}

func TestBoatSendsNoEnvAndSnapshotsOff(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	size, ok := p.SizeFor(provider.Resources{VCPU: 4, MemoryMiB: 8 << 10})
	if !ok || size.Name != "default" {
		t.Fatalf("size for 4 vCPU and 8 GiB: %+v, %v", size, ok)
	}
	labels := map[string]string{"bugnet": "1", "bugnet_deployment": "main", "placement": "p-1"}
	req := provider.CreateRequest{
		IdempotencyKey: "order-1|0|boat", Size: size, Labels: labels,
		Lifetime: 15*time.Minute + 1500*time.Millisecond,
	}
	before := time.Now().UTC().Truncate(time.Second)
	ref, err := p.Create(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}

	call := f.lastCreate()
	want := map[string]any{
		"type": "default", "ttlSeconds": 902.0, "noEnv": true, "snapshots": false,
		"monitoring": false, "failFast": true,
		"labels": map[string]any{"bugnet": "1", "bugnet_deployment": "main", "placement": "p-1"},
	}
	if !reflect.DeepEqual(call.body, want) {
		t.Errorf("create body %v, want exactly %v", call.body, want)
	}
	sum := sha256.Sum256([]byte("order-1|0|boat"))
	if got := call.header.Get("Idempotency-Key"); got != hex.EncodeToString(sum[:]) {
		t.Errorf("Idempotency-Key %q, want the hex SHA-256 of the key", got)
	}
	s, _ := f.sandbox(ref.ID)
	if ref.Provider != "boat" || s.id != ref.ID || ref.Meta["arch"] != "amd64" {
		t.Errorf("ref %+v, want boat sandbox %s with arch amd64", ref, s.id)
	}
	created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
	if err != nil || created.Before(before) || created.After(time.Now()) {
		t.Errorf("created_at %q, want the creation time", ref.Meta["created_at"])
	}

	again, err := p.Create(t.Context(), req)
	if err != nil || again.ID != ref.ID || f.liveCount() != 1 {
		t.Fatalf("repeated create gave %+v, %v with %d sandboxes, want %s", again, err,
			f.liveCount(), ref.ID)
	}
	calls := f.createCalls()
	first, second := calls[0], calls[1]
	if !reflect.DeepEqual(first.body, second.body) ||
		first.header.Get("Idempotency-Key") != second.header.Get("Idempotency-Key") {
		t.Errorf("repeated create sent another body or key, Boat refuses that")
	}
}

func TestBoatCreateRejectsBadInput(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	size, _ := p.SizeFor(provider.Resources{})
	many := map[string]string{}
	for i := range 21 {
		many["l"+strconv.Itoa(i)] = "1"
	}
	reqs := map[string]provider.CreateRequest{
		"unknown size":     {Size: provider.Size{Name: "huge"}, Lifetime: time.Hour},
		"no lifetime":      {Size: size},
		"lifetime too far": {Size: size, Lifetime: 31 * 24 * time.Hour},
		"bad label key":    {Size: size, Lifetime: time.Hour, Labels: map[string]string{"-a": "1"}},
		"too many labels":  {Size: size, Lifetime: time.Hour, Labels: many},
		"long label value": {Size: size, Lifetime: time.Hour,
			Labels: map[string]string{"a": strings.Repeat("v", 257)}},
	}
	for name, req := range reqs {
		_, err := p.Create(t.Context(), req)
		if !isCode(err, provider.CodeInvalid) {
			t.Errorf("%s gave %v, want invalid", name, err)
		}
	}
	if n := f.createCount(); n != 0 {
		t.Fatalf("rejected creates reached Boat %d times", n)
	}
}

func TestBoatErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		route    string
		ft       fault
		op       func(ctx context.Context, p *Provider, ref provider.Ref) error
		code     provider.Code
		failOver bool
		wait     time.Duration
	}{
		{
			name: "no ready machine", route: routeCreate, op: create,
			ft: fault{status: 503, code: "no_ready_machine", message: "No machine is ready",
				top: map[string]any{"retryAfterSeconds": 5}},
			code: provider.CodeCapacity, failOver: true, wait: 5 * time.Second,
		},
		{
			name: "out of capacity", route: routeCreate, op: create,
			ft: fault{status: 503, code: "out_of_capacity", message: "Creates are paused",
				top: map[string]any{"retryAfterSeconds": 120}},
			code: provider.CodeCapacity, failOver: true, wait: 120 * time.Second,
		},
		{
			name: "start limit", route: routeCreate, op: create,
			ft: fault{status: 429, code: "rate_limited", message: "Start limit of the minute",
				top: map[string]any{"retryAfterSeconds": 17, "window": "minute"}},
			code: provider.CodeRateLimited, failOver: true, wait: 17 * time.Second,
		},
		{
			name: "start limit with the wait in details", route: routeCreate, op: create,
			ft: fault{status: 429, code: "rate_limited", message: "Start limit of the hour",
				details: map[string]any{"retryAfterSeconds": 9}},
			code: provider.CodeRateLimited, failOver: true, wait: 9 * time.Second,
		},
		{
			name: "ip limit", route: routeCreate, op: create,
			ft: fault{status: 429, code: "IP_RATE_LIMIT_EXCEEDED", message: "Too many requests",
				top:    map[string]any{"retryAfter": 3},
				header: map[string]string{"Retry-After": "3"}},
			code: provider.CodeRateLimited, failOver: true, wait: 3 * time.Second,
		},
		{
			name: "concurrency cap", route: routeCreate, op: create,
			ft:   fault{status: 429, code: "limit_reached", message: "100 sandboxes at once"},
			code: provider.CodeQuota, failOver: true,
		},
		{
			name: "billing required", route: routeCreate, op: create,
			ft:   fault{status: 402, code: "billing_required", message: "Add a payment method"},
			code: provider.CodeUnfunded, failOver: true,
		},
		{
			name: "member cap", route: routeCreate, op: create,
			ft:   fault{status: 402, code: "team_member_cap_reached", message: "Usage cap reached"},
			code: provider.CodeQuota, failOver: true,
		},
		{
			name: "unauthorized", route: routeCreate, op: create,
			ft:   fault{status: 401, code: "unauthorized", message: "Unauthorized"},
			code: provider.CodeAuth, failOver: true,
		},
		{
			name: "expired key", route: routeCreate, op: create,
			ft:   fault{status: 403, code: "api_key_expired", message: "The key expired"},
			code: provider.CodeAuth, failOver: true,
		},
		{
			name: "size needs a plan", route: routeCreate, op: create,
			ft: fault{status: 403, code: "machine_class_plan_required",
				message: "xlarge needs the $100 plan"},
			code: provider.CodeQuota, failOver: true,
		},
		{
			name: "organization suspended", route: routeCreate, op: create,
			ft:   fault{status: 403, code: "org_suspended", message: "The wallet is suspended"},
			code: provider.CodeUnfunded, failOver: true,
		},
		{
			name: "bad request", route: routeCreate, op: create,
			ft:   fault{status: 400, code: "invalid_labels", message: "Label key is invalid"},
			code: provider.CodeInvalid,
		},
		{
			name: "unprocessable", route: routeCreate, op: create,
			ft:   fault{status: 422, code: "invalid_request", message: "Unprocessable"},
			code: provider.CodeInvalid,
		},
		{
			name: "idempotency in progress", route: routeCreate, op: create,
			ft: fault{status: 409, code: "idempotency_in_progress",
				message: "The first request is still minting"},
			code: provider.CodeOutcomeUnknown,
		},
		{
			name: "monitoring forced on", route: routeCreate, op: create,
			ft: fault{status: 409, code: "monitoring_off_for_account",
				message: "Monitoring is off for the account"},
			code: provider.CodeInvalid,
		},
		{
			name: "server error on create", route: routeCreate, op: create,
			ft:   fault{status: 500, code: "http_500", message: "Internal error"},
			code: provider.CodeOutcomeUnknown,
		},
		{
			name: "reply lost on create", route: routeCreate, op: create,
			ft:   fault{drop: true, after: true},
			code: provider.CodeOutcomeUnknown,
		},
		{
			name: "unavailable on create", route: routeCreate, op: create,
			ft:   fault{status: 503, code: "service_unavailable", message: "Try again"},
			code: provider.CodeOutcomeUnknown,
		},
		{
			name: "direct failure on create", route: routeCreate, op: create,
			ft: fault{status: 502, code: "sandbox_direct_failed", message: "sandbox_restoring",
				details: map[string]any{"retryable": true}},
			code: provider.CodeOutcomeUnknown,
		},
		{
			name: "server error outside create", route: routeUpdate, op: setLifetime,
			ft:   fault{status: 500, code: "http_500", message: "Internal error"},
			code: provider.CodeInternal,
		},
		{
			name: "unavailable outside create", route: routeUpdate, op: setLifetime,
			ft:   fault{status: 503, code: "browser_not_ready", message: "Not ready"},
			code: provider.CodeInternal,
		},
		{
			name: "sandbox gone", route: routeGet, op: waitReady,
			ft:   fault{status: 404, code: "not_found", message: "Sandbox not found"},
			code: provider.CodeNotFound,
		},
		{
			name: "legacy not found code", route: routeUpdate, op: setLifetime,
			ft:   fault{status: 404, code: "box_not_found", message: "Box not found"},
			code: provider.CodeNotFound,
		},
		{
			name: "command may have run", route: routeCommand, op: execTrue,
			ft: fault{status: 502, code: "boat_direct_failed", message: "machine went away",
				details: map[string]any{"retryable": false}},
			code: provider.CodeOutcomeUnknown,
		},
		{
			name: "command did not run", route: routeCommand, op: execTrue,
			ft: fault{status: 502, code: "sandbox_direct_failed", message: "sandbox_restoring",
				details: map[string]any{"retryable": true}},
			code: provider.CodeInternal,
		},
		{
			name: "command did not run with the boat code", route: routeCommand, op: execTrue,
			ft: fault{status: 502, code: "boat_direct_failed", message: "sandbox_restoring",
				details: map[string]any{"retryable": true}},
			code: provider.CodeInternal,
		},
		{
			name: "command may have run without details", route: routeCommand, op: execTrue,
			ft:   fault{status: 502, code: "box_direct_failed", message: "machine went away"},
			code: provider.CodeOutcomeUnknown,
		},
		{
			name: "server error on command start", route: routeCommand, op: execTrue,
			ft:   fault{status: 500, code: "http_500", message: "Internal error"},
			code: provider.CodeInternal,
		},
		{
			name: "bad gateway on command start", route: routeCommand, op: execTrue,
			ft:   fault{status: 502, code: "bad_gateway", message: "Upstream failed"},
			code: provider.CodeInternal,
		},
		{
			name: "unavailable on command start", route: routeCommand, op: execTrue,
			ft:   fault{status: 503, code: "service_unavailable", message: "Try again"},
			code: provider.CodeInternal,
		},
		{
			name: "reply lost on command start", route: routeCommand, op: execTrue,
			ft:   fault{drop: true, after: true},
			code: provider.CodeOutcomeUnknown,
		},
		{
			name: "sandbox starting", route: routeInfo, op: readFile,
			ft:   fault{status: 409, code: "boat_starting", message: "Still starting"},
			code: provider.CodeInternal,
		},
		{
			name: "missing file", route: routeInfo, op: readFile,
			ft:   fault{status: 400, code: "file_not_found", message: "No such file"},
			code: provider.CodeNotFound,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			ref := newRef(f.add(map[string]string{"bugnet": "1"}).id, time.Now())
			f.inject(c.route, c.ft)
			err := c.op(t.Context(), p, ref)
			e, ok := provider.AsError(err)
			if !ok {
				t.Fatalf("got %v, want a provider error", err)
			}
			if e.Code != c.code || e.CanFailOver != c.failOver || e.RetryAfter != c.wait {
				t.Fatalf("got code %s fail over %v retry after %v, want %s, %v and %v (%v)",
					e.Code, e.CanFailOver, e.RetryAfter, c.code, c.failOver, c.wait, err)
			}
			if c.ft.message != "" && (!strings.Contains(err.Error(), c.ft.message) ||
				!strings.Contains(err.Error(), "req_fake")) {
				t.Fatalf("error %q lacks the message %q or the request id", err, c.ft.message)
			}
		})
	}
}

func TestBoatUnreachableAPIFailsOver(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	p, err := New(Options{APIKey: fakeKey, BaseURL: "http://" + addr + "/api/v1"})
	if err != nil {
		t.Fatal(err)
	}
	err = create(t.Context(), p, provider.Ref{})
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInternal || !e.CanFailOver {
		t.Fatalf("create that never reached Boat gave %v, want internal that can fail over", err)
	}
}

func TestBoatWrongKeyIsAuth(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, "boat_wrong")
	_, err := p.ListOwned(t.Context(), map[string]string{"bugnet": "1"})
	if !isCode(err, provider.CodeAuth) {
		t.Fatalf("list with a wrong key gave %v, want auth", err)
	}
}

func TestBoatWaitReady(t *testing.T) {
	f := newFake(t)
	f.startPolls = 3
	p := f.provider(t, fakeKey)
	ref := createSmall(t, p, "key-ready")
	f.inject(routeGet, fault{status: 500, code: "http_500", message: "blip", times: 2})
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	if n := f.callCount(routeGet); n != 6 {
		t.Fatalf("WaitReady read the sandbox %d times, want 2 failures, 3 starts and 1 ready", n)
	}

	ends := []struct {
		state    string
		code     provider.Code
		failOver bool
	}{
		{"cancelled", provider.CodeCapacity, true},
		{"error", provider.CodeInternal, true},
		{"archived", provider.CodeInternal, true},
	}
	for _, end := range ends {
		f.endState = end.state
		ref := createSmall(t, p, "key-"+end.state)
		err := p.WaitReady(t.Context(), ref)
		e, ok := provider.AsError(err)
		if !ok || e.Code != end.code || e.CanFailOver != end.failOver {
			t.Errorf("wait on a sandbox ending %s gave %v, want %s", end.state, err, end.code)
		}
	}

	f.endState = ""
	f.startPolls = 1 << 20
	ref = createSmall(t, p, "key-slow")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := p.WaitReady(ctx, ref)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), ref.ID) {
		t.Fatalf("wait past the deadline gave %v, want the deadline and the sandbox id", err)
	}
}

func TestBoatSetLifetime(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := createSmall(t, p, "key-life")
	if err := p.SetLifetime(t.Context(), ref, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if s, _ := f.sandbox(ref.ID); s.ttl < 3599 || s.ttl > 3601 {
		t.Fatalf("ttl %d s, want an hour from now", s.ttl)
	}
	if err := p.SetLifetime(t.Context(), ref, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if s, _ := f.sandbox(ref.ID); s.ttl != 1 {
		t.Fatalf("a past lifetime set ttl %d s, want 1", s.ttl)
	}
	calls := f.callCount(routeUpdate)
	err := p.SetLifetime(t.Context(), ref, time.Now().Add(30*24*time.Hour+time.Minute))
	if !isCode(err, provider.CodeInvalid) || f.callCount(routeUpdate) != calls {
		t.Fatalf("lifetime past 30 days gave %v, want invalid without a call", err)
	}
}

func TestBoatDestroy(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := createSmall(t, p, "key-destroy")
	for i := range 2 {
		if err := p.Destroy(t.Context(), ref); err != nil {
			t.Fatalf("destroy %d: %v", i+1, err)
		}
	}
	if s, _ := f.sandbox(ref.ID); !s.deleted {
		t.Fatal("sandbox still exists after Destroy")
	}
	if err := p.Destroy(t.Context(), provider.Ref{Provider: "e2b", ID: "x"}); err != nil {
		t.Fatalf("destroy of another provider's ref: %v", err)
	}
	other := newRef(f.add(nil).id, time.Now())
	f.inject(routeDelete, fault{status: 500, code: "http_500", message: "Internal error"})
	if err := p.Destroy(t.Context(), other); !isCode(err, provider.CodeInternal) {
		t.Fatalf("failed delete gave %v, want internal", err)
	}
}

func TestBoatListOwned(t *testing.T) {
	f := newFake(t)
	f.pageSize = 2
	p := f.provider(t, fakeKey)
	want := map[string]string{"bugnet": "1", "bugnet_deployment": "main"}
	var ids []string
	for i := range 3 {
		labels := maps.Clone(want)
		labels["placement"] = "p-" + strconv.Itoa(i)
		ref, err := p.Create(t.Context(), provider.CreateRequest{
			IdempotencyKey: "key-" + strconv.Itoa(i), Size: smallSize(t, p), Labels: labels,
			Lifetime: time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, ref.ID)
	}
	f.add(map[string]string{"bugnet": "1", "bugnet_deployment": "other"})
	f.add(nil)

	owned, err := p.ListOwned(t.Context(), want)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, s := range owned {
		got = append(got, s.Ref.ID)
		if s.Ref.Provider != "boat" || s.Ref.Meta["arch"] != "amd64" || s.CreatedAt.IsZero() ||
			!labelsHave(s.Labels, want) || s.Labels["placement"] == "" {
			t.Errorf("summary %+v lacks its labels, creation time or ref", s)
		}
	}
	if !slices.Equal(got, ids) {
		t.Fatalf("ListOwned gave %v, want %v", got, ids)
	}
	queries := f.listQueries()
	if len(queries) != 2 || queries[1].Get("cursor") != "2" {
		t.Fatalf("list queries %v, want two pages with the cursor of the first", queries)
	}
	labels := queries[0]["label"]
	slices.Sort(labels)
	if !slices.Equal(labels, []string{"bugnet=1", "bugnet_deployment=main"}) {
		t.Fatalf("label filters %v, want one key=value per label", labels)
	}
}

func TestBoatExecSendsOneDetachedCommand(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	cwd := filepath.Join(fakeDir(t), "it's a dir")
	if err := os.Mkdir(cwd, 0o755); err != nil {
		t.Fatal(err)
	}
	greeting := "it's $HOME `id` \"q\" \\"
	spec := provider.ExecSpec{
		Argv: []string{"sh", "-c", `printf '%s|' "$GREETING" "$1" "$(pwd)"`, "argv0", "a b"},
		Cwd:  cwd,
		Env:  map[string]string{"GREETING": greeting},
	}
	out, code, err := runExec(t.Context(), p, ref, spec)
	if err != nil {
		t.Fatal(err)
	}
	if want := greeting + "|a b|" + cwd + "|"; string(out) != want || code != 0 {
		t.Fatalf("exec gave %q exit %d, want %q", out, code, want)
	}
	line, _ := shell.Command(spec)
	if body := f.lastCommand(); !reflect.DeepEqual(body, map[string]any{
		"command": line, "detached": true,
	}) {
		t.Fatalf("command body %v, want only the shell line and detached", body)
	}

	if _, err := p.Exec(t.Context(), ref, provider.ExecSpec{}); !isCode(err,
		provider.CodeInvalid) {
		t.Fatalf("exec without a command gave %v, want invalid", err)
	}
}

func TestBoatExecStreams(t *testing.T) {
	f := newFake(t)
	f.heartbeat = 5 * time.Millisecond
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)

	spec := provider.ExecSpec{Argv: []string{"sh", "-c", "echo out; echo err >&2; exit 3"}}
	out, code, err := runExec(t.Context(), p, ref, spec)
	if err != nil || string(out) != "out\n" || code != 3 {
		t.Fatalf("exec gave %q exit %d, %v, want stdout only and exit 3", out, code, err)
	}

	spec = provider.ExecSpec{Argv: []string{"sh", "-c", "sleep 0.1; echo late"}}
	out, code, err = runExec(t.Context(), p, ref, spec)
	if err != nil || string(out) != "late\n" || code != 0 {
		t.Fatalf("quiet command gave %q exit %d, %v, want heartbeats skipped", out, code, err)
	}

	spec = provider.ExecSpec{Argv: []string{"sh", "-c", "head -c 300000 /dev/zero | tr '\\0' a"}}
	out, _, err = runExec(t.Context(), p, ref, spec)
	if err != nil || len(out) != 300000 || bytes.Count(out, []byte("a")) != 300000 {
		t.Fatalf("large output gave %d bytes, %v", len(out), err)
	}
}

func TestBoatExecReconnectsWithoutLosingOutput(t *testing.T) {
	const lines = 40
	var want strings.Builder
	for i := range lines {
		fmt.Fprintf(&want, "line %d\n", i)
	}
	cases := []struct {
		name string
		set  func(f *fakeBoat)
	}{
		{"reconnect lines", func(f *fakeBoat) { f.chunkBytes, f.reconnectEvery = 5, 3 }},
		{"dropped connections", func(f *fakeBoat) { f.chunkBytes, f.dropEvery = 5, 2 }},
		{"rewound streams", func(f *fakeBoat) {
			f.chunkBytes, f.reconnectEvery, f.rewind = 5, 3, 7
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			ref := readySandbox(t, f)
			c.set(f)
			script := fmt.Sprintf(`i=0; while [ $i -lt %d ]; do echo "line $i"; i=$((i+1)); `+
				`sleep 0.002; done; exit 4`, lines)
			out, code, err := runExec(t.Context(), p, ref,
				provider.ExecSpec{Argv: []string{"sh", "-c", script}})
			if err != nil || string(out) != want.String() || code != 4 {
				t.Fatalf("exec gave %q exit %d, %v, want every line once and exit 4", out, code,
					err)
			}
			attaches := f.attachCalls()
			if len(attaches) < 3 || attaches[0].stdout != 0 {
				t.Fatalf("attached %d times from %+v, want a fresh start and reattaches",
					len(attaches), attaches)
			}
			for i := 1; i < len(attaches); i++ {
				if attaches[i].stdout <= attaches[i-1].stdout ||
					attaches[i].stdout > want.Len() {
					t.Fatalf("reattach offsets %+v, want each past the output delivered",
						attaches)
				}
			}
		})
	}
}

func TestBoatExecReattachesAQuietStream(t *testing.T) {
	for _, beat := range []time.Duration{0, 5 * time.Millisecond} {
		t.Run(fmt.Sprintf("heartbeat %v", beat), func(t *testing.T) {
			f := newFake(t)
			f.heartbeat = beat
			p := f.provider(t, fakeKey)
			// 40 heartbeats fit in one idle timeout and a second of quiet holds five
			p.idleTimeout = 200 * time.Millisecond
			ref := readySandbox(t, f)
			out, code, err := runExec(t.Context(), p, ref,
				provider.ExecSpec{Argv: []string{"sh", "-c", "sleep 1; echo x"}})
			if err != nil || string(out) != "x\n" || code != 0 {
				t.Fatalf("quiet command gave %q exit %d, %v", out, code, err)
			}
			n := len(f.attachCalls())
			if beat == 0 && n < 2 {
				t.Fatalf("a stream without heartbeats was attached %d times, want reattaches", n)
			}
			if beat > 0 && n != 1 {
				t.Fatalf("a stream with heartbeats was attached %d times, want once", n)
			}
		})
	}
}

func TestBoatExecGivesUpOnABrokenStream(t *testing.T) {
	f := newFake(t)
	f.dropEvery = 1
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	stream, err := p.Exec(t.Context(), ref,
		provider.ExecSpec{Argv: []string{"sh", "-c", "echo a; sleep 30"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	f.inject(routeAttach, fault{status: 500, code: "http_500", message: "attach failed"})
	_, err = io.Copy(io.Discard, stream)
	if !isCode(err, provider.CodeInternal) || !strings.Contains(err.Error(), "attach failed") {
		t.Fatalf("stream that cannot reattach gave %v, want internal with the last failure", err)
	}
	if n := f.callCount(routeAttach); n != maxReattach+1 {
		t.Fatalf("attached %d times, want the first and %d retries", n, maxReattach)
	}
}

func TestBoatExecLastLines(t *testing.T) {
	const attached = `{"type":"attached","processId":1,"stdoutOffset":0,"stderrOffset":0}` + "\n"
	cases := []struct {
		name   string
		script string
		out    string
		code   int
		err    provider.Code
		msg    string
	}{
		{
			name: "killed by a signal",
			script: `{"type":"stdout","data":"a\n","nextOffset":2}` + "\n" +
				`{"type":"exit","status":"exited","exitCode":null,"signal":"SIGKILL",` +
				`"stdoutOffset":2,"stderrOffset":0}`,
			out: "a\n", code: 137,
		},
		{
			name:   "terminated by a signal number",
			script: `{"type":"exit","status":"exited","exitCode":null,"signal":"15"}`,
			code:   143,
		},
		{
			name: "lines it does not know",
			script: `{"type":"progress","percent":5}` + "\n" + `{"type":"heartbeat"}` + "\n" +
				`{"type":"stderr","data":"e","nextOffset":1}` + "\n" +
				`{"type":"stdout","data":"ok","nextOffset":2}` + "\n" +
				`{"type":"exit","status":"exited","exitCode":0,"signal":null}`,
			out: "ok",
		},
		{
			name:   "lost by the agent",
			script: `{"type":"exit","status":"lost","exitCode":null,"signal":null}`,
			err:    provider.CodeOutcomeUnknown, msg: "lost",
		},
		{
			name:   "no code and no signal",
			script: `{"type":"exit","status":"exited","exitCode":null,"signal":null}`,
			err:    provider.CodeOutcomeUnknown,
		},
		{
			name:   "unknown signal",
			script: `{"type":"exit","status":"exited","exitCode":null,"signal":"SIGWINKY"}`,
			err:    provider.CodeOutcomeUnknown, msg: "SIGWINKY",
		},
		{
			name: "error line",
			script: `{"type":"error","error":"agent_gone","message":"the agent restarted",` +
				`"stdoutOffset":0,"stderrOffset":0}`,
			err: provider.CodeInternal, msg: "the agent restarted",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			ref := readySandbox(t, f)
			script := attached + c.script + "\n"
			f.script = &script
			out, code, err := runExec(t.Context(), p, ref, provider.ExecSpec{Argv: []string{"x"}})
			if c.err != "" {
				if !isCode(err, c.err) || !strings.Contains(err.Error(), c.msg) {
					t.Fatalf("exec gave exit %d, %v, want %s naming %q", code, err, c.err, c.msg)
				}
				return
			}
			if err != nil || string(out) != c.out || code != c.code {
				t.Fatalf("exec gave %q exit %d, %v, want %q exit %d", out, code, err, c.out,
					c.code)
			}
		})
	}
}

func TestBoatExecCloseOnlyStopsFollowing(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	stream, err := p.Exec(t.Context(), ref, provider.ExecSpec{Argv: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return f.running() == 1 && f.openStreams() == 1 })
	read := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, stream)
		read <- err
	}()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if killed := f.killed(); len(killed) != 0 {
		t.Fatalf("Close stopped commands %v, want it only to stop following", killed)
	}
	select {
	case err := <-read:
		if err == nil {
			t.Fatal("a Read cut short by Close reported the end of the output")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not end a Read waiting on the stream")
	}
	waitFor(t, func() bool { return f.openStreams() == 0 })
	if n := f.running(); n != 1 {
		t.Fatalf("%d commands run after Close, want the one it stopped following", n)
	}
	if _, err := stream.Wait(); err == nil {
		t.Fatal("Wait after Close reported a clean exit")
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestBoatExecAttachFailures(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	f.inject(routeAttach, fault{status: 500, code: "http_500", message: "blip", times: 2})
	out, code, err := runExec(t.Context(), p, ref, provider.ExecSpec{Argv: []string{"echo", "x"}})
	if err != nil || string(out) != "x\n" || code != 0 {
		t.Fatalf("exec through two failed attaches gave %q exit %d, %v", out, code, err)
	}

	f.inject(routeAttach, fault{status: 403, code: "api_key_action_forbidden",
		message: "the key lacks exec"})
	_, err = p.Exec(t.Context(), ref, provider.ExecSpec{Argv: []string{"sleep", "30"}})
	if !isCode(err, provider.CodeAuth) {
		t.Fatalf("exec whose attach is refused gave %v, want auth", err)
	}
	if killed, n := f.killed(), f.running(); len(killed) != 0 || n != 1 {
		t.Fatalf("a refused attach stopped commands %v leaving %d running, want it left running",
			killed, n)
	}
}

func TestBoatFiles(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	ctx := t.Context()
	dir := fakeDir(t)

	data := []byte("hello, boat files\n")
	name := dir + "/deep/er/data.txt"
	if err := p.WriteFile(ctx, ref, name, data, 0o640); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("written file %v, %v, want mode 0640", info, err)
	}
	if perms := f.permissionCalls(); !slices.Equal(perms, []string{name + " 640"}) {
		t.Fatalf("permission calls %v, want mode 640 on the file", perms)
	}

	reads := []struct {
		offset int64
		length int
		want   string
		eof    bool
	}{
		{0, 1024, string(data), true},
		{7, 4, "boat", false},
		{int64(len(data) - 1), 10, "\n", true},
		{int64(len(data)), 10, "", true},
		{int64(len(data) + 5), 10, "", true},
	}
	for _, r := range reads {
		got, eof, err := p.ReadFile(ctx, ref, name, r.offset, r.length)
		if err != nil || string(got) != r.want || eof != r.eof {
			t.Errorf("read at %d for %d gave %q eof %v, %v, want %q eof %v", r.offset,
				r.length, got, eof, err, r.want, r.eof)
		}
	}

	binary := make([]byte, 1024)
	_, _ = rand.Read(binary)
	if err := p.WriteFile(ctx, ref, dir+"/blob", binary, 0o600); err != nil {
		t.Fatal(err)
	}
	got, eof, err := p.ReadFile(ctx, ref, dir+"/blob", 0, 4096)
	if err != nil || !bytes.Equal(got, binary) || !eof {
		t.Fatalf("binary read gave %d bytes eof %v, %v", len(got), eof, err)
	}
	if err := p.WriteFile(ctx, ref, dir+"/empty", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	got, eof, err = p.ReadFile(ctx, ref, dir+"/empty", 0, 10)
	if err != nil || len(got) != 0 || !eof {
		t.Fatalf("empty file read gave %q eof %v, %v", got, eof, err)
	}

	bad := []struct {
		name string
		err  error
		code provider.Code
	}{
		{"missing file", readErr(ctx, p, ref, dir+"/missing", 0), provider.CodeNotFound},
		{"directory", readErr(ctx, p, ref, dir, 0), provider.CodeInvalid},
		{"negative offset", readErr(ctx, p, ref, name, -1), provider.CodeInvalid},
		{"read outside tmp", readErr(ctx, p, ref, "/etc/hosts", 0), provider.CodeInvalid},
		{"write outside tmp", p.WriteFile(ctx, ref, "/etc/bugnet-x", data, 0o644),
			provider.CodeInvalid},
	}
	for _, b := range bad {
		if !isCode(b.err, b.code) {
			t.Errorf("%s gave %v, want %s", b.name, b.err, b.code)
		}
	}
}

func TestBoatLargeFiles(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	ctx := t.Context()
	dir := fakeDir(t)

	fits := make([]byte, 3<<20)
	_, _ = rand.Read(fits)
	if err := p.WriteFile(ctx, ref, dir+"/fits", fits, 0o644); err != nil {
		t.Fatal(err)
	}
	if f.uploadCount() != 0 {
		t.Fatal("a 3 MiB file went through an upload URL, want the JSON call")
	}
	runner := make([]byte, 4<<20)
	_, _ = rand.Read(runner)
	name := dir + "/bin/bugnet-run"
	if err := p.WriteFile(ctx, ref, name, runner, 0o755); err != nil {
		t.Fatal(err)
	}
	if f.uploadCount() != 1 {
		t.Fatalf("a 4 MiB file made %d uploads, want one through an upload URL", f.uploadCount())
	}
	for _, n := range f.bodySizes(routeWrite) {
		if n > fileCap {
			t.Fatalf("a JSON write body held %d bytes, over Boat's %d", n, fileCap)
		}
	}
	disk, err := os.ReadFile(name)
	info, _ := os.Stat(name)
	if err != nil || !bytes.Equal(disk, runner) || info.Mode().Perm() != 0o755 {
		t.Fatalf("uploaded file %d bytes mode %v, %v, want the data with mode 0755", len(disk),
			info.Mode(), err)
	}

	huge := make([]byte, 6<<20+3)
	_, _ = rand.Read(huge)
	if err := p.WriteFile(ctx, ref, dir+"/huge", huge, 0o644); err != nil {
		t.Fatal(err)
	}
	got, eof, err := p.ReadFile(ctx, ref, dir+"/huge", int64(len(huge)-10), 8)
	if err != nil || !bytes.Equal(got, huge[len(huge)-10:len(huge)-2]) || eof {
		t.Fatalf("read inside a 6 MiB file gave %d bytes eof %v, %v", len(got), eof, err)
	}
	got, eof, err = p.ReadFile(ctx, ref, dir+"/huge", int64(len(huge)-4), 100)
	if err != nil || !bytes.Equal(got, huge[len(huge)-4:]) || !eof {
		t.Fatalf("read at the end of a 6 MiB file gave %d bytes eof %v, %v", len(got), eof, err)
	}
	if cmd, _ := f.lastCommand()["command"].(string); !strings.Contains(cmd, "tail") {
		t.Fatalf("large read ran %q, want a tail of the file", cmd)
	}
}

func TestBoatRefusesUploadRedirects(t *testing.T) {
	agent, reached := contract.Redirector(t)
	f := newFake(t)
	f.uploadBase = agent
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	runner := make([]byte, 4<<20)
	if err := p.WriteFile(t.Context(), ref, fakeDir(t)+"/bin/bugnet-run", runner,
		0o755); err == nil {
		t.Error("upload through a redirecting URL succeeded")
	}
	if reached.Load() {
		t.Fatal("the adapter followed an upload redirect")
	}
}

func TestBoatReadsAFileThatOutgrewTheCap(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	name := fakeDir(t) + "/growing.log"
	data := make([]byte, fileCap+2000)
	_, _ = rand.Read(data)
	reads := []struct {
		offset int64
		length int
		eof    bool
	}{
		{fileCap - 150, 100, false},
		{fileCap - 150, 1 << 20, true},
	}
	for _, r := range reads {
		if err := os.WriteFile(name, data[:fileCap-100], 0o644); err != nil {
			t.Fatal(err)
		}
		f.growAfterInfo(name, data[fileCap-100:])
		got, eof, err := p.ReadFile(t.Context(), ref, name, r.offset, r.length)
		end := min(r.offset+int64(r.length), int64(len(data)))
		if err != nil || !bytes.Equal(got, data[r.offset:end]) || eof != r.eof {
			t.Fatalf("read at %d for %d of a file grown past 5 MiB gave %d bytes eof %v, %v, "+
				"want %d bytes eof %v", r.offset, r.length, len(got), eof, err, end-r.offset, r.eof)
		}
		if cmd, _ := f.lastCommand()["command"].(string); !strings.Contains(cmd, "tail") {
			t.Fatalf("read of a grown file ran %q, want a tail of the file", cmd)
		}
	}

	small := fakeDir(t) + "/small.txt"
	if err := os.WriteFile(small, []byte("small"), 0o644); err != nil {
		t.Fatal(err)
	}
	commands := len(f.commandCalls())
	f.inject(routeRead, fault{status: 400, code: "invalid_encoding", message: "Bad encoding"})
	if err := readErr(t.Context(), p, ref, small, 0); !isCode(err, provider.CodeInvalid) ||
		len(f.commandCalls()) != commands {
		t.Fatalf("a refused read of a small file gave %v, want invalid without a tail", err)
	}
}

func TestBoatListFiles(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	ctx := t.Context()
	root := fakeDir(t)
	for _, d := range []string{"a/b/c"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"top.txt", "a/mid.txt", "a/b/low.txt", "a/b/c/deep.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("12345"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(root, "a"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
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
			out = append(out, rel)
			switch rel {
			case "top.txt":
				if e.IsDir || e.Size != 5 || e.Mode != 0o600 || e.Modified.IsZero() {
					t.Errorf("file entry %+v, want 5 bytes, mode 0600 and a time", e)
				}
			case "a":
				if !e.IsDir || e.Mode != fs.ModeDir|0o755 {
					t.Errorf("directory entry %+v, want a directory with mode 0755", e)
				}
			case "link":
				if e.IsDir || e.Mode&fs.ModeSymlink == 0 {
					t.Errorf("symlink entry %+v, want a symlink that is not followed", e)
				}
			}
		}
		slices.Sort(out)
		return out
	}
	want := []string{"a", "link", "top.txt"}
	if got := paths(1); !slices.Equal(got, want) {
		t.Fatalf("depth 1 gave %v, want %v", got, want)
	}
	want = []string{"a", "a/b", "a/mid.txt", "link", "top.txt"}
	if got := paths(2); !slices.Equal(got, want) {
		t.Fatalf("depth 2 gave %v, want %v", got, want)
	}
	want = []string{"a", "a/b", "a/b/c", "a/b/low.txt", "a/mid.txt", "link", "top.txt"}
	if got := paths(3); !slices.Equal(got, want) {
		t.Fatalf("depth 3 gave %v, want %v", got, want)
	}

	if _, err := listAll(ctx, p, ref, root, 0); !isCode(err, provider.CodeInvalid) {
		t.Errorf("depth 0 gave %v, want invalid", err)
	}
	if _, err := listAll(ctx, p, ref, root+"/missing", 1); !isCode(err, provider.CodeNotFound) {
		t.Errorf("missing directory gave %v, want not_found", err)
	}
}

func TestBoatListFilesMarksATruncatedListing(t *testing.T) {
	f := newFake(t)
	f.listLimit = 2
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	ctx := t.Context()
	root := fakeDir(t)
	if err := os.Mkdir(filepath.Join(root, "wide"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"1", "2", "3"} {
		if err := os.WriteFile(filepath.Join(root, "wide", name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	within := provider.NewListing(100)
	if err := p.ListFiles(ctx, ref, root, 1, within); err != nil ||
		len(within.Entries) != 1 || within.Truncated {
		t.Fatalf("listing within Boat's limit gave %+v, %v, want the one directory", within, err)
	}
	for _, depth := range []int{1, 2} {
		dir := root
		if depth == 1 {
			dir = filepath.Join(root, "wide")
		}
		l := provider.NewListing(100)
		if err := p.ListFiles(ctx, ref, dir, depth, l); err != nil || !l.Truncated ||
			len(l.Entries) != depth+1 {
			t.Errorf("listing %s to depth %d past Boat's limit gave %+v, %v, want the entries "+
				"Boat sent marked truncated", dir, depth, l, err)
		}
	}
}

func TestBoatListFilesStopsAtTheLimit(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	contract.ListLimit(t, p, readySandbox(t, f), fakeDir(t))
}

func TestBoatReadsDriftedReplies(t *testing.T) {
	for _, drift := range []string{"layout", "types"} {
		t.Run(drift, func(t *testing.T) {
			f := newFake(t)
			f.drift = drift
			p := f.provider(t, fakeKey)
			ctx := t.Context()
			labels := map[string]string{"bugnet": "1", "placement": "p-drift"}
			before := time.Now().UTC().Truncate(time.Second)
			ref, err := p.Create(ctx, provider.CreateRequest{
				IdempotencyKey: "key-drift", Size: smallSize(t, p), Lifetime: time.Hour,
				Labels: labels,
			})
			if err != nil {
				t.Fatalf("create with a drifted reply: %v", err)
			}
			s, _ := f.sandbox(ref.ID)
			created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
			if err != nil || drift == "layout" && !created.Equal(s.created) ||
				drift == "types" && (created.Before(before) || created.After(time.Now())) {
				t.Errorf("created_at %q, want %v or the local clock", ref.Meta["created_at"],
					s.created)
			}
			if err := p.WaitReady(ctx, ref); err != nil {
				t.Fatalf("wait with drifted replies: %v", err)
			}
			owned, err := p.ListOwned(ctx, labels)
			if err != nil || len(owned) != 1 || owned[0].Ref.ID != ref.ID {
				t.Fatalf("list with drifted replies gave %+v, %v, want %s", owned, err, ref.ID)
			}
			if drift == "layout" && !owned[0].CreatedAt.Equal(s.created) ||
				drift == "types" && !owned[0].CreatedAt.IsZero() {
				t.Errorf("listed creation time %v, want %v or none", owned[0].CreatedAt, s.created)
			}
			out, code, err := runExec(ctx, p, ref, provider.ExecSpec{Argv: []string{"echo", "x"}})
			if err != nil || string(out) != "x\n" || code != 0 {
				t.Fatalf("exec with a drifted start reply gave %q exit %d, %v", out, code, err)
			}

			dir := fakeDir(t)
			name := dir + "/drift.txt"
			if err := p.WriteFile(ctx, ref, name, []byte("drifted"), 0o644); err != nil {
				t.Fatal(err)
			}
			data, eof, err := p.ReadFile(ctx, ref, name, 0, 64)
			if err != nil || string(data) != "drifted" || !eof {
				t.Fatalf("read with drifted file replies gave %q eof %v, %v", data, eof, err)
			}
			entries, err := listAll(ctx, p, ref, dir, 1)
			if err != nil || len(entries) != 1 || entries[0].Size != 7 ||
				entries[0].Modified.IsZero() != (drift == "types") {
				t.Fatalf("list with drifted file replies gave %+v, %v", entries, err)
			}
		})
	}
}

func TestBoatForeignRefIsNotFound(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := provider.Ref{Provider: "e2b", ID: "bx_23456789"}
	ctx := t.Context()
	_, execErr := p.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"true"}})
	_, listErr := listAll(ctx, p, ref, "/tmp", 1)
	for name, err := range map[string]error{
		"WaitReady":   p.WaitReady(ctx, ref),
		"SetLifetime": p.SetLifetime(ctx, ref, time.Now().Add(time.Hour)),
		"Exec":        execErr,
		"ReadFile":    readErr(ctx, p, ref, "/tmp/x", 0),
		"WriteFile":   p.WriteFile(ctx, ref, "/tmp/x", nil, 0o644),
		"ListFiles":   listErr,
	} {
		if !isCode(err, provider.CodeNotFound) {
			t.Errorf("%s on another provider's ref gave %v, want not_found", name, err)
		}
	}
}

func TestBoatSizesAndPrices(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	want := provider.Capabilities{NeedsRunnerUpload: true, MaxLifetime: 30 * 24 * time.Hour}
	if got := p.Capabilities(); got != want {
		t.Errorf("capabilities %+v, want %+v", got, want)
	}
	if p.Name() != "boat" || p.BillingIncrement() != time.Second {
		t.Errorf("name %q, increment %v", p.Name(), p.BillingIncrement())
	}
	cases := []struct {
		floor provider.Resources
		size  string
		micro int64
	}{
		{provider.Resources{}, "small", 18_000},
		{provider.Resources{VCPU: 2, MemoryMiB: 4 << 10, DiskGiB: 51}, "small", 18_000},
		{provider.Resources{VCPU: 3}, "default", 36_000},
		{provider.Resources{DiskGiB: 52}, "large", 72_000},
		{provider.Resources{VCPU: 8, MemoryMiB: 16 << 10}, "large", 72_000},
		{provider.Resources{VCPU: 16, MemoryMiB: 32 << 10, DiskGiB: 251}, "xlarge", 200_000},
		{provider.Resources{VCPU: 17}, "", 0},
		{provider.Resources{MemoryMiB: 33 << 10}, "", 0},
		{provider.Resources{GPUCount: 1}, "", 0},
	}
	for _, c := range cases {
		size, ok := p.SizeFor(c.floor)
		if size.Name != c.size || ok != (c.size != "") {
			t.Errorf("size for %+v: %+v, %v, want %q", c.floor, size, ok, c.size)
			continue
		}
		if ok && p.HourlyCostMicros(size) != c.micro {
			t.Errorf("size %s costs %d, want %d", size.Name, p.HourlyCostMicros(size), c.micro)
		}
	}
	if got := p.HourlyCostMicros(provider.Size{Name: "unknown"}); got != 200_000 {
		t.Errorf("unknown size costs %d, want the dearest type", got)
	}

	q, err := New(Options{
		APIKey: fakeKey, BaseURL: f.srv.URL + "/api/v1",
		TypeBySize: map[string]string{"cpu2": "small", "cpu8": "large"},
	})
	if err != nil {
		t.Fatal(err)
	}
	size, ok := q.SizeFor(provider.Resources{VCPU: 3})
	if !ok || size.Name != "cpu8" || size.Resources.VCPU != 8 ||
		q.HourlyCostMicros(size) != 72_000 {
		t.Fatalf("configured size for 3 vCPU: %+v, %v", size, ok)
	}
	if _, err := q.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if typ := f.lastCreate().body["type"]; typ != "large" {
		t.Fatalf("size cpu8 created type %v, want large", typ)
	}
	if _, ok := q.SizeFor(provider.Resources{VCPU: 16}); ok {
		t.Fatal("configured sizes offer xlarge")
	}
}

func TestBoatOptions(t *testing.T) {
	bad := map[string]Options{
		"no key":            {},
		"unknown type":      {APIKey: fakeKey, TypeBySize: map[string]string{"a": "tiny"}},
		"unnamed size":      {APIKey: fakeKey, TypeBySize: map[string]string{"": "small"}},
		"size without type": {APIKey: fakeKey, TypeBySize: map[string]string{"cpu2": ""}},
	}
	for name, opts := range bad {
		if _, err := New(opts); err == nil {
			t.Errorf("%s: New accepted %+v", name, opts)
		}
	}
	const pasted = "e2b_pasted_value"
	_, err := New(Options{APIKey: fakeKey, TypeBySize: map[string]string{"cpu2": pasted}})
	if err == nil || !strings.Contains(err.Error(), `"cpu2"`) ||
		strings.Contains(err.Error(), pasted) {
		t.Errorf("an unknown type gave %v, want an error naming the size and not the type", err)
	}
}

/* Helpers */

func smallSize(t *testing.T, p *Provider) provider.Size {
	t.Helper()
	size, ok := p.SizeFor(provider.Resources{})
	if !ok {
		t.Fatal("no size")
	}
	return size
}

func createSmall(t *testing.T, p *Provider, key string) provider.Ref {
	t.Helper()
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: key, Size: smallSize(t, p), Lifetime: time.Hour,
		Labels: map[string]string{"bugnet": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func readySandbox(t *testing.T, f *fakeBoat) provider.Ref {
	t.Helper()
	return newRef(f.add(map[string]string{"bugnet": "1"}).id, time.Now())
}

func create(ctx context.Context, p *Provider, _ provider.Ref) error {
	_, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: "key-err", Size: provider.Size{Name: "small"}, Lifetime: time.Hour,
		Labels: map[string]string{"bugnet": "1"},
	})
	return err
}

func waitReady(ctx context.Context, p *Provider, ref provider.Ref) error {
	return p.WaitReady(ctx, ref)
}

func setLifetime(ctx context.Context, p *Provider, ref provider.Ref) error {
	return p.SetLifetime(ctx, ref, time.Now().Add(time.Hour))
}

func execTrue(ctx context.Context, p *Provider, ref provider.Ref) error {
	_, _, err := runExec(ctx, p, ref, provider.ExecSpec{Argv: []string{"true"}})
	return err
}

func readFile(ctx context.Context, p *Provider, ref provider.Ref) error {
	return readErr(ctx, p, ref, "/tmp/bugnet-boat-missing", 0)
}

func readErr(ctx context.Context, p *Provider, ref provider.Ref, name string, off int64) error {
	_, _, err := p.ReadFile(ctx, ref, name, off, 16)
	return err
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

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5 s")
		}
		time.Sleep(10 * time.Millisecond)
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

// startCost waits until the adapter prices the next start of size as want
func startCost(t *testing.T, p *Provider, size provider.Size, fee int64, ok bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, gotOK := p.StartCostMicros(size)
		if got == fee && gotOK == ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("start cost %d, %v, want %d, %v", got, gotOK, fee, ok)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBoatStartCostFollowsLimits(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	p.limitsTTL = 0
	small := smallSize(t, p)
	types := `"machineTypes": [{"type": "small", "available": %s}]`
	for _, tc := range []struct {
		body string
		fee  int64
		ok   bool
	}{
		{`{"ok": true, "canStart": true, "nextStartDollars": 0, ` +
			fmt.Sprintf(types, "true") + `}`, 0, true},
		{`{"ok": true, "canStart": true, "nextStartDollars": 0.03, ` +
			fmt.Sprintf(types, "true") + `}`, 30000, true},
		{`{"ok": true, "canStart": false, "nextStartDollars": null}`, 0, false},
		{`{"ok": true, "canStart": true, "nextStartDollars": 0, ` +
			fmt.Sprintf(types, "false") + `}`, 0, false},
	} {
		f.setLimits(http.StatusOK, tc.body)
		startCost(t, p, small, tc.fee, tc.ok)
	}
}

func TestBoatStartCostCountsStartsWithoutLimits(t *testing.T) {
	f := newFake(t)
	f.setLimits(http.StatusForbidden, "")
	p := f.provider(t, fakeKey)
	small := smallSize(t, p)
	// The $20 plan includes 12 starts a minute, then charges by how far past them a start lies
	for n, want := range map[int]int64{12: 30000, 18: 50000, 24: 70000} {
		g := newFake(t)
		g.setLimits(http.StatusForbidden, "")
		q := g.provider(t, fakeKey)
		for i := range n {
			createSmall(t, q, fmt.Sprintf("key-%d", i))
		}
		if fee, ok := q.StartCostMicros(small); fee != want || !ok {
			t.Errorf("start after %d starts costs %d, %v, want %d", n, fee, ok, want)
		}
	}
	if fee, ok := p.StartCostMicros(small); fee != 0 || !ok {
		t.Fatalf("first start costs %d, %v, want an included start", fee, ok)
	}
	for i := range 36 {
		createSmall(t, p, fmt.Sprintf("key-%d", i))
	}
	if fee, ok := p.StartCostMicros(small); ok {
		t.Fatalf("start past three times the minute's starts costs %d, want it refused", fee)
	}
}

// sharedProvider builds an adapter whose start count lives in vk under prefix
func (f *fakeBoat) sharedProvider(t *testing.T, vk valkey.Client, prefix string) *Provider {
	t.Helper()
	p, err := New(Options{
		APIKey: fakeKey, BaseURL: f.srv.URL + "/api/v1", Valkey: vk, Prefix: prefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestBoatStartCountIsSharedThroughValkey(t *testing.T) {
	f := newFake(t)
	f.setLimits(http.StatusForbidden, "")
	vk, prefix := testenv.Valkey(t)
	replicas := []*Provider{f.sharedProvider(t, vk, prefix), f.sharedProvider(t, vk, prefix)}
	small := smallSize(t, replicas[0])
	// Twelve starts split over two replicas use up the minute's included starts for both
	for i := range 12 {
		createSmall(t, replicas[i%2], fmt.Sprintf("key-%d", i))
	}
	for i, p := range replicas {
		if fee, ok := p.StartCostMicros(small); fee != 30000 || !ok {
			t.Fatalf("replica %d prices the 13th start at %d, %v, want the 30000 extra fee", i,
				fee, ok)
		}
	}
	ttl, err := vk.Do(t.Context(), vk.B().Pttl().Key(prefix+"boat:starts").Build()).AsInt64()
	if err != nil || ttl <= (23*time.Hour).Milliseconds() || ttl > (24*time.Hour).Milliseconds() {
		t.Fatalf("the shared start count expires in %dms, %v, want with the day's window", ttl,
			err)
	}
	other, _ := testenv.Valkey(t)
	elsewhere := f.sharedProvider(t, other, prefix+"other:")
	if fee, ok := elsewhere.StartCostMicros(small); fee != 0 || !ok {
		t.Fatalf("a replica under another prefix prices a start at %d, %v, want included", fee,
			ok)
	}
}

func TestBoatStartCountFallsBackWithoutValkey(t *testing.T) {
	f := newFake(t)
	f.setLimits(http.StatusForbidden, "")
	gone, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{testenv.ValkeyAddr()}})
	if err != nil {
		t.Fatal(err)
	}
	gone.Close()
	p := f.sharedProvider(t, gone, "bn:test:gone:")
	small := smallSize(t, p)
	for i := range 12 {
		createSmall(t, p, fmt.Sprintf("key-%d", i))
	}
	if fee, ok := p.StartCostMicros(small); fee != 30000 || !ok {
		t.Fatalf("with Valkey gone the 13th start costs %d, %v, want this process's own count",
			fee, ok)
	}
}

func TestBoatSkipsAFailingSharedCountForAWhile(t *testing.T) {
	f := &fees{shared: &sharedStarts{}}
	now := time.Now()
	if !f.useShared(now) {
		t.Fatal("a healthy shared count is skipped")
	}
	f.noteShared(errors.New("valkey hung"))
	if f.useShared(now.Add(time.Second)) {
		t.Fatal("a failing shared count is asked again at once")
	}
	if !f.useShared(now.Add(sharedRetryAfter + time.Second)) {
		t.Fatalf("the shared count is still skipped %v after it failed", sharedRetryAfter)
	}
	f.noteShared(nil)
	if !f.useShared(now) {
		t.Fatal("a shared count that answers again is skipped")
	}
}

func TestCapabilitiesReportNoSSHOrPreview(t *testing.T) {
	c := newFake(t).provider(t, fakeKey).Capabilities()
	eq(t, c.SSH || c.Preview, false)
}
