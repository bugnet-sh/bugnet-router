package boxd

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/boxd/boxdpb"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
)

func TestBoxdContract(t *testing.T) {
	f := newFake(t)
	contract.Run(t, func(t *testing.T) provider.Provider { return f.provider(t, fakeKey) })
}

func TestBoxdErrorMapping(t *testing.T) {
	cases := []struct {
		name     string
		method   string
		err      error
		after    bool
		op       func(ctx context.Context, p *Provider, ref provider.Ref) error
		code     provider.Code
		failOver bool
	}{
		{
			name: "machine cap", method: "CreateVm",
			err: status.Error(codes.ResourceExhausted, "org machine limit of 50 reached"),
			op:  create, code: provider.CodeCapacity, failOver: true,
		},
		{
			name: "rate limit", method: "CreateVm",
			err: status.Error(codes.ResourceExhausted, "rate limit exceeded, slow down"),
			op:  create, code: provider.CodeRateLimited, failOver: true,
		},
		{
			name: "unauthenticated", method: "CreateVm",
			err: status.Error(codes.Unauthenticated, "expired token"),
			op:  create, code: provider.CodeAuth, failOver: true,
		},
		{
			name: "permission denied", method: "CreateVm",
			err: status.Error(codes.PermissionDenied, "region_not_enabled"),
			op:  create, code: provider.CodeAuth, failOver: true,
		},
		{
			name: "invalid argument", method: "CreateVm",
			err: status.Error(codes.InvalidArgument, "spec.size: not allowed"),
			op:  create, code: provider.CodeInvalid,
		},
		{
			name: "server error", method: "CreateVm",
			err: status.Error(codes.Internal, "scheduler fell over"),
			op:  create, code: provider.CodeInternal, failOver: true,
		},
		{
			name: "deadline on create", method: "CreateVm",
			err: status.Error(codes.DeadlineExceeded, "deadline exceeded at the proxy"),
			op:  create, code: provider.CodeOutcomeUnknown,
		},
		{
			name: "unavailable on create", method: "CreateVm",
			err: status.Error(codes.Unavailable, "connection reset by peer"),
			op:  create, code: provider.CodeOutcomeUnknown,
		},
		{
			name: "machine gone", method: "GetVm",
			err: status.Error(codes.NotFound, "machine vm-x not found"),
			op:  waitReady, code: provider.CodeNotFound,
		},
		{
			name: "unavailable outside create", method: "SetDeleteAfter",
			err: status.Error(codes.Unavailable, "connection reset by peer"),
			op:  setLifetime, code: provider.CodeInternal, failOver: true,
		},
		{
			name: "exec on a missing machine", method: "Exec",
			err: status.Error(codes.NotFound, "machine vm-x not found"),
			op:  execTrue, code: provider.CodeNotFound,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			ref := provider.Ref{Provider: "boxd", ID: f.add("existing", nil).id}
			f.inject(c.method, fault{err: c.err, after: c.after})
			err := c.op(t.Context(), p, ref)
			e, ok := provider.AsError(err)
			if !ok {
				t.Fatalf("got %v, want a provider error", err)
			}
			if e.Code != c.code || e.CanFailOver != c.failOver {
				t.Fatalf("got code %s fail over %v, want %s and %v (%v)",
					e.Code, e.CanFailOver, c.code, c.failOver, err)
			}
			if msg := status.Convert(c.err).Message(); !strings.Contains(err.Error(), msg) {
				t.Fatalf("error %q lacks the status message %q", err, msg)
			}
		})
	}
}

func TestBoxdWrongKeyIsAuth(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, "bxd_wrong")
	_, err := p.ListOwned(t.Context(), map[string]string{"bugnet": "1"})
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeAuth {
		t.Fatalf("list with a wrong key gave %v, want auth", err)
	}
}

func TestBoxdCreateRequest(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	size, ok := p.SizeFor(provider.Resources{VCPU: 2, MemoryMiB: 8 << 10})
	if !ok {
		t.Fatal("no size for 2 vCPU and 8 GiB")
	}
	labels := map[string]string{"bugnet": "1", "bugnet_deployment": "main", "placement": "p-1"}
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "key-1", Size: size, Labels: labels, Lifetime: 15 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Provider != "boxd" || ref.Meta["arch"] != "amd64" {
		t.Fatalf("ref %+v, want provider boxd with arch amd64", ref)
	}

	req := f.lastCreate()
	spec := req.GetSpec()
	if req.GetName() != machineName("key-1") || !strings.HasPrefix(req.GetName(), "bn-") {
		t.Errorf("name %q, want the name derived from the key", req.GetName())
	}
	if spec.GetSize() != "2x8" || !spec.GetIsolated() || len(spec.GetEgress()) != 0 {
		t.Errorf("spec %v, want size 2x8, isolated and no egress allowlist", spec)
	}
	if req.DeleteAfterSecs == nil || req.GetDeleteAfterSecs() != 900 {
		t.Errorf("delete after %v, want 900 s", req.DeleteAfterSecs)
	}
	srf := req.GetConfig().GetSrf()
	if srf == nil || srf.AutoSuspendTimeoutSecs == nil || srf.GetAutoSuspendTimeoutSecs() != 0 {
		t.Errorf("srf %v, want auto suspend sent as 0", srf)
	}
	want := map[string]string{
		"bugnet": "1", "bugnet.deployment": "main", "placement": "p-1",
		keyLabel: keyDigest("key-1"),
	}
	if !maps.Equal(spec.GetLabels(), want) {
		t.Errorf("labels %v, want %v", spec.GetLabels(), want)
	}

	owned, err := p.ListOwned(t.Context(), map[string]string{"bugnet_deployment": "main"})
	if err != nil {
		t.Fatal(err)
	}
	if len(owned) != 1 || owned[0].Ref.ID != ref.ID || !maps.Equal(owned[0].Labels, labels) {
		t.Fatalf("ListOwned %+v, want the one machine with labels %v", owned, labels)
	}
}

func TestBoxdCreateRejectsBadInput(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	size, _ := p.SizeFor(provider.Resources{})
	reqs := map[string]provider.CreateRequest{
		"unknown size": {IdempotencyKey: "k", Size: provider.Size{Name: "3x9"}},
		"label key with a dot": {IdempotencyKey: "k", Size: size,
			Labels: map[string]string{"a.b": "1"}},
		"upper case label key": {IdempotencyKey: "k", Size: size,
			Labels: map[string]string{"Bugnet": "1"}},
		"label value with a newline": {IdempotencyKey: "k", Size: size,
			Labels: map[string]string{"bugnet": "1\n"}},
	}
	for name, req := range reqs {
		_, err := p.Create(t.Context(), req)
		if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
			t.Errorf("%s gave %v, want invalid", name, err)
		}
	}
	if f.count() != 0 {
		t.Fatalf("rejected creates left %d machines", f.count())
	}
}

func TestBoxdCreateLostReplyFindsMachine(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			f.inject("CreateVm", fault{err: status.Error(code, "reply lost"), after: true})
			ref, err := create2x8(t, p, "key-lost")
			if err != nil {
				t.Fatalf("create whose reply was lost: %v", err)
			}
			vm, err := f.vm(ref.ID)
			if err != nil || vm.name != machineName("key-lost") || f.count() != 1 {
				t.Fatalf("ref %+v names %v, %d machines exist", ref, err, f.count())
			}
		})
	}
}

func TestBoxdCreateSameKeyAdoptsMachine(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	first, err := create2x8(t, p, "key-again")
	if err != nil {
		t.Fatal(err)
	}
	again, err := create2x8(t, p, "key-again")
	if err != nil || again.ID != first.ID || f.count() != 1 {
		t.Fatalf("second create gave %+v, %v with %d machines, want %s", again, err, f.count(),
			first.ID)
	}
}

func TestBoxdCreateNameHeldByAnotherMachine(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	f.add(machineName("key-taken"), map[string]string{"owner": "someone-else"})
	_, err := create2x8(t, p, "key-taken")
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeInternal || !e.CanFailOver {
		t.Fatalf("create onto a foreign name gave %v, want internal that can fail over", err)
	}
	if f.count() != 1 {
		t.Fatalf("%d machines, want only the foreign one", f.count())
	}
}

func TestBoxdCreatePinsDefaultProxy(t *testing.T) {
	f, p := newTestProvider(t)
	ref := mustCreate(t, p, "key-1")
	mustCreate(t, p, "key-1")
	pin := setProxyPort{vm: ref.ID, name: "default", port: "29417"}
	eq(t, f.proxyPins(), []setProxyPort{pin, pin})
	vm, _ := f.vm(ref.ID)
	eq(t, vm.proxyPort, "29417")
}

func TestBoxdCreatePinsAfterALostReply(t *testing.T) {
	f, p := newTestProvider(t)
	f.inject("CreateVm", fault{err: status.Error(codes.Unavailable, "reply lost"), after: true})
	ref := mustCreate(t, p, "key-lost-pin")
	eq(t, f.proxyPins(), []setProxyPort{{vm: ref.ID, name: "default", port: "29417"}})
}

func TestBoxdCreatePinsTheConfiguredGatePort(t *testing.T) {
	f := newFake(t)
	p, err := New(Options{APIKey: fakeKey, Endpoint: f.addr, Insecure: true, GatePort: 30001})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ref := mustCreate(t, p, "key-port")
	eq(t, f.proxyPins(), []setProxyPort{{vm: ref.ID, name: "default", port: "30001"}})
}

func TestBoxdRefusesAGatePortOutOfRange(t *testing.T) {
	for _, port := range []int{-1, 65536} {
		if _, err := New(Options{APIKey: fakeKey, GatePort: port}); err == nil {
			t.Errorf("New with gate port %d succeeded", port)
		}
	}
}

func TestBoxdCreatePinFailureDestroysMachine(t *testing.T) {
	for _, code := range []codes.Code{codes.Internal, codes.PermissionDenied, codes.NotFound} {
		t.Run(code.String(), func(t *testing.T) {
			f, p := newTestProvider(t)
			f.inject("SetProxyPort", fault{err: status.Error(code, "boom")})
			_, err := create2x8(t, p, "key-2")
			e, ok := provider.AsError(err)
			eq(t, ok && e.Code == provider.CodeInternal && e.CanFailOver, true)
			eq(t, f.count(), 0)
			eq(t, f.callCount("DestroyVm"), 1)
		})
	}
}

func TestBoxdCreatePinFailureOnAdoptDestroysMachine(t *testing.T) {
	f, p := newTestProvider(t)
	mustCreate(t, p, "key-adopt-pin")
	f.inject("SetProxyPort", fault{err: status.Error(codes.Unavailable, "proxy busy")})
	_, err := create2x8(t, p, "key-adopt-pin")
	e, ok := provider.AsError(err)
	eq(t, ok && e.Code == provider.CodeInternal && e.CanFailOver, true)
	eq(t, f.count(), 0)
}

func TestBoxdCreateTurnsSshOff(t *testing.T) {
	f, p := newTestProvider(t)
	ref := mustCreate(t, p, "key-3")
	eq(t, f.creates[0].GetConfig().GetNetwork().GetSsh(), false)
	eq(t, f.creates[0].GetConfig().GetNetwork().Ssh != nil, true)
	eq(t, f.sshCalls(), []setSSH(nil))
	vm, _ := f.vm(ref.ID)
	eq(t, vm.sshOff, true)
}

func TestBoxdCreateFallsBackToSetSsh(t *testing.T) {
	f, p := newTestProvider(t)
	f.refuseNetworkConfig = true
	ref := mustCreate(t, p, "key-4")
	eq(t, f.sshCalls(), []setSSH{{vm: ref.ID, enabled: false}})
	eq(t, f.callCount("CreateVm"), 2)
	eq(t, f.lastCreate().GetConfig().GetNetwork() == nil, true)
	vm, _ := f.vm(ref.ID)
	eq(t, vm.sshOff, true)
}

func TestBoxdCreateTurnsSshOffWhenTheReplyCannotShowIt(t *testing.T) {
	f, p := newTestProvider(t)
	f.omitMachine = true
	ref := mustCreate(t, p, "key-old-ssh")
	eq(t, f.sshCalls(), []setSSH{{vm: ref.ID, enabled: false}})
}

func TestBoxdCreateSshFailureDestroysMachine(t *testing.T) {
	f, p := newTestProvider(t)
	f.refuseNetworkConfig = true
	f.inject("SetSsh", fault{err: status.Error(codes.PermissionDenied, "not for api keys")})
	_, err := create2x8(t, p, "key-ssh-fail")
	e, ok := provider.AsError(err)
	eq(t, ok && e.Code == provider.CodeInternal && e.CanFailOver, true)
	eq(t, f.count(), 0)
}

func TestBoxdCreateRecordsHibernateOff(t *testing.T) {
	f, p := newTestProvider(t)
	ref := mustCreate(t, p, "key-5")
	eq(t, ref.Meta["hibernate_off"], "1")
	vm, _ := f.vm(ref.ID)
	eq(t, vm.autoHibernate, uint32(0))
	refusals := []codes.Code{codes.PermissionDenied, codes.Unimplemented, codes.Unavailable}
	for i, code := range refusals {
		var logs bytes.Buffer
		p.log = slog.New(slog.NewTextHandler(&logs, nil))
		f.inject("SetAutoHibernateTimeout", fault{err: status.Error(code, "not for api keys")})
		ref := mustCreate(t, p, fmt.Sprintf("key-6-%d", i))
		eq(t, ref.Meta["hibernate_off"], "0")
		if got := logs.String(); !strings.Contains(got, "level=WARN") ||
			!strings.Contains(got, ref.ID) || !strings.Contains(got, "not for api keys") {
			t.Fatalf("log %q, want a warning naming the machine and the refusal", got)
		}
	}
	eq(t, f.count(), 1+len(refusals))
}

func TestBoxdCreateClosesRoutesFirstAndStopsHibernationLast(t *testing.T) {
	f, p := newTestProvider(t)
	f.omitMachine = true
	mustCreate(t, p, "key-order")
	want := []string{"CreateVm", "SetProxyPort", "SetSsh", "SetDeleteAfter",
		"SetAutoHibernateTimeout"}
	eq(t, f.callOrder(), want)
}

func TestCapabilitiesReportNoSSHOrPreview(t *testing.T) {
	_, p := newTestProvider(t)
	c := p.Capabilities()
	eq(t, c.SSH || c.Preview, false)
}

func TestBoxdCreateSetsTimerOnOldServer(t *testing.T) {
	f := newFake(t)
	f.omitMachine = true
	p := f.provider(t, fakeKey)
	ref, err := create2x8(t, p, "key-old")
	if err != nil {
		t.Fatal(err)
	}
	vm, _ := f.vm(ref.ID)
	if left := time.Until(vm.deleteAt); left < 14*time.Minute || left > 16*time.Minute {
		t.Fatalf("delete timer %v away, want about 15 minutes", left)
	}
}

func TestBoxdSetLifetime(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref, err := create2x8(t, p, "key-life")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetLifetime(t.Context(), ref, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	vm, _ := f.vm(ref.ID)
	left := time.Until(vm.deleteAt)
	if left < time.Hour-2*time.Second || left > time.Hour+2*time.Second {
		t.Fatalf("delete timer %v away, want an hour", left)
	}
	if err := p.SetLifetime(t.Context(), ref, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	vm, _ = f.vm(ref.ID)
	if vm.deleteAt.IsZero() || time.Until(vm.deleteAt) > 2*time.Second {
		t.Fatalf("a past lifetime left the timer at %v, want it due at once", vm.deleteAt)
	}
}

func TestBoxdWaitReady(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref, err := create2x8(t, p, "key-ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	vm, _ := f.vm(ref.ID)
	if vm.status != "running" || vm.agentDownExecs != 0 {
		t.Fatalf("machine %+v, want running and probed", vm)
	}
	if n := f.callCount("SetAutoHibernateTimeout"); n != 1 {
		t.Fatalf("auto hibernate asked %d times, want once by Create and never by WaitReady", n)
	}

	failed := f.add("failed-one", nil)
	failed.status = "failed"
	err = p.WaitReady(t.Context(), provider.Ref{Provider: "boxd", ID: failed.id})
	if err == nil || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("wait on a failed machine gave %v", err)
	}
}

func TestBoxdWaitReadyProbeGivesUp(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code provider.Code
	}{
		{"invalid", status.Error(codes.FailedPrecondition, "machine is migrating"),
			provider.CodeInvalid},
		{"auth", status.Error(codes.PermissionDenied, "exec is not for api keys"),
			provider.CodeAuth},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			vm := f.add("probe-"+c.name, nil)
			f.inject("Exec", fault{err: c.err})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := p.WaitReady(ctx, provider.Ref{Provider: "boxd", ID: vm.id})
			if !isCode(err, c.code) || ctx.Err() != nil {
				t.Fatalf("wait gave %v, want %s before the deadline", err, c.code)
			}
			if n := f.callCount("Exec"); n != 1 {
				t.Fatalf("probe ran %d times, want once", n)
			}
		})
	}

	f := newFake(t)
	p := f.provider(t, fakeKey)
	vm := f.add("probe-transient", nil)
	f.inject("Exec", fault{
		err: status.Error(codes.Unavailable, "cannot connect to VM agent"), times: 3,
	})
	if err := p.WaitReady(t.Context(), provider.Ref{Provider: "boxd", ID: vm.id}); err != nil {
		t.Fatalf("wait through an agent that comes up gave %v", err)
	}
	if n := f.callCount("Exec"); n != 4 {
		t.Fatalf("probe ran %d times, want three refusals and one success", n)
	}
}

func TestBoxdExecQuoting(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	cwd := filepath.Join(t.TempDir(), "it's a dir")
	if err := os.Mkdir(cwd, 0o755); err != nil {
		t.Fatal(err)
	}
	greeting := "it's $HOME `id` \"q\" \\"
	spec := provider.ExecSpec{
		Argv: []string{"sh", "-c", `printf '%s|' "$GREETING" "$1" "$2" "$(pwd)"`, "argv0",
			"a b", "it's;$(echo no)"},
		Cwd: cwd,
		Env: map[string]string{"GREETING": greeting},
	}
	out, code, err := runExec(t.Context(), p, ref, spec)
	if err != nil {
		t.Fatal(err)
	}
	want := greeting + "|a b|it's;$(echo no)|" + cwd + "|"
	if string(out) != want || code != 0 {
		t.Fatalf("exec gave %q exit %d, want %q", out, code, want)
	}
	if cmd := f.lastCommand(); !strings.HasPrefix(cmd, "cd '") || !strings.Contains(cmd, " env ") {
		t.Fatalf("command %q does not cd and set env first", cmd)
	}

	bad := map[string]provider.ExecSpec{
		"no command":       {},
		"bad env key":      {Argv: []string{"true"}, Env: map[string]string{"A-B": "1"}},
		"nul byte":         {Argv: []string{"echo", "a\x00b"}},
		"invalid utf-8":    {Argv: []string{"echo", "\xff"}},
		"env reads argv0":  {Argv: []string{"A=B"}, Env: map[string]string{"X": "1"}},
		"env option argv0": {Argv: []string{"-i"}, Env: map[string]string{"X": "1"}},
	}
	for name, spec := range bad {
		_, err := p.Exec(t.Context(), ref, spec)
		if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
			t.Errorf("%s gave %v, want invalid", name, err)
		}
	}
}

func TestBoxdExecStreams(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)

	spec := provider.ExecSpec{Argv: []string{"sh", "-c", "echo out; echo err >&2; exit 3"}}
	out, code, err := runExec(t.Context(), p, ref, spec)
	if err != nil || string(out) != "out\n" || code != 3 {
		t.Fatalf("exec gave %q exit %d, %v, want stdout only and exit 3", out, code, err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	out, code, err = runExec(ctx, p, ref, provider.ExecSpec{Argv: []string{"cat"}})
	if err != nil || len(out) != 0 || code != 0 {
		t.Fatalf("cat gave %q exit %d, %v, want stdin closed at once", out, code, err)
	}

	big := provider.ExecSpec{Argv: []string{"sh", "-c", "head -c 300000 /dev/zero"}}
	out, _, err = runExec(t.Context(), p, ref, big)
	if err != nil || len(out) != 300000 {
		t.Fatalf("large output gave %d bytes, %v", len(out), err)
	}
}

func TestBoxdExecRunsOnAfterExecReturns(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	spec := provider.ExecSpec{
		Argv: []string{"sh", "-c", "echo early; sleep 0.5; echo late; exit 7"},
	}
	out, code, err := runExec(ctx, p, ref, spec)
	if err != nil || string(out) != "early\nlate\n" || code != 7 {
		t.Fatalf("long command gave %q exit %d, %v, want both lines and exit 7", out, code, err)
	}
}

func TestBoxdExecExitCode(t *testing.T) {
	cases := []struct {
		name  string
		reply []*boxdpb.ExecChunk
		out   string
		code  int
	}{
		{
			name: "exit chunk then empty chunks",
			reply: []*boxdpb.ExecChunk{
				{Data: []byte("out")}, {ExitCode: 3}, {IsStderr: true}, {},
			},
			out: "out", code: 3,
		},
		{
			name:  "exit code on the last data chunk",
			reply: []*boxdpb.ExecChunk{{Data: []byte("o")}, {Data: []byte("ut"), ExitCode: 4}},
			out:   "out", code: 4,
		},
		{
			name:  "empty chunk before the exit chunk",
			reply: []*boxdpb.ExecChunk{{Data: []byte("out")}, {}, {ExitCode: 5}},
			out:   "out", code: 5,
		},
		{
			name:  "clean close after data",
			reply: []*boxdpb.ExecChunk{{Data: []byte("o")}, {Data: []byte("ut")}},
			out:   "out", code: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			ref := readyMachine(t, f)
			f.replyExec(c.reply...)
			out, code, err := runExec(t.Context(), p, ref, provider.ExecSpec{Argv: []string{"x"}})
			if err != nil || string(out) != c.out || code != c.code {
				t.Fatalf("exec gave %q exit %d, %v, want %q exit %d", out, code, err, c.out,
					c.code)
			}
		})
	}

	t.Run("no chunk at all", func(t *testing.T) {
		f := newFake(t)
		p := f.provider(t, fakeKey)
		ref := readyMachine(t, f)
		f.replyExec()
		_, code, err := runExec(t.Context(), p, ref, provider.ExecSpec{Argv: []string{"x"}})
		if !isCode(err, provider.CodeOutcomeUnknown) {
			t.Fatalf("stream without any chunk gave exit %d, %v, want outcome_unknown", code, err)
		}
	})
}

func TestBoxdExecCloseKillsCommand(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	stream, err := p.Exec(t.Context(), ref, provider.ExecSpec{Argv: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for f.running.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
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

func TestBoxdFiles(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	ctx := t.Context()
	dir := t.TempDir()

	data := make([]byte, 3<<20+5)
	_, _ = rand.Read(data)
	name := filepath.Join(dir, "deep", "er", "blob.bin")
	if err := p.WriteFile(ctx, ref, name, data, 0o640); err != nil {
		t.Fatal(err)
	}
	chunks := f.lastWriteChunks()
	if len(chunks) != 4 || slices.Max(chunks) > writeChunkBytes {
		t.Fatalf("write went in chunks %v, want four of at most %d bytes", chunks,
			writeChunkBytes)
	}
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("written file %v, %v, want mode 0640", info, err)
	}

	got, eof, err := p.ReadFile(ctx, ref, name, 0, len(data)+10)
	if err != nil || !bytes.Equal(got, data) || !eof {
		t.Fatalf("full read gave %d bytes eof %v, %v", len(got), eof, err)
	}
	got, eof, err = p.ReadFile(ctx, ref, name, int64(len(data)-3), 2)
	if err != nil || !bytes.Equal(got, data[len(data)-3:len(data)-1]) || eof {
		t.Fatalf("read near the end gave %v eof %v, %v", got, eof, err)
	}
	if err := p.WriteFile(ctx, ref, filepath.Join(dir, "empty"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	got, eof, err = p.ReadFile(ctx, ref, filepath.Join(dir, "empty"), 0, 10)
	if err != nil || len(got) != 0 || !eof {
		t.Fatalf("empty file read gave %q eof %v, %v", got, eof, err)
	}

	_, _, err = p.ReadFile(ctx, ref, filepath.Join(dir, "missing"), 0, 10)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeNotFound {
		t.Errorf("read of a missing file gave %v, want not_found", err)
	}
	_, _, err = p.ReadFile(ctx, ref, dir, 0, 10)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Errorf("read of a directory gave %v, want invalid", err)
	}
	_, _, err = p.ReadFile(ctx, ref, name, -1, 10)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Errorf("read at a negative offset gave %v, want invalid", err)
	}
}

func TestBoxdListFiles(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
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
			if e.IsDir != (e.Mode&fs.ModeDir != 0) {
				t.Errorf("entry %+v has IsDir and mode disagreeing", e)
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

	_, err := listAll(ctx, p, ref, root, 0)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Errorf("depth 0 gave %v, want invalid", err)
	}
	_, err = listAll(ctx, p, ref, filepath.Join(root, "nope"), 1)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeNotFound {
		t.Errorf("listing a missing directory gave %v, want not_found", err)
	}
}

func TestBoxdForeignRefIsNotFound(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	vm := f.add("theirs", nil)
	ref := provider.Ref{Provider: "local", ID: vm.id}
	if err := p.WaitReady(t.Context(), ref); !isCode(err, provider.CodeNotFound) {
		t.Errorf("WaitReady of another provider's ref gave %v", err)
	}
	if err := p.Destroy(t.Context(), ref); err != nil || f.count() != 1 {
		t.Errorf("Destroy of another provider's ref gave %v with %d machines", err, f.count())
	}
}

func TestBoxdSizesAndPrices(t *testing.T) {
	p, err := New(Options{APIKey: fakeKey})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	caps := p.Capabilities()
	want := provider.Capabilities{EgressControl: true, NeedsRunnerUpload: true}
	if caps != want {
		t.Errorf("capabilities %+v, want %+v", caps, want)
	}
	if p.Name() != "boxd" || p.BillingIncrement() != time.Second {
		t.Errorf("name %q increment %v", p.Name(), p.BillingIncrement())
	}
	cases := []struct {
		floor  provider.Resources
		size   string
		hourly int64
	}{
		{provider.Resources{}, "1x4", 115000},
		{provider.Resources{VCPU: 2}, "2x8", 207000},
		{provider.Resources{MemoryMiB: 9 << 10}, "4x16", 414000},
		{provider.Resources{VCPU: 5, DiskGiB: 100}, "8x32", 805000},
	}
	for _, c := range cases {
		s, ok := p.SizeFor(c.floor)
		if !ok || s.Name != c.size || p.HourlyCostMicros(s) != c.hourly {
			t.Errorf("floor %+v gave %+v %v at %d, want %s at %d", c.floor, s, ok,
				p.HourlyCostMicros(s), c.size, c.hourly)
		}
		if s.Resources.DiskGiB != 100 {
			t.Errorf("size %+v, want the fixed 100 GiB disk", s)
		}
	}
	for _, floor := range []provider.Resources{
		{VCPU: 9}, {MemoryMiB: 33 << 10}, {DiskGiB: 101}, {GPUCount: 1},
	} {
		if s, ok := p.SizeFor(floor); ok {
			t.Errorf("floor %+v gave %+v, want none", floor, s)
		}
	}
	if got := p.HourlyCostMicros(provider.Size{Name: "unknown"}); got != 805000 {
		t.Errorf("unknown size costs %d, want the dearest size", got)
	}
}

func TestBoxdOptions(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Error("New without an API key succeeded")
	}
	cases := []struct {
		opts Options
		want string
	}{
		{Options{}, "boxd.sh:9443"},
		{Options{Region: "fra"}, "boxd.sh:9443"},
		{Options{Region: "nyc"}, "nyc.boxd.sh:9443"},
		{Options{Region: "nyc", Endpoint: "localhost:1"}, "localhost:1"},
	}
	for _, c := range cases {
		if got := endpoint(c.opts); got != c.want {
			t.Errorf("endpoint for %+v is %q, want %q", c.opts, got, c.want)
		}
	}
}

/* Helpers */

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

func setLifetime(ctx context.Context, p *Provider, ref provider.Ref) error {
	return p.SetLifetime(ctx, ref, time.Now().Add(time.Hour))
}

func execTrue(ctx context.Context, p *Provider, ref provider.Ref) error {
	_, _, err := runExec(ctx, p, ref, provider.ExecSpec{Argv: []string{"true"}})
	return err
}

func create2x8(t *testing.T, p *Provider, key string) (provider.Ref, error) {
	t.Helper()
	size, _ := p.SizeFor(provider.Resources{VCPU: 2})
	return p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: key, Size: size, Labels: map[string]string{"bugnet": "1"},
		Lifetime: 15 * time.Minute,
	})
}

func readyMachine(t *testing.T, f *fakeBoxd) provider.Ref {
	t.Helper()
	vm := f.add("ready-"+t.Name(), map[string]string{"bugnet": "1"})
	return provider.Ref{Provider: "boxd", ID: vm.id}
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

func TestBoxdListFilesStopsAtTheLimit(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	contract.ListLimit(t, p, readyMachine(t, f), t.TempDir())
}

func TestBoxdReadFileKeepsToTheLength(t *testing.T) {
	f := newFake(t)
	f.ignoreLength = true
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	name := filepath.Join(t.TempDir(), "big")
	data := make([]byte, 4<<20)
	_, _ = rand.Read(data)
	if err := os.WriteFile(name, data, 0o644); err != nil {
		t.Fatal(err)
	}
	got, eof, err := p.ReadFile(t.Context(), ref, name, 10, 100)
	if err != nil || !bytes.Equal(got, data[10:110]) || eof {
		t.Fatalf("read of 100 bytes from a host sending the whole file gave %d bytes eof %v, %v",
			len(got), eof, err)
	}
}

func TestBoxdClassifiesADestroyedMachineAsNotFound(t *testing.T) {
	cases := []struct {
		msg  string
		code provider.Code
	}{
		{"cannot set delete-after on vm in status 'destroyed'", provider.CodeNotFound},
		{"cannot exec on vm in status 'destroying'", provider.CodeNotFound},
		{"machine is migrating", provider.CodeInvalid},
	}
	for _, c := range cases {
		err := classify(t.Context(), status.Error(codes.FailedPrecondition, c.msg))
		if !isCode(err, c.code) {
			t.Errorf("FailedPrecondition %q gave %v, want %s", c.msg, err, c.code)
		}
	}
}
