package local_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
	"github.com/oynozan/bugnet-router/internal/provider/local"
)

var small = provider.Size{
	Name:      "small",
	Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
}

func newLocal(t *testing.T, opts local.Options) *local.Provider {
	t.Helper()
	if opts.Name == "" {
		opts.Name = "local"
	}
	if opts.Root == "" {
		opts.Root = t.TempDir()
	}
	if opts.Sizes == nil {
		opts.Sizes = []provider.Size{small}
	}
	return local.New(opts)
}

func createReady(t *testing.T, p *local.Provider, labels map[string]string) provider.Ref {
	t.Helper()
	ref, err := p.Create(t.Context(), provider.CreateRequest{Size: small, Labels: labels})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	return ref
}

func execOut(
	t *testing.T, p *local.Provider, ref provider.Ref, spec provider.ExecSpec,
) (string, int, error) {
	t.Helper()
	stream, err := p.Exec(t.Context(), ref, spec)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = stream.Close() }()
	out, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("read exec output: %v", err)
	}
	code, err := stream.Wait()
	return string(out), code, err
}

func requireCode(t *testing.T, what string, err error, code provider.Code) {
	t.Helper()
	if e, ok := provider.AsError(err); !ok || e.Code != code {
		t.Errorf("%s gave %v, want an *Error with code %s", what, err, code)
	}
}

func TestLocalContract(t *testing.T) {
	contract.Run(t, func(t *testing.T) provider.Provider {
		return newLocal(t, local.Options{ReadyDelay: 50 * time.Millisecond})
	})
}

func TestLocalCreateHookFailure(t *testing.T) {
	hookErr := &provider.Error{
		Code:        provider.CodeCapacity,
		CanFailOver: true,
		Err:         errors.New("no room"),
	}
	var attempts []int
	p := newLocal(t, local.Options{
		CreateHook: func(attempt int, _ provider.CreateRequest) error {
			attempts = append(attempts, attempt)
			if attempt == 1 {
				return hookErr
			}
			return nil
		},
	})
	req := provider.CreateRequest{IdempotencyKey: "k1", Labels: map[string]string{"t": "hook"}}

	if _, err := p.Create(t.Context(), req); err != hookErr {
		t.Fatalf("Create gave %v, want the hook's error unchanged", err)
	}
	owned, err := p.ListOwned(t.Context(), req.Labels)
	if err != nil || len(owned) != 0 {
		t.Fatalf("after a failing hook ListOwned gave %v, %v, want nothing", owned, err)
	}
	if _, err := p.Create(t.Context(), req); err != nil {
		t.Fatalf("second Create: %v", err)
	}
	if len(attempts) != 2 || attempts[0] != 1 || attempts[1] != 2 {
		t.Fatalf("hook saw attempts %v, want [1 2]", attempts)
	}
}

func TestLocalRefusesWithoutFlag(t *testing.T) {
	opts := local.Options{Name: "local", Root: t.TempDir()}
	for _, v := range []string{"", "0", "true"} {
		t.Setenv("BUGNET_ENABLE_LOCAL_PROVIDER", v)
		if p, err := local.FromEnv(opts); err == nil || p != nil {
			t.Errorf("FromEnv with the flag set to %q gave %v, %v, want an error", v, p, err)
		}
	}

	t.Setenv("BUGNET_ENABLE_LOCAL_PROVIDER", "1")
	if p, err := local.FromEnv(opts); err != nil || p == nil {
		t.Fatalf("FromEnv with the flag set gave %v, %v", p, err)
	}
	relative := local.Options{Name: "local", Root: "relative/root"}
	if _, err := local.FromEnv(relative); err == nil {
		t.Error("FromEnv accepted a relative Root")
	}
}

func TestLocalStateSharedAcrossInstances(t *testing.T) {
	root := t.TempDir()
	a := newLocal(t, local.Options{Root: root})
	b := newLocal(t, local.Options{Root: root})
	req := provider.CreateRequest{
		IdempotencyKey: "shared-key",
		Size:           small,
		Labels:         map[string]string{"t": "shared"},
		Lifetime:       time.Hour,
	}

	refs := make([]provider.Ref, 16)
	errs := make([]error, len(refs))
	var wg sync.WaitGroup
	for i := range refs {
		p := a
		if i%2 == 1 {
			p = b
		}
		wg.Go(func() { refs[i], errs[i] = p.Create(t.Context(), req) })
	}
	wg.Wait()
	for i := range refs {
		if errs[i] != nil {
			t.Fatalf("Create %d: %v", i, errs[i])
		}
		if refs[i].ID != refs[0].ID || refs[i].Provider != "local" {
			t.Fatalf("Create %d gave %+v, Create 0 gave %+v", i, refs[i], refs[0])
		}
	}
	ref := refs[0]

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var sandboxes []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			sandboxes = append(sandboxes, e.Name())
		}
	}
	if len(sandboxes) != 1 || sandboxes[0] != ref.ID {
		t.Fatalf("sandbox directories %v, want only %s", sandboxes, ref.ID)
	}

	owned, err := b.ListOwned(t.Context(), req.Labels)
	if err != nil || len(owned) != 1 || owned[0].Ref.ID != ref.ID {
		t.Fatalf("ListOwned through the other instance gave %+v, %v", owned, err)
	}

	until := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := a.SetLifetime(t.Context(), ref, until); err != nil {
		t.Fatalf("SetLifetime: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, ref.ID, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta struct {
		Labels         map[string]string `json:"labels"`
		IdempotencyKey string            `json:"idempotency_key"`
		CreatedAt      time.Time         `json:"created_at"`
		Deadline       time.Time         `json:"deadline"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("meta.json: %v", err)
	}
	if meta.Labels["t"] != "shared" || meta.IdempotencyKey != req.IdempotencyKey ||
		meta.CreatedAt.IsZero() || !meta.Deadline.Equal(until) {
		t.Fatalf("meta.json holds %s", raw)
	}

	if err := b.Destroy(t.Context(), ref); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	_, err = a.ListFiles(t.Context(), ref, "/", 1)
	requireCode(t, "ListFiles after Destroy through the other instance", err, provider.CodeNotFound)
	if _, err := os.Stat(filepath.Join(root, ref.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sandbox directory still there after Destroy: %v", err)
	}
	again, err := a.Create(t.Context(), req)
	if err != nil || again.ID != ref.ID {
		t.Fatalf("Create after Destroy with the same key gave %+v, %v, want %s", again, err, ref.ID)
	}
	_, err = a.ListFiles(t.Context(), again, "/", 1)
	requireCode(t, "ListFiles after a used key was reused", err, provider.CodeNotFound)
}

func TestLocalListOwnedMatchesAllLabels(t *testing.T) {
	p := newLocal(t, local.Options{})
	one := createReady(t, p, map[string]string{"bugnet": "1", "placement": "p1"})
	createReady(t, p, map[string]string{"bugnet": "1", "placement": "p2"})

	cases := []struct {
		labels map[string]string
		want   int
	}{
		{nil, 2},
		{map[string]string{"bugnet": "1"}, 2},
		{map[string]string{"bugnet": "1", "placement": "p1"}, 1},
		{map[string]string{"placement": "p3"}, 0},
	}
	for _, c := range cases {
		owned, err := p.ListOwned(t.Context(), c.labels)
		if err != nil || len(owned) != c.want {
			t.Errorf("ListOwned(%v) gave %+v, %v, want %d", c.labels, owned, err, c.want)
		}
		if c.want == 1 && owned[0].Ref.ID != one.ID {
			t.Errorf("ListOwned(%v) found %s, want %s", c.labels, owned[0].Ref.ID, one.ID)
		}
	}
}

func TestLocalSandboxPaths(t *testing.T) {
	root := t.TempDir()
	p := newLocal(t, local.Options{Root: root})
	ref := createReady(t, p, nil)
	ctx := t.Context()
	fsDir := filepath.Join(root, ref.ID, "fs")

	if err := p.WriteFile(ctx, ref, "/etc/app.conf", []byte("a"), 0o600); err != nil {
		t.Fatalf("WriteFile absolute: %v", err)
	}
	if err := p.WriteFile(ctx, ref, "notes.txt", []byte("b"), 0o644); err != nil {
		t.Fatalf("WriteFile relative: %v", err)
	}
	for host, want := range map[string]string{
		filepath.Join(fsDir, "etc", "app.conf"):   "a",
		filepath.Join(fsDir, "root", "notes.txt"): "b",
	} {
		if got, err := os.ReadFile(host); err != nil || string(got) != want {
			t.Errorf("host file %s holds %q, %v, want %q", host, got, err, want)
		}
	}
	got, _, err := p.ReadFile(ctx, ref, "/root/notes.txt", 0, 16)
	if err != nil || string(got) != "b" {
		t.Errorf("ReadFile of a relative write gave %q, %v", got, err)
	}

	for _, bad := range []string{"/../escape", "../../escape", "/etc/../../escape"} {
		requireCode(t, "WriteFile "+bad, p.WriteFile(ctx, ref, bad, []byte("x"), 0o644),
			provider.CodeInvalid)
		_, _, err := p.ReadFile(ctx, ref, bad, 0, 1)
		requireCode(t, "ReadFile "+bad, err, provider.CodeInvalid)
		_, err = p.ListFiles(ctx, ref, bad, 1)
		requireCode(t, "ListFiles "+bad, err, provider.CodeInvalid)
		_, _, err = execOut(t, p, ref, provider.ExecSpec{Argv: []string{"true"}, Cwd: bad})
		requireCode(t, "Exec in "+bad, err, provider.CodeInvalid)
	}
	outside := []string{filepath.Join(root, ref.ID, "escape"), filepath.Join(root, "escape")}
	for _, host := range outside {
		if _, err := os.Stat(host); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("an escaping write reached %s", host)
		}
	}

	entries, err := p.ListFiles(ctx, ref, "/", 2)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	paths := map[string]bool{}
	for _, e := range entries {
		paths[e.Path] = true
	}
	for _, want := range []string{"/etc", "/etc/app.conf", "/root", "/root/notes.txt", "/tmp"} {
		if !paths[want] {
			t.Errorf("ListFiles(/, 2) lacks %s: %+v", want, entries)
		}
	}
	shallow, err := p.ListFiles(ctx, ref, "/etc", 1)
	if err != nil || len(shallow) != 1 || shallow[0].Path != "/etc/app.conf" {
		t.Errorf("ListFiles(/etc, 1) gave %+v, %v", shallow, err)
	}
}

func TestLocalExecEnvironment(t *testing.T) {
	root := t.TempDir()
	p := newLocal(t, local.Options{Root: root})
	ref := createReady(t, p, nil)
	home := filepath.Join(root, ref.ID, "fs", "root")
	t.Setenv("BUGNET_LOCAL_TEST_LEAK", "leaked")

	script := `pwd -P; echo "$HOME"; echo "$PATH"; echo "$GREETING"; ` +
		`echo "${BUGNET_LOCAL_TEST_LEAK:-unset}"`
	spec := provider.ExecSpec{
		Argv: []string{"sh", "-c", script},
		Env:  map[string]string{"GREETING": "hi"},
	}
	out, code, err := execOut(t, p, ref, spec)
	if err != nil || code != 0 {
		t.Fatalf("exec gave exit %d, %v", code, err)
	}
	physicalHome, err := filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatal(err)
	}
	lines := []string{physicalHome, home, os.Getenv("PATH"), "hi", "unset"}
	want := strings.Join(lines, "\n") + "\n"
	if out != want {
		t.Fatalf("exec printed %q, want %q", out, want)
	}

	if err := p.WriteFile(t.Context(), ref, "/srv/app/x", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	spec = provider.ExecSpec{Argv: []string{"pwd", "-P"}, Cwd: "/srv/app"}
	out, _, err = execOut(t, p, ref, spec)
	wantDir, _ := filepath.EvalSymlinks(filepath.Join(root, ref.ID, "fs", "srv", "app"))
	if err != nil || out != wantDir+"\n" {
		t.Fatalf("exec in /srv/app printed %q, %v, want %q", out, err, wantDir)
	}
}

func TestLocalRunnerArgv(t *testing.T) {
	runner := filepath.Join(t.TempDir(), "fake-runner")
	script := []byte("#!/bin/sh\nprintf '%s\\n' \"$@\"\n")
	if err := os.WriteFile(runner, script, 0o755); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	p := newLocal(t, local.Options{Root: root, RunnerPath: runner})
	ref := createReady(t, p, nil)
	fsDir := filepath.Join(root, ref.ID, "fs")
	jobs := filepath.Join(fsDir, "tmp", "bugnet", "jobs")
	if err := p.WriteFile(t.Context(), ref, "/srv/.keep", nil, 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		argv []string
		cwd  string
		want []string
	}{
		{[]string{"bugnet-run", "ls"}, "", []string{"ls", "--dir", jobs}},
		{
			[]string{"/tmp/bugnet/bin/bugnet-run", "start", "--job", "j", "--", "sh", "--dir", "x"},
			"",
			[]string{"start", "--dir", jobs, "--job", "j", "--", "sh", "--dir", "x"},
		},
		{
			[]string{"bugnet-run", "attach", "--job", "j1", "--dir=/elsewhere"},
			"",
			[]string{"attach", "--job", "j1", "--dir=/elsewhere"},
		},
		{
			[]string{"bugnet-run", "cancel", "-dir", "/elsewhere", "--job", "j1"},
			"",
			[]string{"cancel", "-dir", "/elsewhere", "--job", "j1"},
		},
		{[]string{"bugnet-run", "ping"}, "", []string{"ping"}},
		{
			[]string{
				"bugnet-run", "start", "--job", "j", "--cwd", "/srv/app", "--", "sh", "--cwd", "/a",
			},
			"",
			[]string{
				"start", "--dir", jobs, "--job", "j", "--cwd", filepath.Join(fsDir, "srv", "app"),
				"--", "sh", "--cwd", "/a",
			},
		},
		{
			[]string{"bugnet-run", "start", "--cwd=app", "--job", "j"},
			"",
			[]string{
				"start", "--dir", jobs,
				"--cwd=" + filepath.Join(fsDir, "root", "app"), "--job", "j",
			},
		},
		{
			[]string{"bugnet-run", "start", "-cwd", "app", "--job", "j"},
			"/srv",
			[]string{
				"start", "--dir", jobs, "-cwd", filepath.Join(fsDir, "srv", "app"), "--job", "j",
			},
		},
		{
			[]string{"bugnet-run", "start", "--cwd=", "--job", "j"},
			"",
			[]string{"start", "--dir", jobs, "--cwd=", "--job", "j"},
		},
	}
	for _, c := range cases {
		out, code, err := execOut(t, p, ref, provider.ExecSpec{Argv: c.argv, Cwd: c.cwd})
		want := strings.Join(c.want, "\n") + "\n"
		if err != nil || code != 0 || out != want {
			t.Errorf("argv %q in %q reached the runner as %q (exit %d, %v), want %q",
				c.argv, c.cwd, out, code, err, want)
		}
	}
	for _, argv := range [][]string{
		{"bugnet-run", "start", "--cwd=/../escape", "--", "true"},
		{"bugnet-run", "start", "--cwd", "../../escape", "--", "true"},
		{"bugnet-run", "start", "-cwd", "../..", "--", "true"},
	} {
		_, err := p.Exec(t.Context(), ref, provider.ExecSpec{Argv: argv})
		requireCode(t, "Exec of "+strings.Join(argv, " "), err, provider.CodeInvalid)
	}

	bare := newLocal(t, local.Options{Root: root})
	_, err := bare.Exec(t.Context(), ref, provider.ExecSpec{Argv: []string{"bugnet-run", "ping"}})
	requireCode(t, "Exec of the runner without a RunnerPath", err, provider.CodeInvalid)
}

func TestLocalExecStream(t *testing.T) {
	p := newLocal(t, local.Options{})
	ref := createReady(t, p, nil)

	t.Run("stdout only", func(t *testing.T) {
		spec := provider.ExecSpec{Argv: []string{"sh", "-c", "echo out; echo err >&2; exit 3"}}
		out, code, err := execOut(t, p, ref, spec)
		if out != "out\n" || code != 3 || err != nil {
			t.Fatalf("exec gave %q, exit %d, %v, want \"out\\n\", exit 3, no error", out, code, err)
		}
	})

	t.Run("start failure", func(t *testing.T) {
		_, err := p.Exec(t.Context(), ref, provider.ExecSpec{Argv: []string{"/nonexistent/cmd"}})
		requireCode(t, "Exec of a missing program", err, provider.CodeInvalid)
	})

	t.Run("signal death carries stderr", func(t *testing.T) {
		script := `head -c 70000 /dev/zero | tr '\0' x >&2; kill -KILL $$`
		_, code, err := execOut(t, p, ref, provider.ExecSpec{Argv: []string{"sh", "-c", script}})
		if code != -1 || err == nil {
			t.Fatalf("exec gave exit %d, %v, want -1 and an error", code, err)
		}
		if n := strings.Count(err.Error(), "x"); n != 64<<10 {
			t.Fatalf("error carries %d bytes of stderr, want %d", n, 64<<10)
		}
	})

	t.Run("context cancel kills the process group", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		stream, err := p.Exec(ctx, ref, sleeper())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		pid := readPID(t, stream)
		cancel()
		begin := time.Now()
		if _, err := io.ReadAll(stream); err != nil {
			t.Fatalf("read after cancel: %v", err)
		}
		code, err := stream.Wait()
		if code != -1 || !errors.Is(err, context.Canceled) || time.Since(begin) > 10*time.Second {
			t.Fatalf("Wait after cancel gave %d, %v after %v", code, err, time.Since(begin))
		}
		requireGone(t, pid)
	})

	t.Run("close kills the process group", func(t *testing.T) {
		stream, err := p.Exec(t.Context(), ref, sleeper())
		if err != nil {
			t.Fatal(err)
		}
		pid := readPID(t, stream)
		if err := stream.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if code, err := stream.Wait(); code != -1 || err == nil {
			t.Fatalf("Wait after Close gave %d, %v", code, err)
		}
		requireGone(t, pid)
	})

	t.Run("close kills a child the exited leader left behind", func(t *testing.T) {
		spec := provider.ExecSpec{Argv: []string{"sh", "-c", "sleep 30 & echo $!"}}
		stream, err := p.Exec(t.Context(), ref, spec)
		if err != nil {
			t.Fatal(err)
		}
		pid := readPID(t, stream)
		if code, err := stream.Wait(); code != 0 || err != nil {
			t.Fatalf("Wait gave %d, %v, want the leader's exit 0", code, err)
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		requireGone(t, pid)
	})

	t.Run("stderr written after the leader exits", func(t *testing.T) {
		spec := provider.ExecSpec{
			Argv: []string{"sh", "-c", "(while :; do echo e >&2; sleep 0.01; done) & exit 0"},
		}
		stream, err := p.Exec(t.Context(), ref, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		if code, err := stream.Wait(); code != 0 || err != nil {
			t.Fatalf("Wait gave %d, %v, want the leader's exit 0", code, err)
		}
	})
}

// sleeper starts a background child that holds stdout open, so only a group kill ends the stream
func sleeper() provider.ExecSpec {
	return provider.ExecSpec{Argv: []string{"sh", "-c", "sleep 30 & echo $!; wait"}}
}

func readPID(t *testing.T, r io.Reader) int {
	t.Helper()
	line, err := bufio.NewReader(r).ReadString('\n')
	if err != nil {
		t.Fatalf("read pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("pid line %q: %v", line, err)
	}
	return pid
}

func requireGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("background process %d still runs", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestLocalDestroyKillsRunnerJobs(t *testing.T) {
	p := newLocal(t, local.Options{})
	ref := createReady(t, p, nil)

	group := startHost(t, "sh", "-c", "sleep 30 & echo $!; wait")
	child := readPID(t, group.stdout)
	supervisor := startHost(t, "sleep", "30")
	bystander := startHost(t, "sleep", "30")
	files := map[string]string{
		"/tmp/bugnet/jobs/j1/pid":  strconv.Itoa(group.pid) + "\n",
		"/tmp/bugnet/jobs/j1/spid": strconv.Itoa(supervisor.pid) + "\n",
		"/tmp/bugnet/jobs/j2/spid": strconv.Itoa(bystander.pid) + "\n",
		"/tmp/bugnet/jobs/j2/exit": "0\n",
		"/tmp/bugnet/jobs/j3/pid":  "not-a-pid\n",
		"/tmp/bugnet/jobs/j3/spid": "\n",
	}
	for name, data := range files {
		if err := p.WriteFile(t.Context(), ref, name, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := p.Destroy(t.Context(), ref); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	group.requireKilled(t)
	supervisor.requireKilled(t)
	requireGone(t, child)
	select {
	case <-bystander.done:
		t.Fatal("Destroy killed the pid in the spid file of a job that had already exited")
	case <-time.After(200 * time.Millisecond):
	}
}

// hostProcess is a process the test starts in its own group, standing in for a runner job
type hostProcess struct {
	pid    int
	stdout io.Reader
	done   chan struct{}
	err    error
}

func startHost(t *testing.T, argv ...string) *hostProcess {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout = w
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	err = cmd.Start()
	_ = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	h := &hostProcess{pid: cmd.Process.Pid, stdout: r, done: make(chan struct{})}
	go func() {
		h.err = cmd.Wait()
		close(h.done)
	}()
	t.Cleanup(func() {
		_ = syscall.Kill(-h.pid, syscall.SIGKILL)
		<-h.done
		_ = r.Close()
	})
	return h
}

func (h *hostProcess) requireKilled(t *testing.T) {
	t.Helper()
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("process %d still runs", h.pid)
	}
	var exitErr *exec.ExitError
	if !errors.As(h.err, &exitErr) {
		t.Fatalf("process %d ended with %v, want a SIGKILL", h.pid, h.err)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("process %d ended with %v, want a SIGKILL", h.pid, h.err)
	}
}

func TestLocalIOErrorsAreClassified(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the permission bits this test relies on")
	}
	root := t.TempDir()
	p := newLocal(t, local.Options{Root: root})
	ref := createReady(t, p, map[string]string{"t": "io"})
	ctx := t.Context()

	if err := p.WriteFile(ctx, ref, "/locked/f", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	locked := filepath.Join(root, ref.ID, "fs", "locked")
	lock(t, locked, 0o755)
	_, _, err := p.ReadFile(ctx, ref, "/locked/f", 0, 1)
	requireCode(t, "ReadFile in an unreadable directory", err, provider.CodeInternal)
	err = p.WriteFile(ctx, ref, "/locked/g", []byte("y"), 0o644)
	requireCode(t, "WriteFile in an unwritable directory", err, provider.CodeInternal)
	_, err = p.ListFiles(ctx, ref, "/locked", 1)
	requireCode(t, "ListFiles of an unreadable directory", err, provider.CodeInternal)

	lock(t, filepath.Join(root, ref.ID, "meta.json"), 0o644)
	requireCode(t, "WaitReady with unreadable metadata", p.WaitReady(ctx, ref),
		provider.CodeInternal)
	_, err = p.ListOwned(ctx, map[string]string{"t": "io"})
	requireCode(t, "ListOwned with unreadable metadata", err, provider.CodeInternal)
}

// lock removes every permission from name until the test ends
func lock(t *testing.T, name string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(name, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(name, mode) })
}

func TestLocalCreateWithDoneContext(t *testing.T) {
	hooked := false
	p := newLocal(t, local.Options{
		CreateHook: func(int, provider.CreateRequest) error {
			hooked = true
			return nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	req := provider.CreateRequest{IdempotencyKey: "k", Labels: map[string]string{"t": "done"}}
	if _, err := p.Create(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create with a cancelled context gave %v, want context.Canceled", err)
	}
	if hooked {
		t.Fatal("Create with a cancelled context ran the hook")
	}
	owned, err := p.ListOwned(t.Context(), req.Labels)
	if err != nil || len(owned) != 0 {
		t.Fatalf("after a cancelled Create ListOwned gave %+v, %v, want nothing", owned, err)
	}
}

func TestLocalWaitReadyHonoursDelay(t *testing.T) {
	p := newLocal(t, local.Options{ReadyDelay: 300 * time.Millisecond})
	begin := time.Now()
	ref, err := p.Create(t.Context(), provider.CreateRequest{Size: small})
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := p.WaitReady(short, ref); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady before the delay gave %v, want a deadline error", err)
	}
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if waited := time.Since(begin); waited < 300*time.Millisecond {
		t.Fatalf("WaitReady returned after %v, before the ready delay", waited)
	}
}

func TestLocalSizesAndPricing(t *testing.T) {
	gpu := provider.Size{
		Name: "gpu",
		Resources: provider.Resources{
			VCPU: 8, MemoryMiB: 32768, DiskGiB: 100, GPUCount: 1, GPUMemoryGiB: 24,
		},
	}
	p := newLocal(t, local.Options{
		Sizes:            []provider.Size{small, gpu},
		HourlyCostMicros: 120000,
		Increment:        time.Minute,
	})

	cases := []struct {
		floor provider.Resources
		want  string
		ok    bool
	}{
		{provider.Resources{}, "small", true},
		{provider.Resources{VCPU: 2, MemoryMiB: 2048}, "small", true},
		{provider.Resources{VCPU: 4}, "gpu", true},
		{provider.Resources{GPUCount: 1, GPUMemoryGiB: 20}, "gpu", true},
		{provider.Resources{GPUCount: 1, GPUMemoryGiB: 40}, "", false},
	}
	for _, c := range cases {
		size, ok := p.SizeFor(c.floor)
		if ok != c.ok || size.Name != c.want {
			t.Errorf("SizeFor(%+v) gave %q, %v, want %q, %v", c.floor, size.Name, ok, c.want, c.ok)
		}
	}
	if got := p.HourlyCostMicros(gpu); got != 120000 {
		t.Errorf("HourlyCostMicros gave %d", got)
	}
	if got := p.BillingIncrement(); got != time.Minute {
		t.Errorf("BillingIncrement gave %v", got)
	}
	want := provider.Capabilities{MaxLifetime: 24 * time.Hour}
	if got := p.Capabilities(); got != want {
		t.Errorf("Capabilities gave %+v, want %+v", got, want)
	}
	if p.Name() != "local" {
		t.Errorf("Name gave %q", p.Name())
	}
}
