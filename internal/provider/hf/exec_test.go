package hf

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// readyJob starts a Job through the adapter and waits until its sbx-server serves
func readyJob(t *testing.T, f *fakeHF, p *Provider) provider.Ref {
	t.Helper()
	ref, _ := created(t, f, p)
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

// run executes spec and returns its stdout and exit status
func run(t *testing.T, p *Provider, ref provider.Ref, spec provider.ExecSpec) (string, int, error) {
	t.Helper()
	s, err := p.Exec(t.Context(), ref, spec)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = s.Close() }()
	out, err := io.ReadAll(s)
	if err != nil {
		return string(out), 0, err
	}
	code, err := s.Wait()
	return string(out), code, err
}

func sh(script string) provider.ExecSpec {
	return provider.ExecSpec{Argv: []string{"sh", "-c", script}}
}

// execBodies are the exec bodies the fake got, decoded
func execBodies(t *testing.T, f *fakeHF) []fakeExec {
	t.Helper()
	var out []fakeExec
	for _, r := range f.requestsTo("POST", "/v1/exec") {
		var b fakeExec
		if err := json.Unmarshal(r.body, &b); err != nil {
			t.Fatal(err)
		}
		out = append(out, b)
	}
	return out
}

// within polls until ok, failing the test once d passes
func within(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not come within %v", what, d)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHFExec(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)

	out, code, err := run(t, p, ref, sh("echo hi; echo hidden >&2; exit 5"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, out, "hi\n")
	eq(t, code, 5)

	// A signal exit is 128 plus the signal
	_, code, err = run(t, p, ref, sh("kill -TERM $$"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, code, 128+15)

	// The working directory, the environment and words a shell would split all reach argv
	dir := t.TempDir()
	out, code, err = run(t, p, ref, provider.ExecSpec{
		Argv: []string{"sh", "-c", `pwd; printf '%s|' "$FOO" "$1"`, "sh", "a b'c $HOME"},
		Cwd:  dir, Env: map[string]string{"FOO": "x y"},
	})
	if err != nil || code != 0 {
		t.Fatalf("exit %d, %v", code, err)
	}
	want, _ := filepath.EvalSymlinks(dir)
	got, _ := filepath.EvalSymlinks(strings.SplitN(out, "\n", 2)[0])
	eq(t, got, want)
	eq(t, strings.SplitN(out, "\n", 2)[1], "x y|a b'c $HOME|")

	// A first word a shell would read as an option or an assignment still runs as a command
	for _, argv0 := range []string{"-x", "A=B"} {
		_, code, err = run(t, p, ref, provider.ExecSpec{Argv: []string{argv0}})
		if err != nil || code != 127 {
			t.Fatalf("%s gave exit %d, %v, want 127 for a command not found", argv0, code, err)
		}
	}

	// Each call's pid file is gone once its command ends
	left, _ := filepath.Glob(filepath.Join(f.sbx.execDir, "*"))
	eq(t, len(left), 0)

	// A user PATH never hides the wrapper's own tools
	_, code, err = run(t, p, ref, provider.ExecSpec{
		Argv: []string{"/bin/sh", "-c", "exit 3"}, Env: map[string]string{"PATH": "/nowhere"},
	})
	if err != nil || code != 3 {
		t.Fatalf("exit %d, %v", code, err)
	}

	// stdin is closed, so a command that reads it ends
	out, code, err = run(t, p, ref, sh("cat; echo end"))
	if err != nil || code != 0 || out != "end\n" {
		t.Fatalf("a read of stdin gave %q, exit %d, %v", out, code, err)
	}

	// Every exec goes as argv with shell false and a timeout, the command in its own session
	for _, b := range execBodies(t, f) {
		var argv []string
		if err := json.Unmarshal(b.Cmd, &argv); err != nil {
			t.Fatal(err)
		}
		if b.Shell == nil || *b.Shell || b.Timeout == nil || *b.Timeout <= 0 ||
			len(argv) < 5 || argv[0] != "/bin/sh" || argv[1] != "-c" ||
			!strings.HasPrefix(argv[2], quote(p.setsid)+" -w /bin/sh -c ") {
			t.Fatalf("exec body %+v, argv %q", b, argv)
		}
	}
}

func TestHFExecLine(t *testing.T) {
	p, err := newProvider(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	argv := p.groupArgv([]string{"bugnet-run", "attach"}, "c0ffee")
	eq(t, argv[0], "/bin/sh")
	eq(t, argv[1], "-c")
	eq(t, argv[3], "sh")
	eq(t, argv[4:], []string{"bugnet-run", "attach"})
	line := argv[2]
	if !strings.HasPrefix(line, `/usr/bin/setsid -w /bin/sh -c `) ||
		!strings.Contains(line, "echo $$ >/tmp/bugnet/exec/c0ffee.pid") ||
		!strings.Contains(line, `"$@" </dev/null`) ||
		!strings.Contains(line, ` sh "$@"; s=$?; `) ||
		!strings.Contains(line, `for f in /tmp/bugnet/exec/*.pid;`) ||
		!strings.HasSuffix(line, "; exit $s") {
		t.Fatalf("line %q", line)
	}
	kill := p.killArgv("c0ffee")
	eq(t, kill[:2], []string{"/bin/sh", "-c"})
	if !strings.Contains(kill[2], `kill -KILL "-$p"`) ||
		!strings.Contains(kill[2], "/tmp/bugnet/exec/c0ffee.pid") ||
		!strings.Contains(kill[2], "/tmp/bugnet/exec/c0ffee.cancel") {
		t.Fatalf("kill %q", kill)
	}

	f := newFake(t)
	p = f.provider(t, testOptions())
	j := f.addJob("RUNNING", nil)
	ref := provider.Ref{Provider: "hf", ID: j.id, Meta: map[string]string{"secret": testKeyToken}}
	for _, bad := range []provider.ExecSpec{
		{}, {Argv: []string{"x"}, Env: map[string]string{"A B": "1"}},
		{Argv: []string{"x"}, Env: map[string]string{"": "1"}},
		{Argv: []string{"x\x00"}}, {Argv: []string{"x\xff"}},
		{Argv: []string{"x"}, Env: map[string]string{"A": "\xff"}},
		{Argv: []string{"x"}, Env: map[string]string{"A": "a\x00b"}},
		{Argv: []string{"x"}, Cwd: "/tmp/\xff"},
		{Argv: []string{"x", strings.Repeat("y", maxBodyBytes)}},
	} {
		_, err := p.Exec(t.Context(), ref, bad)
		if code, _ := codeOf(err); code != provider.CodeInvalid {
			t.Errorf("exec of %.40q gave %v, want invalid", bad.Argv, err)
		}
	}
	// A ref this adapter never made is not found, with no call
	_, err = p.Exec(t.Context(), provider.Ref{Provider: "lium", ID: j.id}, sh("true"))
	if code, _ := codeOf(err); code != provider.CodeNotFound {
		t.Fatalf("a lium ref gave %v", err)
	}
	eq(t, len(f.all()), 0)
}

func TestHFExecTimeoutIsTheJobsEnd(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	j := f.addJob("RUNNING", nil)
	f.set(j, func(j *fakeJob) { j.token = testKeyToken })
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	p.now = func() time.Time { return now }
	ref := provider.Ref{Provider: "hf", ID: j.id, Meta: map[string]string{
		"secret": testKeyToken, "created_at": "2026-11-11T11:00:00Z", "timeout": "5400",
	}}
	if _, _, err := run(t, p, ref, sh("true")); err != nil {
		t.Fatal(err)
	}
	// Created an hour ago with 90 minutes, the Job ends in 30
	eq(t, *execBodies(t, f)[0].Timeout, float64(1800))

	// A ref without its end gives the longest a Job can run
	delete(ref.Meta, "created_at")
	if _, _, err := run(t, p, ref, sh("true")); err != nil {
		t.Fatal(err)
	}
	eq(t, *execBodies(t, f)[1].Timeout, float64(p.timeoutFor(p.maxLifetime)))

	// A Job at its end still gets a timeout, which the proxy outlives by nothing
	ref.Meta["created_at"] = "2026-11-11T10:00:00Z"
	if _, _, err := run(t, p, ref, sh("true")); err != nil {
		t.Fatal(err)
	}
	eq(t, *execBodies(t, f)[2].Timeout, float64(1))
}

func TestHFExecTextIsLossy(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	out, code, err := run(t, p, ref, sh(`printf '\377ok\n'`))
	if err != nil || code != 0 {
		t.Fatalf("exit %d, %v", code, err)
	}
	// sbx-server sends text, so the byte is replaced and transfer must encode what it reads
	eq(t, out, "�ok\n")
	eq(t, p.Capabilities().LossyExecText, true)
}

func TestHFExecEvents(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	calls := 0
	// answer serves body to one exec, which a kill an earlier case sent never takes
	answer := func(t *testing.T, body string) (string, int, error) {
		t.Helper()
		calls++
		marker := "case-" + strconv.Itoa(calls)
		f.inject(sbxExecRoute, fault{status: 200, raw: body, times: 1, only: marker})
		s, err := p.Exec(t.Context(), ref, sh("true "+marker))
		if err != nil {
			return "", 0, err
		}
		defer func() { _ = s.Close() }()
		out, rerr := io.ReadAll(s)
		code, err := s.Wait()
		if rerr != nil && !errors.Is(err, rerr) {
			t.Fatalf("Read gave %v, then Wait %v", rerr, err)
		}
		return string(out), code, err
	}
	t.Run("pings, stderr and unknown events are skipped", func(t *testing.T) {
		out, code, err := answer(t, ndjson(`{"event":"start","pid":7}`, `{"event":"ping"}`,
			`{"event":"stdout","data":"a"}`, `{"event":"stderr","data":"no"}`,
			`{"event":"later","data":"no"}`, `{"event":"stdout","data":"b\n"}`,
			`{"event":"exit","exit_code":4,"signal":null,"timed_out":false,"duration_ms":3}`))
		if err != nil || out != "ab\n" || code != 4 {
			t.Fatalf("gave %q, exit %d, %v", out, code, err)
		}
	})
	t.Run("a signal exit", func(t *testing.T) {
		_, code, err := answer(t, ndjson(
			`{"event":"exit","exit_code":null,"signal":9,"timed_out":false,"duration_ms":3}`))
		if err != nil || code != 137 {
			t.Fatalf("exit %d, %v", code, err)
		}
	})
	for name, line := range map[string]string{
		"a timeout":        `{"event":"exit","exit_code":null,"signal":9,"timed_out":true}`,
		"no code":          `{"event":"exit","exit_code":null,"signal":null,"timed_out":false}`,
		"a code past 255":  `{"event":"exit","exit_code":300,"signal":null}`,
		"a signal past 64": `{"event":"exit","exit_code":null,"signal":200}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, code, err := answer(t, ndjson(`{"event":"start","pid":7}`, line))
			if c, _ := codeOf(err); c != provider.CodeInternal || code != -1 {
				t.Fatalf("exit %d, %v, want an internal error", code, err)
			}
		})
	}
	// A stream that cannot be read further leaves the command's outcome unknown
	for name, body := range map[string]string{
		"a line that is not JSON": ndjson(`{"event":"start","pid":7}`, `not json`),
		"an endless line": ndjson(`{"event":"stdout","data":"` +
			strings.Repeat("x", maxEventBytes) + `"}`),
	} {
		t.Run(name, func(t *testing.T) {
			_, code, err := answer(t, body)
			if c, _ := codeOf(err); c != provider.CodeOutcomeUnknown || code != -1 {
				t.Fatalf("exit %d, %v, want outcome_unknown", code, err)
			}
		})
	}
	t.Run("a stream that ends with no exit", func(t *testing.T) {
		out, code, err := answer(t, ndjson(`{"event":"start","pid":7}`,
			`{"event":"stdout","data":"part"}`))
		if c, _ := codeOf(err); c != provider.CodeOutcomeUnknown || code != -1 || out != "part" {
			t.Fatalf("gave %q, exit %d, %v", out, code, err)
		}
		// No one reads the command any more, so its group is killed as on a cancel
		marker := "case-" + strconv.Itoa(calls)
		var call string
		for _, argv := range f.execLines() {
			if slices.Contains(argv, "true") && slices.Contains(argv, marker) {
				call = callOf(t, argv[2])
			}
		}
		within(t, 5*time.Second, "the kill", func() bool {
			return slices.ContainsFunc(f.execLines(), func(argv []string) bool {
				return len(argv) == 4 && strings.Contains(argv[2], `kill -KILL "-$p"`) &&
					strings.Contains(argv[2], call+".pid")
			})
		})
	})
	t.Run("the token never shows in an error", func(t *testing.T) {
		for _, line := range []string{
			`{"event":"exit","exit_code":null,"signal":null,"why":"` + testKeyToken + `"}`,
			`{"event":"exit","exit_code":"` + testKeyToken + `"}`,
			testKeyToken,
		} {
			_, _, err := answer(t, ndjson(line))
			if err == nil || strings.Contains(err.Error(), testKeyToken) {
				t.Fatalf("gave %v", err)
			}
		}
	})
}

func TestHFExecErrors(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	cases := []struct {
		name string
		ft   fault
		code provider.Code
	}{
		{"a refused token", fault{status: 403,
			message: "invalid or missing X-Sandbox-Token " + testKeyToken}, provider.CodeInternal},
		{"a bad request", fault{status: 400, message: "bad cmd"}, provider.CodeInvalid},
		{"a full disk", fault{status: 500, message: "No space left on device (os error 28)"},
			provider.CodeNoSpace},
		{"a server error", fault{status: 500, message: "boom"}, provider.CodeInternal},
		{"the connection cap", fault{status: 503, message: "busy"}, provider.CodeRateLimited},
		{"a proxy error on a running Job", fault{status: 502, raw: "<html>"},
			provider.CodeInternal},
		{"no answer", fault{drop: true}, provider.CodeInternal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.ft.times = 1
			f.inject(sbxExecRoute, c.ft)
			_, _, err := run(t, p, ref, sh("true"))
			e, ok := provider.AsError(err)
			if !ok || e.Code != c.code {
				t.Fatalf("gave %v, want %s", err, c.code)
			}
			if strings.Contains(err.Error(), testKeyToken) {
				t.Fatalf("the error %q carries the token", err)
			}
			if c.code == provider.CodeRateLimited && e.RetryAfter != time.Second {
				t.Fatalf("retry after %v", e.RetryAfter)
			}
			if c.ft.status == 403 && !strings.Contains(err.Error(), "hf_token_refused") {
				t.Fatalf("a 403 gave %v", err)
			}
		})
	}
	t.Run("a Job gone behind the proxy", func(t *testing.T) {
		f.set(f.job(ref.ID), func(j *fakeJob) { j.stage = "CANCELED" })
		_, _, err := run(t, p, ref, sh("true"))
		if c, _ := codeOf(err); c != provider.CodeNotFound {
			t.Fatalf("gave %v", err)
		}
	})
}

func TestHFExecDerivesAMissingToken(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	bare := provider.Ref{Provider: "hf", ID: ref.ID}
	out, code, err := run(t, p, bare, sh("echo ok"))
	if err != nil || code != 0 || out != "ok\n" {
		t.Fatalf("gave %q, exit %d, %v", out, code, err)
	}
	execs := f.requestsTo("POST", "/v1/exec")
	eq(t, execs[len(execs)-1].header.Get("X-Sandbox-Token"), testKeyToken)

	// A Job without our key gives no token to derive
	other := f.addJob("RUNNING", map[string]string{"bugnet_key": "not-a-key"})
	_, _, err = run(t, p, provider.Ref{Provider: "hf", ID: other.id}, sh("true"))
	if c, _ := codeOf(err); c != provider.CodeInternal {
		t.Fatalf("gave %v", err)
	}
	// A token of the wrong shape never goes in a header
	bad := provider.Ref{Provider: "hf", ID: ref.ID, Meta: map[string]string{"secret": "a\r\nb"}}
	_, _, err = run(t, p, bad, sh("true"))
	if c, _ := codeOf(err); c != provider.CodeInternal {
		t.Fatalf("gave %v", err)
	}
}

// callOf reads the call id out of a wrapped command's script
func callOf(t *testing.T, script string) string {
	t.Helper()
	m := regexp.MustCompile(`/([0-9a-f]{32})\.pid`).FindStringSubmatch(script)
	if m == nil {
		t.Fatalf("no call id in %q", script)
	}
	return m[1]
}

// groupAlive lists the processes of a group that have not exited, zombies left out
func groupAlive(t *testing.T, pgid int) []string {
	t.Helper()
	out, err := exec.Command("ps", "-A", "-o", "pid=,pgid=,stat=").Output()
	if err != nil {
		t.Fatal(err)
	}
	var alive []string
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 3 && f[1] == strconv.Itoa(pgid) && !strings.HasPrefix(f[2], "Z") {
			alive = append(alive, f[0])
		}
	}
	return alive
}

// startedGroup starts a command that leaves two sleeps in its group and returns its pgid
func startedGroup(
	ctx context.Context, t *testing.T, f *fakeHF, p *Provider, ref provider.Ref,
) (provider.ExecStream, int) {
	t.Helper()
	s, err := p.Exec(ctx, ref, sh("sleep 60 & sleep 60 & echo started; wait"))
	if err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(s).ReadString('\n')
	if err != nil || line != "started\n" {
		t.Fatalf("read %q, %v", line, err)
	}
	files, _ := filepath.Glob(filepath.Join(f.sbx.execDir, "*.pid"))
	if len(files) != 1 {
		t.Fatalf("pid files %v", files)
	}
	raw, _ := os.ReadFile(files[0])
	pgid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(groupAlive(t, pgid)); n < 3 {
		t.Fatalf("the group holds %d processes", n)
	}
	return s, pgid
}

// waitGroupGone fails the test unless the group ends soon and leaves no file behind
func waitGroupGone(t *testing.T, f *fakeHF, pgid int) {
	t.Helper()
	within(t, 10*time.Second, "the group's end", func() bool {
		return len(groupAlive(t, pgid)) == 0
	})
	within(t, 10*time.Second, "the pid file's removal", func() bool {
		left, _ := filepath.Glob(filepath.Join(f.sbx.execDir, "*"))
		return len(left) == 0
	})
}

// killExecs counts the execs that killed a group
func killExecs(f *fakeHF) int {
	n := 0
	for _, argv := range f.execLines() {
		if len(argv) == 4 && strings.Contains(argv[2], `kill -KILL "-$p"`) {
			n++
		}
	}
	return n
}

func TestExecCancelKillsGroup(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)

	t.Run("cancel", func(t *testing.T) {
		before := killExecs(f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s, pgid := startedGroup(ctx, t, f, p, ref)
		ended := make(chan error, 1)
		go func() {
			_, err := io.ReadAll(s)
			ended <- err
		}()
		cancel()
		select {
		case err := <-ended:
			if c, _ := codeOf(err); c != provider.CodeInternal {
				t.Fatalf("Read after a cancel gave %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a cancel did not end Read")
		}
		code, err := s.Wait()
		if code != -1 || err == nil {
			t.Fatalf("Wait gave %d, %v", code, err)
		}
		// The fake runs a command on when its client goes, so only the second exec ends it
		waitGroupGone(t, f, pgid)
		eq(t, killExecs(f), before+1)
	})

	t.Run("close", func(t *testing.T) {
		before := killExecs(f)
		s, pgid := startedGroup(context.Background(), t, f, p, ref)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Read(make([]byte, 1)); err == nil {
			t.Fatal("Read after Close gave no error")
		}
		waitGroupGone(t, f, pgid)
		eq(t, killExecs(f), before+1)
	})

	t.Run("a group whose first process returned", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s, err := p.Exec(ctx, ref, sh("ps -o pgid= -p $$; sleep 60 & exit 0"))
		if err != nil {
			t.Fatal(err)
		}
		line, err := bufio.NewReader(s).ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		pgid, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			t.Fatal(err)
		}
		// argv and the wrapper return while the sleep keeps stdout open
		within(t, 5*time.Second, "argv's return", func() bool {
			return len(groupAlive(t, pgid)) == 1
		})
		cancel()
		if code, err := s.Wait(); code != -1 || err == nil {
			t.Fatalf("Wait gave %d, %v", code, err)
		}
		waitGroupGone(t, f, pgid)
	})

	t.Run("a kill before the command starts", func(t *testing.T) {
		ctx := context.Background()
		sb, err := p.sandbox(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.killGroup(ctx, sb, "early"); err != nil {
			t.Fatal(err)
		}
		marks, _ := filepath.Glob(filepath.Join(f.sbx.execDir, "early.cancel"))
		eq(t, len(marks), 1)
		s, err := p.exec(ctx, ref, sh("echo ran"), "early")
		if err != nil {
			t.Fatal(err)
		}
		out, _ := io.ReadAll(s)
		code, err := s.Wait()
		if string(out) != "" || code != 137 || err != nil {
			t.Fatalf("a marked call gave %q, exit %d, %v", out, code, err)
		}
		left, _ := filepath.Glob(filepath.Join(f.sbx.execDir, "*"))
		eq(t, len(left), 0)
	})

	t.Run("a Close right after stdout ends kills nothing", func(t *testing.T) {
		before := len(f.execLines())
		for range 3 {
			s, err := p.Exec(context.Background(), ref, sh("echo done"))
			if err != nil {
				t.Fatal(err)
			}
			if out, _ := io.ReadAll(s); string(out) != "done\n" {
				t.Fatalf("read %q", out)
			}
			_ = s.Close()
		}
		time.Sleep(100 * time.Millisecond)
		eq(t, len(f.execLines()), before+3)
		left, _ := filepath.Glob(filepath.Join(f.sbx.execDir, "*"))
		eq(t, len(left), 0)
	})

	t.Run("a finished command is never killed", func(t *testing.T) {
		before := len(f.execLines())
		ctx, cancel := context.WithCancel(context.Background())
		s, err := p.Exec(ctx, ref, sh("echo done"))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadAll(s)
		if code, err := s.Wait(); code != 0 || err != nil {
			t.Fatalf("exit %d, %v", code, err)
		}
		cancel()
		_ = s.Close()
		time.Sleep(50 * time.Millisecond)
		eq(t, len(f.execLines()), before+1)
	})

	t.Run("an answer that comes after the deadline", func(t *testing.T) {
		gate := make(chan struct{})
		defer close(gate)
		f.mu.Lock()
		f.holdExecs, f.execGate = 1, gate
		f.mu.Unlock()
		file := filepath.Join(t.TempDir(), "pgid")
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, err := p.Exec(ctx, ref, sh("ps -o pgid= -p $$ >"+file+".new && mv "+file+".new "+
			file+"; exec sleep 60"))
		if c, _ := codeOf(err); c != provider.CodeInternal {
			t.Fatalf("Exec past its deadline gave %v", err)
		}
		// The command started before the deadline, and the kill of a start given up on ends it
		var raw []byte
		within(t, 5*time.Second, "the command's start", func() bool {
			raw, err = os.ReadFile(file)
			return err == nil
		})
		pgid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		waitGroupGone(t, f, pgid)
	})

	t.Run("the stale cancel marks go", func(t *testing.T) {
		stale := filepath.Join(f.sbx.execDir, "stale.cancel")
		fresh := filepath.Join(f.sbx.execDir, "fresh.cancel")
		for _, m := range []string{stale, fresh} {
			if err := os.WriteFile(m, nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		old := time.Now().Add(-11 * time.Minute)
		if err := os.Chtimes(stale, old, old); err != nil {
			t.Fatal(err)
		}
		if _, _, err := run(t, p, ref, sh("true")); err != nil {
			t.Fatal(err)
		}
		left, _ := filepath.Glob(filepath.Join(f.sbx.execDir, "*"))
		eq(t, left, []string{fresh})
		_ = os.Remove(fresh)
	})
}
