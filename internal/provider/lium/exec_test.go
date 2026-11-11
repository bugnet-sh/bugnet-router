package lium

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// readyRef rents a pod, lets it run and returns its ref with what ReadyMeta learned
func readyRef(t *testing.T, f *fakeLium, p *Provider) (provider.Ref, fakePod) {
	t.Helper()
	ref, pod := rentedPod(t, f, p)
	f.setPod(pod.id, func(fp *fakePod) { fp.status = "RUNNING" })
	meta, err := p.ReadyMeta(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	ref.Meta = maps.Clone(ref.Meta)
	maps.Copy(ref.Meta, meta)
	return ref, pod
}

// run executes spec and returns its stdout and exit status
func run(t *testing.T, p *Provider, ref provider.Ref, spec provider.ExecSpec) (string, int, error) {
	t.Helper()
	s, err := p.Exec(context.Background(), ref, spec)
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

func TestLiumExec(t *testing.T) {
	f, p := liveFake(t)
	ref, _ := readyRef(t, f, p)

	out, code, err := run(t, p, ref, sh("echo hi; echo hidden >&2; exit 5"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, out, "hi\n")
	eq(t, code, 5)

	// Bytes come through as the command wrote them
	out, code, err = run(t, p, ref, sh(`printf '\377\000\001\n'`))
	if err != nil || code != 0 || out != "\xff\x00\x01\n" {
		t.Fatalf("binary output %q exit %d, %v", out, code, err)
	}

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
	left, _ := filepath.Glob(filepath.Join(f.sh.execDir, "*"))
	eq(t, len(left), 0)

	// A user PATH never hides the wrapper's own tools
	_, code, err = run(t, p, ref, provider.ExecSpec{
		Argv: []string{"/bin/sh", "-c", "exit 3"}, Env: map[string]string{"PATH": "/nowhere"},
	})
	if err != nil || code != 3 {
		t.Fatalf("exit %d, %v", code, err)
	}
}

func TestLiumExecLine(t *testing.T) {
	_, p := liveFake(t)
	p.execDir, p.setsid = "/tmp/bugnet/exec", "/usr/bin/setsid"
	line, err := p.groupCommand(provider.ExecSpec{
		Argv: []string{"bugnet-run", "attach"}, Cwd: "/w", Env: map[string]string{"A": "1"},
	}, "c0ffee")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, `cd '/w' && env 'A=1' '/usr/bin/setsid' '-w' '/bin/sh' '-c' `) ||
		!strings.HasSuffix(line, ` 'sh' 'bugnet-run' 'attach' </dev/null`) ||
		!strings.Contains(line, "/tmp/bugnet/exec/c0ffee.pid") {
		t.Fatalf("line %q", line)
	}
	for _, bad := range []provider.ExecSpec{
		{}, {Argv: []string{"x"}, Env: map[string]string{"A B": "1"}},
		{Argv: []string{"x\x00"}},
	} {
		_, err := p.Exec(context.Background(), provider.Ref{Provider: "lium", ID: "x"}, bad)
		wantCode(t, err, provider.CodeInvalid, false)
	}
	eq(t, p.dial == nil, false)
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
	ctx context.Context, t *testing.T, f *fakeLium, p *Provider, ref provider.Ref,
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
	files, _ := filepath.Glob(filepath.Join(f.sh.execDir, "*.pid"))
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
func waitGroupGone(t *testing.T, f *fakeLium, pgid int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for len(groupAlive(t, pgid)) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("group %d still runs %v", pgid, groupAlive(t, pgid))
		}
		time.Sleep(20 * time.Millisecond)
	}
	for time.Now().Before(deadline) {
		left, _ := filepath.Glob(filepath.Join(f.sh.execDir, "*"))
		if len(left) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	left, _ := filepath.Glob(filepath.Join(f.sh.execDir, "*"))
	t.Fatalf("files left %v", left)
}

func TestExecCancelKillsGroup(t *testing.T) {
	f, p := liveFake(t)
	ref, _ := readyRef(t, f, p)

	t.Run("cancel", func(t *testing.T) {
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
			wantCode(t, err, provider.CodeInternal, false)
		case <-time.After(2 * time.Second):
			t.Fatal("a cancel did not end Read")
		}
		code, err := s.Wait()
		if code != -1 || err == nil {
			t.Fatalf("Wait gave %d, %v", code, err)
		}
		waitGroupGone(t, f, pgid)
		eq(t, f.signalsSent()[len(f.signalsSent())-1], "KILL")
	})

	t.Run("close", func(t *testing.T) {
		s, pgid := startedGroup(context.Background(), t, f, p, ref)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Read(make([]byte, 1)); err == nil {
			t.Fatal("Read after Close gave no error")
		}
		waitGroupGone(t, f, pgid)
	})

	t.Run("a kill before the command starts", func(t *testing.T) {
		ctx := context.Background()
		pl, err := p.poolFor(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.killGroup(ctx, pl, "early"); err != nil {
			t.Fatal(err)
		}
		marks, _ := filepath.Glob(filepath.Join(f.sh.execDir, "early.cancel"))
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
		left, _ := filepath.Glob(filepath.Join(f.sh.execDir, "*"))
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
}
