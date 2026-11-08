package jobs_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-run/frame"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/jobs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/streams"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

var runnerPath string

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "bugnet-run-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	runnerPath = filepath.Join(dir, "bugnet-run")
	build := exec.Command("go", "build", "-o", runnerPath,
		"github.com/bugnet-sh/bugnet-run/cmd/bugnet-run")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build bugnet-run:", err)
		return 1
	}
	return m.Run()
}

var small = provider.Size{
	Name: "small", Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
}

type env struct {
	store  *store.Store
	bus    *streams.Bus
	vk     valkey.Client
	prefix string
	root   string
	prov   *local.Provider
	reg    *provider.Registry
	place  store.Placement
}

func newEnv(t *testing.T) *env {
	t.Helper()
	return newEnvWith(t, nil)
}

// newEnvWith lets a test wrap the local provider the services run jobs through
func newEnvWith(t *testing.T, wrap func(*local.Provider) provider.Provider) *env {
	t.Helper()
	ctx := t.Context()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(ctx, url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	vk, prefix := testenv.Valkey(t)
	root := t.TempDir()
	prov := local.New(local.Options{
		Name: "local", Root: root, Sizes: []provider.Size{small}, RunnerPath: runnerPath,
	})
	var run provider.Provider = prov
	if wrap != nil {
		run = wrap(prov)
	}
	e := &env{
		store: store.New(pool), bus: streams.NewBus(vk, prefix), vk: vk, prefix: prefix,
		root: root, prov: prov, reg: newRegistry(t, run),
	}
	e.place = e.readyPlacement(t)
	return e
}

// newRegistry registers ps with no data-plane features
func newRegistry(t *testing.T, ps ...provider.Provider) *provider.Registry {
	t.Helper()
	reg, err := provider.NewRegistry(ps, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// readyPlacement records a ready placement on a new local sandbox, skipping the saga
func (e *env) readyPlacement(t *testing.T) store.Placement {
	t.Helper()
	ctx := t.Context()
	ref, err := e.prov.Create(ctx, provider.CreateRequest{Size: small})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.prov.Destroy(context.Background(), ref) })
	sha, err := hex.DecodeString(randomHex(t, 32))
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := e.store.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: sha, OrderID: "order-" + randomHex(t, 8), PackageID: "small-10m",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	err = e.store.MarkPlacementReady(ctx, p.ID, ref, small.Name, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	p, err = e.store.PlacementByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (e *env) service(t *testing.T, replica string) *jobs.Service {
	t.Helper()
	return e.serviceWith(t, replica, e.reg)
}

func (e *env) serviceWith(t *testing.T, replica string, reg *provider.Registry) *jobs.Service {
	t.Helper()
	return e.serviceDeps(t, jobs.Deps{Registry: reg, ReplicaID: replica})
}

// serviceDeps builds a service over d, filling in the env's store, bus and registry
func (e *env) serviceDeps(t *testing.T, d jobs.Deps) *jobs.Service {
	t.Helper()
	d.Store, d.Bus = e.store, e.bus
	if d.Registry == nil {
		d.Registry = e.reg
	}
	d.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	s := jobs.New(d)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.Shutdown(ctx)
	})
	return s
}

func (e *env) exec(
	t *testing.T, s *jobs.Service, argv []string, key string, wait time.Duration, u jobs.Until,
) jobs.ExecResult {
	t.Helper()
	res, err := s.Exec(t.Context(), e.place, argv, "", nil, key, wait, u)
	if err != nil {
		t.Fatalf("exec %q: %v", argv, err)
	}
	return res
}

// scan runs s's scanner until the test ends
func scan(t *testing.T, s *jobs.Service) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunScanner(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func watch(
	t *testing.T, s *jobs.Service, id, cursor string, u jobs.Until,
) []streams.Entry {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var got []streams.Entry
	err := s.Watch(ctx, id, cursor, u, func(e streams.Entry) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatalf("watch %s from %q: %v", id, cursor, err)
	}
	return got
}

// output joins the data of kind's frames and fails on a gap or overlap between them
func output(t *testing.T, entries []streams.Entry, kind frame.Kind) string {
	t.Helper()
	var b strings.Builder
	next := int64(-1)
	for _, e := range entries {
		f := e.Frame
		if f.Kind != kind {
			continue
		}
		if next >= 0 && f.Offset != next {
			t.Fatalf("%s frame at offset %d follows one ending at %d", kind, f.Offset, next)
		}
		next = f.Offset + int64(len(f.Data))
		b.Write(f.Data)
	}
	return b.String()
}

// streamed returns the job's stream as stored, without a reader's filtering
func (e *env) streamed(t *testing.T, id string) []streams.Entry {
	t.Helper()
	var all []streams.Entry
	cursor := ""
	for {
		entries, err := e.bus.Read(t.Context(), id, cursor, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			return all
		}
		all = append(all, entries...)
		cursor = entries[len(entries)-1].ID
	}
}

func waitFinished(t *testing.T, st *store.Store, id string) store.Job {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		j, err := st.JobByID(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.State != store.JobRunning {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s still running", id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func lines(from, to int) string {
	var b strings.Builder
	for i := from; i <= to; i++ {
		fmt.Fprintf(&b, "line %d\n", i)
	}
	return b.String()
}

// slowLines prints line 1 to line n, one every delay seconds
func slowLines(n int, delay string) []string {
	script := fmt.Sprintf(
		`i=1; while [ $i -le %d ]; do echo "line $i"; i=$((i+1)); sleep %s; done`, n, delay)
	return []string{"sh", "-c", script}
}

func TestExecEchoReturnsTailsAndExit(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	argv := []string{"sh", "-c", "echo out; echo err >&2; exit 3"}
	res := e.exec(t, s, argv, "", 0, jobs.Until{})

	if !strings.HasPrefix(res.JobID, "j_") || len(res.JobID) != 28 {
		t.Fatalf("job id %q, want j_ and 26 characters", res.JobID)
	}
	if res.State != store.JobExited || res.ExitCode == nil || *res.ExitCode != 3 {
		t.Fatalf("state %s exit %v, want exited with 3", res.State, res.ExitCode)
	}
	if string(res.StdoutTail) != "out\n" || string(res.StderrTail) != "err\n" || res.Truncated {
		t.Fatalf("tails %q %q truncated %v", res.StdoutTail, res.StderrTail, res.Truncated)
	}
	if res.Cursor == "" {
		t.Fatal("no cursor")
	}
	if rest := watch(t, s, res.JobID, res.Cursor, jobs.Until{}); len(rest) != 0 {
		t.Fatalf("watch after the exit cursor returned %+v", rest)
	}

	j := waitFinished(t, e.store, res.JobID)
	if j.State != store.JobExited || j.ExitCode == nil || *j.ExitCode != 3 ||
		j.StdoutBytes != 4 || j.StderrBytes != 4 || j.PlacementID != e.place.ID {
		t.Fatalf("job row %+v", j)
	}
	other := e.exec(t, s, argv, "", 0, jobs.Until{})
	if other.JobID == res.JobID {
		t.Fatal("two execs without a key share a job id")
	}
}

func TestExecReportsAnOOMKillFromTheExitFrame(t *testing.T) {
	edited := &editedStream{edit: func(f frame.Frame, line []byte, emit func([]byte)) bool {
		if f.Kind != frame.KindExit {
			emit(line)
			return false
		}
		f.Code, f.OOM, f.PeakMiB = 137, true, 700
		b, err := json.Marshal(f)
		if err != nil {
			t.Error(err)
		}
		emit(append(b, '\n'))
		return true
	}}
	e := newEnvWith(t, func(p *local.Provider) provider.Provider {
		edited.Provider = p
		return edited
	})
	s := e.service(t, "r1")
	res := e.exec(t, s, []string{"sh", "-c", "echo hi"}, "", 10*time.Second, jobs.Until{})
	if !edited.edited.Load() {
		t.Fatal("the exit frame was never edited")
	}
	if res.State != store.JobExited || res.ExitCode == nil || *res.ExitCode != 137 ||
		!res.OOMKilled {
		t.Fatalf("state %s exit %v oom %v, want exited with 137 and an OOM kill", res.State,
			res.ExitCode, res.OOMKilled)
	}
	row := waitFinished(t, e.store, res.JobID)
	if !row.OOMKilled || row.PeakMemoryMiB != 700 || row.Install {
		t.Fatalf("job row oom %v peak %d install %v", row.OOMKilled, row.PeakMemoryMiB, row.Install)
	}
	plain := newEnv(t)
	quiet := plain.exec(t, plain.service(t, "r1"), []string{"true"}, "", 10*time.Second,
		jobs.Until{})
	if quiet.OOMKilled {
		t.Fatal("a job with no OOM kill reports one")
	}
	if row := waitFinished(t, plain.store, quiet.JobID); row.OOMKilled {
		t.Fatal("a job row with no OOM kill reports one")
	}
}

func TestExecMarksInstallerJobs(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	// Neither command installs anything, the first words alone make a job an installer
	pip := e.exec(t, s, []string{"sudo", "sh", "-c", "pip --version"}, "", 10*time.Second,
		jobs.Until{})
	plain := e.exec(t, s, []string{"sh", "-c", "echo pip install x"}, "", 10*time.Second,
		jobs.Until{})
	if j := waitFinished(t, e.store, pip.JobID); !j.Install {
		t.Fatalf("an installer job is not marked: %+v", j)
	}
	if j := waitFinished(t, e.store, plain.JobID); j.Install {
		t.Fatalf("a job that only names an installer is marked: %+v", j)
	}
}

func TestExecPassesCwdAndEnv(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	ref := *e.place.ProviderRef
	if err := e.prov.WriteFile(t.Context(), ref, "/srv/app/.keep", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	argv := []string{"sh", "-c", `echo "$GREETING $OTHER"; basename "$(pwd)"`}
	env := map[string]string{"GREETING": "hello", "OTHER": "a=b c"}
	res, err := s.Exec(t.Context(), e.place, argv, "/srv/app", env, "", 0, jobs.Until{})
	if err != nil {
		t.Fatal(err)
	}
	if res.State != store.JobExited || string(res.StdoutTail) != "hello a=b c\napp\n" {
		t.Fatalf("state %s stdout %q stderr %q", res.State, res.StdoutTail, res.StderrTail)
	}

	_, err = s.Exec(t.Context(), e.place, nil, "", nil, "", 0, jobs.Until{})
	if !errors.Is(err, jobs.ErrInvalid) {
		t.Fatalf("exec without argv: %v, want ErrInvalid", err)
	}
	bad := map[string]string{"A=B": "c"}
	_, err = s.Exec(t.Context(), e.place, argv, "", bad, "", 0, jobs.Until{})
	if !errors.Is(err, jobs.ErrInvalid) {
		t.Fatalf("exec with env key A=B: %v, want ErrInvalid", err)
	}
	notReady := e.place
	notReady.State = store.PlacementTerminated
	_, err = s.Exec(t.Context(), notReady, argv, "", nil, "", 0, jobs.Until{})
	if !errors.Is(err, jobs.ErrNotReady) {
		t.Fatalf("exec on a terminated placement: %v, want ErrNotReady", err)
	}
}

func TestExecSameIdempotencyKeyAttaches(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	ref := *e.place.ProviderRef
	argv := []string{"sh", "-c", "echo ran >> count.txt; sleep 1; cat count.txt"}

	first := e.exec(t, s, argv, "key-1", 200*time.Millisecond, jobs.Until{})
	if first.State != store.JobRunning {
		t.Fatalf("first exec state %s, want running", first.State)
	}
	second := e.exec(t, s, argv, "key-1", 10*time.Second, jobs.Until{})
	third := e.exec(t, s, argv, "key-1", 10*time.Second, jobs.Until{})
	for _, r := range []jobs.ExecResult{second, third} {
		if r.JobID != first.JobID || r.State != store.JobExited || string(r.StdoutTail) != "ran\n" {
			t.Fatalf("repeat exec %+v, want job %s exited with one run", r, first.JobID)
		}
	}
	data, _, err := e.prov.ReadFile(t.Context(), ref, "count.txt", 0, 1024)
	if err != nil || string(data) != "ran\n" {
		t.Fatalf("count.txt holds %q, %v, want one run", data, err)
	}

	argv = []string{"sh", "-c", "echo ran >> burst.txt; sleep 0.5"}
	var wg sync.WaitGroup
	ids := make([]string, 5)
	for i := range ids {
		wg.Go(func() {
			r, err := s.Exec(
				t.Context(), e.place, argv, "", nil, "key-2", 10*time.Second, jobs.Until{})
			if err != nil || r.State != store.JobExited {
				t.Errorf("concurrent exec: %+v, %v", r, err)
			}
			ids[i] = r.JobID
		})
	}
	wg.Wait()
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("concurrent execs with one key ran jobs %q", ids)
		}
	}
	data, _, err = e.prov.ReadFile(t.Context(), ref, "burst.txt", 0, 1024)
	if err != nil || string(data) != "ran\n" {
		t.Fatalf("burst.txt holds %q, %v, want one run", data, err)
	}

	other := e.exec(t, s, []string{"true"}, "key-1", 0, jobs.Until{})
	if other.JobID != first.JobID {
		t.Fatal("the same key ran a new job for a different command")
	}
	place2 := e.readyPlacement(t)
	argv = []string{"true"}
	elsewhere, err := s.Exec(t.Context(), place2, argv, "", nil, "key-1", 0, jobs.Until{})
	if err != nil || elsewhere.JobID == first.JobID {
		t.Fatalf("key-1 on another placement: %+v, %v, want a different job", elsewhere, err)
	}
}

func TestExecUntilPattern(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	argv := []string{"sh", "-c", `echo "Listening on 8080"; sleep 30`}
	start := time.Now()
	res := e.exec(t, s, argv, "", 0, jobs.Until{Pattern: regexp.MustCompile(`Listening on \d+`)})
	if took := time.Since(start); took > time.Second {
		t.Fatalf("exec returned after %v, want within 1s", took)
	}
	if res.State != store.JobRunning || res.ExitCode != nil {
		t.Fatalf("state %s exit %v, want running", res.State, res.ExitCode)
	}
	if string(res.StdoutTail) != "Listening on 8080\n" {
		t.Fatalf("stdout tail %q", res.StdoutTail)
	}

	argv = []string{"sh", "-c", "echo a; sleep 0.1; echo b; sleep 30"}
	start = time.Now()
	res = e.exec(t, s, argv, "", 0, jobs.Until{Idle: 500 * time.Millisecond})
	took := time.Since(start)
	if took < 500*time.Millisecond || took > 2*time.Second {
		t.Fatalf("idle exec returned after %v, want about 600ms", took)
	}
	if res.State != store.JobRunning || string(res.StdoutTail) != "a\nb\n" {
		t.Fatalf("idle exec state %s stdout %q", res.State, res.StdoutTail)
	}

	start = time.Now()
	res = e.exec(t, s, []string{"sleep", "30"}, "", 300*time.Millisecond, jobs.Until{})
	if took := time.Since(start); res.State != store.JobRunning || took > 2*time.Second {
		t.Fatalf("exec with a 300ms wait: state %s after %v", res.State, took)
	}
}

func TestExecLargeOutputTailsAndCursor(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	const n = 620000
	var want strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&want, "%d\n", i)
	}
	want.WriteString("READY\n")
	head := want.String()
	if len(head) < 4<<20 {
		t.Fatalf("test output is %d bytes, want at least 4 MiB", len(head))
	}
	argv := []string{"sh", "-c",
		fmt.Sprintf("echo warn >&2; seq 1 %d; echo READY; sleep 1; echo end", n)}

	res := e.exec(t, s, argv, "", 0, jobs.Until{Pattern: regexp.MustCompile(`READY`)})
	if res.State != store.JobRunning {
		t.Fatalf("state %s, want running until the pattern", res.State)
	}
	if !res.Truncated || !bytes.Equal(res.StdoutTail, []byte(head[len(head)-8<<10:])) {
		t.Fatalf("truncated %v, stdout tail of %d bytes ends %q",
			res.Truncated, len(res.StdoutTail), res.StdoutTail[max(len(res.StdoutTail)-20, 0):])
	}
	if string(res.StderrTail) != "warn\n" {
		t.Fatalf("stderr tail %q", res.StderrTail)
	}

	rest := watch(t, s, res.JobID, res.Cursor, jobs.Until{})
	if len(rest) == 0 || rest[len(rest)-1].Frame.Kind != frame.KindExit {
		t.Fatalf("watch from the cursor did not end with the exit: %+v", rest)
	}
	if got := output(t, rest, frame.KindStdout); got != "end\n" {
		t.Fatalf("watch from the cursor returned stdout %q, want end", got)
	}
	for _, entry := range rest {
		if entry.Frame.Kind == frame.KindStdout && entry.Frame.Offset != int64(len(head)) {
			t.Fatalf("output after the cursor starts at %d, want %d",
				entry.Frame.Offset, len(head))
		}
	}

	all := watch(t, s, res.JobID, "", jobs.Until{})
	if got := output(t, all, frame.KindStdout); got != head+"end\n" {
		t.Fatalf("full stdout is %d bytes, want %d", len(got), len(head)+4)
	}
	if got := output(t, all, frame.KindStderr); got != "warn\n" {
		t.Fatalf("full stderr %q", got)
	}
	j := waitFinished(t, e.store, res.JobID)
	if j.StdoutBytes != int64(len(head)+4) || j.StderrBytes != 5 {
		t.Fatalf("job row counts %d/%d bytes", j.StdoutBytes, j.StderrBytes)
	}
}

func TestWatchResumesFromCursor(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	res := e.exec(t, s, slowLines(6, "0.2"), "", 100*time.Millisecond, jobs.Until{})

	head := watch(t, s, res.JobID, "", jobs.Until{Pattern: regexp.MustCompile(`line 3\n`)})
	got := output(t, head, frame.KindStdout)
	if !strings.HasSuffix(got, "line 3\n") || strings.Contains(got, "line 4") {
		t.Fatalf("watch until line 3 returned %q", got)
	}
	cursor := head[len(head)-1].ID
	tail := watch(t, s, res.JobID, cursor, jobs.Until{})
	if all := got + output(t, tail, frame.KindStdout); all != lines(1, 6) {
		t.Fatalf("resumed output %q, want lines 1 to 6 once each", all)
	}
	last := tail[len(tail)-1]
	if last.Frame.Kind != frame.KindExit || last.Frame.Code != 0 {
		t.Fatalf("watch ended on %+v, want the exit", last.Frame)
	}

	waitFinished(t, e.store, res.JobID)
	start := time.Now()
	again := watch(t, s, res.JobID, cursor, jobs.Until{})
	if output(t, again, frame.KindStdout) != lines(4, 6) || time.Since(start) > time.Second {
		t.Fatalf("watch of a finished job from a cursor took %v and returned %+v",
			time.Since(start), again)
	}
	if none := watch(t, s, res.JobID, last.ID, jobs.Until{}); len(none) != 0 {
		t.Fatalf("watch from the exit entry returned %+v", none)
	}

	err := s.Watch(t.Context(), "j_missing", "", jobs.Until{}, func(streams.Entry) error {
		return nil
	})
	if !errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("watch of an unknown job: %v, want ErrNotFound", err)
	}
	err = s.Watch(t.Context(), res.JobID, "bad", jobs.Until{}, func(streams.Entry) error {
		return nil
	})
	if !errors.Is(err, streams.ErrBadCursor) {
		t.Fatalf("watch from a malformed cursor: %v, want ErrBadCursor", err)
	}
}

func TestTakeoverAfterOwnerLoss(t *testing.T) {
	e := newEnv(t)
	first := e.service(t, "replica-1")
	second := e.service(t, "replica-2")
	res := e.exec(t, first, slowLines(100, "0.1"), "", 300*time.Millisecond, jobs.Until{})
	if res.State != store.JobRunning {
		t.Fatalf("state %s, want running", res.State)
	}
	if !jobs.StopRenewing(first, res.JobID) {
		t.Fatal("the first replica does not own the job it started")
	}
	scan(t, second)

	var tookOver atomic.Bool
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			if jobs.Owns(second, res.JobID) {
				tookOver.Store(true)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	all := watch(t, second, res.JobID, "", jobs.Until{})
	if !tookOver.Load() {
		t.Fatal("the second replica never owned the job")
	}
	if got := output(t, all, frame.KindStdout); got != lines(1, 100) {
		t.Fatalf("output across the takeover is %q, want lines 1 to 100 once each", got)
	}
	if got := output(t, e.streamed(t, res.JobID), frame.KindStdout); got != lines(1, 100) {
		t.Fatalf("the stream across the takeover holds %q, want lines 1 to 100 once each", got)
	}
	j := waitFinished(t, e.store, res.JobID)
	if j.State != store.JobExited || j.ExitCode == nil || *j.ExitCode != 0 ||
		j.StdoutBytes != int64(len(lines(1, 100))) {
		t.Fatalf("job row %+v", j)
	}
	if jobs.Owns(first, res.JobID) {
		t.Fatal("the first replica still runs an owner loop for the job")
	}
}

func TestShutdownHandsJobsOver(t *testing.T) {
	e := newEnv(t)
	first := e.service(t, "replica-1")
	second := e.service(t, "replica-2")
	res := e.exec(t, first, slowLines(30, "0.1"), "", 300*time.Millisecond, jobs.Until{})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	first.Shutdown(ctx)
	scan(t, second)
	start := time.Now()
	for !jobs.Owns(second, res.JobID) {
		if time.Since(start) > 2500*time.Millisecond {
			t.Fatal("no peer took the released job over within 2.5s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	all := watch(t, second, res.JobID, "", jobs.Until{})
	if got := output(t, all, frame.KindStdout); got != lines(1, 30) {
		t.Fatalf("output across the handover is %q, want lines 1 to 30 once each", got)
	}
	if got := output(t, e.streamed(t, res.JobID), frame.KindStdout); got != lines(1, 30) {
		t.Fatalf("the stream across the handover holds %q, want lines 1 to 30 once each", got)
	}
	if j := waitFinished(t, e.store, res.JobID); j.State != store.JobExited {
		t.Fatalf("job row %+v", j)
	}
}

func TestScannerAdoptsJobsWithoutOwner(t *testing.T) {
	e := newEnv(t)
	first := e.service(t, "replica-1")
	res := e.exec(t, first, slowLines(20, "0.1"), "", 300*time.Millisecond, jobs.Until{})

	// Valkey losing every key of the job leaves only the store's running row
	ctx := t.Context()
	for _, key := range []string{"s", "owner", "epoch"} {
		del := e.vk.B().Del().Key(e.prefix + "job:" + res.JobID + ":" + key).Build()
		if err := e.vk.Do(ctx, del).Error(); err != nil {
			t.Fatal(err)
		}
	}
	zrem := e.vk.B().Zrem().Key(e.prefix + "jobs:leases").Member(res.JobID).Build()
	if err := e.vk.Do(ctx, zrem).Error(); err != nil {
		t.Fatal(err)
	}
	for start := time.Now(); jobs.Owns(first, res.JobID); time.Sleep(20 * time.Millisecond) {
		if time.Since(start) > 5*time.Second {
			t.Fatal("the first replica kept owning a job whose lease vanished")
		}
	}

	second := e.service(t, "replica-2")
	scan(t, second)
	all := watch(t, second, res.JobID, "", jobs.Until{})
	if got := output(t, all, frame.KindStdout); got != lines(1, 20) {
		t.Fatalf("adopted output is %q, want lines 1 to 20 once each", got)
	}
	if got := output(t, e.streamed(t, res.JobID), frame.KindStdout); got != lines(1, 20) {
		t.Fatalf("the adopted stream holds %q, want lines 1 to 20 once each", got)
	}
	if j := waitFinished(t, e.store, res.JobID); j.State != store.JobExited {
		t.Fatalf("job row %+v", j)
	}
}

func TestCancelStopsCommand(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	argv := []string{"sh", "-c", "echo started; sleep 30"}
	until := jobs.Until{Pattern: regexp.MustCompile(`started`)}
	res := e.exec(t, s, argv, "cancel-me", 0, until)
	if res.State != store.JobRunning {
		t.Fatalf("state %s, want running", res.State)
	}

	start := time.Now()
	state, err := s.Cancel(t.Context(), e.place, res.JobID)
	if err != nil || state != store.JobCancelled {
		t.Fatalf("cancel: %q, %v, want cancelled", state, err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("cancel took %v", took)
	}
	j, err := e.store.JobByID(t.Context(), res.JobID)
	if err != nil || j.State != store.JobCancelled || j.ExitCode == nil || *j.ExitCode != 143 {
		t.Fatalf("job row %+v, %v, want cancelled with 143", j, err)
	}
	all := watch(t, s, res.JobID, "", jobs.Until{})
	if last := all[len(all)-1].Frame; last.Kind != frame.KindExit || last.Code != 143 {
		t.Fatalf("stream ends with %+v, want exit 143", last)
	}
	replay := e.exec(t, s, argv, "cancel-me", 0, jobs.Until{})
	if replay.JobID != res.JobID || replay.State != store.JobCancelled || replay.ExitCode == nil ||
		*replay.ExitCode != 143 || string(replay.StdoutTail) != "started\n" {
		t.Fatalf("exec of the cancelled job again: %+v", replay)
	}

	if state, err := s.Cancel(t.Context(), e.place, res.JobID); err != nil ||
		state != store.JobCancelled {
		t.Fatalf("cancel of a cancelled job: %q, %v", state, err)
	}
	place2 := e.readyPlacement(t)
	if _, err := s.Cancel(t.Context(), place2, res.JobID); !errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("cancel through another placement: %v, want ErrNotFound", err)
	}
}

// cutStream ends the first runner stream after limit bytes, as a dropped connection would
type cutStream struct {
	*local.Provider
	limit   int
	execs   atomic.Int64
	attachs atomic.Int64
}

func (c *cutStream) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	n := c.execs.Add(1)
	if len(spec.Argv) > 1 && spec.Argv[1] == "attach" {
		c.attachs.Add(1)
	}
	es, err := c.Provider.Exec(ctx, ref, spec)
	if err != nil || n > 1 {
		return es, err
	}
	return &cutReader{ExecStream: es, left: c.limit}, nil
}

type cutReader struct {
	provider.ExecStream
	left int
}

func (r *cutReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		return 0, io.EOF
	}
	n, err := r.ExecStream.Read(p[:min(len(p), r.left)])
	r.left -= n
	return n, err
}

func TestOwnerReattachesAfterStreamDrops(t *testing.T) {
	var cut *cutStream
	e := newEnvWith(t, func(p *local.Provider) provider.Provider {
		cut = &cutStream{Provider: p, limit: 300}
		return cut
	})
	s := e.service(t, "r1")
	res := e.exec(t, s, slowLines(20, "0.05"), "", 10*time.Second, jobs.Until{})
	if res.State != store.JobExited || cut.attachs.Load() == 0 {
		t.Fatalf("state %s after %d attaches, want exited after reattaching",
			res.State, cut.attachs.Load())
	}
	all := watch(t, s, res.JobID, "", jobs.Until{})
	if got := output(t, all, frame.KindStdout); got != lines(1, 20) {
		t.Fatalf("output across the dropped stream is %q, want lines 1 to 20 once each", got)
	}
	if got := output(t, e.streamed(t, res.JobID), frame.KindStdout); got != lines(1, 20) {
		t.Fatalf("the stream across the drop holds %q, want lines 1 to 20 once each", got)
	}
}

// silentStream lets the first runner stream deliver limit bytes and then go quiet but stay open
type silentStream struct {
	*local.Provider
	limit int
	execs atomic.Int64
}

func (c *silentStream) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	es, err := c.Provider.Exec(ctx, ref, spec)
	if err != nil || c.execs.Add(1) > 1 {
		return es, err
	}
	return &silentReader{ExecStream: es, left: c.limit, ctx: ctx}, nil
}

type silentReader struct {
	provider.ExecStream
	left int
	ctx  context.Context
}

func (r *silentReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		<-r.ctx.Done()
		return 0, r.ctx.Err()
	}
	n, err := r.ExecStream.Read(p[:min(len(p), r.left)])
	r.left -= n
	return n, err
}

func TestOwnerReattachesASilentStream(t *testing.T) {
	jobs.SetIdleAfter(t, 2*time.Second)
	var silent *silentStream
	e := newEnvWith(t, func(p *local.Provider) provider.Provider {
		silent = &silentStream{Provider: p, limit: 30}
		return silent
	})
	s := e.service(t, "r1")
	res := e.exec(t, s, []string{"sh", "-c", "echo hello; sleep 1; echo bye"}, "",
		time.Second, jobs.Until{})
	job := waitFinished(t, e.store, res.JobID)
	if job.State != store.JobExited || silent.execs.Load() < 2 {
		t.Fatalf("job %s after %d runner streams, want exited after giving up the silent one",
			job.State, silent.execs.Load())
	}
	if got := output(t, e.streamed(t, res.JobID), frame.KindStdout); got != "hello\nbye\n" {
		t.Fatalf("output across the silent stream is %q", got)
	}
}

// lossyStream drops one line of the first runner stream as Koyeb's executor may, noting attaches
type lossyStream struct {
	*local.Provider
	// beforeExit drops the stdout line just before the exit frame instead of the third one
	beforeExit bool
	execs      atomic.Int64
	mu         sync.Mutex
	dropped    *frame.Frame
	attaches   [][]string
}

func (l *lossyStream) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	n := l.execs.Add(1)
	if len(spec.Argv) > 1 && spec.Argv[1] == "attach" {
		l.mu.Lock()
		l.attaches = append(l.attaches, slices.Clone(spec.Argv))
		l.mu.Unlock()
	}
	es, err := l.Provider.Exec(ctx, ref, spec)
	if err != nil || n > 1 {
		return es, err
	}
	return &lossyReader{ExecStream: es, l: l, lines: bufio.NewReader(es)}, nil
}

// drop records f as the lost line, once
func (l *lossyStream) drop(f frame.Frame) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.dropped != nil {
		return false
	}
	l.dropped = &f
	return true
}

func (l *lossyStream) seen() (*frame.Frame, [][]string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.dropped, slices.Clone(l.attaches)
}

type lossyReader struct {
	provider.ExecStream
	l      *lossyStream
	lines  *bufio.Reader
	stdout int
	held   []byte
	out    []byte
	err    error
}

func (r *lossyReader) Read(p []byte) (int, error) {
	for len(r.out) == 0 && r.err == nil {
		line, err := r.lines.ReadBytes('\n')
		if len(line) > 0 {
			r.feed(line)
		}
		if err != nil {
			r.out, r.held, r.err = append(r.out, r.held...), nil, err
		}
	}
	if len(r.out) == 0 {
		return 0, r.err
	}
	n := copy(p, r.out)
	r.out = r.out[n:]
	return n, nil
}

// feed passes a frame line on unless it is the one line the stream loses
func (r *lossyReader) feed(line []byte) {
	var f frame.Frame
	_ = json.Unmarshal(line, &f)
	if !r.l.beforeExit {
		if f.Kind == frame.KindStdout {
			r.stdout++
			if r.stdout == 3 && r.l.drop(f) {
				return
			}
		}
		r.out = append(r.out, line...)
		return
	}
	if r.held != nil {
		var held frame.Frame
		_ = json.Unmarshal(r.held, &held)
		lost := f.Kind == frame.KindExit && held.Kind == frame.KindStdout && r.l.drop(held)
		if !lost {
			r.out = append(r.out, r.held...)
		}
	}
	r.held = line
}

func TestOwnerReattachesWhenTheStreamSkipsOutput(t *testing.T) {
	cases := []struct {
		name       string
		beforeExit bool
	}{
		{"a line in the middle", false},
		{"the last line before the exit", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var lossy *lossyStream
			e := newEnvWith(t, func(p *local.Provider) provider.Provider {
				lossy = &lossyStream{Provider: p, beforeExit: c.beforeExit}
				return lossy
			})
			s := e.service(t, "r1")
			res := e.exec(t, s, slowLines(8, "0.05"), "", 10*time.Second, jobs.Until{})
			if res.State != store.JobExited || res.ExitCode == nil || *res.ExitCode != 0 {
				t.Fatalf("state %s exit %v, want exited with 0", res.State, res.ExitCode)
			}
			dropped, attaches := lossy.seen()
			if dropped == nil {
				t.Fatal("the runner stream never lost a line")
			}
			want := []string{
				"/tmp/bugnet/bin/bugnet-run", "attach", "--job", res.JobID,
				"--out", strconv.FormatInt(dropped.Offset, 10), "--err", "0",
			}
			if len(attaches) == 0 || !slices.Equal(attaches[0], want) {
				t.Fatalf("attaches %q, want the first to resume at the lost line: %q",
					attaches, want)
			}
			all := watch(t, s, res.JobID, "", jobs.Until{})
			if got := output(t, all, frame.KindStdout); got != lines(1, 8) {
				t.Fatalf("output across the lost line is %q, want lines 1 to 8", got)
			}
			if got := output(t, e.streamed(t, res.JobID), frame.KindStdout); got != lines(1, 8) {
				t.Fatalf("the stream across the lost line holds %q, want lines 1 to 8", got)
			}
			j := waitFinished(t, e.store, res.JobID)
			if j.StdoutBytes != int64(len(lines(1, 8))) {
				t.Fatalf("job row %+v, want all %d stdout bytes", j, len(lines(1, 8)))
			}
		})
	}
}

// editedStream rewrites the frame lines of the first runner stream as Koyeb's log stream may
type editedStream struct {
	*local.Provider
	edit     func(f frame.Frame, line []byte, emit func([]byte)) bool
	execs    atomic.Int64
	edited   atomic.Bool
	mu       sync.Mutex
	attaches [][]string
}

func (s *editedStream) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	n := s.execs.Add(1)
	if len(spec.Argv) > 1 && spec.Argv[1] == "attach" {
		s.mu.Lock()
		s.attaches = append(s.attaches, slices.Clone(spec.Argv))
		s.mu.Unlock()
	}
	// Frequent heartbeats put one between every two lines of slowLines
	spec.Env = map[string]string{"BUGNET_RUN_HEARTBEAT_MS": "15"}
	es, err := s.Provider.Exec(ctx, ref, spec)
	if err != nil || n > 1 {
		return es, err
	}
	return &editedReader{ExecStream: es, s: s, lines: bufio.NewReader(es)}, nil
}

func (s *editedStream) attached() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.attaches)
}

type editedReader struct {
	provider.ExecStream
	s     *editedStream
	lines *bufio.Reader
	out   []byte
	err   error
}

func (r *editedReader) Read(p []byte) (int, error) {
	for len(r.out) == 0 && r.err == nil {
		line, err := r.lines.ReadBytes('\n')
		if len(line) > 0 {
			var f frame.Frame
			_ = json.Unmarshal(line, &f)
			emit := func(l []byte) { r.out = append(r.out, l...) }
			if r.s.edit(f, line, emit) {
				r.s.edited.Store(true)
			}
		}
		r.err = err
	}
	if len(r.out) == 0 {
		return 0, r.err
	}
	n := copy(p, r.out)
	r.out = r.out[n:]
	return n, nil
}

func frameLine(f frame.Frame) []byte {
	raw, _ := json.Marshal(f)
	return append(raw, '\n')
}

// repeatLine sends the second stdout line again after the fourth, as a history replay would
func repeatLine() func(frame.Frame, []byte, func([]byte)) bool {
	stdout := 0
	var second []byte
	return func(f frame.Frame, line []byte, emit func([]byte)) bool {
		emit(line)
		if f.Kind != frame.KindStdout {
			return false
		}
		switch stdout++; stdout {
		case 2:
			second = line
		case 4:
			emit(second)
			return true
		}
		return false
	}
}

// swapLines sends the fourth stdout line before the third and the heartbeats between them
func swapLines() func(frame.Frame, []byte, func([]byte)) bool {
	stdout := 0
	var held []byte
	return func(f frame.Frame, line []byte, emit func([]byte)) bool {
		if f.Kind == frame.KindStdout {
			stdout++
		}
		switch {
		case stdout == 3:
			held = append(held, line...)
			return false
		case stdout == 4 && held != nil:
			emit(line)
			emit(held)
			held = nil
			return true
		}
		emit(line)
		return false
	}
}

// lateHeartbeats holds the heartbeats after the second stdout line until the third has passed
func lateHeartbeats() func(frame.Frame, []byte, func([]byte)) bool {
	stdout := 0
	var held []byte
	return func(f frame.Frame, line []byte, emit func([]byte)) bool {
		switch {
		case f.Kind == frame.KindHeartbeat && stdout == 2:
			held = append(held, line...)
			return false
		case f.Kind == frame.KindStdout:
			stdout++
		}
		emit(line)
		if stdout == 3 && held != nil {
			emit(held)
			held = nil
			return true
		}
		return false
	}
}

// overlapLine sends the third stdout line with the second one's data in front of it
func overlapLine() func(frame.Frame, []byte, func([]byte)) bool {
	stdout := 0
	var second frame.Frame
	return func(f frame.Frame, line []byte, emit func([]byte)) bool {
		if f.Kind == frame.KindStdout {
			switch stdout++; stdout {
			case 2:
				second = f
			case 3:
				emit(frameLine(frame.Frame{
					Kind: frame.KindStdout, Offset: second.Offset,
					Data: append(slices.Clone(second.Data), f.Data...),
				}))
				return true
			}
		}
		emit(line)
		return false
	}
}

// levelBeats fails when a stored heartbeat or exit frame lags the output stored before it
func levelBeats(t *testing.T, entries []streams.Entry) {
	t.Helper()
	var ends [2]int64
	for _, e := range entries {
		f := e.Frame
		switch f.Kind {
		case frame.KindStdout:
			ends[0] = max(ends[0], f.Offset+int64(len(f.Data)))
		case frame.KindStderr:
			ends[1] = max(ends[1], f.Offset+int64(len(f.Data)))
		case frame.KindHeartbeat, frame.KindExit:
			if f.Out != ends[0] || f.Err != ends[1] {
				t.Fatalf("stored %s frame counts %d/%d after output up to %d/%d", f.Kind, f.Out,
					f.Err, ends[0], ends[1])
			}
		}
	}
}

func TestOwnerStoresRepeatedAndReorderedOutputOnce(t *testing.T) {
	cases := []struct {
		name   string
		edit   func() func(frame.Frame, []byte, func([]byte)) bool
		attach bool
	}{
		{"a line repeated after later ones", repeatLine, false},
		{"two lines swapped", swapLines, true},
		{"heartbeats behind the line after them", lateHeartbeats, false},
		{"a line overlapping stored output", overlapLine, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			edited := &editedStream{edit: c.edit()}
			e := newEnvWith(t, func(p *local.Provider) provider.Provider {
				edited.Provider = p
				return edited
			})
			s := e.service(t, "r1")
			res := e.exec(t, s, slowLines(8, "0.05"), "", 10*time.Second, jobs.Until{})
			if res.State != store.JobExited || res.ExitCode == nil || *res.ExitCode != 0 {
				t.Fatalf("state %s exit %v, want exited with 0", res.State, res.ExitCode)
			}
			if !edited.edited.Load() {
				t.Fatal("the runner stream was never edited")
			}
			if attaches := edited.attached(); (len(attaches) > 0) != c.attach {
				t.Fatalf("attaches %q, want some only after a line came early: %v", attaches,
					c.attach)
			}
			stored := e.streamed(t, res.JobID)
			if got := output(t, stored, frame.KindStdout); got != lines(1, 8) {
				t.Fatalf("the stream holds stdout %q, want lines 1 to 8 once each", got)
			}
			levelBeats(t, stored)
			all := watch(t, s, res.JobID, "", jobs.Until{})
			if got := output(t, all, frame.KindStdout); got != lines(1, 8) {
				t.Fatalf("watch delivered %q, want lines 1 to 8 once each", got)
			}
			j := waitFinished(t, e.store, res.JobID)
			if j.StdoutBytes != int64(len(lines(1, 8))) {
				t.Fatalf("job row %+v, want all %d stdout bytes", j, len(lines(1, 8)))
			}
		})
	}
}

func TestLostSupervisorEndsJobLost(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	argv := []string{"sh", "-c", "echo up; sleep 30"}
	res := e.exec(t, s, argv, "doomed", 0, jobs.Until{Pattern: regexp.MustCompile(`up`)})
	dir := filepath.Join(e.root, e.place.ProviderRef.ID, "fs", "tmp", "bugnet", "jobs", res.JobID)
	// The supervisor dies first so nothing records an exit code for the command
	for _, name := range []string{"spid", "pid"} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		if name == "pid" {
			pid = -pid
		}
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
	}

	j := waitFinished(t, e.store, res.JobID)
	if j.State != store.JobLost || j.ExitCode != nil || j.StdoutBytes != 3 {
		t.Fatalf("job row %+v, want lost with no exit code", j)
	}
	all := watch(t, s, res.JobID, "", jobs.Until{})
	last := all[len(all)-1].Frame
	if last.Kind != frame.KindError || last.Reason != frame.ReasonJobLost {
		t.Fatalf("stream ends with %+v, want a job_lost error", last)
	}
	replay := e.exec(t, s, argv, "doomed", 0, jobs.Until{})
	if replay.JobID != res.JobID || replay.State != store.JobLost || replay.ExitCode != nil ||
		string(replay.StdoutTail) != "up\n" {
		t.Fatalf("exec of the lost job again: %+v", replay)
	}
}

func TestReadersDropRepeatedOutput(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	ctx := t.Context()
	lease, ok, err := e.bus.AcquireLease(ctx, "j_repeats", "writer")
	if err != nil || !ok {
		t.Fatalf("acquire: %v, %v", ok, err)
	}
	stdout := func(off int64, data string) frame.Frame {
		return frame.Frame{Kind: frame.KindStdout, Offset: off, Data: []byte(data)}
	}
	for _, f := range []frame.Frame{
		stdout(0, "abc"), stdout(2, "cde"), stdout(0, "ab"),
		{Kind: frame.KindStderr, Offset: 0, Data: []byte("x")},
		stdout(5, "f"), {Kind: frame.KindExit, Out: 6, Err: 1},
	} {
		if _, err := lease.Append(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	all := watch(t, s, "j_repeats", "", jobs.Until{})
	if got := output(t, all, frame.KindStdout); got != "abcdef" {
		t.Fatalf("watch delivered stdout %q, want abcdef", got)
	}
	if len(all) != 5 {
		t.Fatalf("watch delivered %d entries, want the repeat dropped", len(all))
	}
}

// brokenCancel fails every bugnet-run cancel, by an Exec error or by naming a missing job
type brokenCancel struct {
	*local.Provider
	execFails atomic.Bool
}

func (b *brokenCancel) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	if len(spec.Argv) > 1 && spec.Argv[1] == "cancel" {
		if b.execFails.Load() {
			return nil, errors.New("connection reset")
		}
		spec.Argv = []string{spec.Argv[0], "cancel", "--job", "j_missing"}
	}
	return b.Provider.Exec(ctx, ref, spec)
}

func TestCancelThatFailsLeavesNoMark(t *testing.T) {
	var broken *brokenCancel
	e := newEnvWith(t, func(p *local.Provider) provider.Provider {
		broken = &brokenCancel{Provider: p}
		return broken
	})
	s := e.service(t, "r1")
	argv := []string{"sh", "-c", "echo started; sleep 2; exit 7"}
	res := e.exec(t, s, argv, "", 0, jobs.Until{Pattern: regexp.MustCompile(`started`)})
	unmarked := func(step string) {
		t.Helper()
		if asked, err := e.bus.CancelRequested(t.Context(), res.JobID); err != nil || asked {
			t.Fatalf("%s: cancel mark %v, %v, want none", step, asked, err)
		}
	}

	broken.execFails.Store(true)
	if _, err := s.Cancel(t.Context(), e.place, res.JobID); err == nil {
		t.Fatal("a cancel whose exec failed reported no error")
	}
	unmarked("after a failed exec")
	broken.execFails.Store(false)
	start := time.Now()
	if _, err := s.Cancel(t.Context(), e.place, res.JobID); err == nil {
		t.Fatal("a cancel exiting non-zero reported no error")
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a cancel exiting non-zero returned after %v", took)
	}
	unmarked("after a non-zero exit")

	j := waitFinished(t, e.store, res.JobID)
	if j.State != store.JobExited || j.ExitCode == nil || *j.ExitCode != 7 {
		t.Fatalf("job row %+v, want exited with 7", j)
	}
	all := watch(t, s, res.JobID, "", jobs.Until{})
	if last := all[len(all)-1].Frame; last.Kind != frame.KindExit || last.Reason != "" {
		t.Fatalf("stream ends with %+v, want a plain exit", last)
	}
}

func TestExecDuringShutdownRefuses(t *testing.T) {
	e := newEnv(t)
	first := e.service(t, "replica-1")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	first.Shutdown(ctx)

	argv := []string{"sh", "-c", "echo ran >> late.txt; cat late.txt"}
	_, err := first.Exec(ctx, e.place, argv, "", nil, "late", time.Second, jobs.Until{})
	if !errors.Is(err, jobs.ErrShuttingDown) {
		t.Fatalf("exec during shutdown: %v, want ErrShuttingDown", err)
	}
	id := jobs.JobIDFor(e.place.ID, "late")
	if _, err := e.store.JobByID(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("exec during shutdown recorded job %s: %v", id, err)
	}
	expired, err := e.bus.ExpiredLeases(ctx, time.Now().Add(time.Minute))
	if err != nil || len(expired) != 0 {
		t.Fatalf("exec during shutdown left leases %q, %v", expired, err)
	}

	second := e.service(t, "replica-2")
	res := e.exec(t, second, argv, "late", 0, jobs.Until{})
	if res.JobID != id || res.State != store.JobExited || string(res.StdoutTail) != "ran\n" {
		t.Fatalf("the same key on a peer: %+v, want job %s run once", res, id)
	}
}

func TestTakeoverWithoutTheProviderEndsJobLost(t *testing.T) {
	e := newEnv(t)
	first := e.service(t, "replica-1")
	argv := []string{"sh", "-c", "echo up; sleep 30"}
	res := e.exec(t, first, argv, "", 0, jobs.Until{Pattern: regexp.MustCompile(`up`)})
	if !jobs.StopRenewing(first, res.JobID) {
		t.Fatal("the first replica does not own the job it started")
	}
	second := e.serviceWith(t, "replica-2", newRegistry(t))
	scan(t, second)

	j := waitFinished(t, e.store, res.JobID)
	if j.State != store.JobLost || j.ExitCode != nil || j.StdoutBytes != 3 {
		t.Fatalf("job row %+v, want lost with no exit code", j)
	}
	all := watch(t, second, res.JobID, "", jobs.Until{})
	last := all[len(all)-1].Frame
	if last.Kind != frame.KindError || last.Reason != frame.ReasonJobLost {
		t.Fatalf("stream ends with %+v, want a job_lost error", last)
	}
	expired, err := e.bus.ExpiredLeases(t.Context(), time.Now().Add(time.Minute))
	if err != nil || len(expired) != 0 {
		t.Fatalf("leases left after the job was lost: %q, %v", expired, err)
	}
}

func TestExecWhenTheLeaseHolderRecordsNoJob(t *testing.T) {
	e := newEnv(t)
	s := e.service(t, "r1")
	ctx := t.Context()
	id := jobs.JobIDFor(e.place.ID, "orphan")
	// A holder that never records the job, as one whose CreateJob failed or that crashed
	ghost, ok, err := e.bus.AcquireLease(ctx, id, "ghost")
	if err != nil || !ok {
		t.Fatalf("acquire: %v, %v", ok, err)
	}
	argv := []string{"sh", "-c", "echo ran >> orphan.txt; cat orphan.txt"}

	start := time.Now()
	_, err = s.Exec(ctx, e.place, argv, "", nil, "orphan", 500*time.Millisecond, jobs.Until{})
	if !errors.Is(err, jobs.ErrNotRecorded) || errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("exec behind a holder that records nothing: %v, want ErrNotRecorded", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("exec behind a holder that records nothing returned after %v", took)
	}
	if _, err := e.store.JobByID(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("job %s recorded: %v", id, err)
	}

	gone := make(chan error, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		gone <- ghost.End(context.Background())
	}()
	start = time.Now()
	res := e.exec(t, s, argv, "orphan", 10*time.Second, jobs.Until{})
	if err := <-gone; err != nil {
		t.Fatal(err)
	}
	if res.JobID != id || res.State != store.JobExited || string(res.StdoutTail) != "ran\n" {
		t.Fatalf("exec after the holder gave up: %+v, want job %s run once", res, id)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("exec after the holder gave up took %v", took)
	}
}

/* Output budgets */

// argvLog notes the runner subcommands a provider is asked to run, and can refuse attach --tail
type argvLog struct {
	*local.Provider
	oldRunner bool // answers attach --tail as a bugnet-run without the flag would
	mu        sync.Mutex
	argvs     [][]string
}

func (a *argvLog) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	a.mu.Lock()
	a.argvs = append(a.argvs, slices.Clone(spec.Argv))
	a.mu.Unlock()
	if i := slices.Index(spec.Argv, "--tail"); a.oldRunner && i >= 0 {
		spec.Argv = slices.Clone(spec.Argv)
		spec.Argv[i] = "--no-such-flag"
	}
	return a.Provider.Exec(ctx, ref, spec)
}

func (a *argvLog) tails() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, argv := range a.argvs {
		if slices.Contains(argv, "--tail") {
			n++
		}
	}
	return n
}

// covered checks that a stream's output and skips cover every offset it names, returning cost
func covered(t *testing.T, entries []streams.Entry) (int64, int) {
	t.Helper()
	var cost int64
	skips := 0
	next := [2]int64{}
	for _, e := range entries {
		f := e.Frame
		switch f.Kind {
		case frame.KindSkip:
			skips++
			if f.Out < next[0] || f.Err < next[1] {
				t.Fatalf("skip frame %+v goes back from %v", f, next)
			}
			next = [2]int64{f.Out, f.Err}
		case frame.KindStdout, frame.KindStderr:
			i := 0
			if f.Kind == frame.KindStderr {
				i = 1
			}
			if f.Offset != next[i] {
				t.Fatalf("%s frame at %d, want %d after the output and skips before it", f.Kind,
					f.Offset, next[i])
			}
			next[i] = f.Offset + int64(len(f.Data))
			cost += streams.Cost(f)
		case frame.KindHeartbeat, frame.KindExit:
			if f.Out != next[0] || f.Err != next[1] {
				t.Fatalf("%s frame names %d/%d, want the %v the stream covers", f.Kind, f.Out,
					f.Err, next)
			}
		}
	}
	return cost, skips
}

// tailCost is what one stored snapshot of a flooding job's newest output may cost
var tailCost = 2 * streams.Cost(frame.Frame{Data: make([]byte, 64<<10)})

// finalTailCost is what the 8 KiB final tail of one output costs the placement's reserve
var finalTailCost = streams.Cost(frame.Frame{Data: make([]byte, 8<<10)})

// floodScript prints start, then bursts of a and b on stdout and stderr, then end on stdout
func floodScript(bursts, outBurst, errBurst int) ([]string, int64, int64) {
	errPart := ""
	if errBurst > 0 {
		errPart = fmt.Sprintf(`head -c %d /dev/zero | tr '\0' b >&2; `, errBurst)
	}
	script := fmt.Sprintf(`echo start; i=0; while [ $i -lt %d ]; do `+
		`head -c %d /dev/zero | tr '\0' a; %ssleep 0.1; i=$((i+1)); done; echo; echo end`,
		bursts, outBurst, errPart)
	return []string{"sh", "-c", script}, int64(6 + bursts*outBurst + 5), int64(bursts * errBurst)
}

var started = jobs.Until{Pattern: regexp.MustCompile("start")}

// storedDuring reports whether a stream holds output between its first and last skip frames
func storedDuring(entries []streams.Entry) bool {
	skipped, between := false, false
	for _, e := range entries {
		switch e.Frame.Kind {
		case frame.KindSkip:
			if skipped && between {
				return true
			}
			skipped = true
		case frame.KindStdout, frame.KindStderr:
			between = skipped
		}
	}
	return false
}

// memory samples the Valkey memory of a job's stream until the returned stop is called
func (e *env) memory(t *testing.T, id string) func() int64 {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	var peak atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		key := e.prefix + "job:" + id + ":s"
		for ctx.Err() == nil {
			cmd := e.vk.B().MemoryUsage().Key(key).Samples(0).Build()
			if n, err := e.vk.Do(ctx, cmd).AsInt64(); err == nil && n > peak.Load() {
				peak.Store(n)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return func() int64 {
		cancel()
		<-done
		return peak.Load()
	}
}

// checkFloodEnd checks the job row and the sandbox file of a flood that wrote out and errBytes
func (e *env) checkFloodEnd(t *testing.T, id string, out, errBytes int64) {
	t.Helper()
	job := waitFinished(t, e.store, id)
	if job.State != store.JobExited || job.StdoutBytes != out || job.StderrBytes != errBytes ||
		!job.OutputTruncated {
		t.Fatalf("flooding job row %+v, want exited with %d/%d bytes and truncated", job, out,
			errBytes)
	}
	end, eof, err := e.prov.ReadFile(t.Context(), *e.place.ProviderRef,
		"/tmp/bugnet/jobs/"+id+"/out", out-4, 64)
	if err != nil || string(end) != "end\n" || !eof {
		t.Fatalf("the sandbox's output file ends with %q eof %v, %v, want all of it", end, eof,
			err)
	}
}

func TestFloodingJobKeepsToItsBudget(t *testing.T) {
	var runs *argvLog
	e := newEnvWith(t, func(p *local.Provider) provider.Provider {
		runs = &argvLog{Provider: p}
		return runs
	})
	const budget = 256 << 10
	s := e.serviceDeps(t, jobs.Deps{ReplicaID: "r1", JobBudget: budget})
	script := "head -c 4000000 /dev/zero | tr '\\0' a; echo; echo done"
	res := e.exec(t, s, []string{"sh", "-c", script}, "", 30*time.Second, jobs.Until{})
	if res.State != store.JobExited || !strings.HasSuffix(string(res.StdoutTail), "a\ndone\n") ||
		!res.Truncated {
		t.Fatalf("flooding job gave state %s tail %q truncated %v", res.State,
			res.StdoutTail[max(len(res.StdoutTail)-20, 0):], res.Truncated)
	}
	all := e.streamed(t, res.JobID)
	cost, skips := covered(t, all)
	if most := int64(budget+budget/4) + 2*tailCost; cost > most || skips == 0 {
		t.Fatalf("flooding job's stream costs %d with %d skips, want at most %d after a skip",
			cost, skips, most)
	}
	last := all[len(all)-1].Frame
	if last.Kind != frame.KindExit || last.Out != 4000006 {
		t.Fatalf("last entry %+v, want the exit counting every byte", last)
	}
	if runs.tails() == 0 {
		t.Fatal("the owner never followed the flooding job by its tail")
	}
	job := waitFinished(t, e.store, res.JobID)
	if job.StdoutBytes != 4000006 || !job.OutputTruncated {
		t.Fatalf("job recorded %d stdout bytes truncated %v, want all 4000006 and truncated",
			job.StdoutBytes, job.OutputTruncated)
	}
	var sawSkip bool
	for _, en := range watch(t, s, res.JobID, "", jobs.Until{}) {
		sawSkip = sawSkip || en.Frame.Kind == frame.KindSkip
	}
	if !sawSkip {
		t.Fatal("a reader of the flooding job saw no skip frame")
	}
}

func TestFloodingJobOnAnOldRunnerKeepsToItsBudget(t *testing.T) {
	e := newEnvWith(t, func(p *local.Provider) provider.Provider {
		return &argvLog{Provider: p, oldRunner: true}
	})
	const budget = 128 << 10
	s := e.serviceDeps(t, jobs.Deps{ReplicaID: "r1", JobBudget: budget})
	script := "head -c 2000000 /dev/zero | tr '\\0' b; echo; echo done"
	res := e.exec(t, s, []string{"sh", "-c", script}, "", 30*time.Second, jobs.Until{})
	if res.State != store.JobExited || !strings.HasSuffix(string(res.StdoutTail), "done\n") ||
		!res.Truncated {
		t.Fatalf("flooding job on an old runner gave state %s truncated %v", res.State,
			res.Truncated)
	}
	cost, skips := covered(t, e.streamed(t, res.JobID))
	if most := int64(budget+budget/4) + 2*tailCost; cost > most || skips == 0 {
		t.Fatalf("stream costs %d with %d skips, want at most %d after a skip", cost, skips, most)
	}
}

func TestFloodKeepsValkeyBoundedAndReadersEnd(t *testing.T) {
	jobs.SetFloodEvery(t, 200*time.Millisecond)
	e := newEnv(t)
	const budget = 512 << 10
	s := e.serviceDeps(t, jobs.Deps{ReplicaID: "r1", JobBudget: budget})
	argv, out, errBytes := floodScript(30, 2_000_000, 20_000)
	id := jobs.JobIDFor(e.place.ID, "flood")
	peak := e.memory(t, id)
	if res := e.exec(t, s, argv, "flood", 10*time.Second, started); res.State != store.JobRunning {
		t.Fatalf("flooding job is %s before its output, want running", res.State)
	}
	// A reader follows the flood from its start while it runs and must end at its exit
	live := watch(t, s, id, "", jobs.Until{})
	most := peak()

	last := live[len(live)-1].Frame
	if last.Kind != frame.KindExit || last.Out != out || last.Err != errBytes {
		t.Fatalf("the live reader ended with %+v, want the exit at %d/%d", last, out, errBytes)
	}
	covered(t, live)
	if !storedDuring(live) {
		t.Fatal("the live reader saw no output between skips, want the tail stored mid-flood")
	}
	cost, _ := covered(t, e.streamed(t, id))
	bound := int64(budget+budget/4) + 2*tailCost
	if cost > bound {
		t.Fatalf("the flood's stream costs %d, want at most %d", cost, bound)
	}
	// Valkey keeps base64 in JSON in listpacks, so its memory may pass the estimate somewhat
	if most == 0 || most > bound+bound/2 {
		t.Fatalf("the flood's stream took up to %d bytes of Valkey memory, want at most %d",
			most, bound+bound/2)
	}
	counter := e.vk.B().Get().Key(e.prefix + "placement:" + e.place.ID.String() + ":out").Build()
	if spent, err := e.vk.Do(t.Context(), counter).AsInt64(); err != nil || spent > 2*bound {
		t.Fatalf("the placement's budget counter holds %d, %v, want at most %d", spent, err,
			2*bound)
	}
	e.checkFloodEnd(t, id, out, errBytes)

	res := e.exec(t, s, argv, "flood", time.Second, jobs.Until{})
	if res.State != store.JobExited || !res.Truncated ||
		!strings.HasSuffix(string(res.StdoutTail), "a\nend\n") ||
		!strings.HasSuffix(string(res.StderrTail), "bbb") {
		t.Fatalf("exec of the finished flood gave state %s truncated %v", res.State,
			res.Truncated)
	}
}

func TestFloodTakeoverKeepsToItsBudget(t *testing.T) {
	jobs.SetFloodEvery(t, 200*time.Millisecond)
	e := newEnv(t)
	const budget = 256 << 10
	first := e.serviceDeps(t, jobs.Deps{ReplicaID: "r1", JobBudget: budget})
	second := e.serviceDeps(t, jobs.Deps{ReplicaID: "r2", JobBudget: budget})
	argv, out, errBytes := floodScript(80, 500_000, 1000)
	res := e.exec(t, first, argv, "", 10*time.Second, started)
	for start := time.Now(); ; time.Sleep(20 * time.Millisecond) {
		if st, err := e.bus.LastOffsets(t.Context(), res.JobID); err != nil || st.Skipped {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatal("the first owner never stored a skip frame")
		}
	}
	if !jobs.StopRenewing(first, res.JobID) {
		t.Fatal("the first replica does not own the flooding job")
	}
	scan(t, second)
	var tookOver atomic.Bool
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		for ; ctx.Err() == nil; time.Sleep(20 * time.Millisecond) {
			if jobs.Owns(second, res.JobID) {
				tookOver.Store(true)
			}
		}
	}()
	all := watch(t, second, res.JobID, "", jobs.Until{})
	if !tookOver.Load() {
		t.Fatal("the second replica never owned the flooding job")
	}
	// A stale owner storing nothing in a flood notices its lost lease at its next append
	for start := time.Now(); jobs.Owns(first, res.JobID); time.Sleep(20 * time.Millisecond) {
		if time.Since(start) > 10*time.Second {
			t.Fatal("the first replica kept owning the job after it ended")
		}
	}
	last := all[len(all)-1].Frame
	if last.Kind != frame.KindExit || last.Out != out || last.Err != errBytes {
		t.Fatalf("the reader ended with %+v, want the exit at %d/%d", last, out, errBytes)
	}
	// Each owner may store one snapshot past the allowance before the final one
	cost, _ := covered(t, e.streamed(t, res.JobID))
	if bound := int64(budget+budget/4) + 3*tailCost; cost > bound {
		t.Fatalf("the stream across the takeover costs %d, want at most %d", cost, bound)
	}
	e.checkFloodEnd(t, res.JobID, out, errBytes)
}

func TestAdoptedFloodKeepsToItsBudget(t *testing.T) {
	e := newEnv(t)
	const budget = 256 << 10
	first := e.serviceDeps(t, jobs.Deps{ReplicaID: "r1", JobBudget: budget})
	argv, out, errBytes := floodScript(40, 1_000_000, 0)
	res := e.exec(t, first, argv, "", 10*time.Second, started)
	time.Sleep(500 * time.Millisecond)

	// Valkey losing every key leaves only the running row, and the owner key goes first
	ctx := t.Context()
	keys := []string{"job:" + res.JobID + ":owner", "job:" + res.JobID + ":s",
		"job:" + res.JobID + ":epoch", "placement:" + e.place.ID.String() + ":out"}
	for _, key := range keys {
		if err := e.vk.Do(ctx, e.vk.B().Del().Key(e.prefix+key).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	zrem := e.vk.B().Zrem().Key(e.prefix + "jobs:leases").Member(res.JobID).Build()
	if err := e.vk.Do(ctx, zrem).Error(); err != nil {
		t.Fatal(err)
	}
	for start := time.Now(); jobs.Owns(first, res.JobID); time.Sleep(20 * time.Millisecond) {
		if time.Since(start) > 5*time.Second {
			t.Fatal("the first replica kept owning a job whose lease vanished")
		}
	}

	second := e.serviceDeps(t, jobs.Deps{ReplicaID: "r2", JobBudget: budget})
	scan(t, second)
	all := watch(t, second, res.JobID, "", jobs.Until{})
	last := all[len(all)-1].Frame
	if last.Kind != frame.KindExit || last.Out != out || last.Err != errBytes {
		t.Fatalf("the reader ended with %+v, want the exit at %d/%d", last, out, errBytes)
	}
	// The adopting owner starts from the file's start, so the stream again stops at the budget
	cost, skips := covered(t, e.streamed(t, res.JobID))
	if bound := int64(budget+budget/4) + 2*tailCost; cost > bound || skips == 0 {
		t.Fatalf("the adopted stream costs %d with %d skips, want at most %d after a skip", cost,
			skips, bound)
	}
	e.checkFloodEnd(t, res.JobID, out, errBytes)
}

func TestPlacementBudgetBoundsAllItsJobs(t *testing.T) {
	e := newEnv(t)
	const budget = 200 << 10
	s := e.serviceDeps(t, jobs.Deps{
		ReplicaID: "r1", JobBudget: 1 << 30, PlacementBudget: budget,
	})
	var total int64
	for i := range 3 {
		script := fmt.Sprintf("head -c 150000 /dev/zero | tr '\\0' c; echo; echo done %d", i)
		res := e.exec(t, s, []string{"sh", "-c", script}, "", 30*time.Second, jobs.Until{})
		if res.State != store.JobExited {
			t.Fatalf("job %d is %s, want exited", i, res.State)
		}
		cost, _ := covered(t, e.streamed(t, res.JobID))
		total += cost
		job := waitFinished(t, e.store, res.JobID)
		if i == 2 && (!res.Truncated || !job.OutputTruncated || job.StdoutBytes != 150008 ||
			!strings.HasSuffix(string(res.StdoutTail), "c\ndone 2\n")) {
			t.Fatalf("a job past the placement's budget gave truncated %v, tail %q and row %+v",
				res.Truncated, res.StdoutTail[max(len(res.StdoutTail)-20, 0):], job)
		}
	}
	// Past the budget only the final tails of the two later jobs are kept, from the reserve
	if most := int64(budget) + 2*finalTailCost; total > most {
		t.Fatalf("three jobs stored output costing %d, over the placement's %d and two tails",
			total, most)
	}
}

func TestFinalTailOutlivesThePlacementBudget(t *testing.T) {
	e := newEnv(t)
	const budget = 64 << 10
	reserve := 2*finalTailCost + 1000
	s := e.serviceDeps(t, jobs.Deps{
		ReplicaID: "r1", PlacementBudget: budget, TailReserve: reserve,
	})
	ctx := t.Context()
	counter := func(name string) int64 {
		t.Helper()
		key := e.prefix + "placement:" + e.place.ID.String() + ":" + name
		n, err := e.vk.Do(ctx, e.vk.B().Get().Key(key).Build()).AsInt64()
		if err != nil && !valkey.IsValkeyNil(err) {
			t.Fatal(err)
		}
		return n
	}
	var total int64
	run := func(script string) jobs.ExecResult {
		t.Helper()
		res := e.exec(t, s, []string{"sh", "-c", script}, "", 30*time.Second, jobs.Until{})
		if res.State != store.JobExited {
			t.Fatalf("%q is %s, want exited", script, res.State)
		}
		cost, _ := covered(t, e.streamed(t, res.JobID))
		total += cost
		return res
	}

	fill := run("head -c 200000 /dev/zero | tr '\\0' f")
	if !fill.Truncated || len(fill.StdoutTail) != 8<<10 ||
		strings.Trim(string(fill.StdoutTail), "f") != "" {
		t.Fatalf("the job filling the budget gave truncated %v and a %d byte tail, want its "+
			"last 8 KiB", fill.Truncated, len(fill.StdoutTail))
	}
	if spent := counter("out"); spent < budget-finalTailCost {
		t.Fatalf("the budget counter holds %d after the fill, want the %d budget spent", spent,
			budget)
	}

	done := run("echo done")
	if string(done.StdoutTail) != "done\n" || done.Truncated {
		t.Fatalf("echo done past the budget gave tail %q truncated %v, want done whole",
			done.StdoutTail, done.Truncated)
	}
	if job := waitFinished(t, e.store, done.JobID); job.OutputTruncated || job.StdoutBytes != 5 {
		t.Fatalf("echo done recorded %+v, want 5 bytes and nothing left out", job)
	}
	if got := output(t, watch(t, s, done.JobID, "", jobs.Until{}), frame.KindStdout); got !=
		"done\n" {
		t.Fatalf("watching echo done gave %q, want done", got)
	}

	// The reserve holds two long tails and a little more, so a few long jobs spend it
	tailed := 0
	var last jobs.ExecResult
	for i := range 5 {
		last = run(fmt.Sprintf("head -c 20000 /dev/zero | tr '\\0' g; echo; echo end %d", i))
		if !last.Truncated {
			t.Fatalf("job %d of 20007 bytes past the budget is not truncated", i)
		}
		switch {
		case len(last.StdoutTail) == 0:
		case tailed < i:
			t.Fatalf("job %d kept a tail after job %d found the reserve spent", i, tailed)
		case len(last.StdoutTail) != 8<<10 ||
			!strings.HasSuffix(string(last.StdoutTail), fmt.Sprintf("g\nend %d\n", i)):
			t.Fatalf("job %d past the budget gave a %d byte tail, want its last 8 KiB", i,
				len(last.StdoutTail))
		default:
			tailed++
		}
	}
	if tailed == 0 || len(last.StdoutTail) != 0 {
		t.Fatalf("%d long jobs kept their tail and the last one %d bytes, want the reserve "+
			"to keep some and then refuse", tailed, len(last.StdoutTail))
	}
	if job := waitFinished(t, e.store, last.JobID); !job.OutputTruncated ||
		job.StdoutBytes != 20007 {
		t.Fatalf("the job past the reserve recorded %+v, want all 20007 bytes and truncated", job)
	}
	if used := counter("tail"); used > reserve || used < finalTailCost {
		t.Fatalf("the reserve counter holds %d, want the tails it kept within %d", used, reserve)
	}
	if most := int64(budget) + reserve; total > most {
		t.Fatalf("the placement's streams hold output costing %d, want at most the budget and "+
			"the reserve, %d", total, most)
	}
}

func TestExecRefusesPastTheRunningJobLimit(t *testing.T) {
	e := newEnv(t)
	s := e.serviceDeps(t, jobs.Deps{ReplicaID: "r1", MaxRunning: 2})
	up := jobs.Until{Pattern: regexp.MustCompile("up")}
	for i := range 2 {
		e.exec(t, s, []string{"sh", "-c", "echo up; sleep 30"}, "key-"+strconv.Itoa(i),
			5*time.Second, up)
	}
	_, err := s.Exec(t.Context(), e.place, []string{"true"}, "", nil, "key-more", time.Second,
		jobs.Until{})
	if !errors.Is(err, jobs.ErrTooManyJobs) {
		t.Fatalf("a third running job gave %v, want ErrTooManyJobs", err)
	}
	if want := "jobs: the placement runs as many jobs as it may, 2 at once"; err.Error() != want {
		t.Fatalf("a third running job says %q, want %q", err, want)
	}
	if res := e.exec(t, s, []string{"sh", "-c", "echo up; sleep 30"}, "key-0", time.Second,
		up); res.State != store.JobRunning {
		t.Fatalf("a repeated key of a running job gave %s, want it attached", res.State)
	}
}

func TestFinalTailKeepsOutputARefusedSnapshotHeld(t *testing.T) {
	jobs.SetFloodEvery(t, 200*time.Millisecond)
	e := newEnv(t)
	const budget = 64 << 10
	s := e.serviceDeps(t, jobs.Deps{ReplicaID: "r1", PlacementBudget: budget})
	key := e.prefix + "placement:" + e.place.ID.String() + ":out"
	set := e.vk.B().Set().Key(key).Value(fmt.Sprint(budget)).Build()
	if err := e.vk.Do(t.Context(), set).Error(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ script, out, err string }{
		{"echo result; sleep 1; echo more", "result\nmore\n", ""},
		{"echo result; sleep 1; echo err >&2", "result\n", "err\n"},
	} {
		res := e.exec(t, s, []string{"sh", "-c", c.script}, "", 30*time.Second, jobs.Until{})
		if res.State != store.JobExited || string(res.StdoutTail) != c.out ||
			string(res.StderrTail) != c.err {
			t.Errorf("%q past the budget gave %s stdout %q stderr %q, want %q and %q",
				c.script, res.State, res.StdoutTail, res.StderrTail, c.out, c.err)
		}
	}
}
