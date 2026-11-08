// End-to-end tests run the built router binaries against the local Postgres and Valkey.
//
// TestMain builds bugnet-run, router-api, router-worker and bugnet-dev once. Each test starts
// its own stack with a fresh database and Valkey prefix, secrets from bugnet-dev keys, two
// router-api replicas and one router-worker on the local provider, each with its own settings
// file, and an events receiver that checks every signature. Cleanup stops every process with
// SIGTERM, then SIGKILL after 10 s, and fails the test when a bugnet-run process under the
// stack's local root is still alive.
package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

const (
	smallPkg   = "e2e-small-10m"
	smallPrice = 50000
	morePkg    = "e2e-small-10m-more"
	shortPkg   = "e2e-small-1m"
	shortMore  = "e2e-small-1m-more"
)

const catalogJSON = `{"packages": [
	{"id": "e2e-small-10m", "resources": {"vcpu": 1, "memory_mib": 512, "disk_gib": 1},
		"minutes": 10, "price_micros": 50000},
	{"id": "e2e-small-10m-more", "resources": {"vcpu": 1, "memory_mib": 512, "disk_gib": 1},
		"minutes": 10, "price_micros": 45000, "extension_of": "e2e-small-10m"},
	{"id": "e2e-small-1m", "resources": {"vcpu": 1, "memory_mib": 512, "disk_gib": 1},
		"minutes": 1, "price_micros": 10000},
	{"id": "e2e-small-1m-more", "resources": {"vcpu": 1, "memory_mib": 512, "disk_gib": 1},
		"minutes": 1, "price_micros": 9000, "extension_of": "e2e-small-1m"}
]}`

const (
	readyWait    = 30 * time.Second
	stopGrace    = 10 * time.Second
	callTimeout  = 60 * time.Second
	eventWait    = 30 * time.Second
	expiryWait   = 100 * time.Second
	leftoverWait = 5 * time.Second
	pollWaitMs   = 25000
	logTail      = 60
)

// keyNames are the secrets bugnet-dev keys must print
var keyNames = []string{
	"BUGNET_INTERNAL_TOKEN", "BUGNET_ORDER_PRIVATE_KEY", "BUGNET_HANDLE_KEY",
	"BUGNET_EVENT_SIGNING_KEY", "BUGNET_DATA_PLANE_KEYS",
}

// callerTokens are the caller tokens every router process gets from bugnet-dev keys
var callerTokens = []string{
	"BUGNET_EDGE_TOKEN", "BUGNET_SECRETS_TOKEN", "BUGNET_SSH_TOKEN", "BUGNET_EGRESS_TOKEN",
	"BUGNET_EGRESS_FETCH_TOKEN", "BUGNET_OPERATOR_TOKEN", "BUGNET_PAYMENTS_ROUTER_TOKEN",
}

var bin struct{ runner, api, worker, dev string }

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	dir, err := os.MkdirTemp("", "bugnet-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	build := exec.Command("go", "build", "-o", dir+string(os.PathSeparator),
		"github.com/bugnet-sh/bugnet-run/cmd/bugnet-run",
		"github.com/bugnet-sh/bugnet-router/cmd/router-api",
		"github.com/bugnet-sh/bugnet-router/cmd/router-worker",
		"github.com/bugnet-sh/bugnet-router/cmd/bugnet-dev")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build binaries:", err)
		return 1
	}
	bin.runner = filepath.Join(dir, "bugnet-run")
	bin.api = filepath.Join(dir, "router-api")
	bin.worker = filepath.Join(dir, "router-worker")
	bin.dev = filepath.Join(dir, "bugnet-dev")
	return m.Run()
}

/* Tests */

// fiveLines touches at-N just before printing N, so at-N's time is no later than line N
const fiveLines = `for i in $(seq 1 5); do touch at-$i; echo $i; sleep 0.1; done`

func TestE2EHappyPath(t *testing.T) {
	t.Parallel()
	s := startStack(t)
	a, b := s.client(t, s.a), s.client(t, s.b)
	h, p := s.place(t, a, smallPkg, smallPrice)
	id := p.GetPlacementId()
	ready := s.events.await(t, "placement.ready", id, eventWait)
	if ready.Payload.State != "ready" || ready.Payload.Provider != "local" {
		t.Errorf("ready event %+v, want state ready on local", ready.Payload)
	}

	streamed := execLines(t, a, h, "sh", "-c", fiveLines)
	if want := []string{"1", "2", "3", "4", "5"}; !slices.Equal(streamed.lines, want) {
		t.Fatalf("exec printed %q, want %q", streamed.lines, want)
	}
	touched := modTimes(t, a, h)
	for i := range 4 {
		next := fmt.Sprintf("at-%d", i+2)
		printed, ok := touched[next]
		if !ok {
			t.Fatalf("no %s in the sandbox, files %v", next, touched)
		}
		if !streamed.at[i].Before(printed) {
			t.Errorf("line %s arrived at %s, after line %d was printed at %s",
				streamed.lines[i], streamed.at[i].Format(time.RFC3339Nano), i+2,
				printed.Format(time.RFC3339Nano))
		}
	}

	stdout, stderr, code := s.dev(t, s.a, "exec", "--handle", h, "--",
		"sh", "-c", "echo out; echo err >&2; exit 3")
	if stdout != "out\n" || stderr != "err\n" || code != 3 {
		t.Errorf("bugnet-dev exec printed %q and %q and exited %d, want out, err and 3",
			stdout, stderr, code)
	}

	// A flood past the job's output budget reaches bugnet-dev cut, with a note of the skip
	flood := `head -c 20000000 /dev/zero | tr '\0' a; echo; echo end`
	stdout, stderr, code = s.dev(t, s.a, "exec", "--handle", h, "--", "sh", "-c", flood)
	if code != 0 || !strings.HasSuffix(stdout, "a\nend\n") || len(stdout) >= 20000005 ||
		!strings.Contains(stderr, "output skipped") {
		t.Errorf("bugnet-dev exec of a flood printed %d bytes and %q and exited %d, want "+
			"fewer than all 20000005 ending in end, a skip note and 0", len(stdout), stderr, code)
	}

	listed := s.devOK(t, s.a, "extensions", "--handle", h)
	wantListed := morePkg + " minutes=10 price_micros=45000 available=true reason=\n" +
		smallPkg + " minutes=10 price_micros=50000 available=true reason=\n"
	if listed != wantListed {
		t.Errorf("bugnet-dev extensions printed %q, want %q", listed, wantListed)
	}
	checked := parseKV(s.devOK(t, s.a, "check-extend", "--handle", h, "--package", morePkg))
	after, err := time.Parse(time.RFC3339Nano, checked["deadline_after"])
	if err != nil {
		t.Fatalf("bugnet-dev check-extend printed %v: %v", checked, err)
	}
	want := p.GetDeadline().AsTime().Add(10 * time.Minute)
	if checked["state"] != "ready" || !after.Equal(want) || checked["price_micros"] != "45000" {
		t.Errorf("bugnet-dev check-extend printed %v, want ready, deadline_after %s and "+
			"price_micros 45000", checked, want.Format(time.RFC3339Nano))
	}
	token := strings.TrimSpace(s.devOK(t, s.a, "token", "--kind", "extend",
		"--package", morePkg, "--handle", h))
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	ext, err := b.ExtendPlacement(ctx, connect.NewRequest(&routerv1.ExtendPlacementRequest{
		Handle: h, OrderToken: token,
	}))
	if err != nil {
		t.Fatalf("ExtendPlacement: %v", err)
	}
	if got := ext.Msg.GetPlacement().GetDeadline().AsTime(); !got.Equal(after) {
		t.Errorf("deadline after extend %s, check-extend said %s", got, after)
	}
	s.events.await(t, "placement.extended", id, eventWait)

	stopped, err := b.StopPlacement(ctx, connect.NewRequest(&routerv1.StopPlacementRequest{
		Handle: h,
	}))
	if err != nil {
		t.Fatalf("StopPlacement: %v", err)
	}
	st := stopped.Msg.GetPlacement().GetState()
	if st != routerv1.PlacementState_PLACEMENT_STATE_TERMINATED {
		t.Errorf("state after stop %s, want terminated", st)
	}
	ended := s.events.await(t, "placement.terminated", id, eventWait)
	if ended.Payload.State != "terminated" || ended.Payload.Reason != "stop" {
		t.Errorf("terminated event %+v, want state terminated for reason stop", ended.Payload)
	}
	if n := sandboxes(t, s.root); n != 0 {
		t.Errorf("%d sandboxes left after stop", n)
	}
}

func TestE2EDeadlineExpiry(t *testing.T) {
	t.Parallel()
	s := startStack(t)
	created := parseKV(s.devOK(t, s.b, "create", "--package", shortPkg))
	h, id := created["handle"], created["placement_id"]
	if created["state"] != "ready" || h == "" {
		t.Fatalf("bugnet-dev create printed %v, want a ready placement and its handle", created)
	}
	deadline, err := time.Parse(time.RFC3339Nano, created["deadline"])
	if err != nil {
		t.Fatalf("deadline %q: %v", created["deadline"], err)
	}
	if n := sandboxes(t, s.root); n != 1 {
		t.Fatalf("%d sandboxes while ready, want 1", n)
	}

	ended := s.events.await(t, "placement.terminated", id, expiryWait)
	if ended.Payload.State != "terminated" || ended.Payload.Reason != "deadline" {
		t.Errorf("terminated event %+v, want state terminated for reason deadline",
			ended.Payload)
	}
	// The deadline job ends a placement up to a second before its deadline
	if early := deadline.Add(-time.Second); ended.CreatedAt.Before(early) {
		t.Errorf("terminated at %s, before the deadline %s", ended.CreatedAt, deadline)
	}
	if n := sandboxes(t, s.root); n != 0 {
		t.Errorf("%d sandboxes left after the deadline, want 0", n)
	}
	if got := parseKV(s.devOK(t, s.a, "get", "--handle", h)); got["state"] != "terminated" {
		t.Errorf("bugnet-dev get printed %v after the deadline, want terminated", got)
	}
	// router-worker ended it, so router-worker told the edges
	evs := s.edgeEvents(t)
	if len(evs) != 1 || evs[0].GetPlacementEnded().GetPlacementId() != id ||
		evs[0].GetPlacementEnded().GetReason() != routerv1.EndReason_END_REASON_EXPIRED {
		t.Errorf("edge events %v, want one PlacementEnded EXPIRED for %s", evs, id)
	}
}

// countTo300 prints 1 to 300 with a 10 ms sleep after each line
const countTo300 = `for i in $(seq 1 300); do echo $i; sleep 0.01; done`

func TestE2EOwnerCrashNoGap(t *testing.T) {
	t.Parallel()
	s := startStack(t)
	a, b := s.client(t, s.a), s.client(t, s.b)
	h, _ := s.place(t, a, smallPkg, smallPrice)
	var want strings.Builder
	for i := 1; i <= 300; i++ {
		fmt.Fprintf(&want, "%d\n", i)
	}

	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	res, err := a.Exec(ctx, connect.NewRequest(&routerv1.ExecRequest{
		Handle: h, Argv: []string{"sh", "-c", countTo300}, WaitMs: 1,
	}))
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	job := res.Msg.GetJobId()
	if res.Msg.GetTruncated() {
		t.Fatal("the first Exec reply was already truncated")
	}
	var out bytes.Buffer
	out.Write(res.Msg.GetStdoutTail())
	cursor := res.Msg.GetCursor()

	first, err := a.WatchJob(ctx, connect.NewRequest(&routerv1.WatchJobRequest{
		Handle: h, JobId: job, Cursor: cursor,
	}))
	if err != nil {
		t.Fatalf("WatchJob on A: %v", err)
	}
	killed := false
	for first.Receive() {
		m := first.Msg()
		if m.GetExit() != nil {
			t.Fatal("the job exited before replica A was killed")
		}
		out.Write(stdout(m))
		cursor = m.GetCursor()
		if !killed && bytes.Count(out.Bytes(), []byte("\n")) >= 100 {
			s.a.kill(t)
			killed = true
		}
	}
	_ = first.Close()
	if !killed {
		t.Fatalf("the stream on A ended after %q: %v", out.String(), first.Err())
	}

	rest, err := b.WatchJob(ctx, connect.NewRequest(&routerv1.WatchJobRequest{
		Handle: h, JobId: job, Cursor: cursor,
	}))
	if err != nil {
		t.Fatalf("WatchJob on B: %v", err)
	}
	var exit *routerv1.JobExit
	for rest.Receive() {
		m := rest.Msg()
		out.Write(stdout(m))
		if exit = m.GetExit(); exit != nil {
			break
		}
	}
	_ = rest.Close()
	if exit == nil {
		t.Fatalf("the stream on B ended without an exit frame: %v", rest.Err())
	}
	if exit.GetExitCode() != 0 {
		t.Errorf("exit code %d, want 0", exit.GetExitCode())
	}
	if out.String() != want.String() {
		t.Errorf("combined output is not 1 to 300 once each:\n%s", out.String())
	}

	replay, _, code := s.dev(t, s.b, "watch", "--handle", h, "--job", job)
	if replay != want.String() || code != 0 {
		t.Errorf("bugnet-dev watch exited %d after printing:\n%s", code, replay)
	}
	stopped := parseKV(s.devOK(t, s.b, "stop", "--handle", h))
	if stopped["state"] != "terminated" {
		t.Errorf("bugnet-dev stop printed %v, want terminated", stopped)
	}
}

func TestE2ESandboxOpsLimitIsShared(t *testing.T) {
	t.Parallel()
	// One token a second keeps the spent bucket empty for the few calls that follow
	s := startStackWith(t, stackOptions{
		extra: "[limits]\nsandbox_ops_per_second = 1\nsandbox_ops_burst = 2\n",
	})
	a, b := s.client(t, s.a), s.client(t, s.b)
	h, _ := s.place(t, a, smallPkg, smallPrice)
	list := func(c routerv1connect.SandboxRouterServiceClient) error {
		ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
		defer cancel()
		_, err := c.ListFiles(ctx, connect.NewRequest(&routerv1.ListFilesRequest{Handle: h}))
		return err
	}
	for i, c := range []routerv1connect.SandboxRouterServiceClient{a, b} {
		if err := list(c); err != nil {
			t.Fatalf("ListFiles %d of a burst of 2: %v", i+1, err)
		}
	}
	for name, c := range map[string]routerv1connect.SandboxRouterServiceClient{"a": a, "b": b} {
		if err := list(c); connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Errorf("ListFiles on api-%s past the shared burst gave %v, want resource_exhausted",
				name, err)
		}
	}
}

func TestE2EValkeyOverAUnixSocket(t *testing.T) {
	t.Parallel()
	socket, accepted := testenv.ValkeySocket(t)
	s := startStackWith(t, stackOptions{
		valkey: "unix://" + socket, worker: "dataplane_workers = 3\n",
	})
	if n := accepted.Load(); n < 3 {
		t.Fatalf("%d connections came through the socket, want one from each of 3 processes", n)
	}
	h, p := s.place(t, s.client(t, s.a), smallPkg, smallPrice)
	s.events.await(t, "placement.ready", p.GetPlacementId(), eventWait)
	if got := parseKV(s.devOK(t, s.b, "stop", "--handle", h)); got["state"] != "terminated" {
		t.Errorf("bugnet-dev stop printed %v, want terminated", got)
	}
	raw, err := os.ReadFile(s.worker.log)
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(string(raw)) {
		var l struct {
			Msg     string `json:"msg"`
			Workers int    `json:"dataplane_workers"`
		}
		if json.Unmarshal([]byte(line), &l) == nil && l.Msg == "working" {
			if l.Workers != 3 {
				t.Errorf("router-worker gave the dataplane queue %d workers, want 3", l.Workers)
			}
			return
		}
	}
	t.Error("router-worker logged no working line")
}

/* Stack */

type stack struct {
	root     string            // local provider root every process shares
	db       string            // the stack's database URL
	packages string            // catalog file
	prefix   string            // Valkey key prefix
	keys     map[string]string // what bugnet-dev keys printed
	priv     ed25519.PrivateKey
	env      []string // environment of every router process, the secrets only
	events   *receiver
	procs    []*proc
	a, b     *proc // router-api replicas
	worker   *proc
	opts     stackOptions
}

// stackOptions change the settings files of a stack
type stackOptions struct {
	valkey string // valkey.addr, the test Valkey over TCP when empty
	worker string // lines in router-worker's [worker] table
	extra  string // settings every process's file ends with
}

func startStack(t *testing.T) *stack {
	t.Helper()
	return startStackWith(t, stackOptions{})
}

// startStackWith starts a stack whose settings files hold o
func startStackWith(t *testing.T, o stackOptions) *stack {
	t.Helper()
	dbURL := testenv.DatabaseURL(t)
	_, prefix := testenv.Valkey(t)
	if o.valkey == "" {
		o.valkey = valkeyAddr()
	}
	s := &stack{root: t.TempDir(), db: dbURL, prefix: prefix, keys: devKeys(t), opts: o}
	seed := decodeKey(t, s.keys, "BUGNET_ORDER_PRIVATE_KEY", ed25519.SeedSize)
	s.priv = ed25519.NewKeyFromSeed(seed)
	eventSeed := decodeKey(t, s.keys, "BUGNET_EVENT_SIGNING_KEY", ed25519.SeedSize)
	s.events = newReceiver(t, ed25519.NewKeyFromSeed(eventSeed).Public().(ed25519.PublicKey))
	s.packages = filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(s.packages, []byte(catalogJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	s.env = append(hostEnv(),
		"BUGNET_DATABASE_URL="+dbURL,
		"BUGNET_INTERNAL_TOKEN="+s.keys["BUGNET_INTERNAL_TOKEN"],
		"BUGNET_HANDLE_KEY="+s.keys["BUGNET_HANDLE_KEY"],
		"BUGNET_EVENT_SIGNING_KEY="+s.keys["BUGNET_EVENT_SIGNING_KEY"],
		"BUGNET_DATA_PLANE_KEYS="+s.keys["BUGNET_DATA_PLANE_KEYS"],
	)
	for _, name := range callerTokens {
		s.env = append(s.env, name+"="+s.keys[name])
	}

	migrate := exec.Command(bin.api, "migrate")
	migrate.Env = append(hostEnv(), "BUGNET_DATABASE_URL="+dbURL)
	if out, err := migrate.CombinedOutput(); err != nil {
		t.Fatalf("router-api migrate: %v\n%s", err, out)
	}

	logs := t.TempDir()
	t.Cleanup(func() { s.stop(t) })
	s.a = s.start(t, logs, "api-a", bin.api, "api")
	s.b = s.start(t, logs, "api-b", bin.api, "api")
	s.worker = s.start(t, logs, "worker", bin.worker, "worker")
	for _, p := range s.procs {
		p.waitReady(t)
	}
	return s
}

func devKeys(t *testing.T) map[string]string {
	t.Helper()
	out, err := exec.Command(bin.dev, "keys").Output()
	if err != nil {
		t.Fatalf("bugnet-dev keys: %v", err)
	}
	keys := parseKV(string(out))
	for _, name := range slices.Concat(keyNames, callerTokens) {
		if keys[name] == "" {
			t.Fatalf("bugnet-dev keys printed no %s:\n%s", name, out)
		}
	}
	return keys
}

// settings writes the settings file of the process name, listening on addr in table
func (s *stack) settings(t *testing.T, name, table, addr string) string {
	t.Helper()
	pub := s.priv.Public().(ed25519.PublicKey)
	var lines string
	if table == "worker" {
		lines = s.opts.worker
	}
	content := fmt.Sprintf(`order_token_public_key = %q
packages_file = %q
providers = ["local"]
replica_id = %q
events_url = %q

[%s]
listen_addr = %q
%s
[valkey]
addr = %q
prefix = %q

[names]
transfer_host = "transfer.e2e.example"
preview_domain = "preview.e2e.example"

[local]
enabled = true
root = %q
runner = %q
%s`, base64.StdEncoding.EncodeToString(pub), s.packages, name, s.events.url, table, addr,
		lines, s.opts.valkey, s.prefix, s.root, bin.runner, s.opts.extra)
	path := filepath.Join(t.TempDir(), name+".toml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func decodeKey(t *testing.T, keys map[string]string, name string, size int) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(keys[name])
	if err != nil || len(b) != size {
		t.Fatalf("%s from bugnet-dev keys is not base64 of %d bytes", name, size)
	}
	return b
}

// hostEnv is the test's environment without any BUGNET_ setting
func hostEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "BUGNET_") {
			env = append(env, kv)
		}
	}
	return env
}

func valkeyAddr() string {
	if addr := os.Getenv("BUGNET_TEST_VALKEY_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// proc is one running router binary
type proc struct {
	name    string
	cmd     *exec.Cmd
	url     string
	config  string // settings file
	log     string
	done    chan struct{}
	waitErr error
	killed  atomic.Bool
}

// start runs a router binary listening on a free port, set in the settings table named table
func (s *stack) start(t *testing.T, logs, name, binary, table string) *proc {
	t.Helper()
	addr := freeAddr(t)
	config := s.settings(t, name, table, addr)
	log, err := os.Create(filepath.Join(logs, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	cmd := exec.Command(binary, "-config", config)
	cmd.Env = s.env
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	p := &proc{name: name, cmd: cmd, url: "http://" + addr, config: config, log: log.Name(),
		done: make(chan struct{})}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.done)
	}()
	s.procs = append(s.procs, p)
	return p
}

func (p *proc) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(readyWait)
	for {
		select {
		case <-p.done:
			t.Fatalf("%s exited before it was ready: %v", p.name, p.waitErr)
		default:
		}
		resp, err := http.Get(p.url + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not ready within %s", p.name, readyWait)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// kill ends the process with SIGKILL and waits for it
func (p *proc) kill(t *testing.T) {
	t.Helper()
	p.killed.Store(true)
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill %s: %v", p.name, err)
	}
	<-p.done
}

// stop sends SIGTERM, and SIGKILL when the process still runs after stopGrace
func (p *proc) stop(t *testing.T) {
	select {
	case <-p.done:
		if !p.killed.Load() {
			t.Errorf("%s exited during the test: %v", p.name, p.waitErr)
		}
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
		if p.waitErr != nil {
			t.Errorf("%s after SIGTERM: %v", p.name, p.waitErr)
		}
	case <-time.After(stopGrace):
		t.Errorf("%s still ran %s after SIGTERM, killing it", p.name, stopGrace)
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

func (s *stack) stop(t *testing.T) {
	var wg sync.WaitGroup
	for _, p := range s.procs {
		wg.Go(func() { p.stop(t) })
	}
	wg.Wait()
	s.destroyLeft(t)
	s.checkRunners(t)
	if !t.Failed() {
		return
	}
	for _, p := range s.procs {
		raw, _ := os.ReadFile(p.log)
		lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
		t.Logf("last lines of %s:\n%s", p.name,
			strings.Join(lines[max(len(lines)-logTail, 0):], "\n"))
	}
}

// destroyLeft destroys the sandboxes still ready when the stack stopped, and so their gates
func (s *stack) destroyLeft(t *testing.T) {
	p := local.New(local.Options{Name: "local", Root: s.root})
	owned, err := p.ListOwned(context.Background(), nil)
	if err != nil {
		t.Errorf("list the sandboxes left under %s: %v", s.root, err)
	}
	for _, o := range owned {
		if err := p.Destroy(context.Background(), o.Ref); err != nil {
			t.Errorf("destroy a sandbox left under %s: %v", s.root, err)
		}
	}
}

// checkRunners fails the test and kills what is left when bugnet-run outlives the stack
func (s *stack) checkRunners(t *testing.T) {
	deadline := time.Now().Add(leftoverWait)
	for {
		all := processes(t)
		var left []process
		var lines []string
		for _, p := range all {
			if strings.Contains(p.command, bin.runner) && strings.Contains(p.command, s.root) {
				left = append(left, p)
				lines = append(lines, strconv.Itoa(p.pid)+" "+p.command)
			}
		}
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("bugnet-run processes under %s outlived the stack:\n%s", s.root,
				strings.Join(lines, "\n"))
			for _, r := range left {
				// A supervised command leads its own process group under the supervisor
				for _, p := range all {
					if p.ppid == r.pid {
						_ = syscall.Kill(-p.pid, syscall.SIGKILL)
						_ = syscall.Kill(p.pid, syscall.SIGKILL)
					}
				}
				_ = syscall.Kill(r.pid, syscall.SIGKILL)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type process struct {
	pid, ppid int
	command   string
}

func processes(t *testing.T) []process {
	out, err := exec.Command("ps", "-A", "-ww", "-o", "pid=,ppid=,command=").Output()
	if err != nil {
		t.Errorf("list processes: %v", err)
		return nil
	}
	var all []process
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 3 {
			continue
		}
		pid, perr := strconv.Atoi(f[0])
		ppid, pperr := strconv.Atoi(f[1])
		if perr == nil && pperr == nil {
			all = append(all, process{pid: pid, ppid: ppid, command: strings.Join(f[2:], " ")})
		}
	}
	return all
}

// sandboxes counts the local provider's sandbox directories, skipping its dot directories
func sandboxes(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			n++
		}
	}
	return n
}

/* Clients */

// setBearer sends every request with the internal token
type setBearer struct {
	token string
	next  http.RoundTripper
}

func (b setBearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(r)
}

func (s *stack) client(t *testing.T, p *proc) routerv1connect.SandboxRouterServiceClient {
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	tr := &http.Transport{Protocols: &protocols}
	t.Cleanup(tr.CloseIdleConnections)
	rt := setBearer{token: s.keys["BUGNET_INTERNAL_TOKEN"], next: tr}
	return routerv1connect.NewSandboxRouterServiceClient(&http.Client{Transport: rt}, p.url)
}

// place creates a placement of pkg through c and waits until it is ready
func (s *stack) place(
	t *testing.T, c routerv1connect.SandboxRouterServiceClient, pkg string, price int64,
) (string, *routerv1.Placement) {
	t.Helper()
	token, err := ordertoken.Sign(s.priv, ordertoken.Claims{
		OrderID: "e2e-" + uuid.NewString(), Kind: ordertoken.KindStart, PackageID: pkg,
		AmountMicros: price, ExpiresAt: time.Now().Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	res, err := c.CreatePlacement(ctx, connect.NewRequest(&routerv1.CreatePlacementRequest{
		OrderToken: token,
	}))
	if err != nil {
		t.Fatalf("CreatePlacement: %v", err)
	}
	h, p := res.Msg.GetHandle(), res.Msg.GetPlacement()
	for p.GetState() == routerv1.PlacementState_PLACEMENT_STATE_PLACING {
		r, err := c.GetPlacement(ctx, connect.NewRequest(&routerv1.GetPlacementRequest{
			Handle: h, WaitMs: pollWaitMs, KnownState: p.GetState(),
		}))
		if err != nil {
			t.Fatalf("GetPlacement: %v", err)
		}
		p = r.Msg.GetPlacement()
	}
	if p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY {
		t.Fatalf("placement is %s (%s), want ready", p.GetState(), p.GetFailureReason())
	}
	return h, p
}

// lines are a job's stdout lines and when each one reached the test
type lines struct {
	partial []byte
	lines   []string
	at      []time.Time
}

func (l *lines) add(data []byte, at time.Time) {
	l.partial = append(l.partial, data...)
	for {
		i := bytes.IndexByte(l.partial, '\n')
		if i < 0 {
			return
		}
		l.lines = append(l.lines, string(l.partial[:i]))
		l.at = append(l.at, at)
		l.partial = l.partial[i+1:]
	}
}

// execLines runs argv through Exec and WatchJob and notes when each stdout line arrives
func execLines(
	t *testing.T, c routerv1connect.SandboxRouterServiceClient, h string, argv ...string,
) *lines {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	res, err := c.Exec(ctx, connect.NewRequest(&routerv1.ExecRequest{
		Handle: h, Argv: argv, WaitMs: 1,
	}))
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	got := &lines{}
	got.add(res.Msg.GetStdoutTail(), time.Now())
	stream, err := c.WatchJob(ctx, connect.NewRequest(&routerv1.WatchJobRequest{
		Handle: h, JobId: res.Msg.GetJobId(), Cursor: res.Msg.GetCursor(),
	}))
	if err != nil {
		t.Fatalf("WatchJob: %v", err)
	}
	defer func() { _ = stream.Close() }()
	for stream.Receive() {
		m := stream.Msg()
		got.add(stdout(m), time.Now())
		if exit := m.GetExit(); exit != nil {
			if exit.GetExitCode() != 0 {
				t.Fatalf("exit code %d, want 0", exit.GetExitCode())
			}
			return got
		}
	}
	t.Fatalf("WatchJob ended without an exit frame: %v", stream.Err())
	return nil
}

func stdout(m *routerv1.WatchJobResponse) []byte {
	if out := m.GetOutput(); out.GetStream() == routerv1.OutputStream_OUTPUT_STREAM_STDOUT {
		return out.GetData()
	}
	return nil
}

// modTimes returns when each file in the sandbox's home directory was last modified
func modTimes(
	t *testing.T, c routerv1connect.SandboxRouterServiceClient, h string,
) map[string]time.Time {
	t.Helper()
	res, err := c.ListFiles(t.Context(), connect.NewRequest(&routerv1.ListFilesRequest{
		Handle: h,
	}))
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	out := map[string]time.Time{}
	for _, e := range res.Msg.GetEntries() {
		out[path.Base(e.GetPath())] = e.GetModifiedAt().AsTime()
	}
	return out
}

// dev runs bugnet-dev with the settings of replica p and returns its stdout, stderr and exit code
func (s *stack) dev(t *testing.T, p *proc, args ...string) (string, string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin.dev, append([]string{"--config", p.config}, args...)...)
	cmd.Env = append(hostEnv(),
		"BUGNET_INTERNAL_TOKEN="+s.keys["BUGNET_INTERNAL_TOKEN"],
		"BUGNET_ORDER_PRIVATE_KEY="+s.keys["BUGNET_ORDER_PRIVATE_KEY"],
		"BUGNET_OPERATOR_TOKEN="+s.keys["BUGNET_OPERATOR_TOKEN"],
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		t.Fatalf("run bugnet-dev %s: %v", args[0], err)
	}
	return stdout.String(), stderr.String(), cmd.ProcessState.ExitCode()
}

// devOK is dev for a command that must succeed, returning its stdout
func (s *stack) devOK(t *testing.T, p *proc, args ...string) string {
	t.Helper()
	stdout, stderr, code := s.dev(t, p, args...)
	if code != 0 {
		t.Fatalf("bugnet-dev %s exited %d:\n%s", args[0], code, stderr)
	}
	return stdout
}

func parseKV(out string) map[string]string {
	kv := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			kv[k] = v
		}
	}
	return kv
}

/* Events */

type event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
	Payload   struct {
		PlacementID   string     `json:"placement_id"`
		OrderID       string     `json:"order_id"`
		State         string     `json:"state"`
		Provider      string     `json:"provider"`
		Deadline      *time.Time `json:"deadline"`
		BilledSeconds int64      `json:"billed_seconds"`
		Refundable    bool       `json:"refundable"`
		Reason        string     `json:"reason"`
	} `json:"payload"`
}

// receiver keeps every event whose signature verifies and fails the test on any other delivery
type receiver struct {
	url     string
	pub     ed25519.PublicKey
	mu      sync.Mutex
	events  []event
	refused []string
	changed chan struct{} // closed and replaced whenever an event arrives
}

func newReceiver(t *testing.T, pub []byte) *receiver {
	r := &receiver{pub: pub, changed: make(chan struct{})}
	srv := httptest.NewServer(r)
	r.url = srv.URL
	t.Cleanup(func() {
		srv.Close()
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, why := range r.refused {
			t.Errorf("events receiver refused a delivery: %s", why)
		}
	})
	return r
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		r.refuse(w, fmt.Sprintf("read body: %v", err))
		return
	}
	sig, err := base64.StdEncoding.DecodeString(req.Header.Get("X-Bugnet-Signature"))
	if err != nil || !ed25519.Verify(r.pub, body, sig) {
		r.refuse(w, fmt.Sprintf("bad signature on %s", body))
		return
	}
	var ev event
	if err := json.Unmarshal(body, &ev); err != nil {
		r.refuse(w, fmt.Sprintf("decode %s: %v", body, err))
		return
	}
	if ev.ID == "" || req.Header.Get("X-Bugnet-Event-Id") != ev.ID {
		r.refuse(w, fmt.Sprintf("event id header %q on %s", req.Header.Get("X-Bugnet-Event-Id"),
			body))
		return
	}
	r.mu.Lock()
	r.events = append(r.events, ev)
	close(r.changed)
	r.changed = make(chan struct{})
	r.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (r *receiver) refuse(w http.ResponseWriter, why string) {
	r.mu.Lock()
	r.refused = append(r.refused, why)
	r.mu.Unlock()
	http.Error(w, why, http.StatusBadRequest)
}

// await returns the first event of type typ for the placement, failing after within
func (r *receiver) await(t *testing.T, typ, placementID string, within time.Duration) event {
	t.Helper()
	timer := time.NewTimer(within)
	defer timer.Stop()
	for {
		r.mu.Lock()
		changed := r.changed
		for _, ev := range r.events {
			if ev.Type == typ && ev.Payload.PlacementID == placementID {
				r.mu.Unlock()
				return ev
			}
		}
		r.mu.Unlock()
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("no %s event for placement %s within %s", typ, placementID, within)
		}
	}
}
