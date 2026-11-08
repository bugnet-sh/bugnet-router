package worker_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"google.golang.org/protobuf/proto"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
	"github.com/bugnet-sh/bugnet-router/internal/x402"
)

const egressSpecPath = "/tmp/bugnet/egress-spec.json"

var egressTarget = worker.Egress{
	Host: "egress.bugnet.sh",
	IPs:  []netip.Addr{netip.MustParseAddr("203.0.113.7"), netip.MustParseAddr("2001:db8::7")},
}

// sentSpec is the egress spec as bugnet-run reads it
type sentSpec struct {
	V              int      `json:"v"`
	Token          *string  `json:"token"`
	Endpoint       string   `json:"endpoint"`
	IPs            []string `json:"ips"`
	Port           int      `json:"port"`
	Listen         string   `json:"listen"`
	Hosts          []string `json:"hosts"`
	Env            []envVar `json:"env"`
	Home           string   `json:"home"`
	GitPlaceholder string   `json:"git_placeholder"`
	NPMPlaceholder string   `json:"npm_placeholder"`
	X402Hosts      []string `json:"x402_hosts"`
}

type envVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// egressFake is a local provider that records egress specs and answers egress apply itself
type egressFake struct {
	*local.Provider
	mu    sync.Mutex
	specs []sentSpec
	// raw holds each spec as written
	raw [][]byte
	// answer is the line and exit code of apply number n, success when nil
	answer func(n int, s sentSpec) (string, int)
	// during runs inside apply number n, before it answers
	during func(n int)
}

func (f *egressFake) WriteFile(
	ctx context.Context, ref provider.Ref, path string, data []byte, mode fs.FileMode,
) error {
	if path != egressSpecPath {
		return f.Provider.WriteFile(ctx, ref, path, data, mode)
	}
	if mode != 0o600 {
		return &provider.Error{Code: provider.CodeInvalid, Err: io.ErrUnexpectedEOF}
	}
	var s sentSpec
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	f.specs = append(f.specs, s)
	f.raw = append(f.raw, bytes.Clone(data))
	f.mu.Unlock()
	return nil
}

func (f *egressFake) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	argv := spec.Argv
	if len(argv) > 2 && argv[0] == "sudo" && argv[1] == "-n" {
		argv = argv[2:]
	}
	if len(argv) < 3 || argv[1] != "egress" || argv[2] != "apply" {
		return f.Provider.Exec(ctx, ref, spec)
	}
	if !slices.Equal(argv[3:], []string{"--spec", egressSpecPath}) {
		return nil, &provider.Error{Code: provider.CodeInvalid, Err: io.ErrUnexpectedEOF}
	}
	f.mu.Lock()
	n := len(f.specs)
	s := f.specs[n-1]
	answer, during := f.answer, f.during
	f.mu.Unlock()
	if during != nil {
		during(n)
	}
	line, code := `{"forward":"listening","hosts":1,"secrets":1}`, 0
	if answer != nil {
		line, code = answer(n, s)
	}
	return &doneStream{Reader: strings.NewReader(line + "\n"), code: code}, nil
}

func (f *egressFake) sent() []sentSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.specs)
}

type doneStream struct {
	*strings.Reader
	code int
}

func (d *doneStream) Wait() (int, error) { return d.code, nil }
func (d *doneStream) Close() error       { return nil }

// secretsEnv is a worker env whose local provider serves secrets through an egressFake
func secretsEnv(t *testing.T) (*env, *egressFake) {
	t.Helper()
	fake := &egressFake{Provider: newLocal(t, "local")}
	e := newEnv(t, options{
		providers: []provider.Provider{fake},
		features:  provider.Features{Secrets: []string{"local"}},
		egress:    egressTarget,
	})
	return e, fake
}

// putSecret stores an applying secret of the placement, fetch-only when asked
func (e *env) putSecret(
	t *testing.T, p store.Placement, name string, fetchOnly bool, hosts ...string,
) store.Secret {
	t.Helper()
	sum := sha256.Sum256([]byte(p.ID.String() + name))
	row := store.Secret{
		ID: uuid.New(), PlacementID: p.ID, Name: name, Hosts: hosts, PlaceholderSHA256: sum[:],
		Sealed: []byte("box"), KeyID: "k1", Source: store.SourceGateway,
		State: store.SecretApplying, FetchOnly: fetchOnly,
	}
	if fetchOnly {
		row.State = store.SecretActive
	}
	got, _, err := e.store.PutSecret(t.Context(), row)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func (e *env) placeholder(t *testing.T, p store.Placement, name string) string {
	t.Helper()
	ph, err := e.keys.Placeholder(p.KeyID, p.ID, name)
	if err != nil {
		t.Fatal(err)
	}
	return ph
}

func (e *env) states(t *testing.T, p store.Placement) map[string]string {
	t.Helper()
	all, err := e.store.Secrets(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, s := range all {
		out[s.Name] = strings.TrimSpace(s.State + " " + s.FailureReason)
	}
	return out
}

// secretsChanged counts the SecretsChanged events of the placement on the edge event stream
func (e *env) secretsChanged(t *testing.T, p store.Placement) int {
	t.Helper()
	cmd := e.vk.B().Xrange().Key(e.prefix + "edge:events").Start("-").End("+").Build()
	entries, err := e.vk.Do(t.Context(), cmd).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, en := range entries {
		ev := &routerv1.EdgeEvent{}
		if err := proto.Unmarshal([]byte(en.FieldValues["e"]), ev); err != nil {
			t.Fatal(err)
		}
		if ev.GetSecretsChanged().GetPlacementId() == p.ID.String() {
			n++
		}
	}
	return n
}

// completed waits for the apply_secrets jobs ids to complete, through any number of snoozes
func (e *env) completed(t *testing.T, ids ...int64) {
	t.Helper()
	waiting := map[int64]bool{}
	for _, id := range ids {
		waiting[id] = true
	}
	for len(waiting) > 0 {
		ev := e.next(t, "apply_secrets")
		switch {
		case !waiting[ev.Job.ID] || ev.Kind == river.EventKindJobSnoozed:
		case ev.Kind == river.EventKindJobCompleted:
			delete(waiting, ev.Job.ID)
		default:
			t.Fatalf("job %d ended %s", ev.Job.ID, ev.Kind)
		}
	}
}

func (e *env) apply(t *testing.T, p store.Placement) {
	t.Helper()
	e.run(t, jobargs.ApplySecrets{PlacementID: p.ID})
}

func TestApplySpecShape(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-shape")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.putSecret(t, p, "GH_TOKEN", false, "github.com", "api.github.com")
	e.putSecret(t, p, "GITHUB_TOKEN", true, "api.github.com")
	e.putSecret(t, p, "NPM_TOKEN", false, "registry.npmjs.org")
	e.apply(t, p)

	specs := fake.sent()
	eq(t, len(specs), 1)
	s := specs[0]
	if s.Token == nil || !strings.HasPrefix(*s.Token, "bnt_") || len(*s.Token) != 47 {
		t.Fatalf("the first apply sent token %v", s.Token)
	}
	eq(t, s.V, 1)
	eq(t, s.Endpoint, "egress.bugnet.sh")
	eq(t, s.IPs, []string{"203.0.113.7", "2001:db8::7"})
	eq(t, s.Port, 443)
	eq(t, s.Listen, "127.77.0.1:443")
	eq(t, s.Hosts, []string{"api.github.com", "github.com", "huggingface.co",
		"registry.npmjs.org"})
	eq(t, s.Env, []envVar{
		{"GH_TOKEN", e.placeholder(t, p, "GH_TOKEN")},
		{"HF_TOKEN", e.placeholder(t, p, "HF_TOKEN")},
		{"NPM_TOKEN", e.placeholder(t, p, "NPM_TOKEN")},
	})
	eq(t, s.Home, "/root")
	eq(t, s.GitPlaceholder, e.placeholder(t, p, "GH_TOKEN"))
	eq(t, s.NPMPlaceholder, e.placeholder(t, p, "NPM_TOKEN"))
	eq(t, e.states(t, p), map[string]string{
		"GH_TOKEN": "active", "GITHUB_TOKEN": "active", "HF_TOKEN": "active",
		"NPM_TOKEN": "active",
	})
	eq(t, e.reload(t, p.ID).SecretsVersion, int64(1))
	eq(t, e.secretsChanged(t, p), 1)
	has, err := e.store.HasEgressToken(t.Context(), p.ID)
	if err != nil || !has {
		t.Fatalf("no tunnel token stored: %v", err)
	}

	// A later run sends no token, and a run with nothing pending writes nothing
	e.putSecret(t, p, "OPENAI_API_KEY", false, "api.openai.com")
	e.apply(t, p)
	e.apply(t, p)
	specs = fake.sent()
	eq(t, len(specs), 2)
	eq(t, specs[1].Token, (*string)(nil))
	eq(t, len(specs[1].Env), 4)
	eq(t, e.reload(t, p.ID).SecretsVersion, int64(2))
}

func TestApplyAfterADeleteDropsTheSecret(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-delete")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.apply(t, p)
	if _, err := e.store.DeleteSecret(t.Context(), p.ID, "HF_TOKEN"); err != nil {
		t.Fatal(err)
	}
	if err := e.store.BumpSecretsVersion(t.Context(), p.ID, true); err != nil {
		t.Fatal(err)
	}
	e.apply(t, p)
	e.apply(t, p)
	specs := fake.sent()
	eq(t, len(specs), 2)
	eq(t, specs[1].Hosts, []string{})
	eq(t, specs[1].Env, []envVar{})

	// A placement whose only secret went before any apply writes nothing
	q, _ := e.placeReady(t, "o-apply-delete-2")
	e.putSecret(t, q, "HF_TOKEN", false, "huggingface.co")
	if _, err := e.store.DeleteSecret(t.Context(), q.ID, "HF_TOKEN"); err != nil {
		t.Fatal(err)
	}
	e.apply(t, q)
	eq(t, len(fake.sent()), 2)
	pending, _, err := e.store.SecretsPending(t.Context(), q.ID)
	if err != nil || pending {
		t.Fatalf("pending %v %v", pending, err)
	}
}

// deleteDuring removes the placement's secret of name inside apply number 1, as DeleteSecret does
func (e *env) deleteDuring(t *testing.T, fake *egressFake, p store.Placement, name string) {
	fake.during = func(n int) {
		if n != 1 {
			return
		}
		err := e.store.WithTx(context.Background(), func(tx pgx.Tx) error {
			ts := e.store.Tx(tx)
			if _, err := ts.PlacementForUpdate(context.Background(), p.ID); err != nil {
				return err
			}
			if _, err := ts.DeleteSecret(context.Background(), p.ID, name); err != nil {
				return err
			}
			return ts.BumpSecretsVersion(context.Background(), p.ID, true)
		})
		if err != nil {
			t.Error(err)
		}
	}
}

func TestADeleteDuringAnApplyIsAppliedNext(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-delete-during")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.putSecret(t, p, "OPENAI_API_KEY", false, "api.openai.com")
	e.deleteDuring(t, fake, p, "HF_TOKEN")
	e.apply(t, p)
	eq(t, e.states(t, p), map[string]string{"OPENAI_API_KEY": "active"})
	pending, _, err := e.store.SecretsPending(t.Context(), p.ID)
	if err != nil || !pending {
		t.Fatalf("a delete during the run is not pending after it: %v %v", pending, err)
	}
	// The delete's own job writes the set again without the secret
	e.apply(t, p)
	specs := fake.sent()
	eq(t, len(specs), 2)
	eq(t, specs[0].Hosts, []string{"api.openai.com", "huggingface.co"})
	eq(t, specs[1].Hosts, []string{"api.openai.com"})
	eq(t, specs[1].Env, []envVar{{"OPENAI_API_KEY", e.placeholder(t, p, "OPENAI_API_KEY")}})
	e.apply(t, p)
	eq(t, len(fake.sent()), 2)
}

func TestAFetchOnlyReplacementDuringAnApplyIsAppliedNext(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-fetch-during")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	fake.during = func(n int) {
		if n != 1 {
			return
		}
		e.putSecret(t, p, "HF_TOKEN", true, "huggingface.co")
		if err := e.store.BumpSecretsVersion(context.Background(), p.ID, true); err != nil {
			t.Error(err)
		}
	}
	e.apply(t, p)
	e.apply(t, p)
	specs := fake.sent()
	eq(t, len(specs), 2)
	eq(t, specs[1].Hosts, []string{})
	eq(t, specs[1].Env, []envVar{})
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "active"})
}

// two writes at once, each with its own job, leave one apply that sends both
func TestTwoWritesAtOnceOneApplySendsBoth(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-two")
	held, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := e.store.TryLock(context.Background(), "apply:"+p.ID.String(), func() error {
			close(held)
			<-release
			return nil
		})
		done <- err
	}()
	<-held
	var wg sync.WaitGroup
	ids := make([]int64, 2)
	for i, name := range []string{"HF_TOKEN", "OPENAI_API_KEY"} {
		wg.Go(func() {
			err := e.store.WithTx(context.Background(), func(tx pgx.Tx) error {
				ts := e.store.Tx(tx)
				if _, err := ts.PlacementForUpdate(context.Background(), p.ID); err != nil {
					return err
				}
				sum := sha256.Sum256([]byte(name))
				if _, _, err := ts.PutSecret(context.Background(), store.Secret{
					ID: uuid.New(), PlacementID: p.ID, Name: name, Hosts: []string{"a.example"},
					PlaceholderSHA256: sum[:], Sealed: []byte("box"), KeyID: "k1",
					Source: store.SourceGateway, State: store.SecretApplying,
				}); err != nil {
					return err
				}
				res, err := e.river.InsertTx(context.Background(), tx,
					jobargs.ApplySecrets{PlacementID: p.ID}, nil)
				if err == nil {
					ids[i] = res.Job.ID
				}
				return err
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	e.completed(t, ids...)
	specs := fake.sent()
	eq(t, len(specs), 1)
	eq(t, len(specs[0].Env), 2)
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "active", "OPENAI_API_KEY": "active"})
}

func TestFetchOnlySecretsNeedNoApply(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-fetch")
	e.putSecret(t, p, "GITHUB_TOKEN", true, "api.github.com")
	e.apply(t, p)
	eq(t, len(fake.sent()), 0)
	eq(t, e.states(t, p), map[string]string{"GITHUB_TOKEN": "active"})
}

func TestPort443TakenFailsTheSecret(t *testing.T) {
	e, fake := secretsEnv(t)
	fake.answer = func(int, sentSpec) (string, int) { return `{"error":"port_443_taken"}`, 1 }
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-443")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.apply(t, p)
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "failed PORT_443_TAKEN"})
	eq(t, e.secretsChanged(t, p), 1)
	// The token minted for the failed run is never kept, so the next run sends a new one
	has, err := e.store.HasEgressToken(t.Context(), p.ID)
	if err != nil || has {
		t.Fatalf("has token %v %v", has, err)
	}
	fake.mu.Lock()
	fake.answer = nil
	fake.mu.Unlock()
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.apply(t, p)
	specs := fake.sent()
	if specs[1].Token == nil || *specs[1].Token == *specs[0].Token {
		t.Fatal("the run after a failure did not send a fresh token")
	}
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "active"})
}

func TestTokenMissingMintsANewToken(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-missing")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.apply(t, p)
	fake.mu.Lock()
	fake.answer = func(_ int, s sentSpec) (string, int) {
		if s.Token == nil {
			return `{"error":"token_missing"}`, 1
		}
		return `{"forward":"listening"}`, 0
	}
	fake.mu.Unlock()
	e.putSecret(t, p, "OPENAI_API_KEY", false, "api.openai.com")
	e.apply(t, p)
	specs := fake.sent()
	eq(t, len(specs), 3)
	eq(t, specs[1].Token, (*string)(nil))
	if specs[2].Token == nil || *specs[2].Token == *specs[0].Token {
		t.Fatal("token_missing did not mint a new token")
	}
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "active", "OPENAI_API_KEY": "active"})
}

func TestApplyFailsWithoutTheSecretsFeature(t *testing.T) {
	fake := &egressFake{Provider: newLocal(t, "local")}
	e := newEnv(t, options{providers: []provider.Provider{fake}, egress: egressTarget})
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-off")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.apply(t, p)
	eq(t, len(fake.sent()), 0)
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "failed FEATURE_UNAVAILABLE"})
}

func TestApplyConvergesWhileARunHoldsTheLock(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-lock")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	held, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := e.store.TryLock(context.Background(), "apply:"+p.ID.String(), func() error {
			close(held)
			<-release
			return nil
		})
		done <- err
	}()
	<-held
	res, err := e.river.Insert(t.Context(), jobargs.ApplySecrets{PlacementID: p.ID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ev := e.next(t, "apply_secrets")
	if ev.Kind != river.EventKindJobSnoozed || ev.Job.ID != res.Job.ID {
		t.Fatalf("the job ended %s while another run held the lock", ev.Kind)
	}
	eq(t, ev.Job.Queue, jobargs.QueueDataplane)
	// A secret added while the lock is held is applied by the snoozed job
	e.putSecret(t, p, "OPENAI_API_KEY", false, "api.openai.com")
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for {
		ev := e.next(t, "apply_secrets")
		if ev.Kind == river.EventKindJobCompleted {
			break
		}
		if ev.Kind != river.EventKindJobSnoozed {
			t.Fatalf("the job ended %s", ev.Kind)
		}
	}
	eq(t, len(fake.sent()), 1)
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "active", "OPENAI_API_KEY": "active"})
}

// a secret rewritten between step 1 and step 5 keeps applying, the next run marks it
func TestApplyMarksOnlyTheSecretsItLoaded(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-marks")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.putSecret(t, p, "OPENAI_API_KEY", false, "api.openai.com")
	fake.during = func(n int) {
		if n == 1 {
			e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
		}
	}
	e.apply(t, p)
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "applying", "OPENAI_API_KEY": "active"})
	e.apply(t, p)
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "active", "OPENAI_API_KEY": "active"})
	eq(t, len(fake.sent()), 2)
}

func TestApplyTimeoutIsUnderTheRescue(t *testing.T) {
	timeout := worker.ApplyTimeout()
	if timeout != 90*time.Second {
		t.Fatalf("apply_secrets times out after %v, want 90 s", timeout)
	}
}

const payTo = "11111111111111111111111111111111"

// putX402 stores an applying GET service of the placement
func (e *env) putX402(t *testing.T, p store.Placement, host, prefix string) store.X402Service {
	t.Helper()
	row := store.X402Service{
		PlacementID: p.ID, ServiceID: x402.ServiceID("GET", host, prefix),
		Spec: x402.Spec{
			Host: host, Prefix: prefix, Method: "GET", MaxPriceMicros: 10_000, PayTo: payTo,
			Indexed: true,
		},
	}
	if err := e.store.PutX402Service(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	return row
}

// x402States answers the state of each service of the placement by its prefix and host
func (e *env) x402States(t *testing.T, p store.Placement) map[string]string {
	t.Helper()
	rows, err := e.store.X402Services(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, r := range rows {
		out[r.Spec.Host+r.Spec.Prefix] = strings.TrimSpace(r.State + " " + r.FailureReason)
	}
	return out
}

func TestApplyWritesX402HostsAndField(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-x402")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.putX402(t, p, "agent402.tools", "/a/")
	e.putX402(t, p, "agent402.tools", "/b/")
	e.putX402(t, p, "api.example.com", "/v1/")
	e.apply(t, p)
	specs := fake.sent()
	eq(t, len(specs), 1)
	eq(t, specs[0].Hosts, []string{"agent402.tools", "api.example.com", "huggingface.co"})
	eq(t, specs[0].X402Hosts, []string{"agent402.tools", "api.example.com"})
	eq(t, e.x402States(t, p), map[string]string{
		"agent402.tools/a/": "active", "agent402.tools/b/": "active",
		"api.example.com/v1/": "active",
	})
	got := e.reload(t, p.ID)
	eq(t, got.AppliedHosts, []string{"agent402.tools", "api.example.com", "huggingface.co"})
	eq(t, got.SecretsVersion, int64(1))
	eq(t, e.secretsChanged(t, p), 1)
}

func TestApplySpecMatchesRunnerGolden(t *testing.T) {
	golden, err := os.ReadFile(filepath.Join("testdata", "spec-x402.json"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, string(worker.GoldenSpec())+"\n", string(golden))
}

func TestSecretsOnlySpecHasNoX402Key(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-nox402")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	e.apply(t, p)
	fake.mu.Lock()
	raw := string(fake.raw[0])
	fake.mu.Unlock()
	if strings.Contains(raw, "x402") {
		t.Fatalf("a secrets-only spec holds an x402 key: %s", raw)
	}
}

// the main case, a sandbox with x402 services and no secret at all
func TestX402OnlyPlacementGetsATunnel(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-x402only")
	e.putX402(t, p, "agent402.tools", "/a/")
	e.apply(t, p)
	specs := fake.sent()
	eq(t, len(specs), 1)
	s := specs[0]
	if s.Token == nil || !strings.HasPrefix(*s.Token, "bnt_") {
		t.Fatalf("no tunnel token was minted: %v", s.Token)
	}
	eq(t, s.Hosts, []string{"agent402.tools"})
	eq(t, s.X402Hosts, []string{"agent402.tools"})
	eq(t, s.Env, []envVar{})
	eq(t, e.x402States(t, p), map[string]string{"agent402.tools/a/": "active"})
	sum := sha256.Sum256([]byte(*s.Token))
	got, err := e.store.PlacementByEgressToken(t.Context(), sum[:])
	if err != nil || got.ID != p.ID {
		t.Fatalf("the token resolves to %v (%v), want %s", got.ID, err, p.ID)
	}
	left, err := e.store.Secrets(t.Context(), p.ID)
	eq(t, len(left), 0)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDisableAloneRemovesHost(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-disable")
	row := e.putX402(t, p, "agent402.tools", "/a/")
	e.apply(t, p)
	eq(t, e.reload(t, p.ID).SecretsVersion, int64(1))
	if err := e.store.DisableX402Service(t.Context(), p.ID, row.ServiceID); err != nil {
		t.Fatal(err)
	}
	if err := e.store.BumpSecretsVersion(t.Context(), p.ID, false); err != nil {
		t.Fatal(err)
	}
	e.apply(t, p)
	specs := fake.sent()
	eq(t, len(specs), 2)
	eq(t, specs[1].Hosts, []string{})
	eq(t, specs[1].X402Hosts, []string(nil))
	got := e.reload(t, p.ID)
	eq(t, len(got.AppliedHosts), 0)
	eq(t, got.SecretsVersion, int64(3))
}

func TestEarlyExitCountsX402Work(t *testing.T) {
	e, _ := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-early")
	pending := func(want bool, why string) {
		t.Helper()
		got, _, err := e.store.SecretsPending(t.Context(), p.ID)
		if err != nil || got != want {
			t.Fatalf("%s: pending %v (%v), want %v", why, got, err, want)
		}
	}
	pending(false, "nothing")
	a := e.putX402(t, p, "agent402.tools", "/a/")
	b := e.putX402(t, p, "agent402.tools", "/b/")
	pending(true, "an applying row")
	e.apply(t, p)
	pending(false, "after the apply")
	ctx := t.Context()
	// Another live row keeps the host, so a disabled row alone is no work
	if err := e.store.DisableX402Service(ctx, p.ID, a.ServiceID); err != nil {
		t.Fatal(err)
	}
	pending(false, "a disabled row on a host a live row keeps")
	// The last row of the host goes, so its host must leave the sandbox
	if err := e.store.DisableX402Service(ctx, p.ID, b.ServiceID); err != nil {
		t.Fatal(err)
	}
	pending(true, "a disabled row on an applied host")
	e.apply(t, p)
	pending(false, "after the host left")
	// A secret on the host keeps it written, so the disabled row is no work
	e.putSecret(t, p, "EX_KEY", false, "api.example.com")
	c := e.putX402(t, p, "api.example.com", "/v1/")
	e.apply(t, p)
	pending(false, "after the shared host")
	if err := e.store.DisableX402Service(ctx, p.ID, c.ServiceID); err != nil {
		t.Fatal(err)
	}
	pending(false, "a disabled row on a secret host")
}

// a row re-priced between the load and the mark keeps applying, the next run marks it
func TestApplyMarksOnlyLoadedVersion(t *testing.T) {
	e, fake := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-x402marks")
	e.putX402(t, p, "agent402.tools", "/a/")
	e.putX402(t, p, "api.example.com", "/v1/")
	e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
	fake.during = func(n int) {
		if n == 1 {
			row := store.X402Service{PlacementID: p.ID,
				ServiceID: x402.ServiceID("GET", "agent402.tools", "/a/"),
				Spec: x402.Spec{Host: "agent402.tools", Prefix: "/a/", Method: "GET",
					MaxPriceMicros: 20_000, PayTo: payTo, Indexed: true}}
			if err := e.store.PutX402Service(t.Context(), row); err != nil {
				t.Error(err)
			}
			e.putSecret(t, p, "HF_TOKEN", false, "huggingface.co")
		}
	}
	e.apply(t, p)
	eq(t, e.x402States(t, p), map[string]string{
		"agent402.tools/a/": "applying", "api.example.com/v1/": "active"})
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "applying"})
	e.apply(t, p)
	eq(t, e.x402States(t, p), map[string]string{
		"agent402.tools/a/": "active", "api.example.com/v1/": "active"})
	eq(t, e.states(t, p), map[string]string{"HF_TOKEN": "active"})
	eq(t, len(fake.sent()), 2)
}

func TestApplyPort443TakenFailsX402Rows(t *testing.T) {
	e, fake := secretsEnv(t)
	fake.answer = func(int, sentSpec) (string, int) { return `{"error":"port_443_taken"}`, 1 }
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-x402-443")
	e.putX402(t, p, "agent402.tools", "/a/")
	e.apply(t, p)
	eq(t, e.x402States(t, p), map[string]string{"agent402.tools/a/": "failed PORT_443_TAKEN"})
	eq(t, len(e.reload(t, p.ID).AppliedHosts), 0)
}

// EnableX402 takes the placement row first, as step 7 does, so the two never wait on each other
func TestApplyAndEnableNeverDeadlock(t *testing.T) {
	e, _ := secretsEnv(t)
	e.start(t)
	p, _ := e.placeReady(t, "o-apply-deadlock")
	for round := range 100 {
		done := make(chan error, 1)
		go func() {
			done <- e.store.WithTx(t.Context(), func(tx pgx.Tx) error {
				ts := e.store.Tx(tx)
				if _, err := ts.PlacementForUpdate(t.Context(), p.ID); err != nil {
					return err
				}
				row := store.X402Service{PlacementID: p.ID,
					ServiceID: x402.ServiceID("GET", "agent402.tools", "/a/"),
					Spec: x402.Spec{Host: "agent402.tools", Prefix: "/a/", Method: "GET",
						MaxPriceMicros: int64(10_000 + round), PayTo: payTo, Indexed: true}}
				if err := ts.PutX402Service(t.Context(), row); err != nil {
					return err
				}
				return ts.BumpSecretsVersion(t.Context(), p.ID, true)
			})
		}()
		e.apply(t, p)
		if err := <-done; err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}
}
