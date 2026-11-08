package saga_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"maps"
	"math/big"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// answer is what a scripted runner subcommand prints and exits with
type answer struct {
	out  string
	code int
}

// scripted answers chosen runner subcommands itself, runs the rest, and records each root call
type scripted struct {
	*local.Provider
	answers map[string]answer
	mu      *sync.Mutex
	// ran lists each root call as its subcommand, setup with the sample flag its spec carried
	ran *[]string
	// argv lists each root call's argv as the saga sent it
	argv *[][]string
	caps func(provider.Capabilities) provider.Capabilities
	reqs *[]provider.CreateRequest
}

func newScripted(p *local.Provider, answers map[string]answer) scripted {
	return scripted{Provider: p, answers: answers, mu: &sync.Mutex{}, ran: &[]string{},
		argv: &[][]string{}, reqs: &[]provider.CreateRequest{}}
}

func (s scripted) Capabilities() provider.Capabilities {
	c := s.Provider.Capabilities()
	if s.caps != nil {
		c = s.caps(c)
	}
	return c
}

func (s scripted) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	s.mu.Lock()
	*s.reqs = append(*s.reqs, req)
	s.mu.Unlock()
	return s.Provider.Create(ctx, req)
}

func (s scripted) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	argv := spec.Argv
	if len(argv) > 2 && argv[0] == "sudo" {
		argv = argv[2:]
	}
	if len(argv) < 2 || argv[0] != "/tmp/bugnet/bin/bugnet-run" ||
		!slices.Contains([]string{"setup", "harden", "ssh-up"}, argv[1]) {
		return s.Provider.Exec(ctx, ref, spec)
	}
	sub := argv[1]
	call := sub
	if sub == "setup" {
		raw, _, err := s.ReadFile(ctx, ref, "/tmp/bugnet/setup-spec.json", 0, 1<<20)
		var got struct{ Sample bool }
		if err == nil && json.Unmarshal(raw, &got) == nil {
			call += " sample=" + strconv.FormatBool(got.Sample)
		}
	}
	s.mu.Lock()
	*s.ran = append(*s.ran, call)
	*s.argv = append(*s.argv, slices.Clone(spec.Argv))
	s.mu.Unlock()
	a, ok := s.answers[sub]
	if !ok {
		return s.Provider.Exec(ctx, ref, spec)
	}
	// An ssh-up answer names the first port its spec offered wherever it says $PORT
	if sub == "ssh-up" {
		raw, _, err := s.ReadFile(ctx, ref, "/tmp/bugnet/ssh-spec.json", 0, 1<<20)
		var got struct{ Ports []int }
		if err == nil && json.Unmarshal(raw, &got) == nil && len(got.Ports) > 0 {
			a.out = strings.ReplaceAll(a.out, "$PORT", strconv.Itoa(got.Ports[0]))
		}
	}
	return s.Provider.Exec(ctx, ref, provider.ExecSpec{Argv: []string{
		"sh", "-c", `printf '%s\n' "$1"; exit "$2"`, "sh", a.out, strconv.Itoa(a.code),
	}})
}

func (s scripted) calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(*s.ran)
}

func (s scripted) argvs() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(*s.argv)
}

// hostGate is a local provider whose route needs a gate_host entry that only PrepareGate gives
type hostGate struct{ *local.Provider }

func (h hostGate) PrepareGate(
	ctx context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	meta, err := h.Provider.PrepareGate(ctx, ref, port)
	if err != nil {
		return nil, err
	}
	meta = maps.Clone(meta)
	if meta == nil {
		meta = map[string]string{}
	}
	meta["gate_host"] = "x"
	return meta, nil
}

func (h hostGate) GateRoute(ref provider.Ref, port int) (provider.GateRoute, error) {
	if ref.Meta["gate_host"] != "x" {
		return provider.GateRoute{}, provider.GateUnprepared(h.Name(), ref.ID, "gate_host")
	}
	return h.Provider.GateRoute(ref, port)
}

// closedGate is a local provider whose gate route leads to a port nothing listens on
type closedGate struct{ *local.Provider }

func (closedGate) GateRoute(provider.Ref, int) (provider.GateRoute, error) {
	return provider.GateRoute{BaseURL: "http://127.0.0.1:1"}, nil
}

// created makes a ready sandbox on p, outside any saga
func created(t *testing.T, p provider.Provider) provider.Ref {
	t.Helper()
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		Size: small, Labels: map[string]string{"t": "boot"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func sandboxFS(root string, ref provider.Ref) string {
	return filepath.Join(root, ref.ID, "fs")
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestBootRunsSetupWithoutRunnerUpload(t *testing.T) {
	root := t.TempDir()
	a := newLocal(t, local.Options{Name: "a", Root: root})
	e := newEnv(t, a)
	id := uuid.New()
	keyID := e.keys.Current().ID
	ref, rep, err := e.saga.Boot(t.Context(), a, created(t, a),
		saga.BootPlan{Placement: id, KeyID: keyID})
	eq(t, err == nil && rep.GateReady, true)
	eq(t, rep.GateReadyAt.IsZero(), false)
	eq(t, rep.CAVersion, 0)
	key, err := os.ReadFile(sandboxFS(root, ref) + "/etc/bugnet/gate.key")
	if err != nil {
		t.Fatal(err)
	}
	want, err := e.keys.GateKey(keyID, id)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, strings.TrimSpace(string(key)), want)
	// The spec held the gate key, so setup deleted it
	eq(t, exists(sandboxFS(root, ref)+"/tmp/bugnet/setup-spec.json"), false)
	eq(t, ref.Meta["gate_port"] != "", true)
}

func TestBootRefusesRestrictedWithoutEgressControl(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	_, _, err := e.saga.Boot(t.Context(), a, created(t, a), saga.BootPlan{
		Placement: uuid.New(), KeyID: e.keys.Current().ID, Restricted: []string{"x.example"},
	})
	if err == nil || !strings.Contains(err.Error(), "egress control") {
		t.Fatalf("a restricted boot on local: %v", err)
	}
}

func TestBootSetupFailureFailsOver(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}),
		map[string]answer{"setup": {`{"error":"gate_failed"}`, 1}})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "b")
	attempts, err := e.store.Attempts(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(attempts), 2)
	eq(t, attempts[0].Provider, "a")
	eq(t, attempts[0].State, "failed")
	if !strings.Contains(attempts[0].Error, "setup exited 1 with gate_failed") {
		t.Fatalf("the failed attempt says %q", attempts[0].Error)
	}
	owned, _ := a.ListOwned(t.Context(), nil)
	eq(t, len(owned), 0)
}

func TestBootDropsAnErrorValueItCannotName(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}),
		map[string]answer{"setup": {`{"error":"/etc/bugnet/x: denied"}`, 1}})
	e := newEnv(t, a, newLocal(t, local.Options{Name: "b"}))
	p, _ := e.placeReady(t, "order-1")
	attempts, err := e.store.Attempts(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(attempts), 2)
	got := attempts[0].Error
	if !strings.Contains(got, "setup exited 1 with no reason") ||
		strings.Contains(got, "/etc/bugnet") || strings.Contains(got, "denied") {
		t.Fatalf("the failed attempt says %q", got)
	}
}

func TestBootHardenRequired(t *testing.T) {
	for _, required := range []bool{true, false} {
		t.Run(strconv.FormatBool(required), func(t *testing.T) {
			boxd := newScripted(newLocal(t, local.Options{Name: "boxd"}),
				map[string]answer{"harden": {`{"error":"firewall_unavailable"}`, 1}})
			b := newLocal(t, local.Options{Name: "b"})
			e := newEnvWith(t, func(d *saga.Deps) {
				d.Boot.Harden = map[string]config.HardenRule{"boxd": {Required: required}}
			}, boxd, b)
			p, _ := e.placeReady(t, "order-1")
			if required {
				eq(t, p.Provider, "b")
				attempts, _ := e.store.Attempts(t.Context(), p.ID)
				if len(attempts) != 2 || !strings.Contains(attempts[0].Error,
					"harden exited 1 with firewall_unavailable") {
					t.Fatalf("attempts %+v, want boxd's to fail on harden", attempts)
				}
				return
			}
			eq(t, p.Provider, "boxd")
			eq(t, p.GateReadyAt != nil, true)
		})
	}
}

func TestBootOrderSetupHardenThenTheSampler(t *testing.T) {
	boxd := newScripted(newLocal(t, local.Options{Name: "boxd"}),
		map[string]answer{"harden": {`{"firewall":"nft"}`, 0}})
	e := newEnvWith(t, func(d *saga.Deps) {
		d.Boot.Harden = map[string]config.HardenRule{"boxd": {Required: true}}
	}, boxd)
	e.placeReady(t, "order-1")
	// An order that needs no SSH runs no ssh-up
	eq(t, boxd.calls(), []string{"setup sample=false", "harden", "setup sample=true"})
}

func TestBootRootCommandsUseSudoForANonRootUser(t *testing.T) {
	const runner = "/tmp/bugnet/bin/bugnet-run"
	for user, prefix := range map[string][]string{
		"boxd": {"sudo", "-n", runner}, "": {runner}, "root": {runner},
	} {
		t.Run("user="+user, func(t *testing.T) {
			boxd := newScripted(newLocal(t, local.Options{Name: "boxd"}),
				map[string]answer{"harden": {`{"firewall":"nft"}`, 0}})
			boxd.caps = func(c provider.Capabilities) provider.Capabilities {
				c.ExecUser = user
				return c
			}
			e := newEnvWith(t, func(d *saga.Deps) {
				d.Boot.Harden = map[string]config.HardenRule{"boxd": {Required: true}}
			}, boxd)
			e.placeReady(t, "order-1")
			want := [][]string{
				append(slices.Clone(prefix), "setup", "--spec", "/tmp/bugnet/setup-spec.json"),
				append(slices.Clone(prefix), "harden", "--spec", "/tmp/bugnet/harden-spec.json"),
				append(slices.Clone(prefix), "setup", "--spec", "/tmp/bugnet/setup-spec.json"),
			}
			eq(t, boxd.argvs(), want)
		})
	}
}

func TestBootKeepsThePreparedGateMeta(t *testing.T) {
	a := hostGate{newLocal(t, local.Options{Name: "a"})}
	e := newEnv(t, a)
	ref := created(t, a)
	_, ok := ref.Meta["gate_host"]
	eq(t, ok, false)
	got, rep, err := e.saga.Boot(t.Context(), a, ref,
		saga.BootPlan{Placement: uuid.New(), KeyID: e.keys.Current().ID, NeedPreview: true})
	eq(t, err, nil)
	eq(t, rep.GateReady, true)
	eq(t, got.Meta["gate_host"], "x")
	eq(t, got.Meta["gate_port"], ref.Meta["gate_port"])

	// The ready transaction stores the ref with both the create's and the gate's entries
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.GateReadyAt != nil, true)
	m := p.ProviderRef.Meta
	eq(t, m["gate_host"], "x")
	if m["gate_port"] == "" || m["sshd_port"] == "" {
		t.Fatalf("the stored ref lost the create's ports: %v", m)
	}
}

func TestBootGateFailureWithoutNeedsIsLogged(t *testing.T) {
	a := closedGate{newLocal(t, local.Options{Name: "a"})}
	e := newEnv(t, a)
	plan := saga.BootPlan{Placement: uuid.New(), KeyID: e.keys.Current().ID}
	_, rep, err := e.saga.Boot(t.Context(), a, created(t, a), plan)
	eq(t, err, nil)
	eq(t, rep.GateReady, false)
	for _, need := range []saga.BootPlan{{NeedPreview: true}, {NeedSSH: true}} {
		need.Placement, need.KeyID = plan.Placement, plan.KeyID
		_, _, err := e.saga.Boot(t.Context(), a, created(t, a), need)
		if err == nil || !strings.Contains(err.Error(), "gate health") {
			t.Fatalf("a closed gate for %+v: %v", need, err)
		}
	}
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.GateReadyAt, (*time.Time)(nil))
}

func TestGateReadyAtRecorded(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	p, _ := e.placeReady(t, "order-1")
	if p.GateReadyAt == nil || p.GateReadyAt.After(*p.ReadyAt) ||
		p.GateReadyAt.Before(p.CreatedAt) {
		t.Fatalf("gate ready at %v, want between created %v and ready %v", p.GateReadyAt,
			p.CreatedAt, p.ReadyAt)
	}
	eq(t, p.EgressCAVersion, 0)
	eq(t, p.ProviderRef.Meta["gate_port"] != "", true)
}

// testCA is a self-signed CA certificate in PEM
func testCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestEgressCAVersionStored(t *testing.T) {
	ca := testCA(t)
	root := t.TempDir()
	// A local sandbox named boxd gets the PEM, which setup installs into its root
	boxd := newLocal(t, local.Options{Name: "boxd", Root: root})
	e := newEnvWith(t, func(d *saga.Deps) { d.Boot.CAPEM, d.Boot.CAVersion = ca, 3 }, boxd)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.EgressCAVersion, 3)
	got, err := os.ReadFile(filepath.Join(sandboxFS(root, *p.ProviderRef),
		"usr/local/share/ca-certificates/bugnet-egress-v3.crt"))
	if err != nil || string(got) != string(ca) {
		t.Fatalf("the CA in the sandbox: %v", err)
	}

	// local gets no CA, so setup skips it and the column stays null
	plain := newEnvWith(t, func(d *saga.Deps) { d.Boot.CAPEM, d.Boot.CAVersion = ca, 3 },
		newLocal(t, local.Options{Name: "local"}))
	q, _ := plain.placeReady(t, "order-2")
	eq(t, q.EgressCAVersion, 0)
	eq(t, q.GateReadyAt != nil, true)
}

func TestSetupSpecPerProvider(t *testing.T) {
	ca := testCA(t)
	sum := sha256.Sum256(ca)
	b := saga.BootSettings{CAPEM: ca, CAVersion: 2, PreviewDomain: "preview.bugnet.sh"}
	ref := provider.Ref{ID: "x"}
	e2b := saga.SetupSpecFor(b, "e2b", ref, "key", false)
	eq(t, e2b.CAPEM, "")
	eq(t, e2b.CASHA256, hex.EncodeToString(sum[:]))
	eq(t, e2b.CAVersion, 2)
	eq(t, e2b.GateListen, ":29417")
	eq(t, e2b.DenyPorts, []int{22, 2022, 29417, 49983})
	eq(t, e2b.PreviewDomain, "preview.bugnet.sh")
	for _, name := range []string{"boxd", "koyeb", "boat"} {
		spec := saga.SetupSpecFor(b, name, ref, "key", true)
		eq(t, spec.CAPEM, string(ca))
		eq(t, spec.CASHA256, "")
		eq(t, spec.Sample, true)
	}
	eq(t, saga.SetupSpecFor(b, "koyeb", ref, "key", false).DenyPorts,
		[]int{22, 2022, 3030, 3031, 29417})
	boat := saga.SetupSpecFor(b, "boat", ref, "key", false).DenyPorts
	eq(t, boat[:6], []int{22, 2022, 6080, 8090, 8911, 29417})
	eq(t, boat[6:], func() []int {
		var p []int
		for n := 47984; n <= 48010; n++ {
			p = append(p, n)
		}
		return p
	}())
	eq(t, saga.SetupSpecFor(b, "boxd", ref, "key", false).DenyPorts, []int{22, 2022, 29417})

	// local listens on its own loopback port and skips the CA
	lref := provider.Ref{ID: "y", Meta: map[string]string{"gate_port": "40001",
		"sshd_port": "40002"}}
	l := saga.SetupSpecFor(b, "local", lref, "key", false)
	eq(t, l.GateListen, "127.0.0.1:40001")
	eq(t, l.DenyPorts, []int{22, 2022, 29417, 40001, 40002})
	eq(t, l.CAPEM+l.CASHA256, "")
	eq(t, l.CAVersion, 0)

	// No CA yet means neither field, so setup answers skipped
	none := saga.SetupSpecFor(saga.BootSettings{GatePort: 30000}, "boxd", ref, "key", false)
	eq(t, none.CAPEM+none.CASHA256, "")
	eq(t, none.GateListen, ":30000")
	raw, err := json.Marshal(none)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	for _, k := range []string{"ca_pem", "ca_sha256", "ca_version", "preview_domain"} {
		if _, ok := fields[k]; ok {
			t.Fatalf("a spec without a CA or names carries %s: %s", k, raw)
		}
	}
}

func TestHardenSpecPerProvider(t *testing.T) {
	defaults, err := config.LoadFile(writeFile(t, t.TempDir(), "s.toml", "", 0o644))
	if err != nil {
		t.Fatal(err)
	}
	h := defaults.Hardening
	b := saga.BootSettings{
		MailPorts: h.MailPorts, DenyRanges: h.DenyRanges, Pools: []string{"203.0.113.0/24"},
		Harden: h.Rules,
	}
	hardenSpec := func(t *testing.T, name string) saga.HardenSpecFields {
		t.Helper()
		spec, ok := saga.HardenSpecFor(b, name)
		if !ok {
			t.Fatalf("%s runs no harden", name)
		}
		return spec
	}
	// E2B blocks the IPv4 ranges at create but MMDS, and leaves IPv6 link-local to envd
	eq(t, hardenSpec(t, "e2b").DenyRanges,
		[]string{"fc00::/7", "169.254.169.254/32", "fd00:ec2::254/128"})
	eq(t, hardenSpec(t, "e2b").MailPorts, []int{25, 465, 587})
	eq(t, hardenSpec(t, "e2b").Pools, []string{"203.0.113.0/24"})
	for _, p := range []string{"boxd", "koyeb", "boat"} {
		eq(t, hardenSpec(t, p).DenyRanges, h.DenyRanges)
		eq(t, hardenSpec(t, p).Allow, []string{})
	}
	if _, ok := saga.HardenSpecFor(b, "local"); ok {
		t.Fatal("local runs harden")
	}
	for _, c := range []struct {
		deny []string
		want []string
	}{
		{[]string{"10.0.0.0/8", "fc00::/7"}, []string{"fc00::/7"}},
		{[]string{"169.254.169.254/32"}, []string{"169.254.169.254/32"}},
		{[]string{"169.254.0.0/16", "169.254.169.254/32", "0.0.0.0/0"},
			[]string{"169.254.169.254/32"}},
		{[]string{"169.254.169.0/31"}, []string{}},
	} {
		e2b := b
		e2b.DenyRanges = c.deny
		spec, _ := saga.HardenSpecFor(e2b, "e2b")
		eq(t, spec.DenyRanges, c.want)
	}
	b.Harden = map[string]config.HardenRule{"boat": {Required: true,
		Allow: []string{"100.64.0.1/32"}}}
	eq(t, hardenSpec(t, "boat").Allow, []string{"100.64.0.1/32"})
	raw, _ := json.Marshal(hardenSpec(t, "boat"))
	eq(t, string(raw), `{"v":1,"mail_ports":[25,465,587],"deny_ranges":`+
		mustJSON(t, h.DenyRanges)+`,"pools":["203.0.113.0/24"],"allow":["100.64.0.1/32"]}`)
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestRestrictedCreateCarriesTheEgressAllow(t *testing.T) {
	boxd := newScripted(newLocal(t, local.Options{Name: "boxd"}), nil)
	boxd.caps = func(c provider.Capabilities) provider.Capabilities {
		c.EgressControl = true
		return c
	}
	proxy := []netip.Addr{netip.MustParseAddr("203.0.113.7")}
	e := newEnvWith(t, func(d *saga.Deps) {
		d.Boot.EgressHost, d.Boot.EgressIPs = "egress.bugnet.sh", proxy
	}, boxd)
	e.placeReady(t, "order-1")
	eq(t, (*boxd.reqs)[0].Egress, (*provider.EgressAllow)(nil))
	tok := e.token(t, ordertoken.Claims{
		OrderID: "order-2", Kind: ordertoken.KindStart, PackageID: restrictedPkg,
		AmountMicros: smallPrice,
	})
	q, _, err := e.saga.Create(t.Context(), tok, placement.Requirements{})
	if err != nil {
		t.Fatal(err)
	}
	e.saga.Wait()
	eq(t, e.reload(t, q.ID).State, store.PlacementReady)
	eq(t, (*boxd.reqs)[1].Egress, &provider.EgressAllow{
		Hosts: []string{"huggingface.co", "egress.bugnet.sh"}, Proxy: proxy,
	})
}
