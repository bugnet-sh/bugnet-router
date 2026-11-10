// Package live runs the data-plane and control-plane live rows against a stack on this Mac.
//
// scripts/live-test.sh runs it with -providers and -front. A stack is router-api, router-worker,
// bugnet-edge, its secrets and sessions roles and bugnet-egress, each built from the workspace,
// on a fresh database and Valkey prefix with fresh secrets from bugnet-dev keys, and one
// provider. The local mode listens on free loopback ports with a fresh self-signed pair for the
// dev names, and tests reach the edge with curl --resolve through a loopback front that adds the
// PROXY v2 header HAProxy adds on the VPS.
// The vps mode listens on the fixed ports the VPS's tunnel lands on (config/dev-front.toml), gets
// the exact names from Let's Encrypt over the tunnel with its caches kept in
// ~/.local/state/bugnet-dev/acme, serves the wildcard make dev-certs copied, and checks that
// HTTPS to the transfer host reaches it before any row runs. Provider settings come from
// config/dev.toml and provider keys from the environment.
package live

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
	"golang.org/x/crypto/ssh"

	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

var (
	providersFlag = flag.String("providers", "", "providers the rows run on, comma separated")
	frontFlag     = flag.String("front", "local", "local, or vps for the dev front of DP 15.6")
)

// knownProviders are the providers a stack runs on
var knownProviders = []string{"boxd", "e2b", "koyeb", "boat"}

const (
	readyWait = 60 * time.Second
	stopGrace = 10 * time.Second
	reachWait = 30 * time.Second
)

// repo is the bugnet-router checkout, found from this file's own path
func repo(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("live: the source path of the stack is unknown")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

/* The fronted mode's settings */

// front is config/dev-front.toml
type front struct {
	Names struct {
		PreviewDomain string `toml:"preview_domain"`
		TransferHost  string `toml:"transfer_host"`
		HooksHost     string `toml:"hooks_host"`
		SecretsHost   string `toml:"secrets_host"`
		SessionsHost  string `toml:"sessions_host"`
		EgressHost    string `toml:"egress_host"`
		SSHHost       string `toml:"ssh_host"`
		SSHPort       int    `toml:"ssh_port"`
	} `toml:"names"`
	Ports struct {
		Edge     int `toml:"edge"`
		Secrets  int `toml:"secrets"`
		Sessions int `toml:"sessions"`
		Egress   int `toml:"egress"`
		SSH      int `toml:"ssh"`
	} `toml:"ports"`
	VPS struct {
		EgressIPs []string `toml:"egress_ips"`
	} `toml:"vps"`
	ACME struct {
		Directory   string `toml:"directory"`
		Email       string `toml:"email"`
		StateDir    string `toml:"state_dir"`
		WildcardDir string `toml:"wildcard_dir"`
	} `toml:"acme"`
	Edge struct {
		AbuseContact string `toml:"abuse_contact"`
	} `toml:"edge"`
	Egress struct {
		CADir string `toml:"ca_dir"`
	} `toml:"egress"`
}

// loadFront reads the fronted mode's settings strictly, expanding ~ in its directories
func loadFront(path string) (front, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return front{}, fmt.Errorf("live: %w", err)
	}
	var f front
	dec := toml.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		var missing *toml.StrictMissingError
		if errors.As(err, &missing) {
			var keys []string
			for _, e := range missing.Errors {
				keys = append(keys, strings.Join(e.Key(), "."))
			}
			return front{}, fmt.Errorf("live: %s: unknown keys %s", path,
				strings.Join(keys, ", "))
		}
		return front{}, fmt.Errorf("live: %s: %w", path, err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return front{}, fmt.Errorf("live: %w", err)
	}
	for _, dir := range []*string{&f.ACME.StateDir, &f.ACME.WildcardDir, &f.Egress.CADir} {
		if rest, ok := strings.CutPrefix(*dir, "~/"); ok {
			*dir = filepath.Join(home, rest)
		}
	}
	return f, nil
}

// fixedPorts are the tunnel's ends on the Mac, by the service each one serves
func (f front) fixedPorts() []struct {
	name string
	port int
} {
	return []struct {
		name string
		port int
	}{
		{"edge", f.Ports.Edge}, {"secrets route", f.Ports.Secrets},
		{"sessions route", f.Ports.Sessions}, {"egress", f.Ports.Egress},
		{"SSH gateway", f.Ports.SSH},
	}
}

// checkPlan refuses a stack that cannot start, naming what is missing, before anything starts
func checkPlan(providers []string, mode string, f front, now time.Time) error {
	if len(providers) == 0 {
		return errors.New("live: name at least one provider with -providers, " +
			"as scripts/live-test.sh does")
	}
	for _, p := range providers {
		if !slices.Contains(knownProviders, p) {
			return fmt.Errorf("live: unknown provider %q, want boxd, e2b, koyeb or boat", p)
		}
	}
	switch mode {
	case "local":
		return nil
	case "vps":
	default:
		return fmt.Errorf("live: -front must be local or vps, not %q", mode)
	}
	for _, fp := range f.fixedPorts() {
		ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(fp.port)))
		if err != nil {
			return fmt.Errorf("live: port %d (%s) is taken, the fronted mode needs it free "+
				"for the VPS's tunnel", fp.port, fp.name)
		}
		_ = ln.Close()
	}
	return checkWildcard(f.ACME.WildcardDir, now)
}

// checkWildcard refuses a dev wildcard pair that is missing, expired or not one pair
func checkWildcard(dir string, now time.Time) error {
	chain, err := os.ReadFile(filepath.Join(dir, "fullchain.pem"))
	if err == nil {
		var key []byte
		if key, err = os.ReadFile(filepath.Join(dir, "key.pem")); err == nil {
			_, err = tls.X509KeyPair(chain, key)
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("live: the dev wildcard pair is missing from %s, run make dev-certs "+
			"in bugnet-edges", dir)
	}
	if err != nil {
		return fmt.Errorf("live: the dev wildcard pair in %s is not one pair: %w", dir, err)
	}
	block, _ := pem.Decode(chain)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("live: the dev wildcard in %s does not parse: %w", dir, err)
	}
	if !now.Before(leaf.NotAfter) {
		return fmt.Errorf("live: the dev wildcard in %s expired on %s, run make dev-certs "+
			"in bugnet-edges again", dir, leaf.NotAfter.Format(time.DateOnly))
	}
	return nil
}

/* Binaries */

var built struct {
	once sync.Once
	dir  string
	err  error
}

// TestMain removes the binaries a stack built once every test ran
func TestMain(m *testing.M) {
	code := m.Run()
	if built.dir != "" {
		_ = os.RemoveAll(built.dir)
	}
	os.Exit(code)
}

// binaries builds the router's binaries, bugnet-edge, bugnet-egress and bugnet-run once per run
func binaries(t *testing.T) string {
	t.Helper()
	built.once.Do(func() {
		dir, err := os.MkdirTemp("", "bugnet-live-")
		if err != nil {
			built.err = err
			return
		}
		built.dir = dir
		root := repo(t)
		for _, b := range []struct{ dir, pkg string }{
			{root, "./cmd/router-api"}, {root, "./cmd/router-worker"}, {root, "./cmd/bugnet-dev"},
			{filepath.Join(root, "..", "bugnet-edges"), "./cmd/bugnet-edge"},
			{filepath.Join(root, "..", "bugnet-edges"), "./cmd/bugnet-ssh"},
			{filepath.Join(root, "..", "bugnet-edges"), "./cmd/bugnet-egress"},
			{filepath.Join(root, "..", "bugnet-run"), "./cmd/bugnet-run"},
		} {
			cmd := exec.Command("go", "build", "-o", dir+string(os.PathSeparator), b.pkg)
			cmd.Dir = b.dir
			if out, err := cmd.CombinedOutput(); err != nil {
				built.err = fmt.Errorf("build %s: %w\n%s", b.pkg, err, out)
				return
			}
		}
	})
	if built.err != nil {
		t.Fatalf("live: %v", built.err)
	}
	return built.dir
}

/* The stack */

// egressFiles are what the stack's bugnet-egress and the router's secrets settings share
type egressFiles struct {
	listen   string // where the proxy listens, which fetch.egress_addr dials
	certFile string // the CA certificate the router hands every sandbox
	credDir  string // $CREDENTIALS_DIRECTORY of the proxy, holding the CA key and the box key
	boxPub   string // the box key's public half, base64
}

// stack is one provider's router-api, router-worker, bugnet-edge, its secrets role and
// bugnet-egress
type stack struct {
	provider string
	mode     string
	f        front
	root     string // the router checkout, every router process's working directory
	bin      string
	dir      string // settings, logs and the local mode's TLS pair
	keys     map[string]string
	env      []string // every router process's environment
	settings string   // router settings file
	apiURL   string
	edgeAddr string // where TLS to the edge connects, 127.0.0.1 and a port
	// secretsAddr and sessionsAddr are where TLS to the secrets and sessions roles connect
	secretsAddr  string
	sessionsAddr string
	tlsDir       string // the local mode's TLS pair, made once
	egress       egressFiles
	// egressMetrics is where the proxy's metrics listen, which the source rows read
	egressMetrics string
	procs         []*proc
	fronts        []net.Listener // the local mode's PROXY v2 fronts, none in the vps mode
	// tweaks change the router's settings once they are built, for the rows that need more
	tweaks []func(string, map[string]any)
	// sshAddr is where SSH clients connect, and sshKnown their known_hosts file
	sshAddr  string
	sshKnown string
	sshHost  string // the gateway's host key in authorized_keys form
	stopOnce sync.Once
	// previews is the preview rows' client, made once per stack
	previews    *http.Client
	previewOnce sync.Once
	// telemetry is the router's [telemetry] table once traces are on, which every edge binary but
	// the sessions role takes as its own
	telemetry map[string]any
}

// startStack starts a stack on provider, or on local for the stack's own test
func startStack(
	t *testing.T, provider, mode string, f front, tweaks ...func(string, map[string]any),
) *stack {
	t.Helper()
	checked := provider
	if provider == "local" {
		checked = knownProviders[0]
	}
	if err := checkPlan([]string{checked}, mode, f, time.Now()); err != nil {
		t.Fatal(err)
	}
	s := &stack{provider: provider, mode: mode, f: f, root: repo(t), bin: binaries(t),
		tweaks: tweaks}
	dir, err := os.MkdirTemp("", "bugnet-live-stack-")
	if err != nil {
		t.Fatal(err)
	}
	s.dir = dir
	runner := filepath.Join(s.root, "..", "bugnet-run", "dist", "bugnet-run-linux-amd64")
	if _, err := os.Stat(runner); err != nil && provider != "local" {
		t.Fatalf("live: no bugnet-run build at %s, run make -C ../bugnet-run build", runner)
	}
	s.keys = s.devKeys(t)
	s.egress = s.prepareEgress(t)
	dbURL := testenv.DatabaseURL(t)
	_, prefix := testenv.Valkey(t)
	s.env = append(hostEnv(), "BUGNET_DATABASE_URL="+dbURL)
	// The estimator's source tokens are the only BUGNET_ values the stack takes from the host
	for _, name := range []string{"BUGNET_GITHUB_TOKEN", "BUGNET_HF_TOKEN"} {
		if v := os.Getenv(name); v != "" {
			s.env = append(s.env, name+"="+v)
		}
	}
	for k, v := range s.keys {
		s.env = append(s.env, k+"="+v)
	}
	migrate := exec.Command(filepath.Join(s.bin, "router-api"), "migrate")
	migrate.Env = s.env
	if out, err := migrate.CombinedOutput(); err != nil {
		t.Fatalf("live: router-api migrate: %v\n%s", err, out)
	}
	s.sshHostKey(t)
	apiAddr, workerAddr := freeAddr(t), freeAddr(t)
	s.apiURL = "http://" + apiAddr
	s.settings = s.routerSettings(t, prefix, apiAddr, workerAddr, runner)
	s.start(t, "router-api", "-config", s.settings)
	s.start(t, "router-worker", "-config", s.settings)
	s.waitReady(t, "http://"+apiAddr+"/readyz")
	s.waitReady(t, "http://"+workerAddr+"/readyz")
	metrics := freeAddr(t)
	edge := net.JoinHostPort("127.0.0.1", strconv.Itoa(f.Ports.Edge))
	if mode == "local" {
		edge = freeAddr(t)
	}
	s.start(t, "bugnet-edge", "-config", s.edgeSettings(t, edge, metrics))
	s.waitReady(t, "http://"+metrics+"/readyz")
	secrets, secretsMetrics := net.JoinHostPort("127.0.0.1", strconv.Itoa(f.Ports.Secrets)),
		freeAddr(t)
	if mode == "local" {
		secrets = freeAddr(t)
	}
	s.start(t, "bugnet-edge", "-config", s.secretsSettings(t, secrets, secretsMetrics),
		"--role", "secrets")
	s.waitReady(t, "http://"+secretsMetrics+"/readyz")
	sessions, sessionsMetrics := net.JoinHostPort("127.0.0.1", strconv.Itoa(f.Ports.Sessions)),
		freeAddr(t)
	if mode == "local" {
		sessions = freeAddr(t)
	}
	s.start(t, "bugnet-edge", "-config", s.sessionsSettings(t, sessions, sessionsMetrics),
		"--role", "sessions")
	s.waitReady(t, "http://"+sessionsMetrics+"/readyz")
	s.edgeAddr, s.secretsAddr, s.sessionsAddr = edge, secrets, sessions
	egressMetrics := freeAddr(t)
	s.egressMetrics = egressMetrics
	s.startWith(t, append(slices.Clone(s.env), "CREDENTIALS_DIRECTORY="+s.egress.credDir),
		"bugnet-egress", "serve", "-config", s.egressSettings(t, egressMetrics))
	s.waitReady(t, "http://"+egressMetrics+"/readyz")
	if mode == "local" {
		s.edgeAddr = s.proxyFront(t, edge)
		s.secretsAddr = s.proxyFront(t, secrets)
		s.sessionsAddr = s.proxyFront(t, sessions)
	}
	s.startSSH(t)
	if mode == "vps" {
		if err := s.reaches(); err != nil {
			s.fail(t, "%v", err)
		}
	}
	return s
}

// devKeys runs bugnet-dev keys and adds the order key's public half for the settings
func (s *stack) devKeys(t *testing.T) map[string]string {
	t.Helper()
	out, err := exec.Command(filepath.Join(s.bin, "bugnet-dev"), "keys").Output()
	if err != nil {
		t.Fatalf("live: bugnet-dev keys: %v", err)
	}
	keys := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			keys[k] = v
		}
	}
	return keys
}

// routerSettings is config/dev.toml on provider alone, on this stack's addresses and names
func (s *stack) routerSettings(t *testing.T, prefix, apiAddr, workerAddr, runner string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(s.root, "config", "dev.toml"))
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]any{}
	if err := toml.Unmarshal(raw, &m); err != nil {
		t.Fatalf("live: config/dev.toml: %v", err)
	}
	seed, err := base64.StdEncoding.DecodeString(s.keys["BUGNET_ORDER_PRIVATE_KEY"])
	if err != nil || len(seed) != ed25519.SeedSize {
		t.Fatal("live: bugnet-dev keys printed no order key")
	}
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	m["order_token_public_key"] = base64.StdEncoding.EncodeToString(pub)
	m["providers"] = []string{s.provider}
	if s.provider != "local" {
		// dev.toml's warm pool and starts before payment are local's, which this stack lacks
		delete(table(m, "pools"), "pool")
		speculative := []string{}
		if s.provider != "boat" {
			speculative = []string{s.provider}
		}
		table(m, "reserve")["speculative_providers"] = speculative
	}
	m["deployment_id"] = "live-" + strings.ToLower(rand.Text()[:8])
	table(m, "api")["listen_addr"] = apiAddr
	table(m, "worker")["listen_addr"] = workerAddr
	table(m, "valkey")["addr"] = testenv.ValkeyAddr()
	table(m, "valkey")["prefix"] = prefix
	table(m, "runner")["linux_amd64"] = runner
	if s.provider == "local" {
		local := table(m, "local")
		local["enabled"], local["root"] = true, filepath.Join(s.dir, "local")
		local["runner"] = filepath.Join(s.bin, "bugnet-run")
	}
	n := s.f.Names
	m["names"] = map[string]any{
		"preview_domain": n.PreviewDomain, "transfer_host": n.TransferHost,
		"secrets_host": n.SecretsHost, "ssh_host": n.SSHHost, "egress_host": n.EgressHost,
		"ssh_port": n.SSHPort,
	}
	table(m, "sessions")["base_url"] = "https://" + n.SessionsHost
	if sessionsKeep != "" {
		table(m, "sessions")["keep"] = sessionsKeep
	}
	egress := []string{"127.0.0.1"}
	if s.mode == "vps" {
		egress = s.f.VPS.EgressIPs
	}
	secrets := table(m, "secrets")
	secrets["egress_ips"] = egress
	secrets["ca_cert_file"], secrets["ca_version"] = s.egress.certFile, 1
	secrets["box_public_keys"] = []map[string]any{{"id": "k1", "key": s.egress.boxPub}}
	if s.provider != "local" {
		secrets["providers"] = []string{s.provider}
	}
	s.sourceCheckSettings(m)
	table(m, "fetch")["egress_addr"] = s.egress.listen
	if s.mode == "local" {
		// The local proxy's outer certificate is self-signed, so the fetcher trusts that one
		table(m, "fetch")["egress_roots_file"] = filepath.Join(s.selfSigned(t), "fullchain.pem")
	}
	// The gateway's host key goes into the known_hosts lines the router hands out
	table(m, "ssh")["providers"] = []string{s.provider}
	table(m, "ssh")["host_keys"] = []string{s.sshHost}
	// A registered webhook secret turns the provider's hooks on, for the reconciliation rows
	if env, ok := hookEnv[s.provider]; ok && s.mode == "vps" && os.Getenv(env) != "" {
		table(m, "hooks")["providers"] = []string{s.provider}
	}
	// The stack offers previews on its provider, so the preview rows can earn its place there
	table(m, "previews")["providers"] = []string{s.provider}
	for _, tweak := range s.tweaks {
		tweak(s.provider, m)
	}
	if tel := table(m, "telemetry"); tel["traces"] == true {
		s.telemetry = tel
		// A backend that needs a credential is the only place the Authorization header goes
		if ep, _ := tel["otlp_endpoint"].(string); strings.HasPrefix(ep, "https://") {
			if v := os.Getenv("BUGNET_OTLP_AUTHORIZATION"); v != "" {
				s.env = append(s.env, "BUGNET_OTLP_AUTHORIZATION="+v)
			}
		}
	}
	out, err := toml.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return s.write(t, "router.toml", out)
}

// telemetryTOML is the router's [telemetry] table as an edge binary's file takes it, empty while
// traces are off
func (s *stack) telemetryTOML(t *testing.T) string {
	t.Helper()
	if s.telemetry == nil {
		return ""
	}
	out, err := toml.Marshal(map[string]any{"telemetry": s.telemetry})
	if err != nil {
		t.Fatal(err)
	}
	return "\n" + string(out)
}

func table(m map[string]any, name string) map[string]any {
	if t, ok := m[name].(map[string]any); ok {
		return t
	}
	t := map[string]any{}
	m[name] = t
	return t
}

// edgeSettings is bugnet-edge's file, a self-signed pair locally and ACME on the dev front
func (s *stack) edgeSettings(t *testing.T, listen, metrics string) string {
	t.Helper()
	n := s.f.Names
	acme := fmt.Sprintf("enabled = true\ndirectory = %q\nemail = %q\nstate_dir = %q\n"+
		"wildcard_dir = %q\n", s.f.ACME.Directory, s.f.ACME.Email,
		filepath.Join(s.f.ACME.StateDir, "edge"), s.f.ACME.WildcardDir)
	if s.mode == "local" {
		acme = fmt.Sprintf("enabled = false\nstatic_dir = %q\n", s.selfSigned(t))
	} else if err := os.MkdirAll(filepath.Join(s.f.ACME.StateDir, "edge"), 0o700); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`router_url = %q
edge_id = "live-%s"
tls_listen_addr = %q
redirect_listen_addr = ""
metrics_listen_addr = %q
proxy_protocol_from = ["127.0.0.1/32"]
drain = "1s"

[names]
preview_domain = %q
transfer_host = %q
hooks_host = %q

[previews]
abuse_contact = %q

[acme]
%s%s`, s.apiURL, s.provider, listen, metrics, n.PreviewDomain, n.TransferHost,
		n.HooksHost, s.f.Edge.AbuseContact, acme, s.telemetryTOML(t))
	return s.write(t, "edge.toml", []byte(content))
}

// secretsSettings is the secrets role's file and its page bundle, on the dev front's secrets host
func (s *stack) secretsSettings(t *testing.T, listen, metrics string) string {
	t.Helper()
	page := filepath.Join(s.dir, "secrets-page")
	if err := os.MkdirAll(filepath.Join(page, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	index := []byte("<!doctype html><title>Bugnet secrets</title>")
	if err := os.WriteFile(filepath.Join(page, "index.html"), index, 0o644); err != nil {
		t.Fatal(err)
	}
	acme := fmt.Sprintf("acme_enabled = true\nacme_directory = %q\nacme_email = %q\n"+
		"acme_dir = %q\n", s.f.ACME.Directory, s.f.ACME.Email,
		filepath.Join(s.f.ACME.StateDir, "secrets"))
	if s.mode == "local" {
		acme = fmt.Sprintf("acme_enabled = false\nstatic_dir = %q\n", s.selfSigned(t))
	} else if err := os.MkdirAll(filepath.Join(s.f.ACME.StateDir, "secrets"), 0o700); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`router_url = %q
tls_listen_addr = %q
metrics_listen_addr = %q
proxy_protocol_from = ["127.0.0.1/32"]
host = %q
page_dir = %q
%s%s`, s.apiURL, listen, metrics, s.f.Names.SecretsHost, page, acme, s.telemetryTOML(t))
	return s.write(t, "secrets.toml", []byte(content))
}

// sessionsSettings is the sessions role's file, serving the built session page on the dev front's
// sessions host
func (s *stack) sessionsSettings(t *testing.T, listen, metrics string) string {
	t.Helper()
	acme := fmt.Sprintf("acme_enabled = true\nacme_directory = %q\nacme_email = %q\n"+
		"acme_dir = %q\n", s.f.ACME.Directory, s.f.ACME.Email,
		filepath.Join(s.f.ACME.StateDir, "sessions"))
	if s.mode == "local" {
		acme = fmt.Sprintf("acme_enabled = false\nstatic_dir = %q\n", s.selfSigned(t))
	} else if err := os.MkdirAll(filepath.Join(s.f.ACME.StateDir, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`router_url = %q
tls_listen_addr = %q
metrics_listen_addr = %q
proxy_protocol_from = ["127.0.0.1/32"]
host = %q
page_dir = %q
%s`, s.apiURL, listen, metrics, s.f.Names.SessionsHost, s.sessionPage(t), acme)
	return s.write(t, "sessions.toml", []byte(content))
}

// sessionPage is the directory of a built session page. BUGNET_SESSION_PAGE_DIR names it, else
// the bugnet-website checkout beside the repositories holds one in packages/session-page/dist.
func (s *stack) sessionPage(t *testing.T) string {
	t.Helper()
	dirs := []string{filepath.Join(s.root, "..", "bugnet-website", "packages", "session-page",
		"dist")}
	if dir := os.Getenv("BUGNET_SESSION_PAGE_DIR"); dir != "" {
		dirs = []string{dir}
	}
	for _, dir := range dirs {
		if _, err := os.Stat(filepath.Join(dir, "headers.txt")); err == nil {
			return dir
		}
	}
	t.Fatalf("live: no built session page, build bugnet-website's packages/session-page " +
		"or set BUGNET_SESSION_PAGE_DIR to its dist directory")
	return ""
}

// sshHostKey makes the gateway's host key in a credentials directory, as the unit gets it
func (s *stack) sshHostKey(t *testing.T) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(s.dir, "credentials")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ssh_host_ed25519_key"),
		pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	pub, err := ssh.NewPublicKey(priv.Public())
	if err != nil {
		t.Fatal(err)
	}
	s.sshHost = strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub)))
	s.env = append(s.env, "CREDENTIALS_DIRECTORY="+dir)
}

// startSSH starts bugnet-ssh on a free port locally and on the tunnel's port in the vps mode
func (s *stack) startSSH(t *testing.T) {
	t.Helper()
	listen := freeAddr(t)
	if s.mode == "vps" {
		listen = net.JoinHostPort("127.0.0.1", strconv.Itoa(s.f.Ports.SSH))
	}
	metrics := freeAddr(t)
	content := fmt.Sprintf(`router_url = %q
gateway_id = "live-%s"
listen_addr = %q
metrics_listen_addr = %q
proxy_protocol_from = ["127.0.0.1/32"]
drain_max = "1s"
%s`, s.apiURL, s.provider, listen, metrics, s.telemetryTOML(t))
	s.start(t, "bugnet-ssh", "-config", s.write(t, "ssh.toml", []byte(content)))
	s.waitReady(t, "http://"+metrics+"/readyz")
	host, port := "127.0.0.1", ""
	if s.mode == "local" {
		_, port, _ = net.SplitHostPort(s.proxyFront(t, listen))
		s.sshAddr = net.JoinHostPort(host, port)
	} else {
		host, port = s.f.Names.SSHHost, strconv.Itoa(s.f.Names.SSHPort)
		s.sshAddr = net.JoinHostPort(host, port)
	}
	line := fmt.Sprintf("[%s]:%s %s\n", host, port, s.sshHost)
	s.sshKnown = s.write(t, "ssh_known_hosts", []byte(line))
}

// selfSigned writes a fresh pair for every dev name the edge serves and answers its directory
func (s *stack) selfSigned(t *testing.T) string {
	t.Helper()
	if s.tlsDir != "" {
		return s.tlsDir
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	n := s.f.Names
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: n.TransferHost},
		DNSNames: []string{n.TransferHost, n.HooksHost, n.SecretsHost, n.SessionsHost,
			n.PreviewDomain, "*." + n.PreviewDomain, n.EgressHost},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(s.dir, "tls")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, block := range map[string]*pem.Block{
		"fullchain.pem": {Type: "CERTIFICATE", Bytes: der},
		"key.pem":       {Type: "EC PRIVATE KEY", Bytes: keyDER},
	} {
		err := os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(block), 0o600)
		if err != nil {
			t.Fatal(err)
		}
	}
	s.tlsDir = dir
	return dir
}

// prepareEgress makes the proxy's CA, credentials and box key, and picks where it listens
//
// The dev CA that the E2B templates carry is used when ca_dir holds one. Without it, a fresh CA
// serves the providers whose setup installs the CA, and E2B is refused.
func (s *stack) prepareEgress(t *testing.T) egressFiles {
	t.Helper()
	bin := filepath.Join(s.bin, "bugnet-egress")
	files := egressFiles{credDir: filepath.Join(s.dir, "egress-creds")}
	if err := os.MkdirAll(files.credDir, 0o700); err != nil {
		t.Fatal(err)
	}
	caDir := s.f.Egress.CADir
	_, keyErr := os.Stat(filepath.Join(caDir, "egress-ca.key"))
	_, certErr := os.Stat(filepath.Join(caDir, "egress-ca.pem"))
	if caDir == "" || keyErr != nil || certErr != nil {
		if s.provider == "e2b" {
			t.Fatalf("live: E2B sandboxes trust only the dev CA baked into the dev templates, "+
				"make it with bugnet-egress ca init --out %s and rebuild them with pnpm run "+
				"build:dev in deploy/e2b", caDir)
		}
		caDir = filepath.Join(s.dir, "egress-ca")
		if err := os.MkdirAll(caDir, 0o700); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command(bin, "ca", "init", "--out", caDir).CombinedOutput()
		if err != nil {
			t.Fatalf("live: bugnet-egress ca init: %v\n%s", err, out)
		}
	}
	files.certFile = filepath.Join(caDir, "egress-ca.pem")
	if err := os.Symlink(filepath.Join(caDir, "egress-ca.key"),
		filepath.Join(files.credDir, "egress_ca_key")); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(bin, "box-key", "new", "--id", "k1", "--out", files.credDir).Output()
	if err != nil {
		t.Fatalf("live: bugnet-egress box-key new: %v", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if pub, ok := strings.CutPrefix(line, "public_key: "); ok {
			files.boxPub = pub
		}
	}
	if files.boxPub == "" {
		t.Fatal("live: bugnet-egress box-key new printed no public key")
	}
	// The dev pair lands in the proxy's credential directory, where its key is a credential
	dev := exec.Command(filepath.Join(s.bin, "bugnet-dev"), "mtls", "init", "--dev",
		"--out", files.credDir)
	if out, err := dev.CombinedOutput(); err != nil {
		t.Fatalf("live: bugnet-dev mtls init --dev: %v\n%s", err, out)
	}
	files.listen = net.JoinHostPort("127.0.0.1", strconv.Itoa(s.f.Ports.Egress))
	if s.mode == "local" {
		files.listen = freeAddr(t)
	}
	return files
}

// egressSettings is bugnet-egress' file, a self-signed pair locally and ACME on the dev front
func (s *stack) egressSettings(t *testing.T, metrics string) string {
	t.Helper()
	acme := fmt.Sprintf("acme_enabled = true\nacme_directory = %q\nacme_email = %q\n"+
		"acme_dir = %q\n", s.f.ACME.Directory, s.f.ACME.Email,
		filepath.Join(s.f.ACME.StateDir, "egress"))
	if s.mode == "local" {
		acme = fmt.Sprintf("acme_enabled = false\nstatic_dir = %q\n", s.selfSigned(t))
	} else if err := os.MkdirAll(filepath.Join(s.f.ACME.StateDir, "egress"), 0o700); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`router_url = %q
listen_addr = %q
metrics_listen_addr = %q
proxy_protocol_from = ["127.0.0.1/32"]
egress_host = %q
ca_cert_file = %q
ca_version = 1
box_keys = [{ id = "k1", credential = "egress_box_k1" }]
%s
[x402]
payments_url = %q
payments_ca_file = %q
payments_client_cert_file = %q
%s`, s.apiURL, s.egress.listen, metrics, s.f.Names.EgressHost, s.egress.certFile, acme,
		"https://"+freeAddr(t), filepath.Join(s.egress.credDir, "internal-ca.pem"),
		filepath.Join(s.egress.credDir, "egress-x402-client.pem"), s.telemetryTOML(t))
	return s.write(t, "egress.toml", []byte(content))
}

// proxyFront passes each loopback connection to the edge behind a PROXY v2 header, as HAProxy
func (s *stack) proxyFront(t *testing.T, edge string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.fronts = append(s.fronts, ln)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go relay(c, edge)
		}
	}()
	return ln.Addr().String()
}

func relay(c net.Conn, edge string) {
	defer func() { _ = c.Close() }()
	up, err := net.Dial("tcp", edge)
	if err != nil {
		return
	}
	defer func() { _ = up.Close() }()
	if _, err := up.Write(proxyHeader(c.RemoteAddr(), c.LocalAddr())); err != nil {
		return
	}
	go func() {
		_, _ = io.Copy(up, c)
		_ = up.(*net.TCPConn).CloseWrite()
	}()
	_, _ = io.Copy(c, up)
}

// proxyHeader is a PROXY v2 header for a TCP connection over IPv4 from src to dst
func proxyHeader(src, dst net.Addr) []byte {
	from, to := src.(*net.TCPAddr), dst.(*net.TCPAddr)
	h := append([]byte("\r\n\r\n\x00\r\nQUIT\n"), 0x21, 0x11, 0, 12)
	h = append(append(h, from.IP.To4()...), to.IP.To4()...)
	h = binary.BigEndian.AppendUint16(h, uint16(from.Port))
	return binary.BigEndian.AppendUint16(h, uint16(to.Port))
}

// reaches checks that the transfer host's TLS comes from this stack's edge, by its certificate
func (s *stack) reaches() error {
	host := s.f.Names.TransferHost
	c := &http.Client{Timeout: reachWait}
	resp, err := c.Get("https://" + host + "/")
	if err != nil {
		return fmt.Errorf("live: https://%s/ does not reach this stack: %w", host, err)
	}
	_ = resp.Body.Close()
	cached, err := os.ReadFile(filepath.Join(s.f.ACME.StateDir, "edge", host))
	if err != nil {
		return fmt.Errorf("live: https://%s/ answered, but this stack's edge holds no "+
			"certificate for it: %w", host, err)
	}
	for rest := cached; ; {
		var block *pem.Block
		if block, rest = pem.Decode(rest); block == nil {
			break
		}
		if block.Type == "CERTIFICATE" && resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 &&
			bytes.Equal(block.Bytes, resp.TLS.PeerCertificates[0].Raw) {
			return nil
		}
	}
	return fmt.Errorf("live: https://%s/ answered with a certificate this stack's edge never "+
		"served, another stack holds the tunnel", host)
}

func (s *stack) write(t *testing.T, name string, content []byte) string {
	t.Helper()
	path := filepath.Join(s.dir, name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// hostEnv is the test's environment without any BUGNET_ setting, provider keys kept
func hostEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "BUGNET_") {
			env = append(env, kv)
		}
	}
	return env
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

/* Processes */

// proc is one running binary of the stack
type proc struct {
	name string
	cmd  *exec.Cmd
	log  string
	done chan struct{}
	err  error
}

func (s *stack) start(t *testing.T, name string, args ...string) {
	t.Helper()
	s.startWith(t, s.env, name, args...)
}

// startWith is start under its own environment
func (s *stack) startWith(t *testing.T, env []string, name string, args ...string) {
	t.Helper()
	log, err := os.Create(filepath.Join(s.dir, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	cmd := exec.Command(filepath.Join(s.bin, name), args...)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = s.root, env, log, log
	if err := cmd.Start(); err != nil {
		t.Fatalf("live: start %s: %v", name, err)
	}
	p := &proc{name: name, cmd: cmd, log: log.Name(), done: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	s.procs = append(s.procs, p)
}

func (s *stack) waitReady(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(readyWait)
	for {
		for _, p := range s.procs {
			select {
			case <-p.done:
				s.fail(t, "live: %s exited before the stack was ready: %v", p.name, p.err)
			default:
			}
		}
		if resp, err := http.Get(url); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			s.fail(t, "live: %s was not ready within %s", url, readyWait)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// fail stops the stack, logging each process's last lines, and ends the test
func (s *stack) fail(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Errorf(format, args...)
	s.stop(t)
	t.FailNow()
}

// stop ends every process with SIGTERM, then SIGKILL, and logs their tails when t failed
func (s *stack) stop(t testing.TB) {
	s.stopOnce.Do(func() {
		for _, ln := range s.fronts {
			_ = ln.Close()
		}
		var wg sync.WaitGroup
		for _, p := range s.procs {
			wg.Go(func() {
				_ = p.cmd.Process.Signal(syscall.SIGTERM)
				select {
				case <-p.done:
				case <-time.After(stopGrace):
					_ = p.cmd.Process.Kill()
					<-p.done
				}
			})
		}
		wg.Wait()
		if s.provider == "local" {
			// A local sandbox's gate and sampler outlive the stack until a destroy
			p := local.New(local.Options{Name: "local", Root: filepath.Join(s.dir, "local")})
			owned, _ := p.ListOwned(context.Background(), nil)
			for _, o := range owned {
				_ = p.Destroy(context.Background(), o.Ref)
			}
		}
		if t.Failed() {
			for _, p := range s.procs {
				raw, _ := os.ReadFile(p.log)
				lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
				t.Logf("last lines of %s:\n%s", p.name,
					strings.Join(lines[max(len(lines)-40, 0):], "\n"))
			}
		}
		_ = os.RemoveAll(s.dir)
	})
}

// dev runs bugnet-dev against the stack's router and answers its stdout
func (s *stack) dev(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command(filepath.Join(s.bin, "bugnet-dev"),
		append([]string{"--config", s.settings}, args...)...)
	cmd.Dir, cmd.Env = s.root, s.env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("live: bugnet-dev %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return string(out)
}

/* Without a provider */

// testFront is dev-front.toml with its ports and wildcard moved to ones a test owns
func testFront(t *testing.T) front {
	t.Helper()
	f, err := loadFront(filepath.Join(repo(t), "config", "dev-front.toml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []*int{&f.Ports.Edge, &f.Ports.Secrets, &f.Ports.Egress, &f.Ports.SSH} {
		_, port, _ := net.SplitHostPort(freeAddr(t))
		*p, _ = strconv.Atoi(port)
	}
	f.ACME.WildcardDir = t.TempDir()
	return f
}

// writePair writes a self-signed pair valid until notAfter into dir
func writePair(t *testing.T, dir string, notAfter time.Time) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"*.x.example"},
		NotBefore: notAfter.Add(-90 * 24 * time.Hour), NotAfter: notAfter}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	chain := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(filepath.Join(dir, "fullchain.pem"), chain, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "key.pem"), keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
}

func refused(t *testing.T, err error, words ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("no refusal, want one naming %q", words)
	}
	for _, w := range words {
		if !strings.Contains(err.Error(), w) {
			t.Fatalf("refusal %q does not name %q", err, w)
		}
	}
}

func TestStackFrontedRefusals(t *testing.T) {
	now := time.Now()
	f := testFront(t)
	refused(t, checkPlan(nil, "local", f, now), "-providers")
	refused(t, checkPlan([]string{"nope"}, "local", f, now), `"nope"`)
	refused(t, checkPlan([]string{"e2b"}, "fronted", f, now), "-front")
	eq(t, checkPlan([]string{"e2b", "boat"}, "local", f, now), nil)

	refused(t, checkPlan([]string{"e2b"}, "vps", f, now), "missing", f.ACME.WildcardDir,
		"make dev-certs")
	writePair(t, f.ACME.WildcardDir, now.Add(-time.Hour))
	refused(t, checkPlan([]string{"e2b"}, "vps", f, now), "expired", f.ACME.WildcardDir)
	writePair(t, f.ACME.WildcardDir, now.Add(30*24*time.Hour))
	eq(t, checkPlan([]string{"e2b"}, "vps", f, now), nil)

	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.Ports.Egress)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	refused(t, checkPlan([]string{"e2b"}, "vps", f, now), strconv.Itoa(f.Ports.Egress),
		"egress", "taken")
}

func TestStackStartsOnTheLocalProvider(t *testing.T) {
	if os.Getenv("BUGNET_TEST_STACK") != "1" {
		t.Skip("set BUGNET_TEST_STACK=1 to build and start the whole stack")
	}
	f, err := loadFront(filepath.Join(repo(t), "config", "dev-front.toml"))
	if err != nil {
		t.Fatal(err)
	}
	s := startStack(t, "local", "local", f)
	t.Cleanup(func() { s.stop(t) })
	created := kv(s.dev(t, "create", "--package", "dev-small-10m"))
	eq(t, created["state"], "ready")
	file := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(file, []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	minted := kv(s.dev(t, "transfer", "--handle", created["handle"], "--upload", file))
	if !strings.HasPrefix(minted["url"], "https://"+f.Names.TransferHost+"/bnx_") ||
		minted["size"] != "3" {
		t.Fatalf("bugnet-dev transfer printed %v", minted)
	}
	pem, err := os.ReadFile(filepath.Join(s.dir, "tls", "fullchain.pem"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)
	conn, err := tls.Dial("tcp", s.edgeAddr, &tls.Config{
		RootCAs: roots, ServerName: f.Names.TransferHost, NextProtos: []string{"http/1.1"},
	})
	if err != nil {
		t.Fatalf("the edge's TLS for %s: %v", f.Names.TransferHost, err)
	}
	_ = conn.Close()
	secrets, err := tls.Dial("tcp", s.secretsAddr, &tls.Config{
		RootCAs: roots, ServerName: f.Names.SecretsHost, NextProtos: []string{"http/1.1"},
	})
	if err != nil {
		t.Fatalf("the secrets route's TLS for %s: %v", f.Names.SecretsHost, err)
	}
	_ = secrets.Close()

	sessions, err := tls.Dial("tcp", s.sessionsAddr, &tls.Config{
		RootCAs: roots, ServerName: f.Names.SessionsHost, NextProtos: []string{"http/1.1"},
	})
	if err != nil {
		t.Fatalf("the sessions route's TLS for %s: %v", f.Names.SessionsHost, err)
	}
	_ = sessions.Close()
	robots, err := exec.Command("curl", "-sS", "--connect-to",
		f.Names.SessionsHost+":443:"+s.sessionsAddr, "--cacert",
		filepath.Join(s.dir, "tls", "fullchain.pem"),
		"https://"+f.Names.SessionsHost+"/robots.txt").Output()
	if err != nil || !strings.Contains(string(robots), "Disallow: /") {
		t.Fatalf("the sessions route's robots.txt: %v %q", err, robots)
	}

	// The URL carries the file through the edge and back, byte for byte
	got := s.curlRetry(t, "-fsS", "-T", file, minted["url"])
	if !strings.Contains(got, `"size":3`) {
		t.Fatalf("the upload answered %s", got)
	}
	back := filepath.Join(t.TempDir(), "back.txt")
	s.download(t, created["handle"], "~/f.txt", back)
	raw, err := os.ReadFile(back)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, string(raw), "abc")
	code, body := s.curlStatus(t, "-sS", "-T", file, minted["url"])
	eq(t, code, "410")
	if !strings.Contains(body, "token_used") {
		t.Fatalf("a used URL answered %s", body)
	}

	// bugnet-ssh joins the stack, reached through its own PROXY v2 front
	if _, err := exec.LookPath("ssh"); err == nil {
		a := s.sshKey(t, created["handle"])
		want := strings.TrimSpace(s.sh(t, created["handle"], "id -un"))
		eq(t, strings.TrimSpace(s.sshMust(t, a, "id -un")), want)
	}
}

// kv reads bugnet-dev's key=value lines
func kv(out string) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			m[k] = v
		}
	}
	return m
}

func TestDevFrontSettings(t *testing.T) {
	f, err := loadFront(filepath.Join(repo(t), "config", "dev-front.toml"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, [5]int{f.Ports.Edge, f.Ports.Secrets, f.Ports.Sessions, f.Ports.Egress, f.Ports.SSH},
		[5]int{8443, 8445, 8448, 9443, 2223})
	eq(t, f.Names.TransferHost, "transfer.dev.bugnet.sh")
	eq(t, f.Names.SSHPort, 2200)
	eq(t, f.Edge.AbuseContact, "support@bugnet.org")
	home, _ := os.UserHomeDir()
	eq(t, f.ACME.StateDir, filepath.Join(home, ".local", "state", "bugnet-dev", "acme"))
	eq(t, f.ACME.WildcardDir, filepath.Join(home, ".local", "state", "bugnet-dev", "preview-dev"))
	eq(t, f.Egress.CADir, filepath.Join(home, ".local", "state", "bugnet-dev", "egress-ca"))
	bad := filepath.Join(t.TempDir(), "front.toml")
	if err := os.WriteFile(bad, []byte("[ports]\nedge = 1\nweb = 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = loadFront(bad)
	refused(t, err, "ports.web")
}

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
