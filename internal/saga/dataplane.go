package saga

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/gateclient"
	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Sandbox paths of the spec files the root commands read and then delete
const (
	setupSpecPath  = "/tmp/bugnet/setup-spec.json"
	hardenSpecPath = "/tmp/bugnet/harden-spec.json"
	egressSpecPath = "/tmp/bugnet/egress-spec.json"
)

const (
	// setupTimeout and hardenTimeout are kept in config, which checks gpu_placing_budget with them
	setupTimeout  = config.SetupTimeout
	hardenTimeout = config.HardenTimeout
	gateTimeout   = 30 * time.Second
	// egressTimeout covers the forwarder's 5 s listen wait and sshd's 10 s reload
	egressTimeout = 60 * time.Second
	gateTries     = 3
	// maxRunnerOutput bounds what one root command may print
	maxRunnerOutput = 64 << 10
)

// gateRetry spaces the gate health tries, a variable so tests can shorten it
var gateRetry = 2 * time.Second

// Values of setup's ca answer that mean the sandbox trusts the Bugnet CA
var caTrusted = []string{"installed", "checked", "appended"}

// BootSettings are the settings Boot writes into each sandbox's spec files
type BootSettings struct {
	// GatePort is previews.gate_port, provider.DefaultGatePort when zero
	GatePort      int
	PreviewDomain string
	// CAPEM is the Bugnet CA certificate, no CA yet when empty
	CAPEM     []byte
	CAVersion int
	// MailPorts, DenyRanges and Pools are the hardening rules every harden spec carries
	MailPorts  []int
	DenyRanges []string
	Pools      []string
	// Harden holds each provider's rule by name, a provider without one never runs harden
	Harden map[string]config.HardenRule
	// EgressHost and EgressIPs are what a restricted package's sandbox may reach
	EgressHost string
	EgressIPs  []netip.Addr
}

// profile is what a provider's sandboxes need beyond the shared spec
type profile struct {
	// denyPorts are the provider's own ports the gate refuses, beside the shared ones
	denyPorts []int
	// ca says the sandbox gets the Bugnet CA, false for local
	ca bool
	// rangesOutside says the IPv4 deny ranges are blocked at create, outside the VM
	rangesOutside bool
	// noGate says the sandbox never has a gate, so it gets no gate key and setup's nogate phase
	noGate bool
}

// profiles holds DP 6.3's deny list, CP 6.9's table and the GPU providers, others are local
var profiles = map[string]profile{
	"e2b":   {denyPorts: []int{49983}, ca: true, rangesOutside: true},
	"boxd":  {ca: true},
	"koyeb": {denyPorts: []int{3030, 3031}, ca: true},
	"boat":  {denyPorts: boatPorts(), ca: true},
	"lium":  {ca: true, noGate: true},
	"hf":    {denyPorts: []int{49983}, ca: true},
}

// boatPorts are Boat's sshd, VNC, agent and streaming ports
func boatPorts() []int {
	ports := []int{22, 6080, 8090, 8911}
	for p := 47984; p <= 48010; p++ {
		ports = append(ports, p)
	}
	return ports
}

// setupSpec is what bugnet-run setup reads from setupSpecPath
type setupSpec struct {
	V             int    `json:"v"`
	GateKey       string `json:"gate_key"`
	GateListen    string `json:"gate_listen"`
	DenyPorts     []int  `json:"deny_ports"`
	CAPEM         string `json:"ca_pem,omitempty"`
	CASHA256      string `json:"ca_sha256,omitempty"`
	CAVersion     int    `json:"ca_version,omitempty"`
	PreviewDomain string `json:"preview_domain,omitempty"`
	Sample        bool   `json:"sample"`
	// Seed is fresh router randomness the runner mixes into the kernel before any other step
	Seed string `json:"seed"`
	// Phase is "pool" for a member's CA and env lines, "nogate" for every step but the gate
	Phase string `json:"phase,omitempty"`
}

// Values of setupSpec.Phase besides the empty full phase
const (
	// phasePool is a pool member, which gets no gate key and no gate yet
	phasePool = "pool"
	// phaseNoGate is every step of the full phase but the gate, with no gate key
	phaseNoGate = "nogate"
)

// seedBytes is the randomness each setup spec carries
const seedBytes = 32

// newSeed is seedBytes of the router's own randomness in base64, fresh for each setup spec
func newSeed() string {
	b := make([]byte, seedBytes)
	// crypto/rand does not fail on the platforms the router runs on
	_, _ = rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

// hardenSpec is what bugnet-run harden reads from hardenSpecPath
type hardenSpec struct {
	V          int      `json:"v"`
	MailPorts  []int    `json:"mail_ports"`
	DenyRanges []string `json:"deny_ranges"`
	Pools      []string `json:"pools"`
	Allow      []string `json:"allow"`
	// NoGateway puts the default gateway under the blocks, left out of the JSON where false
	NoGateway bool `json:"no_gateway,omitempty"`
}

// runnerAnswer is the one JSON line setup and harden print
type runnerAnswer struct {
	Gate     string `json:"gate"`
	CA       string `json:"ca"`
	Init     string `json:"init"`
	Firewall string `json:"firewall"`
	Error    string `json:"error"`
	// Port and HostKey are ssh-up's answer
	Port    int    `json:"port"`
	HostKey string `json:"host_key"`
	// Reseed is setup's answer for the seed, ioctl, write or failed
	Reseed string `json:"reseed"`
}

func (b BootSettings) gatePort() int {
	if b.GatePort == 0 {
		return provider.DefaultGatePort
	}
	return b.GatePort
}

// setupSpecFor builds the spec for a sandbox of provider name, its local ports read from ref
//
// The CA goes as its digest alone when the image's manifest lists it, else with its PEM too.
func (b BootSettings) setupSpecFor(
	name string, ref provider.Ref, gateKey string, sample bool, img *image.Manifest,
) setupSpec {
	pr := profiles[name]
	spec := setupSpec{
		V: 1, GateKey: gateKey, GateListen: ":" + strconv.Itoa(b.gatePort()),
		PreviewDomain: b.PreviewDomain, Sample: sample, DenyPorts: b.denyPorts(name, ref),
		Seed: newSeed(),
	}
	// A local sandbox shares the host, so its gate listens on its own loopback port
	if port := metaPort(ref, "gate_port"); port > 0 {
		spec.GateListen = "127.0.0.1:" + strconv.Itoa(port)
	}
	// A sandbox without a gate never gets a gate key, whatever the caller passed
	if pr.noGate {
		spec.Phase, spec.GateKey, spec.GateListen = phaseNoGate, "", ""
	}
	if pr.ca && len(b.CAPEM) > 0 {
		spec.CAVersion = b.CAVersion
		sum := sha256.Sum256(b.CAPEM)
		if img != nil && img.HasCA(b.CAVersion, sum[:]) {
			spec.CASHA256 = hex.EncodeToString(sum[:])
		} else {
			spec.CAPEM = string(b.CAPEM)
		}
	}
	return spec
}

// poolSetupSpecFor builds the spec of a pool member's setup, which has no gate key to write
func (b BootSettings) poolSetupSpecFor(
	name string, ref provider.Ref, img *image.Manifest,
) setupSpec {
	spec := b.setupSpecFor(name, ref, "", false, img)
	spec.Phase, spec.GateListen, spec.DenyPorts = phasePool, "", nil
	return spec
}

// denyPorts are the ports the gate of a sandbox of provider name refuses, sorted
func (b BootSettings) denyPorts(name string, ref provider.Ref) []int {
	deny := []int{b.gatePort(), 22, 2022}
	for _, key := range []string{"gate_port", "sshd_port"} {
		if port := metaPort(ref, key); port > 0 {
			deny = append(deny, port)
		}
	}
	deny = append(deny, profiles[name].denyPorts...)
	slices.Sort(deny)
	return slices.Compact(deny)
}

// hardenSpecFor builds the harden spec of provider name, false when it has no hardening rule
func (b BootSettings) hardenSpecFor(name string) (hardenSpec, config.HardenRule, bool) {
	rule, ok := b.Harden[name]
	if !ok {
		return hardenSpec{}, rule, false
	}
	spec := hardenSpec{
		V: 1, MailPorts: slices.Clone(b.MailPorts), DenyRanges: slices.Clone(b.DenyRanges),
		Pools: slices.Clone(b.Pools), Allow: slices.Clone(rule.Allow),
		NoGateway: rule.NoGateway,
	}
	if profiles[name].rangesOutside {
		spec.DenyRanges = insideRanges(spec.DenyRanges)
	}
	for _, list := range []*[]string{&spec.DenyRanges, &spec.Pools, &spec.Allow} {
		if *list == nil {
			*list = []string{}
		}
	}
	if spec.MailPorts == nil {
		spec.MailPorts = []int{}
	}
	return spec, rule, true
}

// linkLocal6 stays open inside an E2B VM, since blocking it there could cut envd
var linkLocal6 = netip.MustParsePrefix("fe80::/10")

// mmds is Firecracker's metadata service, answered in the VM before E2B's denyOut sees it
var mmds = netip.MustParsePrefix("169.254.169.254/32")

// insideRanges are the ranges an E2B create leaves open, so harden blocks them in the VM
func insideRanges(ranges []string) []string {
	out := []string{}
	for _, r := range ranges {
		p, err := netip.ParsePrefix(r)
		switch {
		case err != nil:
		case p.Addr().Is4():
			if p.Contains(mmds.Addr()) && !slices.Contains(out, mmds.String()) {
				out = append(out, mmds.String())
			}
		case !p.Overlaps(linkLocal6):
			out = append(out, r)
		}
	}
	return out
}

// egressAllow is what a restricted package's sandbox may reach, nil for an open package
func (b BootSettings) egressAllow(hosts []string) *provider.EgressAllow {
	if len(hosts) == 0 {
		return nil
	}
	e := &provider.EgressAllow{Hosts: slices.Clone(hosts), Proxy: slices.Clone(b.EgressIPs)}
	if b.EgressHost != "" && !slices.Contains(e.Hosts, b.EgressHost) {
		e.Hosts = append(e.Hosts, b.EgressHost)
	}
	return e
}

func metaPort(ref provider.Ref, key string) int {
	port, err := strconv.Atoi(ref.Meta[key])
	if err != nil || port < 1 || port > 65535 {
		return 0
	}
	return port
}

/* Root commands */

// errorValue matches the error values bugnet-run prints, so nothing else reaches a message
var errorValue = regexp.MustCompile(`^[a-z][a-z0-9_]{0,39}$`)

// rootCommand writes spec to path, runs the runner's subcommand as root and reads its answer
func rootCommand(
	ctx context.Context, prov provider.Provider, ref provider.Ref, sub, path string, spec any,
	extra ...string,
) (runnerAnswer, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return runnerAnswer{}, err
	}
	if err := prov.WriteFile(ctx, ref, path, data, 0o600); err != nil {
		return runnerAnswer{}, fmt.Errorf("write the %s spec: %w", sub, err)
	}
	return runRoot(ctx, prov, ref, sub, append([]string{"--spec", path}, extra...)...)
}

// runRoot runs the runner's subcommand sub as root and reads its answer
func runRoot(
	ctx context.Context, prov provider.Provider, ref provider.Ref, sub string, args ...string,
) (runnerAnswer, error) {
	argv := append([]string{image.RunnerFor(ref), sub}, args...)
	if user := prov.Capabilities().ExecUser; user != "" && user != "root" {
		argv = append([]string{"sudo", "-n"}, argv...)
	}
	stream, err := prov.Exec(ctx, ref, provider.ExecSpec{Argv: argv})
	if err != nil {
		return runnerAnswer{}, fmt.Errorf("run %s: %w", sub, err)
	}
	defer func() { _ = stream.Close() }()
	var out bytes.Buffer
	if _, err := io.Copy(&out, io.LimitReader(stream, maxRunnerOutput)); err != nil {
		return runnerAnswer{}, fmt.Errorf("read %s's answer: %w", sub, err)
	}
	_, _ = io.Copy(io.Discard, stream)
	code, err := stream.Wait()
	if err != nil {
		return runnerAnswer{}, fmt.Errorf("run %s: %w", sub, err)
	}
	var ans runnerAnswer
	lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n"))
	if jerr := json.Unmarshal(lines[len(lines)-1], &ans); jerr != nil {
		return ans, fmt.Errorf("%s exited %d with no answer", sub, code)
	}
	if code != 0 || ans.Error != "" {
		value := ans.Error
		if !errorValue.MatchString(value) {
			value = ""
		}
		return ans, &RunnerError{Sub: sub, Code: code, Value: value}
	}
	return ans, nil
}

// RunnerError is a root command that answered an error value of its own, or exited non-zero
type RunnerError struct {
	Sub  string
	Code int
	// Value is the runner's error value, such as port_443_taken, empty when it gave none
	Value string
}

// Error names the subcommand, its exit code and its error value, never anything it printed
func (e *RunnerError) Error() string {
	return fmt.Sprintf("%s exited %d with %s", e.Sub, e.Code, cmp.Or(e.Value, "no reason"))
}

/* Boot steps */

// setup runs bugnet-run setup and answers the sandbox's CA state
func (s *Saga) setup(
	ctx context.Context, prov provider.Provider, ref provider.Ref, gateKey string, sample bool,
	img *image.Manifest,
) (runnerAnswer, error) {
	ctx, span := s.span(ctx, "saga.setup")
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	spec := s.d.Boot.setupSpecFor(prov.Name(), ref, gateKey, sample, img)
	ans, err := rootCommand(ctx, prov, ref, "setup", setupSpecPath, spec)
	finish(span, err)
	return ans, err
}

// setupPool runs bugnet-run setup in its pool phase, which reseeds, installs the CA and writes the
// env lines and nothing that needs a placement
func (s *Saga) setupPool(
	ctx context.Context, prov provider.Provider, ref provider.Ref, img *image.Manifest,
) (runnerAnswer, error) {
	ctx, span := s.span(ctx, "saga.setup")
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	spec := s.d.Boot.poolSetupSpecFor(prov.Name(), ref, img)
	ans, err := rootCommand(ctx, prov, ref, "setup", setupSpecPath, spec)
	finish(span, err)
	return ans, err
}

// harden runs bugnet-run harden where the provider has a rule, reporting whether a failure counts
func (s *Saga) harden(
	ctx context.Context, prov provider.Provider, ref provider.Ref,
) (required bool, err error) {
	spec, rule, ok := s.d.Boot.hardenSpecFor(prov.Name())
	if !ok {
		return false, nil
	}
	ctx, span := s.span(ctx, "saga.harden")
	ctx, cancel := context.WithTimeout(ctx, hardenTimeout)
	defer cancel()
	ans, err := rootCommand(ctx, prov, ref, "harden", hardenSpecPath, spec)
	finish(span, err)
	if err == nil {
		s.d.Log.Info("sandbox hardened", "provider", prov.Name(), "firewall", ans.Firewall)
	}
	return rule.Required, err
}

var errNoGate = errors.New("the provider has no gate route")

// openGate prepares the provider's route, keeps its meta, and checks the gate's health
func (s *Saga) openGate(
	ctx context.Context, prov provider.Provider, ref provider.Ref, gateKey string,
) (provider.Ref, time.Time, error) {
	g, ok := provider.As[provider.Gater](prov)
	if !ok {
		return ref, time.Time{}, errNoGate
	}
	port := s.d.Boot.gatePort()
	sctx, span := s.span(ctx, "saga.prepare_gate")
	pctx, cancel := context.WithTimeout(sctx, gateTimeout)
	meta, err := g.PrepareGate(pctx, ref, port)
	cancel()
	if err != nil {
		finish(span, err)
		return ref, time.Time{}, fmt.Errorf("prepare the gate route: %w", err)
	}
	ref = withMeta(ref, meta)
	route, err := g.GateRoute(ref, port)
	finish(span, err)
	if err != nil {
		return ref, time.Time{}, fmt.Errorf("gate route: %w", err)
	}
	hctx, hspan := s.span(ctx, "saga.gate_health")
	for i := range gateTries {
		if i > 0 {
			timer := time.NewTimer(gateRetry)
			select {
			case <-ctx.Done():
				timer.Stop()
				finish(hspan, ctx.Err())
				return ref, time.Time{}, ctx.Err()
			case <-timer.C:
			}
		}
		if err = gateclient.Health(hctx, route, gateKey); err == nil {
			hspan.End()
			return ref, s.now(), nil
		}
	}
	finish(hspan, err)
	return ref, time.Time{}, fmt.Errorf("gate health: %w", err)
}

// withMeta is ref with meta's entries added, its own map left as it was
func withMeta(ref provider.Ref, meta map[string]string) provider.Ref {
	if len(meta) == 0 {
		return ref
	}
	m := make(map[string]string, len(ref.Meta)+len(meta))
	for k, v := range ref.Meta {
		m[k] = v
	}
	for k, v := range meta {
		m[k] = v
	}
	ref.Meta = m
	return ref
}

// ApplyEgress writes spec as a ready placement's egress spec and runs egress apply as root
func (s *Saga) ApplyEgress(ctx context.Context, p store.Placement, spec any) error {
	prov, ref, err := s.home(p)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, egressTimeout)
	defer cancel()
	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	if err := prov.WriteFile(ctx, ref, egressSpecPath, data, 0o600); err != nil {
		return fmt.Errorf("write the egress spec: %w", err)
	}
	_, err = runRoot(ctx, prov, ref, "egress", "apply", "--spec", egressSpecPath)
	return err
}
