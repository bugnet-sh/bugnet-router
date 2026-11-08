package saga

import (
	"bytes"
	"context"
	"crypto/sha256"
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

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/gateclient"
	"github.com/oynozan/bugnet-router/internal/provider"
)

// Sandbox paths of the spec files the root commands read and then delete
const (
	setupSpecPath  = "/tmp/bugnet/setup-spec.json"
	hardenSpecPath = "/tmp/bugnet/harden-spec.json"
)

const (
	// setupTimeout covers update-ca-certificates and the gate's own 10 s health wait
	setupTimeout = 90 * time.Second
	// hardenTimeout covers an nftables install of up to 60 s and the load
	hardenTimeout = 2 * time.Minute
	gateTimeout   = 30 * time.Second
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
	// caBaked says the template already holds the CA, so setup only checks its digest
	caBaked bool
	// rangesOutside says the IPv4 deny ranges are blocked at create, outside the VM
	rangesOutside bool
}

// profiles holds the four providers of DP 6.3's deny list and CP 6.9's table, others are local
var profiles = map[string]profile{
	"e2b":   {denyPorts: []int{49983}, ca: true, caBaked: true, rangesOutside: true},
	"boxd":  {ca: true},
	"koyeb": {denyPorts: []int{3030, 3031}, ca: true},
	"boat":  {denyPorts: boatPorts(), ca: true},
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
}

// hardenSpec is what bugnet-run harden reads from hardenSpecPath
type hardenSpec struct {
	V          int      `json:"v"`
	MailPorts  []int    `json:"mail_ports"`
	DenyRanges []string `json:"deny_ranges"`
	Pools      []string `json:"pools"`
	Allow      []string `json:"allow"`
}

// runnerAnswer is the one JSON line setup and harden print
type runnerAnswer struct {
	Gate     string `json:"gate"`
	CA       string `json:"ca"`
	Init     string `json:"init"`
	Firewall string `json:"firewall"`
	Error    string `json:"error"`
}

func (b BootSettings) gatePort() int {
	if b.GatePort == 0 {
		return provider.DefaultGatePort
	}
	return b.GatePort
}

// setupSpecFor builds the spec for a sandbox of provider name, its local ports read from ref
func (b BootSettings) setupSpecFor(
	name string, ref provider.Ref, gateKey string, sample bool,
) setupSpec {
	pr := profiles[name]
	spec := setupSpec{
		V: 1, GateKey: gateKey, GateListen: ":" + strconv.Itoa(b.gatePort()),
		PreviewDomain: b.PreviewDomain, Sample: sample,
	}
	deny := []int{b.gatePort(), 22, 2022}
	// A local sandbox shares the host, so its gate listens on its own loopback port
	if port := metaPort(ref, "gate_port"); port > 0 {
		spec.GateListen = "127.0.0.1:" + strconv.Itoa(port)
		deny = append(deny, port)
	}
	if port := metaPort(ref, "sshd_port"); port > 0 {
		deny = append(deny, port)
	}
	deny = append(deny, pr.denyPorts...)
	slices.Sort(deny)
	spec.DenyPorts = slices.Compact(deny)
	if pr.ca && len(b.CAPEM) > 0 {
		spec.CAVersion = b.CAVersion
		if pr.caBaked {
			sum := sha256.Sum256(b.CAPEM)
			spec.CASHA256 = hex.EncodeToString(sum[:])
		} else {
			spec.CAPEM = string(b.CAPEM)
		}
	}
	return spec
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

// insideRanges are the ranges an E2B create leaves open, so harden blocks them in the VM
func insideRanges(ranges []string) []string {
	out := []string{}
	for _, r := range ranges {
		p, err := netip.ParsePrefix(r)
		if err != nil || p.Addr().Is4() || p.Overlaps(linkLocal6) {
			continue
		}
		out = append(out, r)
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
var errorValue = regexp.MustCompile(`^[a-z_]{1,40}$`)

// rootCommand writes spec to path, runs the runner's subcommand as root and reads its answer
func rootCommand(
	ctx context.Context, prov provider.Provider, ref provider.Ref, sub, path string, spec any,
) (runnerAnswer, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return runnerAnswer{}, err
	}
	if err := prov.WriteFile(ctx, ref, path, data, 0o600); err != nil {
		return runnerAnswer{}, fmt.Errorf("write the %s spec: %w", sub, err)
	}
	argv := []string{runnerPath, sub, "--spec", path}
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
		reason := ans.Error
		if !errorValue.MatchString(reason) {
			reason = "no reason"
		}
		return ans, fmt.Errorf("%s exited %d with %s", sub, code, reason)
	}
	return ans, nil
}

/* Boot steps */

// setup runs bugnet-run setup and answers the sandbox's CA state
func (s *Saga) setup(
	ctx context.Context, prov provider.Provider, ref provider.Ref, gateKey string, sample bool,
) (runnerAnswer, error) {
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	spec := s.d.Boot.setupSpecFor(prov.Name(), ref, gateKey, sample)
	return rootCommand(ctx, prov, ref, "setup", setupSpecPath, spec)
}

// harden runs bugnet-run harden where the provider has a rule, reporting whether a failure counts
func (s *Saga) harden(
	ctx context.Context, prov provider.Provider, ref provider.Ref,
) (required bool, err error) {
	spec, rule, ok := s.d.Boot.hardenSpecFor(prov.Name())
	if !ok {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, hardenTimeout)
	defer cancel()
	ans, err := rootCommand(ctx, prov, ref, "harden", hardenSpecPath, spec)
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
	g, ok := prov.(provider.Gater)
	if !ok {
		return ref, time.Time{}, errNoGate
	}
	port := s.d.Boot.gatePort()
	pctx, cancel := context.WithTimeout(ctx, gateTimeout)
	meta, err := g.PrepareGate(pctx, ref, port)
	cancel()
	if err != nil {
		return ref, time.Time{}, fmt.Errorf("prepare the gate route: %w", err)
	}
	ref = withMeta(ref, meta)
	route, err := g.GateRoute(ref, port)
	if err != nil {
		return ref, time.Time{}, fmt.Errorf("gate route: %w", err)
	}
	for i := range gateTries {
		if i > 0 {
			timer := time.NewTimer(gateRetry)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ref, time.Time{}, ctx.Err()
			case <-timer.C:
			}
		}
		if err = gateclient.Health(ctx, route, gateKey); err == nil {
			return ref, s.now(), nil
		}
	}
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
