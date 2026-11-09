package live

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The source-address rows, DP 10.12.12. They run on Boat only, behind the VPS front.
const (
	// sourceModeEnv sets the dev router's Boat mode, enforce when it is empty
	sourceModeEnv = "BUGNET_LIVE_SOURCE_MODE"
	// sourceFamiliesEnv sets boat.egress_families, a comma list, the router's default when empty
	sourceFamiliesEnv = "BUGNET_LIVE_EGRESS_FAMILIES"
	// srcSandboxes is how many sandboxes SRC1 measures
	srcSandboxes = 5
	// srcRoot is where a sandbox's copy of the forwarder keeps its test root
	srcRoot = "/tmp/bugnet-src"
	// srcRunner is the runner setup copied into every sandbox
	srcRunner = "/usr/local/lib/bugnet/bugnet-run"
	// rebootWithin is how long SRC3 gives a rebooted sandbox to come back
	rebootWithin = 3 * time.Minute
	// afterRebootWithin is DP 10.12.12's bound from the sandbox being back
	afterRebootWithin = 70 * time.Second
)

func init() {
	for _, r := range []struct {
		id  string
		run func(t *testing.T, s *stack)
	}{
		{"SRC1", rowSRC1}, {"SRC2", rowSRC2}, {"SRC3", rowSRC3}, {"SRC4", rowSRC4},
		{"SRC5", rowSRC5},
	} {
		dataplaneRows = append(dataplaneRows,
			row{id: r.id, providers: []string{"boat"}, vps: true, run: r.run})
	}
}

// sourceMode is the dev router's Boat mode for this run
func sourceMode() string {
	if m := os.Getenv(sourceModeEnv); m != "" {
		return m
	}
	return "enforce"
}

// sourceCheckSettings puts the run's Boat mode and families into the router's settings
func (s *stack) sourceCheckSettings(m map[string]any) {
	if s.provider != "boat" || s.mode != "vps" {
		return
	}
	table(table(m, "secrets"), "source_check")["modes"] = map[string]any{"boat": sourceMode()}
	var families []string
	for _, f := range strings.Split(os.Getenv(sourceFamiliesEnv), ",") {
		if f = strings.TrimSpace(f); f != "" {
			families = append(families, f)
		}
	}
	if len(families) > 0 {
		table(m, "boat")["egress_families"] = families
	}
}

// needMode skips a row that belongs to another mode of the dev router
func needMode(t *testing.T, want string) {
	t.Helper()
	if got := sourceMode(); got != want {
		t.Skipf("%s needs %s=%s, this run has %s", t.Name(), sourceModeEnv, want, got)
	}
}

/* Measuring */

// metricValue is the value of the series in a Prometheus body, 0 when it is absent
func metricValue(body, series string) float64 {
	for _, line := range strings.Split(body, "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			if err == nil {
				return v
			}
		}
	}
	return 0
}

// sourceCounts are the proxy's verdict counts and its 403 count
type sourceCounts struct {
	admitted, unmatched, refused, forbidden float64
}

func (s *stack) sourceCounts(t *testing.T) sourceCounts {
	t.Helper()
	resp, err := http.Get("http://" + s.egressMetrics + "/metrics")
	if err != nil {
		t.Fatalf("live: the proxy's metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	body := string(b)
	verdict := func(v string) float64 {
		return metricValue(body, `egress_tunnel_sources_total{verdict="`+v+`"}`)
	}
	return sourceCounts{
		admitted: verdict("admitted"), unmatched: verdict("unmatched"), refused: verdict("refused"),
		forbidden: metricValue(body, `egress_tunnel_refused_total{status="403"}`),
	}
}

// egressLogCount counts the proxy's log lines that hold word
func (s *stack) egressLogCount(word string) int {
	b, _ := os.ReadFile(filepath.Join(s.dir, "bugnet-egress.log"))
	return strings.Count(string(b), word)
}

// boatInfo is what Boat's API says about a machine's network
type boatInfo struct {
	IP, SSHEndpoint string
}

func (s *stack) boatInfo(t *testing.T, g gated) boatInfo {
	t.Helper()
	var got struct {
		Sandbox struct {
			IP          *string `json:"ip"`
			SSHEndpoint *string `json:"sshEndpoint"`
		} `json:"sandbox"`
	}
	s.apiGet(t, "https://boat.dev/api/v1/sandboxes/"+g.p.ProviderRef.ID,
		map[string]string{"Authorization": "Bearer " + os.Getenv("BOAT_API_KEY")}, &got)
	var out boatInfo
	if got.Sandbox.IP != nil {
		out.IP = *got.Sandbox.IP
	}
	if got.Sandbox.SSHEndpoint != nil {
		out.SSHEndpoint = *got.Sandbox.SSHEndpoint
	}
	return out
}

// ownIPv4 says Boat's ip is an IPv4 the machine owns, as DP 10.12.2 reads it: no ssh endpoint
func ownIPv4(i boatInfo) bool {
	a, err := netip.ParseAddr(i.IP)
	return err == nil && a.Unmap().Is4() && i.SSHEndpoint == ""
}

// hasFamily says ips holds an address of the family
func hasFamily(ips []string, v6 bool) bool {
	return len(familyOf(ips, v6)) > 0
}

// familyOf keeps the addresses of one family
func familyOf(ips []string, v6 bool) []string {
	return slices.DeleteFunc(slices.Clone(ips), func(ip string) bool {
		a, err := netip.ParseAddr(ip)
		return err != nil || a.Unmap().Is4() == v6
	})
}

/* Sandboxes */

// srcBox is a sandbox with an echo secret and the source the router stored for it
type srcBox struct {
	h, value, placeholder string
	g                     gated
	info                  boatInfo
}

func (s *stack) srcBox(t *testing.T) srcBox {
	t.Helper()
	h, value, ph := s.echoSandbox(t)
	g := s.gated(t, h)
	return srcBox{h: h, value: value, placeholder: ph, g: g, info: s.boatInfo(t, g)}
}

// prefix is the stored source, invalid when the router stored none
func (b srcBox) prefix() netip.Prefix { return b.g.p.EgressSource.Prefix }

// needPrefix skips a row for a sandbox whose address no forwarder can use
func (b srcBox) needPrefix(t *testing.T) {
	t.Helper()
	if !b.prefix().IsValid() {
		t.Skipf("the router stored no source for ip %q (family not served), the case is skipped",
			b.info.IP)
	}
}

// echo asks the echo host from inside the sandbox through its own tunnel
func (s *stack) echo(t *testing.T, h string) string {
	t.Helper()
	return s.shTry(t, h, `curl -sS --max-time 20 -H "Authorization: Bearer $`+secretName+
		`" https://`+echoHost+`/headers`)
}

// tunnelToken reads the sandbox's tunnel token as root. It is never logged
func (s *stack) tunnelToken(t *testing.T, h string) string {
	t.Helper()
	tok := strings.TrimSpace(s.sh(t, h, asRoot+`$S cat /etc/bugnet/egress.token`))
	if !strings.HasPrefix(tok, "bnt_") {
		t.Fatal("live: the sandbox's egress.token holds no tunnel token")
	}
	return tok
}

/* Forwarders outside the sandbox */

// forwardCfg is what a copied-out forwarder dials
type forwardCfg struct {
	Endpoint string
	IPs      []string
	Family   string
	Bind     string
}

// forwardJSON is the egress.json of a test-root forwarder, system roots and a free local port
func forwardJSON(c forwardCfg) []byte {
	m := map[string]any{
		"endpoint": c.Endpoint, "ips": c.IPs, "port": 443, "listen": "127.0.0.1:0",
		"token_file": "/etc/bugnet/egress.token", "hosts": []string{echoHost},
	}
	if c.Family != "" {
		m["family"] = c.Family
	}
	if c.Bind != "" {
		m["bind"] = c.Bind
	}
	b, _ := json.Marshal(m)
	return b
}

func (s *stack) copiedCfg(ips []string, family, bind string) forwardCfg {
	return forwardCfg{Endpoint: s.f.Names.EgressHost, IPs: ips, Family: family, Bind: bind}
}

// macForward runs bugnet-run forward on this machine with the copied token and answers where it
// listens. Only the process started here is ever killed
func (s *stack) macForward(t *testing.T, token string, cfg forwardCfg) string {
	t.Helper()
	root := t.TempDir()
	etc := filepath.Join(root, "etc", "bugnet")
	if err := os.MkdirAll(etc, 0o755); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(etc, "egress.token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	conf := filepath.Join(etc, "egress.json")
	if err := os.WriteFile(conf, forwardJSON(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(filepath.Join(s.bin, "bugnet-run"), "forward", "--config", conf,
		"--root", root)
	if err := cmd.Start(); err != nil {
		t.Fatalf("live: start the Mac's forwarder: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	addr := filepath.Join(root, "run", "bugnet", "forward.addr")
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(addr); err == nil && strings.TrimSpace(string(b)) != "" {
			return strings.TrimSpace(string(b))
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("live: the Mac's forwarder never listened")
	return ""
}

// throughForwarder asks the echo host at a forwarder with the placeholder, trusting the Bugnet CA
func (s *stack) throughForwarder(t *testing.T, listen, placeholder string) (string, error) {
	t.Helper()
	pem, err := os.ReadFile(s.egress.certFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)
	tr := &http.Transport{
		DisableKeepAlives: true,
		DialTLSContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			raw, err := d.DialContext(ctx, "tcp", listen)
			if err != nil {
				return nil, err
			}
			tc := tls.Client(raw, &tls.Config{ServerName: echoHost, RootCAs: roots})
			if err := tc.HandshakeContext(ctx); err != nil {
				_ = raw.Close()
				return nil, err
			}
			return tc, nil
		},
	}
	defer tr.CloseIdleConnections()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"https://"+echoHost+"/headers", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+placeholder)
	resp, err := (&http.Client{Transport: tr, Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return string(body), fmt.Errorf("status %d", resp.StatusCode)
	}
	return string(body), nil
}

// copiedForwardScript starts a copy of the forwarder in a sandbox's test root, with the token and
// config of another sandbox, and asks the echo host through it with the placeholder
func copiedForwardScript(cfg forwardCfg, token, placeholder string) string {
	return `r=` + srcRoot + `; rm -rf $r; mkdir -p $r/etc/bugnet $r/run/bugnet; umask 077; ` +
		`printf '%s\n' '` + token + `' > $r/etc/bugnet/egress.token; ` +
		`cat > $r/etc/bugnet/egress.json <<'JSON'` + "\n" + string(forwardJSON(cfg)) + "\nJSON\n" +
		background(srcRunner+` forward --config $r/etc/bugnet/egress.json --root $r`,
			"src-forward") + "\n" +
		`for i in $(seq 50); do [ -s $r/run/bugnet/forward.addr ] && break; sleep 0.2; done; ` +
		`port=$(cut -d: -f2 $r/run/bugnet/forward.addr); ` +
		`curl -sS --max-time 25 --connect-to ` + echoHost + `:443:127.0.0.1:$port ` +
		`-H "Authorization: Bearer ` + placeholder + `" https://` + echoHost + `/headers 2>&1`
}

// vpsIPs are the proxy's addresses the dev router pins in every forwarder
func (s *stack) vpsIPs() []string { return s.f.VPS.EgressIPs }

/* Rows */

// rowSRC1 measures five sandboxes: the router stores each one's address, and its own tunnels are
// admitted from it
func rowSRC1(t *testing.T, s *stack) {
	needMode(t, "enforce")
	before := s.sourceCounts(t)
	var served, skipped, own4 int
	for i := 1; i <= srcSandboxes; i++ {
		b := s.srcBox(t)
		ifaces := strings.TrimSpace(s.shTry(t, b.h,
			`ip -o addr show 2>&1 | awk '{print $2, $3, $4}'`))
		t.Logf("SRC1 #%d: ip %q, sshEndpoint %q, stored prefix %q, interfaces:\n%s", i,
			b.info.IP, b.info.SSHEndpoint, b.prefix().String(), ifaces)
		if ownIPv4(b.info) {
			own4++
		}
		if !b.prefix().IsValid() {
			skipped++
			t.Logf("SRC1 #%d: no source stored, its family is not served by this run", i)
			continue
		}
		ip, err := netip.ParseAddr(b.info.IP)
		if err != nil || !b.prefix().Contains(ip.Unmap()) {
			t.Errorf("SRC1 #%d: the stored prefix %s does not hold ip %q", i, b.prefix(), b.info.IP)
			continue
		}
		if out := s.echo(t, b.h); !strings.Contains(out, b.value) {
			t.Errorf("SRC1 #%d: the sandbox's own tunnel was not admitted: %s", i,
				strings.TrimSpace(out))
			continue
		}
		served++
	}
	after := s.sourceCounts(t)
	if after.refused != before.refused {
		t.Errorf("SRC1: the proxy refused %v tunnels from the sandboxes' own addresses",
			after.refused-before.refused)
	}
	if after.admitted-before.admitted < float64(served) {
		t.Errorf("SRC1: %d tunnels worked but the proxy admitted %v", served,
			after.admitted-before.admitted)
	}
	t.Logf("SRC1: %d of %d admitted from their own address, %d skipped, %d had an IPv4 of their "+
		"own", served, srcSandboxes, skipped, own4)
	if own4 == 0 {
		t.Log("SRC1: no sandbox had an IPv4 of its own, so ipv4 leaves boat.egress_families " +
			"until a later run proves it")
	}
}

// copies is what the copied token and placeholder got on the Mac and in a second sandbox
type copies struct {
	macBody string
	macErr  error
	boxOut  string
}

// copyOut runs the copied token and placeholder of a through a forwarder on the Mac and in a
// second sandbox
func (s *stack) copyOut(t *testing.T, a srcBox) copies {
	t.Helper()
	token := s.tunnelToken(t, a.h)
	cfg := s.copiedCfg(s.vpsIPs(), "", "")
	listen := s.macForward(t, token, cfg)
	var c copies
	c.macBody, c.macErr = s.throughForwarder(t, listen, a.placeholder)
	other := s.sandbox(t, xferPackage)
	c.boxOut = s.shTry(t, other, copiedForwardScript(cfg, token, a.placeholder))
	return c
}

// rowSRC2 copies a sandbox's token and placeholder out. Both copies get 403 at the upgrade, while
// the sandbox's own tunnels keep working
func rowSRC2(t *testing.T, s *stack) {
	needMode(t, "enforce")
	a := s.srcBox(t)
	a.needPrefix(t)
	if out := s.echo(t, a.h); !strings.Contains(out, a.value) {
		t.Fatalf("SRC2: the sandbox's own tunnel failed before the copy: %s",
			strings.TrimSpace(out))
	}
	before := s.sourceCounts(t)
	got := s.copyOut(t, a)
	if got.macErr == nil || strings.Contains(got.macBody, a.value) {
		t.Errorf("SRC2: the copy on the Mac got through: %s", got.macBody)
	}
	if strings.Contains(got.boxOut, a.value) {
		t.Errorf("SRC2: the copy in a second sandbox got through: %s", got.boxOut)
	}
	after := s.sourceCounts(t)
	if got := after.forbidden - before.forbidden; got < 2 {
		t.Errorf("SRC2: the proxy answered %v 403s at the upgrade, want 2", got)
	}
	if n := s.egressLogCount("tunnel_source_refused"); n < 2 {
		t.Errorf("SRC2: the proxy logged %d tunnel_source_refused lines, want 2", n)
	}
	if out := s.echo(t, a.h); !strings.Contains(out, a.value) {
		t.Errorf("SRC2: the sandbox's own tunnel stopped working: %s", strings.TrimSpace(out))
	}
}

// rowSRC3 reboots a sandbox and expects its secret hosts back within 70 s of its return
func rowSRC3(t *testing.T, s *stack) {
	needMode(t, "enforce")
	a := s.srcBox(t)
	a.needPrefix(t)
	if out := s.echo(t, a.h); !strings.Contains(out, a.value) {
		t.Fatalf("SRC3: the tunnel failed before the reboot: %s", strings.TrimSpace(out))
	}
	// The reboot ends the job's connection, so its error is expected
	s.shTry(t, a.h, asRoot+`$S systemctl reboot`)
	time.Sleep(10 * time.Second)
	deadline := time.Now().Add(rebootWithin)
	for {
		if _, _, err := s.devTry(t, "exec", "--handle", a.h, "--", "true"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("SRC3: the sandbox was not back within %s", rebootWithin)
		}
		time.Sleep(3 * time.Second)
	}
	back := time.Now()
	for {
		if out := s.echo(t, a.h); strings.Contains(out, a.value) {
			break
		}
		if time.Since(back) > afterRebootWithin {
			t.Fatalf("SRC3: the secret host did not work within %s of the sandbox being back",
				afterRebootWithin)
		}
		time.Sleep(3 * time.Second)
	}
	again := s.boatInfo(t, a.g)
	t.Logf("SRC3: the secret host worked %s after the sandbox was back; ip %q before, %q after; "+
		"changed: %t", time.Since(back).Round(time.Second), a.info.IP, again.IP,
		again.IP != a.info.IP)
}

// rowSRC4 is SRC2 in report mode: both copies are served and logged as unmatched
func rowSRC4(t *testing.T, s *stack) {
	needMode(t, "report")
	a := s.srcBox(t)
	a.needPrefix(t)
	before := s.sourceCounts(t)
	got := s.copyOut(t, a)
	if got.macErr != nil || !strings.Contains(got.macBody, a.value) {
		t.Errorf("SRC4: the copy on the Mac was not served: %v %s", got.macErr, got.macBody)
	}
	if !strings.Contains(got.boxOut, a.value) {
		t.Errorf("SRC4: the copy in a second sandbox was not served: %s", got.boxOut)
	}
	after := s.sourceCounts(t)
	if got := after.unmatched - before.unmatched; got < 2 {
		t.Errorf("SRC4: the proxy counted %v unmatched tunnels, want 2", got)
	}
	if n := s.egressLogCount("tunnel_source_unmatched"); n < 2 {
		t.Errorf("SRC4: the proxy logged %d tunnel_source_unmatched lines, want 2", n)
	}
}

// rowSRC5 gives a second sandbox the first one's IPv6 address and dials with it bound. A pass is a
// 403 at the upgrade, or no route or no answer. A failure goes to the founder with the evidence
func rowSRC5(t *testing.T, s *stack) {
	needMode(t, "enforce")
	if !hasFamily(s.vpsIPs(), true) {
		t.Skip("SRC5 needs the VPS's IPv6 in secrets.egress_ips (DP question 15)")
	}
	a := s.srcBox(t)
	a.needPrefix(t)
	if ip, err := netip.ParseAddr(a.info.IP); err != nil || !ip.Is6() {
		t.Skipf("SRC5 is IPv6 only, the sandbox's ip is %q", a.info.IP)
	}
	token := s.tunnelToken(t, a.h)
	other := s.sandbox(t, xferPackage)
	iface := strings.TrimSpace(s.shTry(t, other, `ip -6 route show default | `+
		`awk '{for (i = 1; i < NF; i++) if ($i == "dev") {print $(i+1); exit}}'`))
	if iface == "" {
		t.Skip("SRC5: the second sandbox has no IPv6 default route")
	}
	before := s.sourceCounts(t)
	add := asRoot + `$S ip -6 addr add ` + a.info.IP + `/128 dev ` + iface + ` 2>&1`
	if out := strings.TrimSpace(s.shTry(t, other, add)); out != "" {
		t.Logf("SRC5: ip addr add said: %s", out)
	}
	cfg := s.copiedCfg(familyOf(s.vpsIPs(), true), "ipv6", a.info.IP)
	out := s.shTry(t, other, copiedForwardScript(cfg, token, a.placeholder))
	evidence := s.shTry(t, other, `ip -6 -o addr show 2>&1 | awk '{print $2, $4}'`)
	after := s.sourceCounts(t)
	switch {
	case strings.Contains(out, a.value):
		t.Errorf("SRC5: a co-tenant that took the address got through, other Boat sandboxes are "+
			"not stopped by the check. Send this to the founder beside DP question 17.\n"+
			"second sandbox's interfaces:\n%s", evidence)
	case after.forbidden > before.forbidden:
		t.Logf("SRC5: 403 at the upgrade. Second sandbox's interfaces:\n%s", evidence)
	default:
		t.Logf("SRC5: no route or no answer (%s). Second sandbox's interfaces:\n%s",
			strings.TrimSpace(out), evidence)
	}
}

/* Unit tests of the helpers */

func TestSourceRowsAndTheirProviders(t *testing.T) {
	want := map[string]bool{"SRC1": true, "SRC2": true, "SRC3": true, "SRC4": true, "SRC5": true}
	for _, r := range dataplaneRows {
		if want[r.id] {
			eq(t, r.providers, []string{"boat"})
			eq(t, r.vps, true)
			delete(want, r.id)
		}
	}
	eq(t, len(want), 0)
}

func TestMetricValue(t *testing.T) {
	body := "# HELP x y\negress_tunnel_sources_total{verdict=\"admitted\"} 3\n" +
		"egress_tunnel_sources_total{verdict=\"refused\"} 1.5\n" +
		"egress_tunnel_refused_total{status=\"403\"} 7\n"
	eq(t, metricValue(body, `egress_tunnel_sources_total{verdict="admitted"}`), 3.0)
	eq(t, metricValue(body, `egress_tunnel_sources_total{verdict="refused"}`), 1.5)
	eq(t, metricValue(body, `egress_tunnel_refused_total{status="403"}`), 7.0)
	eq(t, metricValue(body, `egress_tunnel_sources_total{verdict="unmatched"}`), 0.0)
}

func TestForwardJSON(t *testing.T) {
	var got map[string]any
	if err := json.Unmarshal(forwardJSON(forwardCfg{Endpoint: "egress.dev.example",
		IPs: []string{"203.0.113.7"}}), &got); err != nil {
		t.Fatal(err)
	}
	eq(t, got["endpoint"], any("egress.dev.example"))
	eq(t, got["listen"], any("127.0.0.1:0"))
	eq(t, got["port"], any(443.0))
	_, hasFamily := got["family"]
	_, hasBind := got["bind"]
	eq(t, hasFamily || hasBind, false)
	if err := json.Unmarshal(forwardJSON(forwardCfg{Endpoint: "e.example",
		IPs: []string{"2001:db8::7"}, Family: "ipv6", Bind: "2001:db8::5"}), &got); err != nil {
		t.Fatal(err)
	}
	eq(t, got["family"], any("ipv6"))
	eq(t, got["bind"], any("2001:db8::5"))
}

func TestCopiedForwardScriptKeepsTheTokenOutOfTheEnvironment(t *testing.T) {
	cfg := forwardCfg{Endpoint: "egress.dev.example", IPs: []string{"203.0.113.7"}}
	script := copiedForwardScript(cfg, "bnt_x", "bnsec_y")
	for _, want := range []string{"--root $r", srcRunner + " forward", "egress.token",
		"--connect-to " + echoHost + ":443:127.0.0.1:$port", "Bearer bnsec_y"} {
		eq(t, strings.Contains(script, want), true)
	}
	eq(t, strings.Contains(script, "export"), false)
}

func TestFamilyHelpers(t *testing.T) {
	ips := []string{"62.72.21.74", "2a02:4780:c:ab47::1"}
	eq(t, familyOf(ips, true), []string{"2a02:4780:c:ab47::1"})
	eq(t, familyOf(ips, false), []string{"62.72.21.74"})
	eq(t, hasFamily(ips[:1], true), false)
	eq(t, hasFamily(ips, true), true)
	eq(t, ownIPv4(boatInfo{IP: "203.0.113.10"}), true)
	eq(t, ownIPv4(boatInfo{IP: "203.0.113.10", SSHEndpoint: "ssh.boat.dev:2201"}), false)
	eq(t, ownIPv4(boatInfo{IP: "2001:db8::5"}), false)
	eq(t, ownIPv4(boatInfo{}), false)
}

func TestSourceCheckSettings(t *testing.T) {
	m := map[string]any{"secrets": map[string]any{"source_check": map[string]any{
		"modes": map[string]any{}}}}
	(&stack{provider: "e2b", mode: "vps"}).sourceCheckSettings(m)
	eq(t, len(table(table(m, "secrets"), "source_check")["modes"].(map[string]any)), 0)
	t.Setenv(sourceModeEnv, "report")
	t.Setenv(sourceFamiliesEnv, "ipv6, ipv4")
	(&stack{provider: "boat", mode: "vps"}).sourceCheckSettings(m)
	eq(t, table(table(m, "secrets"), "source_check")["modes"],
		any(map[string]any{"boat": "report"}))
	eq(t, table(m, "boat")["egress_families"], any([]string{"ipv6", "ipv4"}))
}
