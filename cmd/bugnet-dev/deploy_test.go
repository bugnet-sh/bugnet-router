package main

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// repoNames is the committed names.toml, from this package's directory
const repoNames = "../../deploy/vps/names.toml"

func eq[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func golden(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// rendered is every file deploy maps writes, by name
func rendered(t *testing.T, namesPath, sitePath string, front bool) map[string]string {
	t.Helper()
	files, err := deployFiles(namesPath, sitePath, front)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, f := range files {
		out[f.name] = f.body
	}
	return out
}

func maps(t *testing.T, namesPath, sitePath string) (string, string, string) {
	t.Helper()
	f := rendered(t, namesPath, sitePath, false)
	return f["sni.map"], f["end.map"], f["haproxy.cfg"]
}

// writeTOML writes a file into a new directory and returns its path
func writeTOML(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// replace swaps old for new in s, failing when old is not there exactly once
func replace(t *testing.T, s, old, with string) string {
	t.Helper()
	if n := strings.Count(s, old); n != 1 {
		t.Fatalf("%q appears %d times, want once", old, n)
	}
	return strings.Replace(s, old, with, 1)
}

const frontendSSH = `frontend ssh
    bind :22
    bind :::22 v6only
    maxconn 4000
    tcp-request connection track-sc0 src table st_ssh
    tcp-request connection reject if { sc0_conn_cur(st_ssh) gt 32 }
    tcp-request connection reject if { sc0_conn_rate(st_ssh) gt 60 }
    default_backend ssh

`

const (
	wantCertsHeader = "# /etc/bugnet/certs.toml, written by bugnet-dev deploy maps " +
		"from names.toml and site.toml\n" +
		"# The DNS token goes in /etc/bugnet/certs.env under the names in dns_env, " +
		"and nowhere else\n"
	wantStagingHeader = "# /etc/bugnet/certs-staging.toml, for one test run " +
		"against Let's Encrypt staging\n" +
		"# Its pairs go to their own directories, so a staging pair never stands in " +
		"for a real one\n"
)

func TestDeployMaps(t *testing.T) {
	sni, end, cfg := maps(t, repoNames, "testdata/site-a.toml")
	eq(t, sni, golden(t, "sni.map"))
	eq(t, end, golden(t, "end.map"))
	eq(t, cfg, golden(t, "haproxy-a.cfg"))
}

func TestDeploySiteBranches(t *testing.T) {
	_, _, cfg := maps(t, repoNames, "testdata/site-b.toml")
	want := golden(t, "haproxy-a.cfg")
	want = replace(t, want,
		"frontend tls\n    bind :443\n    bind :::443 v6only\n    maxconn 14000\n",
		"frontend tls\n    bind 203.0.113.7:443\n    bind :::443 v6only\n    maxconn 7000\n")
	want = replace(t, want, "    default_backend egress\n\nfrontend ssh\n",
		`    default_backend egress

frontend egress_ip
    bind 203.0.113.9:443
    maxconn 7000
    tcp-request connection track-sc0 src table st_tls
    tcp-request connection reject if { sc0_conn_cur(st_tls) gt 2000 }
    tcp-request connection reject if { sc0_conn_rate(st_tls) gt 1000 }
    default_backend egress

frontend ssh
`)
	want = replace(t, want, "frontend ssh\n    bind :22\n    bind :::22 v6only\n",
		"frontend ssh\n    bind 203.0.113.8:22\n")
	eq(t, cfg, want)

	// Each answer alone changes only its own lines
	gateway := writeTOML(t, "site.toml", "ipv4 = \"203.0.113.7\"\nadmin_ssh_port = 22\n"+
		"ssh_gateway_ip = \"203.0.113.8\"\n")
	_, _, cfg = maps(t, repoNames, gateway)
	eq(t, cfg, replace(t, golden(t, "haproxy-a.cfg"), "    bind :22\n    bind :::22 v6only\n",
		"    bind 203.0.113.8:22\n"))
	egress := writeTOML(t, "site.toml", "ipv4 = \"203.0.113.7\"\negress_ip = \"203.0.113.9\"\n")
	_, _, cfg = maps(t, repoNames, egress)
	if !strings.Contains(cfg, "frontend egress_ip\n    bind 203.0.113.9:443\n") ||
		!strings.Contains(cfg, frontendSSH) {
		t.Fatalf("egress_ip alone:\n%s", cfg)
	}
}

func TestSiteUnknownKeyFails(t *testing.T) {
	path := writeTOML(t, "site.toml", "ipv4 = \"203.0.113.7\"\n\nipv5 = \"x\"\n")
	_, err := loadSite(path)
	if err == nil || !strings.Contains(err.Error(), "line 3: unknown key ipv5") {
		t.Fatalf("got %v, want the line and the unknown key", err)
	}
	if _, err := deployFiles(repoNames, path, false); err == nil {
		t.Fatal("deploy maps took a site with an unknown key")
	}
	names := writeTOML(t, "names.toml", "production = []\nstaging = []\n")
	if _, err := loadNames(names); err == nil || !strings.Contains(err.Error(), "staging") {
		t.Fatalf("got %v, want the unknown key named", err)
	}
	entry := writeTOML(t, "names.toml",
		`dev = [{ name = "a.example.test", kind = "exact", backend = "edge", port = 1 }]`)
	if _, err := loadNames(entry); err == nil || !strings.Contains(err.Error(), "port") {
		t.Fatalf("got %v, want the unknown key named", err)
	}
}

func TestDeployFront(t *testing.T) {
	f := rendered(t, repoNames, "testdata/site-a.toml", true)
	eq(t, f["sni.map"], golden(t, "sni.map"))
	eq(t, f["end.map"], golden(t, "end.map"))
	eq(t, f["haproxy.cfg"], replace(t, golden(t, "haproxy-a.cfg"), frontendSSH, ""))
	eq(t, f["certs.toml"], wantCertsHeader+`
directory = "https://acme-v02.api.letsencrypt.org/directory"
dns_provider = "cloudflare"
dns_env = ["CF_DNS_API_TOKEN"]
cert_metrics = []

[[certificates]]
names = ["*.preview.dev.bugnet.sh"]
dir = "/var/lib/bugnet-certs/preview-dev"
`)
	eq(t, f["deploy.env"], `# What front.sh reads, from names.toml and site.toml
ADMIN_SSH_PORT=2222
PRODUCTION_CERT_DIRS=
DEV_CERT_DIRS=/var/lib/bugnet-certs/preview-dev
STAGING_CERT_DIRS=/var/lib/bugnet-certs/staging/preview-dev
`)

	// Under DP question 1 (b) the gateway's address joins in Task 39, so the front needs none
	b := writeTOML(t, "site.toml", "ipv4 = \"203.0.113.7\"\nadmin_ssh_port = 22\n")
	if _, err := deployFiles(repoNames, b, true); err != nil {
		t.Fatal(err)
	}
	_, err := deployFiles(repoNames, b, false)
	if err == nil || !strings.Contains(err.Error(), "ssh_gateway_ip") {
		t.Fatalf("got %v, want admin SSH on 22 to need ssh_gateway_ip", err)
	}
}

func TestDeployCertsSettings(t *testing.T) {
	f := rendered(t, repoNames, "testdata/site-b.toml", false)
	eq(t, f["certs.toml"], wantCertsHeader+`
directory = "https://acme-v02.api.letsencrypt.org/directory"
dns_provider = "route53"
dns_env = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_HOSTED_ZONE_ID"]

[[certificates]]
names = ["*.preview.bugnet.sh"]
dir = "/var/lib/bugnet-certs/preview"

[[certificates]]
names = ["*.preview.dev.bugnet.sh"]
dir = "/var/lib/bugnet-certs/preview-dev"
`)
	eq(t, f["certs-staging.toml"], wantStagingHeader+`
directory = "https://acme-staging-v02.api.letsencrypt.org/directory"
dns_provider = "route53"
dns_env = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_HOSTED_ZONE_ID"]
cert_metrics = []

[[certificates]]
names = ["*.preview.bugnet.sh"]
dir = "/var/lib/bugnet-certs/staging/preview"

[[certificates]]
names = ["*.preview.dev.bugnet.sh"]
dir = "/var/lib/bugnet-certs/staging/preview-dev"
`)
	eq(t, f["deploy.env"], `# What front.sh reads, from names.toml and site.toml
ADMIN_SSH_PORT=22
PRODUCTION_CERT_DIRS=/var/lib/bugnet-certs/preview
DEV_CERT_DIRS=/var/lib/bugnet-certs/preview-dev
STAGING_CERT_DIRS=/var/lib/bugnet-certs/staging/preview /var/lib/bugnet-certs/staging/preview-dev
`)
}

func TestSiteRefusals(t *testing.T) {
	for _, tc := range []struct{ doc, want string }{
		{`ipv4 = "2001:db8::1"`, "ipv4"},
		{`ipv4 = "203.0.113"`, "ipv4"},
		{`ipv4 = "::ffff:203.0.113.7"`, "ipv4"},
		{`ipv6 = "203.0.113.7"`, "ipv6"},
		{`ipv6 = "2001:db8::1%eth0"`, "ipv6"},
		{"admin_ssh_port = 0", "admin_ssh_port"},
		{"admin_ssh_port = 65536", "admin_ssh_port"},
		{"admin_ssh_port = 443", "admin_ssh_port"},
		{"admin_ssh_port = 80", "admin_ssh_port"},
		{"admin_ssh_port = 2200", "admin_ssh_port"},
		{`admin_ssh_port = "2222"`, "admin_ssh_port"},
		{`ssh_gateway_ip = "2001:db8::8"`, "ssh_gateway_ip"},
		{`ipv4 = "203.0.113.7"` + "\n" + `ssh_gateway_ip = "203.0.113.7"`, "ssh_gateway_ip"},
		{`egress_ip = "x"`, "egress_ip"},
		{`ipv4 = "203.0.113.7"` + "\n" + `egress_ip = "203.0.113.7"`, "egress_ip"},
		{`dns_provider = "Cloud Flare"`, "dns_provider"},
		{`dns_provider = ""`, "dns_provider"},
		{"dns_env = []", "dns_env"},
		{`dns_env = ["1TOKEN"]`, "dns_env"},
		{`dns_provider = "route53"`, "dns_env"},
		{"ipv4 = \"203.0.113.7\"\nipv4 = \"203.0.113.8\"", "ipv4 is already defined"},
	} {
		_, err := loadSite(writeTOML(t, "site.toml", tc.doc+"\n"))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: got %v, want an error naming %s", tc.doc, err, tc.want)
		}
	}
	empty := writeTOML(t, "site.toml", "ipv4 = \"\"\n")
	if _, err := loadSite(empty); err != nil {
		t.Fatal(err)
	}
	_, err := deployFiles(repoNames, empty, true)
	if err == nil || !strings.Contains(err.Error(), "ipv4") {
		t.Fatalf("got %v, want deploy maps to need ipv4", err)
	}
}

func TestNamesRefusals(t *testing.T) {
	line := func(entry string) string { return "production = [" + entry + "]\n" }
	ok := `{ name = "a.example.test", kind = "exact", backend = "edge" }, `
	for _, tc := range []struct{ doc, want string }{
		{line(`{ name = "a.example.test", kind = "exact", backend = "ssh" }`), "backend"},
		{line(`{ name = "a.example.test", kind = "exact", backend = "st_tls" }`), "backend"},
		{line(`{ name = "a.example.test", kind = "prefix", backend = "edge" }`), "kind"},
		{line(`{ name = "A.example.test", kind = "exact", backend = "edge" }`), "name"},
		{line(`{ name = "*.example.test", kind = "suffix", backend = "edge", cert = "a" }`),
			"name"},
		{line(`{ name = ".example.test", kind = "suffix", backend = "edge", cert = "a" }`), "name"},
		{line(`{ name = "203.0.113.7", kind = "exact", backend = "edge" }`), "name"},
		{line(ok + `{ name = "a.example.test", kind = "exact", backend = "mcp" }`), "twice"},
		{line(`{ name = "x.test", kind = "suffix", backend = "edge" }`), "cert"},
		{line(`{ name = "x.test", kind = "exact", backend = "edge", cert = "a" }`), "cert"},
		{line(`{ name = "x.test", kind = "suffix", backend = "edge", cert = "A b" }`), "cert"},
		{line(`{ name = "x.test", kind = "suffix", backend = "edge", cert = "staging" }`), "cert"},
		{line(`{ name = "x.test", kind = "suffix", backend = "edge", cert = "acme" }`), "cert"},
		{line(`{ name = "x.test", kind = "suffix", backend = "edge", cert = "a" }, ` +
			`{ name = "y.test", kind = "suffix", backend = "edge", cert = "a" }`), "cert"},
		{"production = [" + ok + "]\ndev = [" + ok + "]\n", "twice"},
		{"production = []\n", "no name"},
	} {
		_, err := loadNames(writeTOML(t, "names.toml", tc.doc))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: got %v, want an error naming %s", tc.doc, err, tc.want)
		}
	}
	// One name may be both an exact name and a suffix, as preview names are
	both := writeTOML(t, "names.toml", line(ok+
		`{ name = "a.example.test", kind = "suffix", backend = "edge", cert = "a" }`))
	if _, err := loadNames(both); err != nil {
		t.Fatal(err)
	}
}

func TestSNIBackendsAreInTheTemplate(t *testing.T) {
	_, _, cfg := maps(t, repoNames, "testdata/site-a.toml")
	defined := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^backend (\S+)\n    server `).
		FindAllStringSubmatch(cfg, -1) {
		defined[m[1]] = true
	}
	for _, b := range sniBackends {
		if !defined[b] {
			t.Errorf("backend %s has no server in haproxy.cfg", b)
		}
	}
	for _, b := range []string{"ssh", "ssh_dev"} {
		if slices.Contains(sniBackends, b) {
			t.Errorf("%s takes SSH, never a TLS name", b)
		}
	}
	// The ports the tunnel user may listen on are the dev backends' servers (DP 15.6)
	var dev []string
	for _, m := range regexp.MustCompile(`server mac (127\.0\.0\.1:\d+) send-proxy-v2`).
		FindAllStringSubmatch(cfg, -1) {
		dev = append(dev, m[1])
	}
	eq(t, strings.Join(dev, " "),
		"127.0.0.1:18443 127.0.0.1:18445 127.0.0.1:18448 127.0.0.1:19443 127.0.0.1:12223")
}

func TestCommittedSiteLoads(t *testing.T) {
	if _, err := loadSite("../../deploy/vps/site.toml"); err != nil {
		t.Fatal(err)
	}
	if _, err := loadNames(repoNames); err != nil {
		t.Fatal(err)
	}
}

func TestDeployMapsCommand(t *testing.T) {
	out := filepath.Join(t.TempDir(), "front")
	stdout, stderr, code := runCLI(t, env(nil), "deploy", "maps", "--front",
		"--names", repoNames, "--site", "testdata/site-a.toml", "--out", out)
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	want := []string{
		"sni.map", "end.map", "haproxy.cfg", "certs.toml", "certs-staging.toml", "deploy.env",
	}
	var lines []string
	for _, name := range want {
		lines = append(lines, "wrote="+filepath.Join(out, name))
		fi, err := os.Stat(filepath.Join(out, name))
		if err != nil {
			t.Fatal(err)
		}
		eq(t, fi.Mode().Perm(), os.FileMode(0o644))
	}
	eq(t, stdout, strings.Join(lines, "\n")+"\n")
	b, err := os.ReadFile(filepath.Join(out, "haproxy.cfg"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, strings.Contains(string(b), "frontend ssh\n"), false)

	for _, args := range [][]string{
		{"deploy"},
		{"deploy", "bogus"},
		{"deploy", "maps", "--site", "testdata/site-a.toml", "--out", out},
		{"deploy", "maps", "--names", repoNames, "--out", out},
		{"deploy", "maps", "--names", repoNames, "--site", "testdata/site-a.toml"},
		{"deploy", "maps", "--names", repoNames, "--site", "testdata/site-a.toml", "--out",
			out, "extra"},
	} {
		if _, stderr, code := runCLI(t, env(nil), args...); code != 2 ||
			!strings.Contains(stderr, "usage") {
			t.Errorf("%q exited %d with %q, want 2 and the usage", args, code, stderr)
		}
	}
	empty := writeTOML(t, "site.toml", "ipv4 = \"\"\n")
	_, stderr, code = runCLI(t, env(nil), "deploy", "maps", "--names", repoNames,
		"--site", empty, "--out", t.TempDir())
	if code != 1 || !strings.Contains(stderr, "ipv4") {
		t.Errorf("an empty ipv4 exited %d with %q, want 1 naming ipv4", code, stderr)
	}
}
