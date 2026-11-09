package vps

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestEgressUnitLoadsX402Credential(t *testing.T) {
	b, err := os.ReadFile("systemd/bugnet-egress@.service")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(b), "\n")
	for _, want := range []string{
		"LoadCredential=egress_x402_client_key:/etc/bugnet/keys/egress-x402-client.key",
		"LoadCredential=egress_ca_key:/etc/bugnet/keys/egress-ca.key",
		"ExecReload=/bin/kill -HUP $MAINPID",
	} {
		found := false
		for _, l := range lines {
			found = found || l == want
		}
		if !found {
			t.Errorf("the egress unit lacks %q", want)
		}
	}
}

func TestCheckComparesPaymentsTimeout(t *testing.T) {
	for _, tc := range []struct {
		egress, payments string
		pass             bool
	}{{"2s", "2s", false}, {"3s", "2s", true}} {
		dir := t.TempDir()
		write := func(name, body string) {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		write("egress.toml", "listen_addr = \"127.0.0.1:9443\"\n\n[x402]\n"+
			"payments_timeout = \""+tc.egress+"\"   # a comment\n")
		write("payments.toml", "[http]\npublic_listen = \"127.0.0.1:8450\"\n\n"+
			"[x402]                 # the signer\nsign_timeout = \""+tc.payments+"\"\n")
		cmd := exec.Command("bash", "check.sh", "timeouts")
		cmd.Env = append(os.Environ(), "BUGNET_ETC="+dir)
		out, err := cmd.CombinedOutput()
		if (err == nil) != tc.pass {
			t.Errorf("egress %s, Payments %s: passed %v, want %v:\n%s", tc.egress, tc.payments,
				err == nil, tc.pass, out)
		}
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// rules are the lines of an nft file that are neither comments nor blank
func rules(src string) []string {
	var out []string
	for _, line := range strings.Split(src, "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out
}

func equal(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestEgressLoopbackRule(t *testing.T) {
	equal(t, rules(read(t, "nftables/egress.nft")), []string{
		"table inet bugnet_egress_lo",
		"delete table inet bugnet_egress_lo",
		"table inet bugnet_egress_lo {",
		"chain output {",
		"type filter hook output priority 0; policy accept;",
		`ip daddr 127.0.0.1 tcp dport 9443 meta skuid "haproxy" accept`,
		`ip daddr 127.0.0.1 tcp dport 9443 meta skuid "bugnet-router" accept`,
		"ip daddr 127.0.0.1 tcp dport 9443 reject with tcp reset",
		"}", "}",
	})
	if !strings.Contains(HAProxyTemplate, "server egress 127.0.0.1:9443 send-proxy-v2") {
		t.Error("the rule guards a port HAProxy's egress backend does not dial")
	}
}

func TestDevFrontLoopbackRule(t *testing.T) {
	equal(t, rules(read(t, "nftables/dev-front.nft")), []string{
		"table inet bugnet_dev_front_lo",
		"delete table inet bugnet_dev_front_lo",
		"table inet bugnet_dev_front_lo {",
		"chain output {",
		"type filter hook output priority 0; policy accept;",
		`ip daddr 127.0.0.1 tcp dport 19443 meta skuid "haproxy" accept`,
		"ip daddr 127.0.0.1 tcp dport 19443 reject with tcp reset",
		"}", "}",
	})
	if !strings.Contains(HAProxyTemplate, "server mac 127.0.0.1:19443 send-proxy-v2") {
		t.Error("the rule guards a port HAProxy's dev backend does not dial")
	}
}

func TestHAProxyRunsAsItsOwnUser(t *testing.T) {
	if !regexp.MustCompile(`(?m)^    user haproxy$`).MatchString(HAProxyTemplate) {
		t.Error("global names no user haproxy, so the loopback rules would refuse its workers")
	}
}

func TestNftUnitLoadsEveryFileAndNeverFlushes(t *testing.T) {
	unit := read(t, "systemd/bugnet-nft.service")
	for _, want := range []string{"Type=oneshot", "RemainAfterExit=yes",
		"for f in /etc/bugnet/nftables/*.nft", "Before=haproxy.service",
		"WantedBy=multi-user.target"} {
		if !strings.Contains(unit, want) {
			t.Errorf("the unit lacks %q", want)
		}
	}
	for _, name := range []string{"nftables/egress.nft", "nftables/dev-front.nft",
		"systemd/bugnet-nft.service"} {
		if strings.Contains(read(t, name), "flush ruleset") {
			t.Errorf("%s flushes the ruleset, which would drop ufw's rules", name)
		}
	}
}
