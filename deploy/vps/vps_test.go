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

func TestShipRefusesMixedHeads(t *testing.T) {
	tmp := t.TempDir()
	git := func(dir string, args ...string) {
		t.Helper()
		args = append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@t",
			"-c", "commit.gpgsign=false"}, args...)
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	ship := func(checkout string) string {
		path := filepath.Join(checkout, "deploy", "vps", "ship.sh")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(read(t, "ship.sh")), 0o755); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for _, name := range []string{"bugnet-proto", "bugnet-run", "bugnet-edges", "bugnet-router"} {
		canon := filepath.Join(tmp, name)
		git(tmp, "init", "-q", "-b", "main", canon)
		git(canon, "commit", "-q", "--allow-empty", "-m", "one")
		git(canon, "commit", "-q", "--allow-empty", "-m", "two")
		git(canon, "update-ref", "refs/remotes/origin/main", "HEAD")
		git(canon, "worktree", "add", "-q", "-b", "lane", filepath.Join(tmp, "lane", name))
	}
	canon, lane := ship(filepath.Join(tmp, "bugnet-router")), ship(filepath.Join(tmp, "lane",
		"bugnet-router"))
	check := func(script string, pass bool, want string, flags ...string) {
		t.Helper()
		cmd := exec.Command("bash", append([]string{script, "--check-heads"}, flags...)...)
		out, err := cmd.CombinedOutput()
		if (err == nil) != pass || !strings.Contains(string(out), want) {
			t.Errorf("%s %v: passed %v, want %v and %q:\n%s", script, flags, err == nil, pass,
				want, out)
		}
	}
	check(canon, true, " main ")
	check(lane, true, " lane ")
	laneProto, laneRun := filepath.Join(tmp, "lane", "bugnet-proto"), filepath.Join(tmp, "lane",
		"bugnet-run")
	git(laneProto, "update-ref", "refs/remotes/origin/lane", "HEAD~1")
	check(lane, false, "not at origin/lane")
	git(laneProto, "update-ref", "-d", "refs/remotes/origin/lane")
	git(laneRun, "reset", "-q", "--hard", "HEAD~1")
	check(lane, false, "behind main")
	git(laneRun, "checkout", "-q", "-b", "old")
	check(lane, false, " old ")
	check(lane, true, "WARNING", "--allow-mixed")
	err := os.WriteFile(filepath.Join(tmp, "bugnet-proto", "x.go"), []byte("package x\n"), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	check(canon, true, "ok, untracked build files (1)")
	git(filepath.Join(tmp, "bugnet-edges"), "commit", "-q", "--allow-empty", "-m", "three")
	check(canon, false, "not at origin/main")
}
