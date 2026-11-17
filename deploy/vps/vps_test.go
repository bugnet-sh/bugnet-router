package vps

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/testenv"
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
	for _, name := range []string{"proto", "run", "edges", "router"} {
		canon := filepath.Join(tmp, name)
		git(tmp, "init", "-q", "-b", "main", canon)
		git(canon, "commit", "-q", "--allow-empty", "-m", "one")
		git(canon, "commit", "-q", "--allow-empty", "-m", "two")
		git(canon, "update-ref", "refs/remotes/origin/main", "HEAD")
		git(canon, "worktree", "add", "-q", "-b", "lane", filepath.Join(tmp, "lane", name))
	}
	canon, lane := ship(filepath.Join(tmp, "router")), ship(filepath.Join(tmp, "lane",
		"router"))
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
	laneProto, laneRun := filepath.Join(tmp, "lane", "proto"), filepath.Join(tmp, "lane",
		"run")
	git(laneProto, "update-ref", "refs/remotes/origin/lane", "HEAD~1")
	check(lane, false, "not at origin/lane")
	git(laneProto, "update-ref", "-d", "refs/remotes/origin/lane")
	git(laneRun, "reset", "-q", "--hard", "HEAD~1")
	check(lane, false, "behind main")
	git(laneRun, "checkout", "-q", "-b", "old")
	check(lane, false, " old ")
	check(lane, true, "WARNING", "--allow-mixed")
	err := os.WriteFile(filepath.Join(tmp, "proto", "x.go"), []byte("package x\n"), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	check(canon, true, "ok, untracked build files (1)")
	git(filepath.Join(tmp, "edges"), "commit", "-q", "--allow-empty", "-m", "three")
	check(canon, false, "not at origin/main")
}

func TestBackupEncryptsKeysAndKeepsFourteen(t *testing.T) {
	for _, tool := range []string{"age", "age-keygen", "pg_dump", "pg_restore"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed", tool)
		}
	}
	db := testenv.DatabaseURL(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "create table orders (id text primary key); insert into orders "+
		"values ('o1')")
	_ = conn.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	identity, recipients := filepath.Join(tmp, "id"), filepath.Join(tmp, "recipients.txt")
	keys, out := filepath.Join(tmp, "keys"), filepath.Join(tmp, "out")
	if b, err := exec.Command("age-keygen", "-o", identity).CombinedOutput(); err != nil {
		t.Fatalf("age-keygen: %v\n%s", err, b)
	}
	pub, err := exec.Command("age-keygen", "-y", identity).Output()
	if err != nil {
		t.Fatal(err)
	}
	for path, body := range map[string]string{recipients: "# a comment\n" + string(pub),
		filepath.Join(keys, "egress-ca.key"):        "not a real key\n",
		filepath.Join(out, "router-keepme.tar.age"): ""} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for d := 1; d <= 15; d++ {
		name := filepath.Join(out, fmt.Sprintf("router-202610%02dT031500Z.tar.age", d))
		if err := os.WriteFile(name, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	backup := func(database string) error {
		cmd := exec.Command("bash", "router-backup.sh", recipients)
		cmd.Env = append(os.Environ(), "BUGNET_ROUTER_BACKUP_DIR="+out,
			"BUGNET_ROUTER_BACKUP_KEYS="+keys, "BUGNET_ROUTER_BACKUP_DB="+database,
			"BUGNET_ROUTER_BACKUP_PG_USER=")
		b, err := cmd.CombinedOutput()
		t.Logf("router-backup.sh:\n%s", b)
		return err
	}
	if err := backup(db); err != nil {
		t.Fatal(err)
	}
	names, err := filepath.Glob(filepath.Join(out, "router-2*.tar.age"))
	if err != nil || len(names) != 14 {
		t.Fatalf("kept %d backups (%v), want 14", len(names), err)
	}
	if _, err := os.Stat(filepath.Join(out, "router-keepme.tar.age")); err != nil {
		t.Errorf("a file the script did not name was removed: %v", err)
	}
	newest := names[len(names)-1]
	if fi, err := os.Stat(newest); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the newest backup is %v (%v), want mode 0600", fi.Mode(), err)
	}
	if fi, err := os.Stat(out); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("the backup folder is %v (%v), want mode 0700", fi.Mode(), err)
	}
	if bytes.Contains([]byte(read(t, newest)), []byte("not a real key")) {
		t.Error("the backup holds the key file in plain text")
	}
	ex := filepath.Join(tmp, "extract")
	if err := os.Mkdir(ex, 0o700); err != nil {
		t.Fatal(err)
	}
	sh := `age -d -i "$1" "$2" | tar -x -C "$3" -f -`
	b, err := exec.Command("bash", "-c", sh, "-", identity, newest, ex).CombinedOutput()
	if err != nil {
		t.Fatalf("decrypt: %v\n%s", err, b)
	}
	if got := read(t, filepath.Join(ex, "keys", "egress-ca.key")); got != "not a real key\n" {
		t.Errorf("keys/egress-ca.key holds %q", got)
	}
	list, err := exec.Command("pg_restore", "--list", filepath.Join(ex, "database.dump")).Output()
	if err != nil || !bytes.Contains(list, []byte("TABLE DATA public orders")) {
		t.Errorf("the dump lacks the orders rows (%v):\n%s", err, list)
	}

	if err := backup(db + "_missing"); err == nil {
		t.Error("a backup of a missing database passed")
	}
	again, _ := filepath.Glob(filepath.Join(out, "router-2*.tar.age"))
	work, _ := filepath.Glob(filepath.Join(out, ".router-*"))
	if len(again) != 14 || len(work) != 0 {
		t.Errorf("a failed backup left %d backups and %d work folders, want 14 and 0",
			len(again), len(work))
	}
}
