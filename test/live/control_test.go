package live

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/livetest"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
)

// controlRows are CP 15's rows, each task adding its own
var controlRows []row

func TestLiveControl(t *testing.T) {
	runRows(t, controlRows)
}

func init() {
	controlRows = append(controlRows,
		row{id: "N1", run: rowN1},
		row{id: "N2", providers: []string{"e2b"}, run: rowN2},
		row{id: "E1", run: rowE1},
		row{id: "E2", run: rowE2},
		row{id: "E3", run: rowE3},
		row{id: "E4", run: rowE4},
		row{id: "F1", run: rowF1},
		row{id: "F2", run: rowF2},
		row{id: "L1", providers: []string{"e2b"}, run: rowLimits},
		row{id: "L2", providers: []string{"boat"}, run: rowLimits},
		row{id: "L3", providers: []string{"boxd"}, run: rowLimits},
		row{id: "L4", providers: []string{"koyeb"}, run: rowLimits},
	)
}

// probeScript tries each host and port for 5 s and prints open or closed for each, by index
const probeScript = `i=0
for t in "aspmx.l.google.com 25" "169.254.169.254 80" "10.0.0.1 80" "fd00:ec2::254 80"; do
	set -- $t
	if command -v bash > /dev/null; then
		timeout 5 bash -c "exec 3<>/dev/tcp/$1/$2" 2> /dev/null && r=open || r=closed
	else
		nc -z -w 5 "$1" "$2" 2> /dev/null && r=open || r=closed
	fi
	echo "$i $r"
	i=$((i + 1))
done`

// probes are probeScript's targets, in its order
var probes = []string{
	"port 25 on a public host", "169.254.169.254:80", "10.0.0.1:80", "[fd00:ec2::254]:80",
}

// probe runs probeScript in the sandbox and answers the targets it found open
func (s *stack) probe(t *testing.T, h string) []string {
	t.Helper()
	var open []string
	for _, line := range strings.Split(strings.TrimSpace(s.sh(t, h, probeScript)), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[1] == "open" {
			for i, p := range probes {
				if f[0] == string(rune('0'+i)) {
					open = append(open, p)
				}
			}
		}
	}
	return open
}

// rowN1 checks harden's rules block mail, metadata and private ranges while the sandbox works
func rowN1(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	rules := s.sh(t, h, `test -f /etc/bugnet/firewall.rules && echo kept; true`)
	if strings.TrimSpace(rules) != "kept" {
		t.Fatal("harden left no /etc/bugnet/firewall.rules, so it never ran or failed")
	}
	if open := s.probe(t, h); len(open) > 0 {
		t.Fatalf("after harden these stay reachable: %v", open)
	}
	// Exec, files and the gate route keep working, and the placement stays ready
	eq(t, strings.TrimSpace(s.sh(t, h, `echo n1 > "$HOME/bugnet-n1" && cat "$HOME/bugnet-n1"`)),
		"n1")
	s.gated(t, h).healthWithin(t, 3)
	eq(t, kv(s.dev(t, "get", "--handle", h))["state"], "ready")
	t.Logf("N1 on %s: the forwarder's check waits for the secrets rows (Tasks 21 to 24)",
		s.provider)
}

// rowN2 removes the rules inside the VM and checks E2B's denyOut still blocks 10.0.0.1
func rowN2(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	s.sh(t, h, `nft delete table inet bugnet 2> /dev/null; `+
		`iptables -F BUGNET 2> /dev/null; ip6tables -F BUGNET 2> /dev/null; true`)
	open := s.probe(t, h)
	// denyOut holds the IPv4 ranges alone, the IPv6 ones stay with harden inside the VM
	if slices.Contains(open, probes[2]) {
		t.Fatalf("with the rules inside removed %s is reachable, so denyOut does not hold",
			probes[2])
	}
	// MMDS answers inside the VM before denyOut sees the packet, so only harden holds it
	if slices.Contains(open, probes[1]) {
		t.Log("N2: 169.254.169.254 is MMDS, held only by harden")
	}
	// E2B filters by CIDR alone, so mail is blocked inside the VM only
	t.Logf("N2: with the rules inside removed, port 25 is %v",
		map[bool]string{true: "reachable", false: "still blocked"}[slices.Contains(open,
			probes[0])])
	route := strings.TrimSpace(s.sh(t, h, `ip -6 route show default 2> /dev/null; true`))
	t.Logf("N2: the VM's IPv6 default route is %q, empty when it has none", route)
}

// liveAdapter builds the real adapter of name from the environment's key and config/dev.toml
func liveAdapter(t *testing.T, name string) provider.Provider {
	t.Helper()
	env, ok := config.ProviderKeyEnv(name)
	if !ok {
		t.Fatalf("%s has no provider key variable", name)
	}
	key := os.Getenv(env)
	if key == "" {
		t.Fatalf("%s is not set, so the %s rows cannot run", env, name)
	}
	cfg := livetest.Settings(t)
	cfg.Providers = []string{name}
	cfg.ProviderKeys = map[string]string{name: key}
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := reg.Get(name)
	return p
}

// rowLimits is CP L1 to L4, the provider's limits and account calls with the runtime key
func rowLimits(t *testing.T, s *stack) {
	p := liveAdapter(t, s.provider)
	reader, ok := p.(provider.LimitsReader)
	if !ok {
		t.Fatalf("%s reads no limits", s.provider)
	}
	checker, ok := p.(provider.AccountChecker)
	if !ok {
		t.Fatalf("%s checks no account", s.provider)
	}
	ctx := t.Context()
	got, err := reader.ReadLimits(ctx)
	if err != nil {
		t.Fatalf("ReadLimits: %v", err)
	}
	// E2B's cap is a setting, so only the others give one
	if got.MaxConcurrent <= 0 && s.provider != "e2b" {
		t.Errorf("%s gave a cap of %d, want the account's", s.provider, got.MaxConcurrent)
	}
	if got.Active < 0 {
		t.Errorf("%s gave %d active sandboxes", s.provider, got.Active)
	}
	t.Logf("%s limits: cap %d, %d active", s.provider, got.MaxConcurrent, got.Active)
	acc, err := checker.CheckAccount(ctx)
	if err != nil {
		t.Fatalf("CheckAccount: %v", err)
	}
	if acc.Block != provider.BlockNone {
		t.Errorf("%s account is blocked as %q: %s", s.provider, acc.Block, acc.Detail)
	}
	if team := livetest.Settings(t).E2B.TeamID; s.provider == "e2b" && team == "" {
		t.Log("L1: e2b.team_id is empty, so the account check read the list call instead")
	}
	t.Logf("%s account: block %q", s.provider, acc.Block)
}

func TestControlRowsAndTheirProviders(t *testing.T) {
	var ids []string
	for _, r := range controlRows {
		ids = append(ids, r.id)
		if r.id == "N2" {
			eq(t, r.providers, []string{"e2b"})
		}
	}
	eq(t, slices.Contains(ids, "N1") && slices.Contains(ids, "N2"), true)
	for _, id := range []string{"E1", "E2", "E3", "E4", "F1", "F2", "L1", "L2", "L3", "L4"} {
		eq(t, slices.Contains(ids, id), true)
	}
	for _, r := range controlRows {
		switch r.id {
		case "L1":
			eq(t, r.providers, []string{"e2b"})
		case "L2":
			eq(t, r.providers, []string{"boat"})
		case "L3":
			eq(t, r.providers, []string{"boxd"})
		case "L4":
			eq(t, r.providers, []string{"koyeb"})
		}
	}
}

// fetchWithin is how long a repository may take to land in the sandbox
const fetchWithin = 3 * time.Minute

// bootstrapDone calls FetchSource and waits until the placement shows the checkout as done
func (s *stack) bootstrapDone(t *testing.T, h string) {
	t.Helper()
	first := kv(s.dev(t, "fetch-source", "--handle", h))
	if first["state"] != "running" && first["state"] != "done" {
		t.Fatalf("live: FetchSource answered %v", first)
	}
	deadline := time.Now().Add(fetchWithin)
	for {
		got := kv(s.dev(t, "get", "--handle", h))
		switch got["bootstrap_state"] {
		case "done":
			return
		case "failed":
			t.Fatalf("live: the fetch failed with %s", got["bootstrap_reason"])
		}
		if time.Now().After(deadline) {
			t.Fatalf("live: the fetch was still %s after %s", got["bootstrap_state"], fetchWithin)
		}
		time.Sleep(2 * time.Second)
	}
}

// rowF1 clones a public repository at its quoted commit through FetchSource
func rowF1(t *testing.T, s *stack) {
	const repo, sha = "github.com/octocat/Hello-World", "7fd1a60b01f91b314f59955a4e4d4e80d8edf11d"
	q, _ := s.quote(t, s.quoter(), repo)
	h := s.sandboxFor(t, xferPackage, q.GetEstimateId())
	s.bootstrapDone(t, h)
	got := strings.TrimSpace(s.sh(t, h, `cd ~/src/Hello-World && git rev-parse HEAD`))
	eq(t, got, sha)
}

// rowF2 fetches a private repository through the proxy, the router holding no token
//
// It needs a throwaway GitHub token that reads one private repository, in
// BUGNET_LIVE_GITHUB_TOKEN, and the repository as owner/name in BUGNET_LIVE_PRIVATE_REPO.
func rowF2(t *testing.T, s *stack) {
	token, repo := os.Getenv("BUGNET_LIVE_GITHUB_TOKEN"), os.Getenv("BUGNET_LIVE_PRIVATE_REPO")
	if token == "" || repo == "" {
		t.Skip("set BUGNET_LIVE_GITHUB_TOKEN and BUGNET_LIVE_PRIVATE_REPO for F2")
	}
	q, _ := s.quote(t, s.quoter(), "github.com/"+repo)
	h := s.sandboxFor(t, xferPackage, q.GetEstimateId())
	if first := kv(s.dev(t, "get", "--handle", h)); first["bootstrap_state"] != "pending" {
		t.Fatalf("live: a private source starts pending, got %v", first)
	}
	if _, stderr, err := s.devTry(t, "fetch-source", "--handle", h); err == nil ||
		!strings.Contains(stderr, "SECRET_REQUIRED") {
		t.Fatalf("live: FetchSource without a secret answered %v %s", err, stderr)
	}
	s.devStdin(t, token, "secret", "--handle", h, "--name", "GITHUB_TOKEN",
		"--host", "api.github.com", "--fetch-only")
	s.bootstrapDone(t, h)
	name := repo[strings.LastIndexByte(repo, '/')+1:]
	if out := strings.TrimSpace(s.sh(t, h, `ls ~/src/`+name+` | head -1`)); out == "" {
		t.Fatalf("live: ~/src/%s is empty", name)
	}
	left := s.shTry(t, h, `test -e ~/.bugnet/src.tar.gz && echo left`)
	if strings.Contains(left, "left") {
		t.Fatal("live: the archive stays in the sandbox")
	}
}
