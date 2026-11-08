package live

import (
	"slices"
	"strings"
	"testing"
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
	)
}

// probeScript tries each host and port for 5 s and prints open or closed for each, by index
const probeScript = `i=0
for t in "aspmx.l.google.com 25" "169.254.169.254 80" "10.0.0.1 80"; do
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
var probes = []string{"port 25 on a public host", "169.254.169.254:80", "10.0.0.1:80"}

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

// rowN2 removes the rules inside the VM and checks E2B's denyOut still blocks the ranges
func rowN2(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	s.sh(t, h, `nft delete table inet bugnet 2> /dev/null; `+
		`iptables -F BUGNET 2> /dev/null; ip6tables -F BUGNET 2> /dev/null; true`)
	open := s.probe(t, h)
	for _, p := range probes[1:] {
		if slices.Contains(open, p) {
			t.Fatalf("with the rules inside removed %s is reachable, so denyOut does not hold", p)
		}
	}
	// E2B filters by CIDR alone, so mail is blocked inside the VM only
	t.Logf("N2: with the rules inside removed, port 25 is %v",
		map[bool]string{true: "reachable", false: "still blocked"}[slices.Contains(open,
			probes[0])])
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
}
