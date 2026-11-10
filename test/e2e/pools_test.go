package e2e

import (
	"strings"
	"testing"
	"time"
)

const poolSettings = `
[pools]
tick = "1s"
headroom = 0

[[pools.pool]]
provider = "local"
size = "local"
min = 1
max = 2
`

// poolsWait polls bugnet-dev pools until the local pool has want warm members
func poolsWait(t *testing.T, s *stack, want string) string {
	t.Helper()
	deadline := time.Now().Add(readyWait)
	var out string
	for time.Now().Before(deadline) {
		out = s.devOK(t, s.a, "pools")
		if strings.Contains(out, "local local kept_by=router") &&
			strings.Contains(out, " warm="+want+" ") {
			return out
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("the pool never had %s warm members, bugnet-dev pools says:\n%s", want, out)
	return ""
}

func TestE2EPoolFillsAnOrderClaimsAndTheNextIsCold(t *testing.T) {
	t.Parallel()
	s := startStackWith(t, stackOptions{extra: poolSettings})
	out := poolsWait(t, s, "1")
	if !strings.Contains(out, " min=1 max=2 ") {
		t.Errorf("bugnet-dev pools says %q, want min 1 and max 2", out)
	}

	_, p := s.place(t, s.client(t, s.a), smallPkg, smallPrice)
	found := parseKV(s.devOK(t, s.a, "abuse", "find", p.GetPlacementId()))
	if found["start_path"] != "pooled" {
		t.Errorf("the order took path %q, want pooled", found["start_path"])
	}

	// The claim left the pool short, so the next tick fills it again
	poolsWait(t, s, "1")
	drained := parseKV(s.devOK(t, s.a, "pools", "drain", "--provider", "local", "--size",
		"local", "--pause", "1m"))
	if drained["retired"] != "1" {
		t.Errorf("drain retired %q members, want 1", drained["retired"])
	}
	out = poolsWait(t, s, "0")
	if !strings.Contains(out, "paused_reason=drained") {
		t.Errorf("bugnet-dev pools after a drain says %q, want a drained pause", out)
	}
	_, cold := s.place(t, s.client(t, s.a), smallPkg, smallPrice)
	found = parseKV(s.devOK(t, s.a, "abuse", "find", cold.GetPlacementId()))
	if found["start_path"] != "cold" {
		t.Errorf("the order after a drain took path %q, want cold", found["start_path"])
	}
}
