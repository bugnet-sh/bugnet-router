package saga_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

func TestSetupSpecNoGateOnLium(t *testing.T) {
	ca := testCA(t)
	b := saga.BootSettings{CAPEM: ca, CAVersion: 2, PreviewDomain: "preview.bugnet.sh"}
	ref := provider.Ref{ID: "x"}
	for _, sample := range []bool{false, true} {
		spec := saga.SetupSpecFor(b, "lium", ref, "", sample)
		eq(t, spec.Phase, "nogate")
		eq(t, spec.GateKey, "")
		eq(t, spec.GateListen, "")
		eq(t, spec.Sample, sample)
		eq(t, spec.CAPEM, string(ca))
		eq(t, spec.CAVersion, 2)
		eq(t, spec.DenyPorts, []int{22, 2022, 29417})
		eq(t, spec.Seed != "", true)
	}
	// A key a caller passes never reaches a nogate spec
	raw, err := json.Marshal(saga.SetupSpecFor(b, "lium", ref, "a-gate-key", false))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "a-gate-key") {
		t.Fatalf("the lium spec carries the gate key: %s", raw)
	}
	eq(t, saga.PoolSetupSpecFor(b, "lium", ref).Phase, "pool")
}

func TestSetupSpecFullPhaseOnHF(t *testing.T) {
	ca := testCA(t)
	b := saga.BootSettings{CAPEM: ca, CAVersion: 2}
	ref := provider.Ref{ID: "x"}
	hf := saga.SetupSpecFor(b, "hf", ref, "key", false)
	eq(t, hf.Phase, "")
	eq(t, hf.GateKey, "key")
	eq(t, hf.GateListen, ":29417")
	eq(t, hf.DenyPorts, []int{22, 2022, 29417, 49983})
	eq(t, hf.CAPEM, string(ca))
	// The other providers keep the full phase
	for _, name := range []string{"e2b", "boxd", "koyeb", "boat", "local", "a"} {
		spec := saga.SetupSpecFor(b, name, ref, "key", true)
		eq(t, spec.Phase, "")
		eq(t, spec.GateKey, "key")
	}
}

func TestHardenSpecNoGatewayOnLium(t *testing.T) {
	b := saga.BootSettings{
		MailPorts: []int{25}, DenyRanges: []string{"10.0.0.0/8"},
		Harden: map[string]config.HardenRule{
			"lium": {Required: true}, "hf": {}, "boat": {Required: true},
		},
	}
	lium, ok := saga.HardenSpecFor(b, "lium")
	eq(t, ok, true)
	eq(t, lium.NoGateway, true)
	raw, _ := json.Marshal(lium)
	eq(t, string(raw), `{"v":1,"mail_ports":[25],"deny_ranges":["10.0.0.0/8"],"pools":[],`+
		`"allow":[],"no_gateway":true}`)
	// Every other provider's spec has no no_gateway field, as before
	for _, name := range []string{"hf", "boat"} {
		spec, ok := saga.HardenSpecFor(b, name)
		eq(t, ok, true)
		eq(t, spec.NoGateway, false)
		raw, _ := json.Marshal(spec)
		eq(t, string(raw), `{"v":1,"mail_ports":[25],"deny_ranges":["10.0.0.0/8"],"pools":[],`+
			`"allow":[]}`)
	}
}
