package api_test

import (
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func x402Caps(c *config.X402) {
	c.Enabled = true
	c.UnpaidRequestsPerHost, c.UnpaidBytesPerHost = 300, 52428800
}

// x402Spec is a service on host that is unlisted, its paid path /search
func x402Spec(host string, indexed bool) *routerv1.X402ServiceSpec {
	s := svc(host, "/")
	s.Prefix, s.PaidPath, s.Indexed = "/api/", "/search", indexed
	if indexed {
		s.PaidPath = ""
	}
	return s
}

func serviceIDs(c *routerv1.X402Config) []string {
	var out []string
	for _, s := range c.GetServices() {
		out = append(out, s.GetServiceId())
	}
	return out
}

func TestResolveTunnelX402Block(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402Caps})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	listed, indexed := x402Spec("agent402.tools", false), x402Spec("b.example.com", true)
	failed, gone := x402Spec("c.example.com", true), x402Spec("d.example.com", true)
	e.putRow(t, id.String(), listed, store.X402Applying)
	e.putRow(t, id.String(), indexed, store.X402Active)
	e.putRow(t, id.String(), failed, store.X402Failed)
	e.putRow(t, id.String(), gone, store.X402Disabled)
	got, err := e.store.PlacementByID(t.Context(), id)
	eq(t, err, nil)
	unpaid := unpaidReport(id.String(), map[string]*routerv1.X402Unpaid{
		"agent402.tools": {Requests: 7, Bytes: 900},
	})
	_, err = e.egress(t).ReportEgressUsage(t.Context(), connect.NewRequest(
		&routerv1.ReportEgressUsageRequest{X402Unpaid: unpaid}))
	eq(t, err, nil)

	c := e.mustTunnel(t, byToken(sha(tunnelToken))).GetX402()
	eq(t, c.GetEnabled(), true)
	eq(t, c.GetAccountId(), got.OrderID)
	eq(t, c.GetUnpaidRequestsPerHost(), uint32(300))
	eq(t, c.GetUnpaidBytesPerHost(), uint64(52428800))
	eq(t, len(c.GetServices()), 3)
	byID := map[string]*routerv1.X402Service{}
	for _, s := range c.GetServices() {
		byID[s.GetServiceId()] = s
	}
	eq(t, byID[idOf(gone)] == nil, true)
	eq(t, byID[idOf(listed)].GetPaidPath(), "/search")
	eq(t, byID[idOf(listed)].GetPrefix(), "/api/")
	eq(t, byID[idOf(indexed)].GetPaidPath(), "")
	eq(t, byID[idOf(failed)].GetHost(), "c.example.com")
	eq(t, c.GetUnpaidUsed()["agent402.tools"].GetRequests(), uint32(7))
}

func TestResolveTunnelKillSwitch(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: func(c *config.X402) {
		x402Caps(c)
		c.Enabled = false
	}})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	e.putRow(t, id.String(), x402Spec("agent402.tools", true), store.X402Active)
	c := e.mustTunnel(t, byToken(sha(tunnelToken))).GetX402()
	eq(t, c.GetEnabled(), false)
	eq(t, len(c.GetServices()), 1)
}

func TestResolveTunnelDropsBlockedHosts(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: func(c *config.X402) {
		x402Caps(c)
		c.BlockedHosts = []string{"bad.example.com"}
	}})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	ok, bad, shared := x402Spec("ok.example.com", true), x402Spec("bad.example.com", true),
		x402Spec("x.workers.dev", true)
	for _, s := range []*routerv1.X402ServiceSpec{ok, bad, shared} {
		e.putRow(t, id.String(), s, store.X402Active)
	}
	c := e.mustTunnel(t, byToken(sha(tunnelToken))).GetX402()
	eq(t, serviceIDs(c), []string{idOf(ok)})
	rows, err := e.store.X402Services(t.Context(), id)
	eq(t, err, nil)
	eq(t, len(rows), 3)
	for _, r := range rows {
		eq(t, r.State, store.X402Active)
	}
}

func TestResolveTunnelWithoutRows(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402Caps})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	eq(t, e.mustTunnel(t, byToken(sha(tunnelToken))).GetX402() == nil, true)
	e.putRow(t, id.String(), x402Spec("a.example.com", true), store.X402Disabled)
	eq(t, e.mustTunnel(t, byToken(sha(tunnelToken))).GetX402() == nil, true)
}

func TestFetchCallerGetsNoX402(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402Caps})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	e.putRow(t, id.String(), x402Spec("a.example.com", true), store.X402Active)
	eq(t, e.mustTunnel(t, byFetch(id.String())).GetX402() == nil, true)
}

func TestResolveTunnelX402OnlyPlacement(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402Caps})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	e.putRow(t, id.String(), x402Spec("a.example.com", true), store.X402Active)
	r := e.mustTunnel(t, byToken(sha(tunnelToken)))
	eq(t, len(r.GetX402().GetServices()), 1)
	eq(t, len(r.GetSecrets()), 0)
}

func TestCustomHostsSwitchLeavesX402Hosts(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402Caps,
		secretSettings: func(c *config.Secrets) { c.CustomHosts = false }})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	e.seeded(t, id, "MY_KEY", false, store.SecretActive, "api.example.com", "huggingface.co")
	e.putRow(t, id.String(), x402Spec("custom.example.net", true), store.X402Active)
	r := e.mustTunnel(t, byToken(sha(tunnelToken)))
	eq(t, r.GetSecrets()[0].GetSpec().GetHosts(), []string{"huggingface.co"})
	eq(t, len(r.GetX402().GetServices()), 1)
	eq(t, r.GetX402().GetServices()[0].GetHost(), "custom.example.net")
}

func TestX402RowsStayAfterEnd(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402Caps})
	h, p := e.readyPlacement(t)
	id := uuid.MustParse(p.GetPlacementId())
	e.putRow(t, id.String(), x402Spec("a.example.com", true), store.X402Active)
	e.stop(t, h)
	rows, err := e.store.X402Services(t.Context(), id)
	eq(t, err, nil)
	eq(t, len(rows), 1)
	eq(t, rows[0].State, store.X402Active)
}
