package api_test

import (
	"crypto/sha256"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const tunnelToken = "bnt_test-tunnel-token"

func sha(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

// armed gives the placement a tunnel token and a gate at CA version ca
func (e *env) armed(t *testing.T, p *routerv1.Placement, ca int) uuid.UUID {
	t.Helper()
	id := uuid.MustParse(p.GetPlacementId())
	if ok, err := e.store.MintEgressToken(t.Context(), id, sha(tunnelToken)); err != nil || !ok {
		t.Fatalf("mint token: %v %v", ok, err)
	}
	now := time.Now()
	if err := e.store.SetPlacementGate(t.Context(), id, &now, ca); err != nil {
		t.Fatal(err)
	}
	return id
}

// seeded stores a secret row of the placement, fetchOnly ones active as the put leaves them
func (e *env) seeded(
	t *testing.T, id uuid.UUID, name string, fetchOnly bool, state string, hosts ...string,
) {
	t.Helper()
	_, _, err := e.store.PutSecret(t.Context(), store.Secret{
		ID: uuid.New(), PlacementID: id, Name: name, Hosts: hosts, FetchOnly: fetchOnly,
		PlaceholderSHA256: sha("placeholder-" + name), Sealed: []byte("box-" + name),
		KeyID: "k2", Source: store.SourceBridge, State: state,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (e *env) tunnel(
	t *testing.T, req *routerv1.ResolveTunnelRequest,
) (*routerv1.ResolveTunnelResponse, error) {
	t.Helper()
	res, err := e.egress(t).ResolveTunnel(t.Context(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func byToken(sum []byte) *routerv1.ResolveTunnelRequest {
	return &routerv1.ResolveTunnelRequest{
		Caller: &routerv1.ResolveTunnelRequest_TokenSha256{TokenSha256: sum},
	}
}

func byFetch(id string) *routerv1.ResolveTunnelRequest {
	return &routerv1.ResolveTunnelRequest{
		Caller: &routerv1.ResolveTunnelRequest_FetchPlacementId{FetchPlacementId: id},
	}
}

func (e *env) mustTunnel(
	t *testing.T, req *routerv1.ResolveTunnelRequest,
) *routerv1.ResolveTunnelResponse {
	t.Helper()
	r, err := e.tunnel(t, req)
	if err != nil {
		t.Fatalf("ResolveTunnel: %v", err)
	}
	return r
}

func TestResolveTunnelByToken(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	e.seeded(t, id, "HF_TOKEN", false, store.SecretApplying, "huggingface.co")
	e.seeded(t, id, "GH_TOKEN", true, store.SecretActive, "api.github.com")
	e.seeded(t, id, "BAD", false, store.SecretFailed, "huggingface.co")

	r := e.mustTunnel(t, byToken(sha(tunnelToken)))
	eq(t, r.GetPlacementId(), id.String())
	eq(t, len(r.GetSecrets()), 1) // the fetch_only and the failed secret are left out
	s := r.GetSecrets()[0]
	eq(t, s.GetSpec().GetName(), "HF_TOKEN")
	eq(t, s.GetSpec().GetHosts()[0], "huggingface.co")
	eq(t, string(s.GetValue().GetBox()), "box-HF_TOKEN")
	eq(t, s.GetValue().GetKeyId(), "k2")
	eq(t, string(s.GetPlaceholderSha256()), string(sha("placeholder-HF_TOKEN")))
	eq(t, r.GetCaVersion(), uint32(1))
	eq(t, r.GetDeadline().AsTime().Equal(*e.placementOf(t, p).Deadline), true)
	eq(t, r.GetBytesRemaining(), uint64(1<<30))
	eq(t, r.GetBytesPerSecond(), uint64(2<<20))
	eq(t, r.GetRequestsInFlight(), uint32(32))
	eq(t, r.GetMaxTunnels(), uint32(64))
	eq(t, r.GetSecretsVersion(), uint64(e.placementOf(t, p).SecretsVersion))

	_, err := e.tunnel(t, byToken(sha("bnt_unknown")))
	mustCode(t, err, connect.CodeNotFound, "TOKEN_UNKNOWN")
	_, err = e.tunnel(t, byToken([]byte("short")))
	mustCode(t, err, connect.CodeNotFound, "TOKEN_UNKNOWN")
	_, err = e.tunnel(t, &routerv1.ResolveTunnelRequest{})
	mustCode(t, err, connect.CodeInvalidArgument, "")
}

func TestFetchCallerGetsFetchOnly(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	e.seeded(t, id, "HF_TOKEN", false, store.SecretActive, "huggingface.co")
	e.seeded(t, id, "GH_TOKEN", true, store.SecretActive, "api.github.com")

	r := e.mustTunnel(t, byFetch(id.String()))
	eq(t, r.GetPlacementId(), id.String())
	names := map[string]bool{}
	for _, s := range r.GetSecrets() {
		names[s.GetSpec().GetName()] = s.GetSpec().GetFetchOnly()
	}
	eq(t, len(names), 2)
	eq(t, names["GH_TOKEN"], true)

	_, err := e.tunnel(t, byFetch(uuid.NewString()))
	mustCode(t, err, connect.CodeNotFound, "TOKEN_UNKNOWN")
	_, err = e.tunnel(t, byFetch("not-a-uuid"))
	mustCode(t, err, connect.CodeNotFound, "TOKEN_UNKNOWN")
}

func TestCustomHostsListedAndSwitchedOff(t *testing.T) {
	hosts := []string{"api.example.com", "huggingface.co"}
	e := newEnv(t, options{secrets: true})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 1)
	e.seeded(t, id, "MY_KEY", false, store.SecretActive, hosts...)
	s := e.mustTunnel(t, byToken(sha(tunnelToken))).GetSecrets()[0]
	eq(t, len(s.GetSpec().GetHosts()), 2)
	eq(t, len(s.GetCustomHosts()), 1)
	eq(t, s.GetCustomHosts()[0], "api.example.com")

	off := newEnv(t, options{secrets: true,
		secretSettings: func(c *config.Secrets) { c.CustomHosts = false }})
	_, p = off.readyPlacement(t)
	id = off.armed(t, p, 1)
	off.seeded(t, id, "MY_KEY", false, store.SecretActive, hosts...)
	s = off.mustTunnel(t, byToken(sha(tunnelToken))).GetSecrets()[0]
	eq(t, len(s.GetCustomHosts()), 0)
	eq(t, len(s.GetSpec().GetHosts()), 1)
	eq(t, s.GetSpec().GetHosts()[0], "huggingface.co")
}

func TestResolveTunnelCAVersionFromRow(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	_, p := e.readyPlacement(t)
	id := e.armed(t, p, 2)
	eq(t, e.mustTunnel(t, byToken(sha(tunnelToken))).GetCaVersion(), uint32(2))
	if err := e.store.SetPlacementGate(t.Context(), id, nil, 0); err != nil {
		t.Fatal(err)
	}
	_, err := e.tunnel(t, byToken(sha(tunnelToken)))
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
	_, err = e.tunnel(t, byFetch(id.String()))
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
}

func TestResolveTunnelRefusesSpentBudget(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	_, p := e.readyPlacement(t)
	e.armed(t, p, 1)
	e.reportEgress(t, usage(p.GetPlacementId(), 1<<30, 0))
	_, err := e.tunnel(t, byToken(sha(tunnelToken)))
	mustCode(t, err, connect.CodeResourceExhausted, "BUDGET_SPENT")
}

func TestResolveTunnelRefusesAnEndedPlacement(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	e.armed(t, p, 1)
	e.stop(t, h)
	_, err := e.tunnel(t, byToken(sha(tunnelToken)))
	mustCode(t, err, connect.CodeNotFound, "TOKEN_UNKNOWN")
	_, err = e.tunnel(t, byFetch(p.GetPlacementId()))
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
}

func TestCheckEgressLeasesVersionMoves(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	id := p.GetPlacementId()
	lease := func() uint64 {
		t.Helper()
		res, err := e.egress(t).CheckEgressLeases(t.Context(), connect.NewRequest(
			&routerv1.CheckEgressLeasesRequest{PlacementIds: []string{id}}))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg.GetLeases()[id].GetSecretsVersion()
	}
	before := lease()
	if err := e.store.BumpSecretsVersion(t.Context(), uuid.MustParse(id), false); err != nil {
		t.Fatal(err)
	}
	eq(t, lease(), before+1)
	eq(t, lease(), uint64(e.placementOf(t, p).SecretsVersion))
	e.mustPutSecret(t, h, hf())
	if err := e.store.BumpSecretsVersion(t.Context(), uuid.MustParse(id), false); err != nil {
		t.Fatal(err)
	}
	eq(t, lease(), uint64(e.placementOf(t, p).SecretsVersion))
}
