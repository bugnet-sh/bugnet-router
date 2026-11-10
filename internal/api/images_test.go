package api_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// readyOnImage records a ready placement of provider name on an image version, skipping the saga
func (e *env) readyOnImage(t *testing.T, name, version string, endIt bool) uuid.UUID {
	t.Helper()
	ctx := t.Context()
	sha := make([]byte, 32)
	copy(sha, uuid.NewString())
	p, _, err := e.store.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: sha, OrderID: "order-" + uuid.NewString(), PackageID: smallPkg,
	})
	eq(t, err, nil)
	now := time.Now()
	img := &store.ReadyImage{Version: version,
		Manifest: []byte(`{"os":"ubuntu-24.04","tools":{"node":"24.22"}}`)}
	if version == "" {
		img = nil
	}
	ref := provider.Ref{Provider: name, ID: "sb-" + p.ID.String()[:8]}
	eq(t, e.store.MarkPlacementReady(ctx, p.ID, ref, "small", now, now.Add(time.Hour), img), nil)
	if endIt {
		_, err := e.pool.Exec(ctx, `update placements set state = 'terminated' where id = $1`,
			p.ID)
		eq(t, err, nil)
	}
	return p.ID
}

func TestListImageUseCountsLivePlacements(t *testing.T) {
	e := newEnv(t, options{})
	e.readyOnImage(t, "e2b", "2026.11.1", false)
	e.readyOnImage(t, "e2b", "2026.11.1", false)
	e.readyOnImage(t, "e2b", "2026.10.1", true)
	e.readyOnImage(t, "boat", "", false)
	op := e.operator(t, operatorToken)
	res, err := op.ListImageUse(t.Context(), connect.NewRequest(&routerv1.ListImageUseRequest{}))
	eq(t, err, nil)
	got := map[[2]string]*routerv1.ImageUse{}
	for _, u := range res.Msg.GetUses() {
		got[[2]string{u.GetProvider(), u.GetImageVersion()}] = u
	}
	eq(t, got[[2]string{"e2b", "2026.11.1"}].GetLivePlacements(), uint32(2))
	eq(t, got[[2]string{"e2b", "2026.11.1"}].GetLastReadyAt() != nil, true)
	// An ended placement keeps the version known and no live count
	eq(t, got[[2]string{"e2b", "2026.10.1"}].GetLivePlacements(), uint32(0))
	eq(t, got[[2]string{"e2b", "2026.10.1"}].GetLastReadyAt() != nil, true)
	eq(t, got[[2]string{"boat", ""}].GetLivePlacements(), uint32(1))
	one, err := op.ListImageUse(t.Context(), connect.NewRequest(
		&routerv1.ListImageUseRequest{Provider: "boat"}))
	eq(t, err, nil)
	eq(t, len(one.Msg.GetUses()), 1)
}

func TestPlacementCarriesItsImage(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	eq(t, p.GetImage() == nil, true)
	_, err := e.pool.Exec(t.Context(), `update placements set image_version = '2026.11.1',
		image_manifest = '{"os":"ubuntu-24.04","tools":{"node":"24.22","docker":"none"}}'
		where id = $1`, p.GetPlacementId())
	eq(t, err, nil)
	got := e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED, 0).GetImage()
	eq(t, got.GetVersion(), "2026.11.1")
	eq(t, got.GetOs(), "ubuntu-24.04")
	eq(t, got.GetTools()["node"], "24.22")
	eq(t, got.GetTools()["docker"], "none")
}

func TestProviderStatusShowsTheImage(t *testing.T) {
	e := newEnv(t, options{
		imageVersions: map[string]string{"local": "2026.10.1"}, imageAccept: []string{"2026.11.1"},
	})
	res, err := e.operator(t, operatorToken).ListProviders(t.Context(), connect.NewRequest(
		&routerv1.ListProvidersRequest{}))
	eq(t, err, nil)
	var local *routerv1.ProviderStatus
	for _, p := range res.Msg.GetProviders() {
		if p.GetProvider() == "local" {
			local = p
		}
	}
	eq(t, local.GetImageVersion(), "2026.10.1")
	eq(t, local.GetImageAccepted(), false)
}

func TestEstimateAndPricePassImageAccept(t *testing.T) {
	e := newEnv(t, options{
		imageVersions: map[string]string{"local": "2026.10.1"}, imageAccept: []string{"2026.11.1"},
	})
	resp, err := e.client.Estimate(t.Context(), connect.NewRequest(&routerv1.EstimateRequest{}))
	eq(t, err, nil)
	first := resp.Msg.GetOptions()[0]
	eq(t, first.GetAvailable(), false)
	eq(t, first.GetUnavailableReason(), "no_candidate")
	ok := newEnv(t, options{
		imageVersions: map[string]string{"local": "2026.11.1"}, imageAccept: []string{"2026.11.1"},
	})
	resp, err = ok.client.Estimate(t.Context(), connect.NewRequest(&routerv1.EstimateRequest{}))
	eq(t, err, nil)
	eq(t, resp.Msg.GetOptions()[0].GetAvailable(), true)
}
