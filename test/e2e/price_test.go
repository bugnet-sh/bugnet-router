package e2e

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
)

const (
	livePkg  = "live-small-10m"
	liveMore = "live-small-10m-more"
)

// liveCatalog prices both packages from the local provider's cost
const liveCatalog = `{"packages": [
	{"id": "live-small-10m", "resources": {"vcpu": 1, "memory_mib": 512, "disk_gib": 1},
		"minutes": 10},
	{"id": "live-small-10m-more", "resources": {"vcpu": 1, "memory_mib": 512, "disk_gib": 1},
		"minutes": 10, "extension_of": "live-small-10m"}
]}`

// liveSettings bill the local provider 36,000 an hour with 5 s of overhead on each start
const liveSettings = `hourly_micros = 36000

[pricing.overhead]
local = "5s"
`

const (
	// livePrice is 605 s at 36,000 an hour, a cost of 6,050, which 10% takes to 6,655
	livePrice = 6655
	// liveMorePrice is the extension's 600 s with no overhead, 6,000 priced at 6,600
	liveMorePrice = 6600
)

func TestE2ELiveCatalogPricesTheRule(t *testing.T) {
	t.Parallel()
	s := startStackWith(t, stackOptions{catalog: liveCatalog, extra: liveSettings})
	a, b := s.client(t, s.a), s.client(t, s.b)
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()

	est, err := a.Estimate(ctx, connect.NewRequest(&routerv1.EstimateRequest{}))
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}
	estimate := est.Msg.GetEstimateId()
	if estimate == "" {
		t.Fatal("Estimate answered no estimate id")
	}
	var listed *routerv1.PackageOption
	for _, o := range est.Msg.GetOptions() {
		if o.GetPackageId() == livePkg {
			listed = o
		}
	}
	if listed.GetPriceMicros() != livePrice || !listed.GetAvailable() {
		t.Fatalf("Estimate listed %v, want %s available at %d", listed, livePkg, livePrice)
	}
	priced, err := b.PricePackage(ctx, connect.NewRequest(&routerv1.PricePackageRequest{
		PackageId: livePkg, EstimateId: estimate,
	}))
	if err != nil {
		t.Fatalf("PricePackage: %v", err)
	}
	if o := priced.Msg.GetOption(); o.GetPriceMicros() != livePrice || !o.GetAvailable() {
		t.Fatalf("PricePackage answered %v, want available at %d", o, livePrice)
	}

	order := "e2e-" + uuid.NewString()
	token, err := ordertoken.Sign(s.priv, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindStart, PackageID: livePkg,
		AmountMicros: priced.Msg.GetOption().GetPriceMicros(),
		ExpiresAt:    time.Now().Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := a.CreatePlacement(ctx, connect.NewRequest(&routerv1.CreatePlacementRequest{
		OrderToken: token, EstimateId: estimate,
	}))
	if err != nil {
		t.Fatalf("CreatePlacement: %v", err)
	}
	h, p := created.Msg.GetHandle(), created.Msg.GetPlacement()
	for p.GetState() == routerv1.PlacementState_PLACEMENT_STATE_PLACING {
		r, err := a.GetPlacement(ctx, connect.NewRequest(&routerv1.GetPlacementRequest{
			Handle: h, WaitMs: pollWaitMs, KnownState: p.GetState(),
		}))
		if err != nil {
			t.Fatalf("GetPlacement: %v", err)
		}
		p = r.Msg.GetPlacement()
	}
	if p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY {
		t.Fatalf("placement is %s (%s), want ready", p.GetState(), p.GetFailureReason())
	}
	s.events.await(t, "placement.ready", p.GetPlacementId(), eventWait)
	// The order claimed the estimate's quote, so it holds for no other order
	conn, err := pgx.Connect(ctx, s.db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var claimed string
	if err := conn.QueryRow(ctx, `select order_id from price_quotes
		where estimate_id = $1 and package_id = $2`, uuid.MustParse(estimate), livePkg).
		Scan(&claimed); err != nil || claimed != order {
		t.Fatalf("the quote's order is %q, %v, want %s", claimed, err, order)
	}

	check, err := b.CheckExtendPlacement(ctx, connect.NewRequest(
		&routerv1.CheckExtendPlacementRequest{Handle: h, PackageId: liveMore}))
	if err != nil {
		t.Fatalf("CheckExtendPlacement: %v", err)
	}
	if got := check.Msg.GetPackage().GetPriceMicros(); got != liveMorePrice {
		t.Fatalf("CheckExtendPlacement priced %d, want %d", got, liveMorePrice)
	}
	sum, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	token, err = ordertoken.Sign(s.priv, ordertoken.Claims{
		OrderID: "e2e-" + uuid.NewString(), Kind: ordertoken.KindExtend, PackageID: liveMore,
		AmountMicros: check.Msg.GetPackage().GetPriceMicros(),
		ExpiresAt:    time.Now().Add(10 * time.Minute), HandleSHA256: hex.EncodeToString(sum),
	})
	if err != nil {
		t.Fatal(err)
	}
	ext, err := a.ExtendPlacement(ctx, connect.NewRequest(&routerv1.ExtendPlacementRequest{
		Handle: h, OrderToken: token,
	}))
	if err != nil {
		t.Fatalf("ExtendPlacement: %v", err)
	}
	want := p.GetDeadline().AsTime().Add(10 * time.Minute)
	if got := ext.Msg.GetPlacement().GetDeadline().AsTime(); !got.Equal(want) {
		t.Errorf("deadline after extend %s, want %s", got, want)
	}
	s.events.await(t, "placement.extended", p.GetPlacementId(), eventWait)
}
