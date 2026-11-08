package e2e

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/protobuf/proto"
)

// bearer returns an HTTP client over h2c sending token as its bearer token
func bearer(t *testing.T, token string) *http.Client {
	t.Helper()
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	tr := &http.Transport{Protocols: &protocols}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: setBearer{token: token, next: tr}}
}

// edgeEvents decodes every entry of the stack's edge event stream, oldest first
func (s *stack) edgeEvents(t *testing.T) []*routerv1.EdgeEvent {
	t.Helper()
	vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{valkeyAddr()}})
	if err != nil {
		t.Fatal(err)
	}
	defer vk.Close()
	cmd := vk.B().Xrange().Key(s.prefix + "edge:events").Start("-").End("+").Build()
	entries, err := vk.Do(t.Context(), cmd).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	var out []*routerv1.EdgeEvent
	for _, en := range entries {
		ev := &routerv1.EdgeEvent{}
		if err := proto.Unmarshal([]byte(en.FieldValues["e"]), ev); err != nil {
			t.Fatal(err)
		}
		out = append(out, ev)
	}
	return out
}

func TestE2ECallerServicesAcrossReplicas(t *testing.T) {
	t.Parallel()
	s := startStack(t)
	h, p := s.place(t, s.client(t, s.a), smallPkg, smallPrice)
	id := p.GetPlacementId()
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	edgeA := routerv1connect.NewEdgeServiceClient(bearer(t, s.keys["BUGNET_EDGE_TOKEN"]), s.a.url)
	edgeB := routerv1connect.NewEdgeServiceClient(bearer(t, s.keys["BUGNET_EDGE_TOKEN"]), s.b.url)

	leases, err := edgeB.CheckEdgeLeases(ctx, connect.NewRequest(
		&routerv1.CheckEdgeLeasesRequest{PlacementIds: []string{id}}))
	if err != nil {
		t.Fatalf("CheckEdgeLeases: %v", err)
	}
	lease := leases.Msg.GetLeases()[id]
	if lease.GetBytesRemaining() != 2<<30 ||
		!lease.GetDeadline().AsTime().Equal(p.GetDeadline().AsTime()) {
		t.Fatalf("lease %v, want 2 GiB until %v", lease, p.GetDeadline().AsTime())
	}
	internal := routerv1connect.NewEdgeServiceClient(
		bearer(t, s.keys["BUGNET_INTERNAL_TOKEN"]), s.a.url)
	_, err = internal.CheckEdgeLeases(ctx, connect.NewRequest(&routerv1.CheckEdgeLeasesRequest{}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("the internal token on EdgeService gave %v, want Unauthenticated", err)
	}
	payments := routerv1connect.NewPaymentsServiceClient(
		bearer(t, s.keys["BUGNET_PAYMENTS_ROUTER_TOKEN"]), s.b.url)
	order, err := payments.GetOrder(ctx, connect.NewRequest(
		&routerv1.GetOrderRequest{OrderId: "e2e-never-taken"}))
	if err != nil || order.Msg.GetRedeemed() {
		t.Fatalf("GetOrder of an unknown order gave %v, %v, want not redeemed", order, err)
	}

	// Both replicas get a report that crosses the 2 GiB budget, one writes BudgetSpent
	var wg sync.WaitGroup
	for _, c := range []routerv1connect.EdgeServiceClient{edgeA, edgeB} {
		wg.Go(func() {
			_, err := c.ReportEdgeUsage(ctx, connect.NewRequest(&routerv1.ReportEdgeUsageRequest{
				Usage: []*routerv1.PlacementUsage{{PlacementId: id, BytesOut: 1200 << 20}},
			}))
			if err != nil {
				t.Errorf("ReportEdgeUsage: %v", err)
			}
		})
	}
	wg.Wait()
	evs := s.edgeEvents(t)
	if len(evs) != 1 || evs[0].GetBudgetSpent().GetPlacementId() != id {
		t.Fatalf("edge events %v, want one BudgetSpent for %s", evs, id)
	}

	// A stop on one replica reaches a reader of the other's stream
	ssh := routerv1connect.NewSshGatewayServiceClient(bearer(t, s.keys["BUGNET_SSH_TOKEN"]),
		s.b.url)
	stream, err := ssh.WatchSshEvents(ctx, connect.NewRequest(&routerv1.WatchSshEventsRequest{}))
	if err != nil {
		t.Fatalf("WatchSshEvents: %v", err)
	}
	time.Sleep(time.Second)
	if _, err := s.client(t, s.a).StopPlacement(ctx, connect.NewRequest(
		&routerv1.StopPlacementRequest{Handle: h})); err != nil {
		t.Fatalf("StopPlacement: %v", err)
	}
	if !stream.Receive() {
		t.Fatalf("the event stream ended: %v", stream.Err())
	}
	ended := stream.Msg().GetEvent().GetPlacementEnded()
	if ended.GetPlacementId() != id || ended.GetReason() != routerv1.EndReason_END_REASON_STOPPED {
		t.Fatalf("event %v, want PlacementEnded STOPPED for %s", stream.Msg(), id)
	}
	_ = stream.Close()
}
