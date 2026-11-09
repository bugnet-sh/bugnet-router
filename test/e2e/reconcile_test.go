package e2e

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

const (
	// reconcileWait covers one sweep, a minute, and the reconcile job it queues
	reconcileWait = 100 * time.Second
	// orphanWait covers the watch failing a silent saga after 60 s and the sweep after it
	orphanWait = 4 * time.Minute
)

// ownedBy returns the stack's sandboxes as its local provider lists them
func ownedBy(t *testing.T, s *stack) (*local.Provider, []provider.Summary) {
	t.Helper()
	p := local.New(local.Options{Name: "local", Root: s.root})
	owned, err := p.ListOwned(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return p, owned
}

// Deleting a ready placement's sandbox ends it as provider_failed within one sweep, refundable,
// and the event carries its unused seconds
func TestE2EDeletedSandboxEndsInOneSweep(t *testing.T) {
	t.Parallel()
	s := startStackWith(t, stackOptions{
		health: quietHealth,
		extra:  "\n[reconcile]\nready_grace = \"1s\"\n",
	})
	a := s.client(t, s.a)
	h, p := s.place(t, a, smallPkg, smallPrice)
	id := p.GetPlacementId()
	s.events.await(t, "placement.ready", id, eventWait)

	lp, owned := ownedBy(t, s)
	if len(owned) != 1 {
		t.Fatalf("%d sandboxes while ready, want 1", len(owned))
	}
	if err := lp.Destroy(t.Context(), owned[0].Ref); err != nil {
		t.Fatal(err)
	}

	ended := s.events.await(t, "placement.terminated", id, reconcileWait)
	if ended.Payload.State != "terminated" || ended.Payload.Reason != "provider_failed" ||
		!ended.Payload.Refundable {
		t.Errorf("terminated event %+v, want a refundable end for provider_failed",
			ended.Payload)
	}
	// A 10 minute package, a minute or so used
	if u := ended.Payload.UnusedSeconds; u == nil || *u < 300 || *u > 600 {
		t.Errorf("terminated event holds unused_seconds %v, want between 300 and 600", u)
	}
	got := parseKV(s.devOK(t, s.a, "get", "--handle", h))
	if got["state"] != "terminated" {
		t.Errorf("bugnet-dev get printed %v, want terminated", got)
	}
	evs := s.edgeEvents(t)
	if len(evs) != 1 || evs[0].GetPlacementEnded().GetPlacementId() != id ||
		evs[0].GetPlacementEnded().GetReason() != routerv1.EndReason_END_REASON_PROVIDER_FAILED {
		t.Errorf("edge events %v, want one PlacementEnded for provider_failed", evs)
	}
}

// Killing router-api mid-saga leaves a placing placement and its sandbox. The sandbox carries a
// backstop lifetime, the watch fails the silent saga and the sweep removes the sandbox.
func TestE2EOrphanDrill(t *testing.T) {
	t.Parallel()
	s := startStack(t)
	a, b := s.client(t, s.a), s.client(t, s.b)
	token, err := ordertoken.Sign(s.priv, ordertoken.Claims{
		OrderID: "e2e-" + uuid.NewString(), Kind: ordertoken.KindStart, PackageID: smallPkg,
		AmountMicros: smallPrice, ExpiresAt: time.Now().Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	res, err := a.CreatePlacement(ctx, connect.NewRequest(&routerv1.CreatePlacementRequest{
		OrderToken: token,
	}))
	if err != nil {
		t.Fatalf("CreatePlacement: %v", err)
	}
	h := res.Msg.GetHandle()

	// The sandbox exists while the saga is still booting it
	var left provider.Summary
	lp := local.New(local.Options{Name: "local", Root: s.root})
	for deadline := time.Now().Add(readyWait); ; {
		owned, err := lp.ListOwned(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(owned) == 1 {
			left = owned[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no sandbox appeared while placing")
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.a.kill(t)

	st, err := lp.Inspect(ctx, left.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if until := time.Until(st.ExpiresAt); until <= 0 || until > 25*time.Minute {
		t.Errorf("the orphan's backstop lifetime ends in %v, want within the package and slack",
			until)
	}

	deadline := time.Now().Add(orphanWait)
	for {
		got, err := b.GetPlacement(t.Context(), connect.NewRequest(&routerv1.GetPlacementRequest{
			Handle: h,
		}))
		if err != nil {
			t.Fatalf("GetPlacement: %v", err)
		}
		_, owned := ownedBy(t, s)
		state := got.Msg.GetPlacement().GetState()
		failed := state == routerv1.PlacementState_PLACEMENT_STATE_FAILED
		if failed && len(owned) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %s the placement is %s with %d sandboxes, want failed and none",
				orphanWait, got.Msg.GetPlacement().GetState(), len(owned))
		}
		time.Sleep(2 * time.Second)
	}
}
