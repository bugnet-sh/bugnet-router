package api_test

import (
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/oynozan/bugnet-router/internal/handle"
	"github.com/oynozan/bugnet-router/internal/store"
)

// stored reads the placement behind h from the store
func (e *env) stored(t *testing.T, h string) store.Placement {
	t.Helper()
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	p, err := e.store.PlacementByHandle(t.Context(), sha)
	if err != nil {
		t.Fatalf("read placement: %v", err)
	}
	return p
}

func wantEndedAt(t *testing.T, name string, p *routerv1.Placement, want *timestamppb.Timestamp) {
	t.Helper()
	if !proto.Equal(p.GetEndedAt(), want) {
		t.Fatalf("%s placement has ended_at %v, want %v", name, p.GetEndedAt(), want)
	}
}

func TestStoppedPlacementEndedAtIsItsTerminatedTime(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	stopped := e.stop(t, h)
	p := e.stored(t, h)
	if p.State != store.PlacementTerminated || p.TerminatedAt == nil {
		t.Fatalf("stored placement is %s with terminated_at %v", p.State, p.TerminatedAt)
	}
	want := timestamppb.New(*p.TerminatedAt)
	wantEndedAt(t, "stopped", stopped, want)

	first := e.get(t, h, 0, 0)
	time.Sleep(100 * time.Millisecond)
	second := e.get(t, h, 0, 0)
	wantEndedAt(t, "first read", first, want)
	wantEndedAt(t, "second read", second, want)
}

func TestFailedPlacementEndedAtIsItsRecordedTime(t *testing.T) {
	e := newEnv(t, options{})
	order, tok := e.startOrder(t, gpuPkg, gpuPrice)
	_, err := e.createWith(t, tok)
	wantCode(t, err, connect.CodeResourceExhausted)
	h, _ := handle.Derive(e.handleKey, order)
	failed := e.get(t, h, 0, 0)
	eq(t, failed.GetState(), routerv1.PlacementState_PLACEMENT_STATE_FAILED)
	wantEndedAt(t, "failed with no recorded time", failed, nil)

	at := time.Date(2026, 11, 6, 1, 2, 3, 456789000, time.UTC)
	_, err = e.pool.Exec(t.Context(),
		`update placements set terminated_at = $2 where id = $1`, e.stored(t, h).ID, at)
	if err != nil {
		t.Fatalf("record the failure time: %v", err)
	}
	wantEndedAt(t, "failed", e.get(t, h, 0, 0), timestamppb.New(at))
}

func TestPlacingAndReadyPlacementsHaveNoEndedAt(t *testing.T) {
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	e := newEnv(t, options{holdReady: hold})
	t.Cleanup(release)
	h, created := e.create(t)
	placing := e.get(t, h, 0, 0)
	eq(t, placing.GetState(), routerv1.PlacementState_PLACEMENT_STATE_PLACING)
	wantEndedAt(t, "created", created, nil)
	wantEndedAt(t, "placing", placing, nil)

	release()
	wantEndedAt(t, "ready", e.waitReady(t, h), nil)
}
