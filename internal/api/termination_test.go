package api_test

import (
	"testing"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
)

func TestStoppedPlacementSaysStop(t *testing.T) {
	e := newEnv(t, options{})
	h, ready := e.readyPlacement(t)
	eq(t, ready.GetTerminationReason(), "")
	stopped := e.stop(t, h)
	eq(t, stopped.GetState(), routerv1.PlacementState_PLACEMENT_STATE_TERMINATED)
	eq(t, stopped.GetTerminationReason(), "stop")
	got := e.get(t, h, 0, 0)
	eq(t, got.GetTerminationReason(), "stop")
	eq(t, got.GetFailureReason(), "")
}

func TestDeadlineEndSaysDeadline(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	p := e.stored(t, h)
	if err := e.saga.Terminate(t.Context(), p.ID, *p.Deadline); err != nil {
		t.Fatalf("terminate at the deadline: %v", err)
	}
	got := e.get(t, h, 0, 0)
	eq(t, got.GetState(), routerv1.PlacementState_PLACEMENT_STATE_TERMINATED)
	eq(t, got.GetTerminationReason(), "deadline")
}
