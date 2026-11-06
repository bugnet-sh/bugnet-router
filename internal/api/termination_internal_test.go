package api

import (
	"testing"

	"github.com/oynozan/bugnet-router/internal/store"
)

func TestTerminationReasonOnlyOnATerminatingOrTerminatedPlacement(t *testing.T) {
	for state, set := range map[string]bool{
		store.PlacementPlacing:     false,
		store.PlacementReady:       false,
		store.PlacementTerminating: true,
		store.PlacementTerminated:  true,
		store.PlacementFailed:      false,
	} {
		want := ""
		if set {
			want = "stop"
		}
		msg := placementMsg(store.Placement{State: state, TerminationReason: "stop"})
		if got := msg.GetTerminationReason(); got != want {
			t.Errorf("%s placement has termination_reason %q, want %q", state, got, want)
		}
	}
	failed := placementMsg(store.Placement{
		State: store.PlacementFailed, FailureReason: "boot_failed",
	})
	if failed.GetFailureReason() != "boot_failed" || failed.GetTerminationReason() != "" {
		t.Errorf("failed placement has failure_reason %q and termination_reason %q",
			failed.GetFailureReason(), failed.GetTerminationReason())
	}
}
