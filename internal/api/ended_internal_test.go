package api

import (
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/store"
)

func TestEndedAtOnlyOnATerminatedOrFailedPlacement(t *testing.T) {
	at := time.Date(2026, 11, 6, 1, 2, 3, 456789000, time.UTC)
	for state, ended := range map[string]bool{
		store.PlacementPlacing:     false,
		store.PlacementReady:       false,
		store.PlacementTerminating: false,
		store.PlacementTerminated:  true,
		store.PlacementFailed:      true,
	} {
		got := placementMsg(store.Placement{State: state, TerminatedAt: &at}).GetEndedAt()
		if (got != nil) != ended || ended && !got.AsTime().Equal(at) {
			t.Errorf("%s placement has ended_at %v, want it set %v at %v", state, got, ended, at)
		}
		if got := placementMsg(store.Placement{State: state}).GetEndedAt(); got != nil {
			t.Errorf("%s placement with no end time has ended_at %v", state, got)
		}
	}
}
