package jobs

import (
	"time"

	"github.com/google/uuid"
)

// StopRenewing keeps s from renewing its lease on the job, so the lease expires while s streams
func StopRenewing(s *Service, jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.owners[jobID]
	if ok {
		o.renewPaused.Store(true)
	}
	return ok
}

// Owns reports whether s runs an owner loop for the job
func Owns(s *Service, jobID string) bool { return s.owns(jobID) }

// JobIDFor returns the id Exec gives the job the key starts in the placement
func JobIDFor(placementID uuid.UUID, key string) string { return newJobID(placementID, key) }

// SetIdleAfter shortens how long an owner waits on a silent runner stream until the test ends
func SetIdleAfter(t interface{ Cleanup(func()) }, d time.Duration) {
	old := idleAfter
	idleAfter = d
	t.Cleanup(func() { idleAfter = old })
}

// SetFloodEvery shortens how often a flooding job's tail is stored until the test ends
func SetFloodEvery(t interface{ Cleanup(func()) }, d time.Duration) {
	old := floodEvery
	floodEvery = d
	t.Cleanup(func() { floodEvery = old })
}
