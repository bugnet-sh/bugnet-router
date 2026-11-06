// Package jobargs holds the River job arguments shared by the API, the saga and the worker
package jobargs

import "github.com/google/uuid"

// QueueDataplane is the River queue of every data-plane job, apart from the default queue
const QueueDataplane = "dataplane"

// Deadline asks the worker to end a placement once its paid deadline has passed
type Deadline struct {
	PlacementID uuid.UUID `json:"placement_id"`
}

// Kind returns the River job kind
func (Deadline) Kind() string { return "deadline" }

// Sweep asks the worker to destroy the sandboxes no placement needs
type Sweep struct{}

// Kind returns the River job kind
func (Sweep) Kind() string { return "sweep" }

// Watch asks the worker to fail stalled placing placements and end overdue ones
type Watch struct{}

// Kind returns the River job kind
func (Watch) Kind() string { return "watch" }

// Outbox asks the worker to deliver the outbox events that are due
type Outbox struct{}

// Kind returns the River job kind
func (Outbox) Kind() string { return "outbox" }
