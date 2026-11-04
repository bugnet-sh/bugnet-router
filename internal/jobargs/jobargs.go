// Package jobargs holds the River job arguments shared by the API, the saga and the worker
package jobargs

import "github.com/google/uuid"

// Deadline asks the worker to end a placement once its paid deadline has passed
type Deadline struct {
	PlacementID uuid.UUID `json:"placement_id"`
}

// Kind returns the River job kind
func (Deadline) Kind() string { return "deadline" }

// Sweep asks the worker to destroy leftover sandboxes and settle overdue or stalled placements
type Sweep struct{}

// Kind returns the River job kind
func (Sweep) Kind() string { return "sweep" }

// Outbox asks the worker to deliver the outbox events that are due
type Outbox struct{}

// Kind returns the River job kind
func (Outbox) Kind() string { return "outbox" }
