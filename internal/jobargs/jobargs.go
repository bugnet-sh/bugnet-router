// Package jobargs holds the River job arguments shared by the API, the saga and the worker
package jobargs

import "github.com/google/uuid"

// Deadline asks the worker to end a placement once its paid deadline has passed
type Deadline struct {
	PlacementID uuid.UUID `json:"placement_id"`
}

// Kind returns the River job kind
func (Deadline) Kind() string { return "deadline" }
