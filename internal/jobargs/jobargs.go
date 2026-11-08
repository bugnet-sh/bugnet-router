// Package jobargs holds the River job arguments shared by the API, the saga and the worker
package jobargs

import (
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

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

// ApplySecrets asks the worker to write a placement's secrets into its sandbox
type ApplySecrets struct {
	PlacementID uuid.UUID `json:"placement_id"`
}

// Kind returns the River job kind
func (ApplySecrets) Kind() string { return "apply_secrets" }

// InsertOpts puts every insert on the dataplane queue, never unique, since a lock serialises runs
func (ApplySecrets) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueDataplane}
}

// PrepareSSH asks the worker to make a ready placement's sshd ready behind its gate
type PrepareSSH struct {
	PlacementID uuid.UUID `json:"placement_id"`
}

// Kind returns the River job kind
func (PrepareSSH) Kind() string { return "prepare_ssh" }

// prepareAttempts bounds the retries of one prepare_ssh job
const prepareAttempts = 3

// InsertOpts puts the job on the dataplane queue, once per placement while one waits or runs
func (PrepareSSH) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: QueueDataplane, MaxAttempts: prepareAttempts,
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: []rivertype.JobState{
			rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning,
			rivertype.JobStateScheduled,
		}},
	}
}

// WalletStopLive asks the worker to end the live placements of a payer just blocked
type WalletStopLive struct {
	Payer string `json:"payer"`
	// BlockedAt is the start of the block that queued the job, which ends nothing once that
	// block is lifted or replaced; nil matches any block in force
	BlockedAt *time.Time `json:"blocked_at,omitempty"`
	// Waits counts the runs that found a placement still placing and waited for it
	Waits int `json:"waits,omitempty"`
}

// Kind returns the River job kind
func (WalletStopLive) Kind() string { return "wallet_stop_live" }

// WalletExpiry asks the worker to lift the wallet blocks whose time has passed
type WalletExpiry struct{}

// Kind returns the River job kind
func (WalletExpiry) Kind() string { return "wallet_expiry" }

// StatsSnapshot asks the worker to write the public stats feed's snapshot row
type StatsSnapshot struct{}

// Kind returns the River job kind
func (StatsSnapshot) Kind() string { return "stats_snapshot" }

// InsertOpts runs it on the default queue once, since the next run starts 30 s later
func (StatsSnapshot) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: river.QueueDefault, MaxAttempts: 1}
}

// SessionCopy asks the worker to copy an ended placement's output into its session record
type SessionCopy struct {
	PlacementID uuid.UUID `json:"placement_id"`
}

// Kind returns the River job kind
func (SessionCopy) Kind() string { return "session_copy" }

// SessionExpiry asks the worker to delete the session records whose page ended
type SessionExpiry struct{}

// Kind returns the River job kind
func (SessionExpiry) Kind() string { return "session_expiry" }

// pendingStates are the job states a unique insert of a polling job skips on
var pendingStates = []rivertype.JobState{
	rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning,
	rivertype.JobStateScheduled,
}

// LimitsPoll asks the worker to read one provider account's cap and use into its capacity row
type LimitsPoll struct {
	Provider string `json:"provider"`
}

// Kind returns the River job kind
func (LimitsPoll) Kind() string { return "limits_poll" }

// InsertOpts runs it once, one at a time per provider, since the next poll comes soon
func (LimitsPoll) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: river.QueueDefault, MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: pendingStates},
	}
}

// AccountCheck asks the worker to read one provider account's standing
type AccountCheck struct {
	Provider string `json:"provider"`
	// Now checks at once, otherwise only when the last check is older than its interval
	Now bool `json:"now,omitempty"`
}

// Kind returns the River job kind
func (AccountCheck) Kind() string { return "account_check" }

// InsertOpts runs it once, one at a time per provider and kind of request
func (AccountCheck) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: river.QueueDefault, MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: pendingStates},
	}
}
