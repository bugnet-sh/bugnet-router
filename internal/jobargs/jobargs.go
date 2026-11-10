// Package jobargs holds the River job arguments shared by the API, the saga and the worker
package jobargs

import (
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// QueueDataplane is the River queue of every data-plane job, apart from the default queue
const QueueDataplane = "dataplane"

// QueueProbes is the River queue of the probes
const QueueProbes = "probes"

// QueuePools is the River queue of the pool fills
const QueuePools = "pools"

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

// Reconcile asks the worker to read one live placement's sandbox and act on what the provider says
type Reconcile struct {
	PlacementID uuid.UUID `json:"placement_id"`
}

// Kind returns the River job kind
func (Reconcile) Kind() string { return "reconcile" }

// InsertOpts runs it once per placement in 10 s, the sweep and the hints folding together
func (Reconcile) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: river.QueueDefault, MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByPeriod: 10 * time.Second},
	}
}

// HookHint asks the worker to re-read one sandbox after a verified provider webhook
type HookHint struct {
	Provider  string `json:"provider" river:"unique"`
	SandboxID string `json:"sandbox_id" river:"unique"`
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	// PlacementLabel and DeploymentLabel are the sandbox's labels when the event carries them
	PlacementLabel  string `json:"placement_label,omitempty"`
	DeploymentLabel string `json:"deployment_label,omitempty"`
}

// Kind returns the River job kind
func (HookHint) Kind() string { return "hook_hint" }

// InsertOpts runs it once per provider and sandbox while one waits, the API folding the rest
func (HookHint) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: river.QueueDefault,
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: []rivertype.JobState{
			rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRetryable,
			rivertype.JobStateRunning, rivertype.JobStateScheduled,
		}},
	}
}

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

// CanaryTick asks the worker to queue a canary for each provider that is due one
type CanaryTick struct{}

// Kind returns the River job kind
func (CanaryTick) Kind() string { return "canary_tick" }

// InsertOpts runs it once on the default queue, since the next tick comes a minute later
func (CanaryTick) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: river.QueueDefault, MaxAttempts: 1}
}

// Probe asks the worker to start one sandbox, time it and remove it, Sample being its kind
type Probe struct {
	Sample   string        `json:"sample" river:"unique"`
	Provider string        `json:"provider" river:"unique"`
	Size     provider.Size `json:"size"`
	BenchID  string        `json:"bench_id,omitempty" river:"unique"`
	Run      int           `json:"run,omitempty" river:"unique"`
	// AllowPaid lets a benchmark probe start a sandbox that costs a fee
	AllowPaid bool `json:"allow_paid,omitempty"`
}

// Kind returns the River job kind
func (Probe) Kind() string { return "probe" }

// InsertOpts runs it once on the probes queue and never again, since a second run would start
// a second sandbox, and one at a time per provider, kind, benchmark and run
func (Probe) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: QueueProbes, MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: pendingStates},
	}
}

// HealthRollup asks the worker to write every provider's shared health row
type HealthRollup struct{}

// Kind returns the River job kind
func (HealthRollup) Kind() string { return "health_rollup" }

// InsertOpts runs it once on the default queue, since the next rollup comes 30 s later
func (HealthRollup) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: river.QueueDefault, MaxAttempts: 1}
}

// Bench asks the worker to run one wave of a benchmark and queue the next, Wave counting from 0
type Bench struct {
	BenchID string `json:"bench_id" river:"unique"`
	Wave    int    `json:"wave" river:"unique"`
	// AllowPaid lets the wave start sandboxes that cost a fee
	AllowPaid bool `json:"allow_paid,omitempty"`
}

// Kind returns the River job kind
func (Bench) Kind() string { return "bench" }

// InsertOpts runs a wave on the default queue, three attempts for a failed read of the database,
// and one at a time per benchmark and wave
func (Bench) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: river.QueueDefault, MaxAttempts: 3,
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: pendingStates},
	}
}

// Retention asks the worker to delete one batch of rows past their age, and to queue itself again
// while more are left
type Retention struct{}

// Kind returns the River job kind
func (Retention) Kind() string { return "retention" }

// InsertOpts runs it once on the default queue, since the next run comes the next day
func (Retention) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: river.QueueDefault, MaxAttempts: 1}
}

// CapacityRecount asks the worker to recount every provider's capacity slots
type CapacityRecount struct{}

// Kind returns the River job kind
func (CapacityRecount) Kind() string { return "capacity_recount" }

// InsertOpts runs it once, since the next recount comes in minutes
func (CapacityRecount) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: river.QueueDefault, MaxAttempts: 1}
}

// ForeignCount asks the worker to count one provider's sandboxes that no known deployment owns
type ForeignCount struct {
	Provider string `json:"provider"`
}

// Kind returns the River job kind
func (ForeignCount) Kind() string { return "foreign_count" }

// InsertOpts runs it once, one at a time per provider, since the next count comes within an hour
func (ForeignCount) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: river.QueueDefault, MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: pendingStates},
	}
}

// CostDailyTick asks the worker to queue the daily cost check of each provider that reports usage
type CostDailyTick struct{}

// Kind returns the River job kind
func (CostDailyTick) Kind() string { return "cost_daily_tick" }

// InsertOpts runs it once, since the next tick comes the next day
func (CostDailyTick) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: river.QueueDefault, MaxAttempts: 1}
}

// CostDaily asks the worker to read one page of a provider's usage for one UTC day, and to queue
// itself again until the last page
type CostDaily struct {
	Provider string `json:"provider"`
	// Day is the UTC date as 2006-01-02
	Day string `json:"day"`
}

// Kind returns the River job kind
func (CostDaily) Kind() string { return "cost_daily" }

// InsertOpts runs a page on the default queue, five attempts for a failed read of the provider
func (CostDaily) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: river.QueueDefault, MaxAttempts: 5}
}

// PoolTick asks the worker to size every pool, fill its deficit and retire what has aged
type PoolTick struct{}

// Kind returns the River job kind
func (PoolTick) Kind() string { return "pool_tick" }

// InsertOpts runs it once, one at a time, since the next tick comes seconds later
func (PoolTick) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: river.QueueDefault, MaxAttempts: 1}
}

// PoolFill asks the worker to create, boot and park one pool member
type PoolFill struct {
	Member uuid.UUID `json:"member"`
}

// Kind returns the River job kind
func (PoolFill) Kind() string { return "pool_fill" }

// poolFillAttempts bounds the retries of one fill, which finds its sandbox again by its key
const poolFillAttempts = 3

// InsertOpts puts the fill on the pools queue
func (PoolFill) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueuePools, MaxAttempts: poolFillAttempts}
}

// PoolRetire asks the worker to destroy one pool member's sandbox and free its slot
type PoolRetire struct {
	Member uuid.UUID `json:"member" river:"unique"`
	Reason string    `json:"reason"`
}

// Kind returns the River job kind
func (PoolRetire) Kind() string { return "pool_retire" }

// InsertOpts runs it once per member while one waits or runs, retried a few times
func (PoolRetire) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: river.QueueDefault, MaxAttempts: 5,
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: pendingStates},
	}
}

// PoolEnsure asks the worker to create or size the provider-kept pools of one provider
type PoolEnsure struct {
	Provider string `json:"provider" river:"unique"`
}

// Kind returns the River job kind
func (PoolEnsure) Kind() string { return "pool_ensure" }

// InsertOpts runs it once per provider while one waits or runs, retried a few times
func (PoolEnsure) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: river.QueueDefault, MaxAttempts: 3,
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: pendingStates},
	}
}

// ReserveWatch asks the worker to lapse and fail reservations, retire the members left without
// one and set or clear the brake
type ReserveWatch struct{}

// Kind returns the River job kind
func (ReserveWatch) Kind() string { return "reserve_watch" }

// InsertOpts runs it once, one at a time, since the next run comes seconds later
func (ReserveWatch) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: river.QueueDefault, MaxAttempts: 1}
}
