// Package gpustatus names the words the Lium and HF adapters put in a Status for reconcile
package gpustatus

import (
	"slices"
	"strings"
)

// Lium pod statuses the Lium adapter writes into Reason as words
const (
	LiumStatusStopped        = "STOPPED"
	LiumStatusStopPending    = "STOP_PENDING"
	LiumStatusBroken         = "BROKEN"
	LiumStatusFailed         = "FAILED"
	LiumStatusRebootFailed   = "REBOOT_FAILED"
	LiumStatusCreationFailed = "CREATION_FAILED"
)

// Lium pod flags the Lium adapter writes into Reason as words while they are set
const (
	LiumFlagNodeOffline = "node_offline"
	LiumFlagNodeOutage  = "node_outage"
)

// Reasons of the Lium adapter's own for a pod that changed under a ready placement
const (
	// LiumMoved is a pod running on another node than the one it was ready on
	LiumMoved = "lium_moved"
	// LiumRebooted is a pod starting again after it was ready, its container new
	LiumRebooted = "lium_rebooted"
)

// Lium pod-lifecycle event reasons, the newest of which the Lium adapter puts in KillReason
const (
	LiumEventInsufficientBalance  = "insufficient_balance"
	LiumEventDisputeBlock         = "dispute_block"
	LiumEventExecutorOffline      = "executor_offline"
	LiumEventExecutorReclaimed    = "executor_reclaimed"
	LiumEventBrokenByProvider     = "broken_by_provider"
	LiumEventUserInitiated        = "user_initiated"
	LiumEventScheduledTermination = "scheduled_termination"
	LiumEventRebootFailed         = "reboot_failed"
	LiumEventUnknown              = "unknown"
)

// HF kill reasons the HF adapter puts in KillReason, a cancel reason lowercased or the stage
const (
	HFNoCredits          = "no_credits"
	HFNoSubscription     = "no_subscription"
	HFSpendLimit         = "spend_limit"
	HFResourceGroupLimit = "resource_group_limit"
	HFTimeout            = "timeout"
	HFCanceled           = "canceled"
	HFDeleted            = "deleted"
	HFNotFound           = "not_found"
	HFCompleted          = "completed"
)

// LiumReason is a Lium pod's Reason, its status, set flags and own reasons as words
func LiumReason(words ...string) string { return strings.Join(words, " ") }

// HasWord reports whether a Reason built by LiumReason holds word as a whole word
func HasWord(reason, word string) bool {
	return slices.Contains(strings.Fields(reason), word)
}
