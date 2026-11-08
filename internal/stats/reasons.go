package stats

import (
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// endWords maps each raw reason to CP 20.6.4's public word, Task 28B's reasons apart
var endWords = map[string]string{
	"stop":                 "stop",
	"deadline":             "deadline",
	"provider_failed":      "provider_failed",
	"abuse":                "stopped_by_bugnet",
	"no_candidate":         "no_capacity",
	"capacity_full":        "no_capacity",
	"candidates_exhausted": "no_capacity",
	"wallet_busy":          "wallet_limit",
	"wallet_rate_limited":  "wallet_limit",
	"boot_failed":          "start_failed",
	"create_failed":        "start_failed",
	"runner_unavailable":   "start_failed",
	"internal_error":       "start_failed",
	"placing_stalled":      "start_failed",
}

// Ends turns raw end reasons into public words
type Ends struct{ extra map[string]string }

// NewEnds reads Task 28B's reasons from their source
func NewEnds(c *config.Config) Ends {
	words, _ := walletEnds(c)
	return Ends{extra: words}
}

// EndReason returns CP 20.6.4's word, nil while the placement runs
func (e Ends) EndReason(state, terminationReason, failureReason string) *string {
	reason := terminationReason
	switch state {
	case store.PlacementReady, store.PlacementPlacing:
		return nil
	case store.PlacementFailed:
		reason = failureReason
	}
	if reason == "" {
		reason = terminationReason + failureReason
	}
	word, ok := endWords[reason]
	if !ok {
		word, ok = e.extra[reason]
	}
	switch {
	case ok:
	case strings.HasPrefix(reason, "provider_"):
		word = "start_failed"
	default:
		// A new reason never goes out raw
		word = "other"
	}
	return &word
}
