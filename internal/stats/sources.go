package stats

import "github.com/bugnet-sh/bugnet-router/internal/config"

// walletEnds returns Task 28B's end reasons as CP 20.6.4's words, and its failed refunds a day.
// Abuse and blocks go out as stopped_by_bugnet, CP 20.12's default until the founder answers
func walletEnds(c *config.Config) (words map[string]string, failedRefundsPerDay int64) {
	return map[string]string{
		"wallet_blocked": "stopped_by_bugnet",
		"control_lost":   "stopped_by_bugnet",
		"sandbox_exited": "sandbox_exited",
		"operator":       "stopped_by_bugnet",
	}, int64(c.Wallets.FailedRefundsPerDay)
}

// walletBytesPerDay is Task 29's switch point, knowing no wallets.bytes_per_day until then
func walletBytesPerDay(_ *config.Config) (int64, bool) {
	return 0, false
}
