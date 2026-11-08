package stats

import "github.com/bugnet-sh/bugnet-router/internal/config"

// walletEnds is Task 28B's switch point, knowing none of its end reasons or refund cap until then
func walletEnds(_ *config.Config) (words map[string]string, failedRefundsPerDay int64, ok bool) {
	return nil, 0, false
}

// walletBytesPerDay is Task 29's switch point, knowing no wallets.bytes_per_day until then
func walletBytesPerDay(_ *config.Config) (int64, bool) {
	return 0, false
}
