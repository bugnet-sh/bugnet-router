package stats

import "github.com/bugnet-sh/bugnet-router/internal/config"

// RouterLimits returns the router's keys of CP 20.6.2, leaving out the limits no one meets
func RouterLimits(c *config.Config) map[string]int64 {
	out := map[string]int64{
		"sandbox.max_running_jobs": int64(c.MaxRunningJobs),
		"transfer.max_file_bytes":  c.Transfers.MaxBytes,
	}
	if w := c.Wallets; w.UsePayer {
		out["wallet.max_concurrent"] = int64(w.MaxConcurrent)
		out["wallet.starts_per_hour"] = int64(w.StartsPerHour)
		out["wallet.starts_per_day"] = int64(w.StartsPerDay)
		if n, ok := walletBytesPerDay(c); ok {
			out["wallet.bytes_per_day"] = n
		}
		_, refunds := walletEnds(c)
		out["wallet.failed_refunds_per_day"] = refunds
	}
	if len(c.Previews.Providers) > 0 {
		out["sandbox.max_previews"] = int64(c.Previews.MaxPerPlacement)
	}
	if len(c.SSH.Providers) > 0 {
		out["sandbox.max_ssh_connections"] = int64(c.SSH.MaxConnectionsPerPlacement)
	}
	if len(c.Secrets.Providers) > 0 {
		out["sandbox.max_secrets"] = int64(c.Secrets.MaxPerPlacement)
	}
	return out
}
