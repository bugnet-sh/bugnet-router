package wallets

import (
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
)

const secondsPerDay = 86400

// StartBuckets are the payer's starts an hour and a day, a start taking one of each
func StartBuckets(w config.Wallets, payer string) []ratelimit.Bucket {
	return []ratelimit.Bucket{
		{
			Key: "rl:wallet:h:" + payer, PerSecond: ratelimit.PerHour(w.StartsPerHour),
			Burst: w.StartsBurst,
		},
		{
			Key:       "rl:wallet:d:" + payer,
			PerSecond: float64(w.StartsPerDay) / secondsPerDay, Burst: w.StartsPerDay,
		},
	}
}

// RefusalBucket is the payer's refused orders an hour, a block once it runs dry
func RefusalBucket(w config.Wallets, payer string) ratelimit.Bucket {
	return ratelimit.Bucket{
		Key: "rl:wallet:refusals:" + payer, PerSecond: ratelimit.PerHour(w.RefusalsPerHour),
		Burst: w.RefusalsPerHour,
	}
}

// OpsBucket is the payer's user operations a second
func OpsBucket(w config.Wallets, payer string) ratelimit.Bucket {
	return ratelimit.Bucket{
		Key: "rl:wallet:ops:" + payer, PerSecond: float64(w.OpsPerSecond), Burst: w.OpsBurst,
	}
}

// AccountBucket is a provider account's user operations a second, its burst one second's worth
func AccountBucket(provider string, perSecond int) ratelimit.Bucket {
	return ratelimit.Bucket{
		Key: "rl:acct:" + provider, PerSecond: float64(perSecond), Burst: perSecond,
	}
}
