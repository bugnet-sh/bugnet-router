package health

import (
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// StartCost estimates what a sandbox of size on prov costs over d, its hourly rate for d rounded
// up to the billing increment, plus the provider's fee for the start
func StartCost(prov provider.Provider, size provider.Size, d time.Duration) int64 {
	if inc := prov.BillingIncrement(); inc > 0 {
		d = (d + inc - 1) / inc * inc
	}
	micros := (prov.HourlyCostMicros(size)*int64(d) + int64(time.Hour) - 1) / int64(time.Hour)
	if c, ok := provider.As[provider.StartCoster](prov); ok {
		if fee, ok := c.StartCostMicros(size); ok {
			micros += fee
		}
	}
	return micros
}
