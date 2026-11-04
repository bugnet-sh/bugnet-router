package saga

import (
	"testing"
	"time"
)

// SetCleanupTimeout shortens the bound on each cleanup call for one test
func SetCleanupTimeout(t testing.TB, d time.Duration) {
	old := cleanupTimeout
	cleanupTimeout = d
	t.Cleanup(func() { cleanupTimeout = old })
}
