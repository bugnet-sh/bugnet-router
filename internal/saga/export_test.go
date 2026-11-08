package saga

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// SetCleanupTimeout shortens the bound on each cleanup call for one test
func SetCleanupTimeout(t testing.TB, d time.Duration) {
	old := cleanupTimeout
	cleanupTimeout = d
	t.Cleanup(func() { cleanupTimeout = old })
}

// ByListing is the order ListExtensions sorts its extensions in
var ByListing = byListing

// SetGateRetry spaces the gate health tries of every saga in this test binary
func SetGateRetry(d time.Duration) { gateRetry = d }

// SetupSpecFields and HardenSpecFields are the spec files Boot writes, field by field
type (
	SetupSpecFields  = setupSpec
	HardenSpecFields = hardenSpec
)

// SetupSpecFor is the setup spec Boot writes for a sandbox of provider name
func SetupSpecFor(
	b BootSettings, name string, ref provider.Ref, key string, sample bool,
) SetupSpecFields {
	return b.setupSpecFor(name, ref, key, sample)
}

// HardenSpecFor is the harden spec Boot writes for provider name, false when it runs none
func HardenSpecFor(b BootSettings, name string) (HardenSpecFields, bool) {
	spec, _, ok := b.hardenSpecFor(name)
	return spec, ok
}

// SSHSpecFields is the ssh-up spec the saga writes, field by field
type SSHSpecFields = sshSpec

// SSHSpecFor is the ssh-up spec the saga writes for a sandbox of prov
func SSHSpecFor(
	s SSHSettings, prov provider.Provider, ref provider.Ref, authorize []string,
) SSHSpecFields {
	return s.specFor(prov, ref, authorize)
}

// Refunds prices a refundable end's unused seconds over a placement's orders, newest first
var Refunds = refunds

// SetBeforeMark runs f between End's first read of a placement and its row lock
func SetBeforeMark(s *Saga, f func()) { s.beforeMark = f }
