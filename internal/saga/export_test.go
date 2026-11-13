package saga

import (
	"github.com/bugnet-sh/bugnet-router/internal/image"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// SetDestroyTimeout shortens the bound on each destroy of placing for one test
func SetDestroyTimeout(t testing.TB, d time.Duration) {
	old := destroyTimeout
	destroyTimeout = d
	t.Cleanup(func() { destroyTimeout = old })
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
	return b.setupSpecFor(name, ref, key, sample, nil)
}

// SetupSpecForImage is SetupSpecFor for a sandbox whose image carries manifest m
func SetupSpecForImage(
	b BootSettings, name string, ref provider.Ref, key string, m *image.Manifest,
) SetupSpecFields {
	return b.setupSpecFor(name, ref, key, false, m)
}

// PoolSetupSpecFor is the setup spec Boot writes for a pool member's sandbox
func PoolSetupSpecFor(b BootSettings, name string, ref provider.Ref) SetupSpecFields {
	return b.poolSetupSpecFor(name, ref, nil)
}

// WithinLifetime is the check an extension of a placement runs against the provider's limit
func WithinLifetime(prov provider.Provider, p store.Placement, deadline time.Time) error {
	return withinLifetime(prov, p, deadline)
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

// SetCreateWait changes how long an attempt waits for a create token in one test
func SetCreateWait(t testing.TB, d time.Duration) {
	old := createWait
	createWait = d
	t.Cleanup(func() { createWait = old })
}

// LifetimeUntil is the lifetime a reserved sandbox is asked for, never past the provider's limit
var LifetimeUntil = lifetimeUntil
