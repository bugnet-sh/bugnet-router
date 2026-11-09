package rules

import (
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
)

// mlRepo makes a floor of 4096 MiB for tests, on the 4096 MiB package
func mlRepo() facts.Facts {
	return facts.Facts{Repo: &facts.Repo{ML: []string{"torch"}}}
}

func withHistory(t *testing.T, f facts.Facts, h History) Output {
	t.Helper()
	in := input(t, Request{Workload: WorkloadTests}, f)
	in.History = h
	return Run(in)
}

func TestRaiseOnlyBelowTen(t *testing.T) {
	base := History{Observations: 5, Payers: 3, MemoryMiB: 1000, HeadroomPercent: 10}
	o := withHistory(t, mlRepo(), base)
	eq(t, o.Floor.MemoryMiB, 4096)
	eq(t, o.Basis, BasisSignals)
	// Past the lowering thresholds a floor may drop, never below the workload default
	base.Lower = true
	o = withHistory(t, mlRepo(), base)
	eq(t, o.Floor.MemoryMiB, 2048)
	eq(t, o.Basis, BasisHistory)
	// Without signals the default is the floor already
	o = withHistory(t, facts.Facts{}, base)
	eq(t, o.Floor.MemoryMiB, 2048)
	eq(t, o.Basis, BasisDefaults)
	// A peak above the floor raises it with the headroom, rounded to 256 MiB
	base.MemoryMiB = 5000
	o = withHistory(t, mlRepo(), base)
	eq(t, o.Floor.MemoryMiB, 5632)
	eq(t, o.Basis, BasisHistory)
}

func TestOneCatalogStep(t *testing.T) {
	// The test catalog holds 1024, 4096, 32768 and 45056 MiB of memory
	h := History{Observations: 5, Payers: 3, MemoryMiB: 44000, HeadroomPercent: 10,
		DiskMiB: 100 << 10, ActiveMinutes: 100}
	o := withHistory(t, mlRepo(), h)
	eq(t, o.Floor.MemoryMiB, 32768)
	// Disk 8 GiB plus ML is 16, covered by 20 GiB, one step up is 50
	eq(t, o.Floor.DiskGiB, 50)
	// 10 minutes is covered by 10, one step up is 30
	eq(t, o.Floor.Minutes, 30)
	// A floor above every package has no step to take
	h = History{Observations: 5, Payers: 3, MemoryMiB: 60000, HeadroomPercent: 10}
	in := input(t, Request{Workload: WorkloadTests}, mlRepo())
	in.Catalog = in.Catalog[:2]
	in.History = h
	eq(t, Run(in).Floor.MemoryMiB, 4096)
}

func TestOOMPushesAboveThePackage(t *testing.T) {
	h := History{Observations: 5, Payers: 3, OOMPackages: []string{"cpu-std-30m"},
		HeadroomPercent: 10}
	o := withHistory(t, facts.Facts{}, h)
	eq(t, o.Floor.MemoryMiB, 4352)
	eq(t, hasWarning(o, WarningRanOutOfMemory), true)
	eq(t, o.Basis, BasisHistory)
	for _, f := range o.Fits {
		if f.PackageID == "cpu-std-30m" {
			eq(t, f.Fits, false)
		}
	}
	// A floor already above the package needs no push
	h.OOMPackages = []string{"cpu-small-10m"}
	o = withHistory(t, facts.Facts{}, h)
	eq(t, o.Floor.MemoryMiB, 2048)
	eq(t, hasWarning(o, WarningRanOutOfMemory), false)
}

func TestHistoryZeroLeavesTheAnswerAlone(t *testing.T) {
	a := withHistory(t, mlRepo(), History{})
	b := Run(input(t, Request{Workload: WorkloadTests}, mlRepo()))
	eq(t, mustCanon(t, a), mustCanon(t, b))
}
