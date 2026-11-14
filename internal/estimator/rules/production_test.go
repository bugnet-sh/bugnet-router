package rules

import (
	"path/filepath"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
)

func productionInput(t *testing.T, req Request, f facts.Facts) Input {
	t.Helper()
	c, err := catalog.Load(filepath.Join("..", "..", "..", "config", "packages.json"))
	if err != nil {
		t.Fatal(err)
	}
	return Input{Request: req, Facts: f, Catalog: c.All()}
}

// handedPrices are the live prices of small-10m and its extension
var handedPrices = map[string]int64{"small-10m": 3355, "small-10m-more": 3300}

// a few MiB of source must still fit small-10m, whose disk is 10 GiB
func TestSmallServeSourceFitsSmall10m(t *testing.T) {
	f := facts.Facts{Repo: &facts.Repo{Tree: facts.TreeSummary{Bytes: 4 << 20, Blobs: 60}}}
	o := Run(productionInput(t, Request{Workload: WorkloadServe}, f))
	if o.Floor.DiskGiB > 10 {
		t.Fatalf("disk floor %d GiB, want at most 10", o.Floor.DiskGiB)
	}
	for _, id := range []string{"small-10m", "large-10m"} {
		var ok bool
		for _, fit := range o.Fits {
			if fit.PackageID == id {
				ok = fit.Fits || fit.ExtensionsNeeded > 0
				if len(fit.ShortOn) > 0 && fit.ShortOn[0] != DimensionMinutes {
					t.Errorf("%s is short on %v, want time only", id, fit.ShortOn)
				}
			}
		}
		if !ok {
			t.Errorf("%s does not fit a small serve source", id)
		}
	}
}

// a 30-minute serve takes a base package and two extensions of the same rate
func TestProductionServeRecommendsExtensions(t *testing.T) {
	in := productionInput(t, Request{Workload: WorkloadServe}, facts.Facts{})
	in.Prices = handedPrices
	o := Run(in)
	var rec *Fit
	for i, fit := range o.Fits {
		if fit.PackageID == "small-10m" {
			rec = &o.Fits[i]
		}
	}
	if rec == nil || rec.ExtensionsNeeded != 2 || rec.ExtensionPackageID != "small-10m-more" {
		t.Fatalf("small-10m fit is %+v, want 2 extensions of small-10m-more", rec)
	}
	eq(t, rec.TotalPriceMicros, int64(3355+2*3300))
}

// the default minutes cover a small install, so a Vite starter still takes two extensions
func TestDefaultMinutesCoverASmallInstall(t *testing.T) {
	f := facts.Facts{Repo: &facts.Repo{Lockfiles: []facts.Lockfile{{Packages: 9}}}}
	eq(t, Run(productionInput(t, Request{Workload: WorkloadServe}, f)).Floor.Minutes, 30)
}

// install time past the covered 5 minutes still adds to the default
func TestALongInstallAddsMinutes(t *testing.T) {
	f := facts.Facts{Repo: &facts.Repo{NativeBuild: true,
		Pins: []facts.Pin{{Tool: "go-toolchain", Version: "1.27"}}}}
	eq(t, Run(productionInput(t, Request{Workload: WorkloadTests}, f)).Floor.Minutes, 12)
	eq(t, Run(productionInput(t, Request{Workload: WorkloadTests, Minutes: 10}, f)).Floor.Minutes,
		10)
}
