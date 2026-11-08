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
	o := Run(productionInput(t, Request{Workload: WorkloadServe}, facts.Facts{}))
	var rec *Fit
	for i, fit := range o.Fits {
		if fit.PackageID == "small-10m" {
			rec = &o.Fits[i]
		}
	}
	if rec == nil || rec.ExtensionsNeeded != 2 || rec.ExtensionPackageID != "small-10m-more" {
		t.Fatalf("small-10m fit is %+v, want 2 extensions of small-10m-more", rec)
	}
	if rec.TotalPriceMicros != 150000 {
		t.Fatalf("total %d micros, want 150000", rec.TotalPriceMicros)
	}
}
