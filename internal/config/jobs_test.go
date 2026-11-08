package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobs"
)

// Outside package config, since jobs reaches config through store and x402
func TestLoadFileOutputDefaultsMatchTheJobsService(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.toml")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.JobOutputBudget != jobs.DefaultJobBudget ||
		c.PlacementOutputBudget != jobs.DefaultPlacementBudget ||
		c.PlacementTailReserve != jobs.DefaultTailReserve ||
		c.MaxRunningJobs != jobs.DefaultMaxRunning {
		t.Fatalf("output limits %d, %d, %d and %d jobs, want the jobs service's defaults",
			c.JobOutputBudget, c.PlacementOutputBudget, c.PlacementTailReserve, c.MaxRunningJobs)
	}
}
