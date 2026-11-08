package saga_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

// runnerBin is the host build of bugnet-run every local sandbox in these tests runs
var runnerBin string

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "bugnet-run-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	runnerBin = filepath.Join(dir, "bugnet-run")
	build := exec.Command("go", "build", "-o", runnerBin,
		"github.com/bugnet-sh/bugnet-run/cmd/bugnet-run")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build bugnet-run:", err)
		return 1
	}
	// A sandbox whose gate never answers would otherwise wait 4 s for its three tries
	saga.SetGateRetry(10 * time.Millisecond)
	return m.Run()
}
