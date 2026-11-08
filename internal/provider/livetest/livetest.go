// Package livetest gives the live provider tests the settings in config/dev.toml
package livetest

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/config"
)

// Settings reads config/dev.toml from the module root, found from this file's own path
func Settings(t testing.TB) config.Config {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("livetest: the source path of livetest is unknown")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	cfg, err := config.LoadFile(filepath.Join(root, "config", "dev.toml"))
	if err != nil {
		t.Fatalf("livetest: %v", err)
	}
	return cfg
}
