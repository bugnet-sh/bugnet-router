//go:build !localrun

package config

import (
	"strings"
	"testing"
)

func TestLocalrunSeamsAbsentInNormalBuilds(t *testing.T) {
	_, err := LoadFile(writeFile(t, "[secrets]\nproviders = [\"local\"]"))
	if err == nil || !strings.Contains(err.Error(), "may not list local") {
		t.Fatal("secrets.providers = local was accepted")
	}
}
