//go:build localrun

package config

import (
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

func TestLocalrunSeams(t *testing.T) {
	c, err := LoadFile(writeFile(t, "[secrets]\nproviders = [\"local\"]"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Secrets.Providers, []string{"local"})
	reg, err := provider.NewRegistry([]provider.Provider{local.New(local.Options{Root: t.TempDir()})},
		provider.Features{Secrets: c.Secrets.Providers})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := reg.Get("local")
	eq(t, reg.Capabilities(p).Secrets, true)
}
