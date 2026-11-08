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
	lp := local.New(local.Options{Root: t.TempDir()})
	reg, err := provider.NewRegistry([]provider.Provider{lp},
		provider.Features{Secrets: c.Secrets.Providers})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := reg.Get("local")
	eq(t, reg.Capabilities(p).Secrets, true)
	c, err = LoadFile(writeFile(t, "[localrun]\nforward_listen = \"127.0.0.1:4443\"\n"+
		"egress_port = 4444"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Localrun, Localrun{ForwardListen: "127.0.0.1:4443", EgressPort: 4444})
}
