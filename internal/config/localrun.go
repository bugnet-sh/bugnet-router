//go:build localrun

package config

// A localrun build lets secrets.providers list local, for the x402 local run
func init() { localSecretsAllowed = true }

type localrunFile struct {
	ForwardListen string `toml:"forward_listen"`
	EgressPort    int    `toml:"egress_port"`
}

func (f localrunFile) read() Localrun {
	return Localrun{ForwardListen: f.ForwardListen, EgressPort: f.EgressPort}
}
