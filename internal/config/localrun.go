//go:build localrun

package config

// A localrun build lets secrets.providers list local, for the x402 local run
func init() { localSecretsAllowed = true }
