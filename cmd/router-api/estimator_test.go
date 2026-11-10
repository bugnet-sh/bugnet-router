package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/config"
)

func manifestsOf(t *testing.T, i config.Images) string {
	t.Helper()
	raw, err := json.Marshal(imageManifests(i, []string{"koyeb", "boat"}))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestCatalogBytesKeepVersionOSAndToolsOnly(t *testing.T) {
	base := config.Images{Koyeb: config.ProviderImage{
		Version: "2026.11.1", OS: "ubuntu-24.04", Ref: "ghcr.io/x/b@sha256:aa",
		Tools: map[string]string{"git": "2.43", "pnpm": "12.18"},
	}}
	was := manifestsOf(t, base)
	if strings.Contains(was, "ghcr.io") || strings.Contains(was, "sha256") {
		t.Fatalf("the manifests carry a ref: %s", was)
	}
	// A new ref alone mints no new catalog
	moved := base
	moved.Koyeb.Ref = "ghcr.io/x/b@sha256:bb"
	if manifestsOf(t, moved) != was {
		t.Fatal("a ref change moved the manifests")
	}
	// A tool the rules never read still moves it, as a version and an OS do
	for name, change := range map[string]func(*config.ProviderImage){
		"tool": func(p *config.ProviderImage) {
			p.Tools = map[string]string{"git": "2.43", "pnpm": "12.19"}
		},
		"version": func(p *config.ProviderImage) { p.Version = "2026.12.1" },
		"os":      func(p *config.ProviderImage) { p.OS = "ubuntu-22.04" },
	} {
		next := base
		next.Koyeb.Tools = map[string]string{"git": "2.43", "pnpm": "12.18"}
		change(&next.Koyeb)
		if manifestsOf(t, next) == was {
			t.Fatalf("a %s change left the manifests alone", name)
		}
	}
}
