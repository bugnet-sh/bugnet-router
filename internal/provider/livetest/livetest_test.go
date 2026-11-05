package livetest

import "testing"

func TestSettingsReadTheDevFile(t *testing.T) {
	cfg := Settings(t)
	if len(cfg.E2B.Templates) == 0 || cfg.Boxd.Region == "" || cfg.Koyeb.Region == "" {
		t.Fatalf("settings %+v, want the e2b templates and provider regions of config/dev.toml",
			cfg)
	}
}
