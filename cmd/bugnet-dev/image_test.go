package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func scanDir(t *testing.T, files map[string]string) (int, string) {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var out, errb bytes.Buffer
	code := run(context.Background(), []string{"image", "scan", "--dir", dir}, &out, &errb,
		func(string) string { return "" })
	return code, out.String() + errb.String()
}

func TestImageScanPlantedTokenIsOneFindingWithoutItsValue(t *testing.T) {
	value := "ghp_" + strings.Repeat("Ab3", 10)
	code, out := scanDir(t, map[string]string{"setup-image.sh": "x=" + value + "\n"})
	if code != 1 || strings.Count(out, "\n") != 1 || strings.Contains(out, value) {
		t.Fatalf("code %d, output %q", code, out)
	}
	if !strings.Contains(out, "shape github at setup-image.sh") {
		t.Fatalf("output %q", out)
	}
}

func TestImageScanNeedleInTheEnvironmentIsFound(t *testing.T) {
	value := "needle-value-1234567890"
	t.Setenv("BUGNET_HF_TOKEN", value)
	code, out := scanDir(t, map[string]string{"pins.conf": "A=" + value})
	if code != 1 || strings.Contains(out, value) || !strings.Contains(out, "BUGNET_HF_TOKEN") {
		t.Fatalf("code %d, output %q", code, out)
	}
}

func TestImageScanCleanPayloadPasses(t *testing.T) {
	code, out := scanDir(t, map[string]string{"VERSION": "2026.11.1\n"})
	if code != 0 || !strings.Contains(out, "no findings") {
		t.Fatalf("code %d, output %q", code, out)
	}
}

func TestImageScanNeedsOneSource(t *testing.T) {
	var out, errb bytes.Buffer
	code := run(context.Background(), []string{"image", "scan"}, &out, &errb,
		func(string) string { return "" })
	if code != 2 {
		t.Fatalf("code %d", code)
	}
}
