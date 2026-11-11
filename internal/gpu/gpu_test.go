package gpu

import (
	"slices"
	"strings"
	"testing"
	"unicode"
)

func TestModels(t *testing.T) {
	want := []string{
		"NVIDIA T4", "NVIDIA L4", "NVIDIA A10G", "NVIDIA L40S", "NVIDIA A100 80GB",
		"NVIDIA H100 80GB", "NVIDIA H200", "NVIDIA B200", "NVIDIA RTX 3090", "NVIDIA RTX 4090",
		"NVIDIA RTX 5090", "NVIDIA RTX A6000", "NVIDIA RTX 6000 Ada", "NVIDIA RTX PRO 6000",
		"NVIDIA RTX 4000 SFF Ada",
	}
	got := Models()
	if !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
		t.Fatalf("Models() = %q, want %q", got, want)
	}
	got[0] = "changed"
	if Models()[0] == "changed" {
		t.Fatal("Models shares its list with the caller")
	}
	for _, m := range want {
		if !Known(m) {
			t.Errorf("%q is not known", m)
		}
		// The public stats feed sends the name as it is
		if len(m) > 64 || strings.TrimSpace(m) != m ||
			strings.ContainsFunc(m, func(r rune) bool { return !unicode.IsPrint(r) }) {
			t.Errorf("%q cannot go on a public page", m)
		}
	}
	for _, m := range []string{
		"", "nvidia t4", "NVIDIA T4 ", "T4", "NVIDIA A100", "NVIDIA A100 40GB", "NVIDIA H100",
		"NVIDIA GB200", "NVIDIA RTX PRO 6000 Blackwell Server Edition", "AMD MI300X",
	} {
		if Known(m) {
			t.Errorf("%q is known, want it refused", m)
		}
	}
}

func TestFromHF(t *testing.T) {
	for name, want := range map[string]string{
		"T4": T4, "L4": L4, "A10G": A10G, "L40S": L40S, "A100": A100, "H200": H200,
		"RTX PRO 6000": RTXPro6000,
	} {
		if got, ok := FromHF(name); !ok || got != want {
			t.Errorf("FromHF(%q) = %q, %v, want %q", name, got, ok, want)
		}
	}
	for _, name := range []string{"", "t4", "H100", "A100 80GB", "NVIDIA T4", "Inferentia2"} {
		if got, ok := FromHF(name); ok || got != "" {
			t.Errorf("FromHF(%q) = %q, %v, want it refused", name, got, ok)
		}
	}
}

func TestFromLium(t *testing.T) {
	// The gpu_class and machine_name values of GET /executors on 2026-11-11, and the A100
	// machine names of the public feed's rented nodes
	for name, want := range map[string]string{
		"NVIDIA T4 Tensor Core GPU": T4, "NVIDIA GeForce RTX 3090": RTX3090,
		"NVIDIA RTX A6000": RTXA6000, "NVIDIA GeForce RTX 4090": RTX4090,
		"NVIDIA GeForce RTX 5090": RTX5090, "NVIDIA RTX 6000 Ada Generation": RTX6000Ada,
		"NVIDIA A100-SXM4-80GB": A100, "NVIDIA A100 80GB PCIe": A100,
		"NVIDIA H100 80GB HBM3": H100, "NVIDIA H100 PCIe": H100, "NVIDIA H200": H200,
		"NVIDIA H200 NVL": H200, "NVIDIA B200": B200,
		"NVIDIA RTX PRO 6000 Blackwell Server Edition":            RTXPro6000,
		"NVIDIA RTX PRO 6000 Blackwell Workstation Edition":       RTXPro6000,
		"NVIDIA RTX PRO 6000 Blackwell Max-Q Workstation Edition": RTXPro6000,
	} {
		if got, ok := FromLium(name); !ok || got != want {
			t.Errorf("FromLium(%q) = %q, %v, want %q", name, got, ok, want)
		}
	}
	for _, name := range []string{
		"", "NVIDIA GB200", "NVIDIA GB300", "NVIDIA A100-SXM4-40GB", "NVIDIA H100 NVL",
		"nvidia geforce rtx 4090", "NVIDIA RTX 4090", "RTX 4090", "T4 Tensor Core GPU",
		"NVIDIA GeForce RTX 3090 Ti", "NVIDIA B300 SXM6 AC",
	} {
		if got, ok := FromLium(name); ok || got != "" {
			t.Errorf("FromLium(%q) = %q, %v, want it refused", name, got, ok)
		}
	}
}

// TestMappedModelsAreKnown holds every provider name to a canonical model
func TestMappedModelsAreKnown(t *testing.T) {
	for _, m := range []map[string]string{hfModels, liumModels} {
		for name, model := range m {
			if !Known(model) {
				t.Errorf("%q maps to %q, which is not a canonical model", name, model)
			}
		}
	}
}

func TestHFFlavors(t *testing.T) {
	// The GPU flavors HF's hardware list gave on 2026-11-11, design 4.5
	want := []string{
		"t4-small", "t4-medium", "l4x1", "l4x4", "a10g-small", "a10g-large", "a10g-largex2",
		"a10g-largex4", "l40sx1", "l40sx4", "l40sx8", "a100-large", "a100x4", "a100x8", "h200",
		"h200x2", "h200x4", "h200x8", "rtx-pro-6000", "rtx-pro-6000x2", "rtx-pro-6000x4",
		"rtx-pro-6000x8",
	}
	got := HFFlavors()
	if !slices.Equal(got, want) {
		t.Fatalf("HFFlavors() = %q, want %q", got, want)
	}
	got[0] = "changed"
	if HFFlavors()[0] == "changed" {
		t.Fatal("HFFlavors shares its list with the caller")
	}
	for _, f := range want {
		if !HFFlavor(f) {
			t.Errorf("%q is not an HF GPU flavor", f)
		}
	}
	// CPU flavors, and those the create schema holds but the hardware list does not offer
	for _, f := range []string{
		"", "cpu-basic", "cpu-upgrade", "cpu-xl", "sprx8", "zero-a10g", "inf2x6", "T4-small",
		"t4-small ", "a100",
	} {
		if HFFlavor(f) {
			t.Errorf("%q is an HF GPU flavor, want it refused", f)
		}
	}
}
