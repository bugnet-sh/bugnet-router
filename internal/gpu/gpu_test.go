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
	for name, want := range map[string]string{
		"T4 Tensor Core GPU": T4, "RTX 3090": RTX3090, "RTX A6000": RTXA6000,
		"RTX 4090": RTX4090, "RTX 5090": RTX5090, "RTX 6000 Ada Generation": RTX6000Ada,
		"A100-SXM4-80GB": A100, "RTX PRO 6000 Blackwell Server Edition": RTXPro6000,
		"RTX PRO 6000 Blackwell Workstation Edition": RTXPro6000, "H100 80GB HBM3": H100,
		"H200": H200, "B200": B200,
	} {
		if got, ok := FromLium(name); !ok || got != want {
			t.Errorf("FromLium(%q) = %q, %v, want %q", name, got, ok, want)
		}
	}
	for _, name := range []string{
		"", "GB200", "GB300", "A100-SXM4-40GB", "H100 PCIe", "rtx 4090", "NVIDIA RTX 4090",
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
