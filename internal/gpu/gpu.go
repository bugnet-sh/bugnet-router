// Package gpu names the GPU models the router sells and reads each provider's names for them
package gpu

import "slices"

// Canonical model names, as a catalog package's gpu_model and a size's GPUModel state them
const (
	T4            = "NVIDIA T4"
	L4            = "NVIDIA L4"
	A10G          = "NVIDIA A10G"
	L40S          = "NVIDIA L40S"
	A100          = "NVIDIA A100 80GB"
	H100          = "NVIDIA H100 80GB"
	H200          = "NVIDIA H200"
	B200          = "NVIDIA B200"
	RTX3090       = "NVIDIA RTX 3090"
	RTX4090       = "NVIDIA RTX 4090"
	RTX5090       = "NVIDIA RTX 5090"
	RTXA6000      = "NVIDIA RTX A6000"
	RTX6000Ada    = "NVIDIA RTX 6000 Ada"
	RTXPro6000    = "NVIDIA RTX PRO 6000"
	RTX4000SFFAda = "NVIDIA RTX 4000 SFF Ada"
)

var models = []string{
	T4, L4, A10G, L40S, A100, H100, H200, B200, RTX3090, RTX4090, RTX5090, RTXA6000, RTX6000Ada,
	RTXPro6000, RTX4000SFFAda,
}

// hfModels maps the accelerator.model values of HF's hardware list
var hfModels = map[string]string{
	"T4": T4, "L4": L4, "A10G": A10G, "L40S": L40S, "A100": A100, "H200": H200,
	"RTX PRO 6000": RTXPro6000,
}

// liumModels maps the GPU type names of Lium's public node feed
var liumModels = map[string]string{
	"T4 Tensor Core GPU": T4, "RTX 3090": RTX3090, "RTX A6000": RTXA6000, "RTX 4090": RTX4090,
	"RTX 5090": RTX5090, "RTX 6000 Ada Generation": RTX6000Ada, "A100-SXM4-80GB": A100,
	"RTX PRO 6000 Blackwell Server Edition":      RTXPro6000,
	"RTX PRO 6000 Blackwell Workstation Edition": RTXPro6000,
	"H100 80GB HBM3": H100, "H200": H200, "B200": B200,
}

// Models lists every canonical model name
func Models() []string { return slices.Clone(models) }

// Known reports whether name is a canonical model name, matched exactly
func Known(name string) bool { return slices.Contains(models, name) }

// FromHF returns the canonical model for an HF accelerator.model, false for a name it lacks
func FromHF(name string) (string, bool) { return lookup(hfModels, name) }

// FromLium returns the canonical model for a Lium GPU type name, false for a name it lacks
func FromLium(name string) (string, bool) { return lookup(liumModels, name) }

func lookup(m map[string]string, name string) (string, bool) {
	model, ok := m[name]
	return model, ok
}
