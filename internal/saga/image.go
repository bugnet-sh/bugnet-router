package saga

import (
	"encoding/json"

	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// recordManifest is the part of a manifest a placement and a member row keep
type recordManifest struct {
	OS    string            `json:"os"`
	Tools map[string]string `json:"tools"`
}

// recordOfManifest is what a ready placement records of its image, nil for none
func recordOfManifest(m *image.Manifest) *store.ReadyImage {
	if m == nil {
		return nil
	}
	// A map encodes with sorted keys, so the same image always gives the same bytes
	raw, err := json.Marshal(recordManifest{OS: m.OS, Tools: m.Tools})
	if err != nil || len(raw) > 2<<10 {
		raw = nil
	}
	return &store.ReadyImage{Version: m.Version, Manifest: raw}
}

// manifestOfRecord rebuilds the manifest fields a row kept, nil for a row with no image
func manifestOfRecord(r *store.ReadyImage) *image.Manifest {
	if r == nil || r.Version == "" {
		return nil
	}
	m := &image.Manifest{Version: r.Version}
	var kept recordManifest
	if json.Unmarshal(r.Manifest, &kept) == nil {
		m.OS, m.Tools = kept.OS, kept.Tools
	}
	return m
}
