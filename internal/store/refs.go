package store

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/google/uuid"

	"github.com/oynozan/bugnet-router/internal/provider"
)

// RefSealer seals a placement's provider credentials and opens them again
type RefSealer interface {
	SealRef(placement uuid.UUID, value string) string
	OpenRef(placement uuid.UUID, sealed string) (string, error)
}

// credentialFields are the Ref.Meta fields that hold a provider credential
var credentialFields = []string{"envd_token", "traffic_token", "secret", "_token"}

// sealedPrefix starts every sealed field, so a field without it was written before sealing
const sealedPrefix = "v1."

var errNoSealer = errors.New("store: provider credentials need a ref sealer")

// sealRef returns a copy of ref with its credential fields sealed for placement
func (s *Store) sealRef(placement uuid.UUID, ref provider.Ref) (provider.Ref, error) {
	out := ref
	out.Meta = maps.Clone(ref.Meta)
	for _, field := range credentialFields {
		v, ok := out.Meta[field]
		if !ok {
			continue
		}
		if s.sealer == nil {
			return provider.Ref{}, fmt.Errorf("seal %s: %w", field, errNoSealer)
		}
		out.Meta[field] = s.sealer.SealRef(placement, v)
	}
	return out, nil
}

// openRef opens the sealed credential fields of p's ref in place
func (s *Store) openRef(p *Placement) error {
	for _, field := range credentialFields {
		v, ok := p.ProviderRef.Meta[field]
		if !ok || !strings.HasPrefix(v, sealedPrefix) {
			continue
		}
		if s.sealer == nil {
			return fmt.Errorf("open %s: %w", field, errNoSealer)
		}
		plain, err := s.sealer.OpenRef(p.ID, v)
		switch {
		case err == nil:
			p.ProviderRef.Meta[field] = plain
		case p.State != PlacementReady:
			// Only a ready placement uses its credentials, a destroy needs only the sandbox id
			delete(p.ProviderRef.Meta, field)
		default:
			return fmt.Errorf("open %s: %w", field, err)
		}
	}
	return nil
}
