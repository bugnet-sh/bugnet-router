package store

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// RefSealer seals a placement's provider credentials and opens them again
type RefSealer interface {
	SealRef(placement uuid.UUID, value string) string
	OpenRef(placement uuid.UUID, sealed string) (string, error)
}

// CredentialFields are the Ref.Meta fields that hold a provider credential
var CredentialFields = []string{"envd_token", "traffic_token", "secret", "_token"}

// sealedPrefix starts every sealed field, so a field without it was written before sealing
const sealedPrefix = "v1."

var errNoSealer = errors.New("store: provider credentials need a ref sealer")

// sealRef returns a copy of ref with its credential fields sealed for placement
func (s *Store) sealRef(placement uuid.UUID, ref provider.Ref) (provider.Ref, error) {
	out := ref
	out.Meta = maps.Clone(ref.Meta)
	for _, field := range CredentialFields {
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

// openRef opens the sealed credential fields of p's ref in place and drops any it cannot open
func (s *Store) openRef(p *Placement) {
	for _, field := range CredentialFields {
		v, ok := p.ProviderRef.Meta[field]
		if !ok || !strings.HasPrefix(v, sealedPrefix) {
			continue
		}
		plain, err := s.open(p.ID, v)
		if err != nil {
			// A destroy needs only the sandbox id, so only a call that uses the credential fails
			delete(p.ProviderRef.Meta, field)
			s.log.Warn("provider credential does not open, it reads as absent",
				"placement", p.ID, "field", field, "err", err)
			continue
		}
		p.ProviderRef.Meta[field] = plain
	}
}

func (s *Store) open(placement uuid.UUID, sealed string) (string, error) {
	if s.sealer == nil {
		return "", errNoSealer
	}
	return s.sealer.OpenRef(placement, sealed)
}
