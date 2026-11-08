// Package preview names previews and checks their tokens.
//
// A preview is one port of one placement, served at <id>.<preview domain>. Its id is 16
// lowercase base32 characters of 80 random bits, the host label the edge reads, and not a
// secret. Its bnp_ token is derived from the id and the row's generation (DP 4.4), so the router
// stores none, and a revocation that moves the generation kills every earlier link.
package preview

import (
	"crypto/rand"
	"encoding/base32"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
)

// IDLength is how many characters a preview id has
const IDLength = 16

// idBytes is the 80 random bits behind an id
const idBytes = 10

// TokenParam is the query parameter that carries a preview's token
const TokenParam = "bugnet_token"

// maxEarlier bounds how many earlier generations Check tries, since only a revocation moves one
const maxEarlier = 8

var b32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// NewID returns a fresh preview id
func NewID() string {
	b := make([]byte, idBytes)
	_, _ = rand.Read(b)
	return b32.EncodeToString(b)
}

// ValidID reports whether id has a preview id's shape
func ValidID(id string) bool {
	if len(id) != IDLength {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < '2' || c > '7') {
			return false
		}
	}
	return true
}

// URL is the link the agent shares, the token in its query
func URL(id, domain, token string) string {
	var b strings.Builder
	b.WriteString("https://")
	b.WriteString(id)
	b.WriteString(".")
	b.WriteString(domain)
	b.WriteString("/?" + TokenParam + "=")
	b.WriteString(token)
	return b.String()
}

// Verdict says which generation, if any, a token belongs to
type Verdict int

// Verdicts of Check
const (
	// Invalid means the token belongs to no generation of the preview under any listed root
	Invalid Verdict = iota
	// Current means the token belongs to the preview's current generation
	Current
	// Earlier means the token belongs to an earlier generation, one a revocation moved past
	Earlier
)

// Check reports which generation of the preview token belongs to, comparing in constant time
func Check(k dpkeys.Keys, id string, generation uint32, token string) Verdict {
	if k.CheckPreviewToken(id, generation, token) {
		return Current
	}
	for back := uint32(1); back <= maxEarlier && back <= generation; back++ {
		if k.CheckPreviewToken(id, generation-back, token) {
			return Earlier
		}
	}
	return Invalid
}
