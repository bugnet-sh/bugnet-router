// Package handle creates and hashes the bearer handles that identify sandboxes
package handle

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"regexp"
	"strings"
)

var (
	shape = regexp.MustCompile(`^sbx_[a-z2-7]{26}$`)
	enc   = base32.StdEncoding.WithPadding(base32.NoPadding)
)

// New returns a fresh handle and the sha256 of the full handle string
func New() (string, []byte) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("handle: entropy source failed: %v", err))
	}
	h := "sbx_" + strings.ToLower(enc.EncodeToString(b))
	sum := sha256.Sum256([]byte(h))
	return h, sum[:]
}

// Hash returns the sha256 of a well formed handle
func Hash(h string) ([]byte, error) {
	if !shape.MatchString(h) {
		return nil, fmt.Errorf("handle: malformed")
	}
	sum := sha256.Sum256([]byte(h))
	return sum[:], nil
}
