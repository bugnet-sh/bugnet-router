// Package ordertoken signs and verifies the order tokens that authorize paid runs
package ordertoken

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Kind says whether a token starts a sandbox or extends one
type Kind string

// Token kinds
const (
	KindStart  Kind = "start"
	KindExtend Kind = "extend"
)

// Verify failures
var (
	ErrMalformed    = errors.New("ordertoken: malformed token")
	ErrBadSignature = errors.New("ordertoken: bad signature")
	ErrExpired      = errors.New("ordertoken: expired")
	ErrBadKind      = errors.New("ordertoken: bad kind")
)

const (
	prefix  = "v1."
	context = "bugnet-order-v1."
)

var b64 = base64.RawURLEncoding

// Claims is the signed payload of an order token
type Claims struct {
	OrderID      string    `json:"order_id"`
	Kind         Kind      `json:"kind"`
	PackageID    string    `json:"package_id"`
	AmountMicros int64     `json:"amount_micros"`
	ExpiresAt    time.Time `json:"expires_at"`
	HandleSHA256 string    `json:"handle_sha256,omitempty"` // hex, extend tokens only
}

// Sign encodes and signs claims
func Sign(priv ed25519.PrivateKey, c Claims) (string, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	seg := b64.EncodeToString(payload)
	sig := ed25519.Sign(priv, []byte(context+seg))
	return prefix + seg + "." + b64.EncodeToString(sig), nil
}

// Verify checks a token's signature, claims and expiry
func Verify(pub ed25519.PublicKey, token string, now time.Time) (Claims, error) {
	var c Claims
	rest, ok := strings.CutPrefix(token, prefix)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return c, ErrMalformed
	}
	parts := strings.Split(rest, ".")
	if len(parts) != 2 {
		return c, ErrMalformed
	}
	sig, err := b64.DecodeString(parts[1])
	if err != nil {
		return c, ErrMalformed
	}
	if !ed25519.Verify(pub, []byte(context+parts[0]), sig) {
		return c, ErrBadSignature
	}
	payload, err := b64.DecodeString(parts[0])
	if err != nil {
		return c, ErrMalformed
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Claims{}, ErrMalformed
	}
	if c.Kind != KindStart && c.Kind != KindExtend {
		return Claims{}, ErrBadKind
	}
	if c.Kind == KindExtend && c.HandleSHA256 == "" {
		return Claims{}, ErrMalformed
	}
	if !now.Before(c.ExpiresAt) {
		return Claims{}, ErrExpired
	}
	return c, nil
}
