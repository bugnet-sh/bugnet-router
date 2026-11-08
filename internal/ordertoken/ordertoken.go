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

	"github.com/bugnet-sh/bugnet-router/internal/solana"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
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
	// Payer is the base58 Solana address that paid, empty when Payments sent none
	Payer string `json:"payer,omitempty"`
	// FeePayer is the payment requirements' extra.feePayer, the facilitator's address
	FeePayer string `json:"fee_payer,omitempty"`
	// Tx is the base58 signature of the payment's transaction
	Tx string `json:"tx,omitempty"`
	// AllowanceMicros is the x402 allowance inside AmountMicros, start tokens only
	AllowanceMicros int64 `json:"allowance_micros,omitempty"`
}

// PackageMicros is what the order paid for its package, the x402 allowance left out
func (c Claims) PackageMicros() int64 { return c.AmountMicros - c.AllowanceMicros }

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
	if c.Kind == KindExtend && c.HandleSHA256 == "" || !payment(c) {
		return Claims{}, ErrMalformed
	}
	if !now.Before(c.ExpiresAt) {
		return Claims{}, ErrExpired
	}
	return c, nil
}

// payment reports whether the payment claims are well formed, each one optional
func payment(c Claims) bool {
	for _, addr := range []string{c.Payer, c.FeePayer} {
		if _, err := wallet.ParseSolana(addr); addr != "" && err != nil {
			return false
		}
	}
	// A signature is checked by decoding, since one of 64 bytes may be 86 to 88 characters
	if _, ok := solana.Signature(c.Tx); c.Tx != "" && !ok {
		return false
	}
	if c.AllowanceMicros != 0 && c.Kind != KindStart {
		return false
	}
	return c.AllowanceMicros >= 0 && c.AllowanceMicros <= c.AmountMicros
}
