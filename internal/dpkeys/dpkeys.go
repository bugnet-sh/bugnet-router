// Package dpkeys derives the data plane's tokens and keys from the router's root keys.
//
// BUGNET_DATA_PLANE_KEYS lists the roots newest first, each an id, a colon and the standard
// base64 of 32 bytes. Each purpose key is HKDF-SHA256 of a root with info bugnet/<purpose>/v1,
// and each derived value is HMAC-SHA256 under a purpose key over its parts joined by zero bytes.
// A value tied to a placement's root takes that root's id. Grants derive from the newest root and
// verify against every listed one, as preview tokens do. Provider credentials are sealed with
// AES-256-GCM under the ref purpose key, with the placement id as additional data.
package dpkeys

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	purposePreview     = "preview"
	purposeGate        = "gate"
	purposeGrant       = "grant"
	purposeTransfer    = "transfer"
	purposeLink        = "link"
	purposePut         = "put"
	purposePair        = "pair"
	purposePlaceholder = "placeholder"
	purposeRef         = "ref"
)

// Purposes lists every purpose key the router derives, newest purpose last
var Purposes = []string{purposePreview, purposeGate, purposeGrant, purposeTransfer, purposeLink,
	purposePut, purposePair, purposePlaceholder, purposeRef}

const (
	keyBytes = 32
	// macBytes is how much of the MAC link tokens, grants and placeholders keep
	macBytes    = 16
	expiryBytes = 8
	pairChars   = 6
	sealVersion = "v1"
)

// Value prefixes
const (
	previewPrefix     = "bnp_"
	grantPrefix       = "g1."
	transferPrefix    = "bnx_"
	linkPrefix        = "bnsl_"
	putPrefix         = "bnsp_"
	placeholderPrefix = "bnsec_"
	tunnelPrefix      = "bnt_"
)

var (
	// ErrInvalid means a value is malformed, forged or made under a root no longer listed
	ErrInvalid = errors.New("dpkeys: invalid value")
	// ErrExpired means a put grant's expiry has passed
	ErrExpired = errors.New("dpkeys: expired")
	// ErrUnknownRoot means a key id names no listed root
	ErrUnknownRoot = errors.New("dpkeys: unknown root")
)

var (
	url64     = base64.RawURLEncoding.Strict()
	b32       = base32.StdEncoding.WithPadding(base32.NoPadding)
	crockford = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").
			WithPadding(base32.NoPadding)
	rootID = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
)

// Root is one root key and the id placements record it by
type Root struct {
	ID  string
	Key []byte // 32 bytes
}

// Keys holds the root keys, newest first, and the zero Keys panics where a value needs the newest
type Keys struct {
	roots []root
}

type root struct {
	id       string
	key      []byte
	purposes map[string][]byte
	seal     cipher.AEAD
}

// Parse reads the roots of BUGNET_DATA_PLANE_KEYS, newest first, quoting no key in its errors
func Parse(env string) (Keys, error) {
	if env == "" {
		return Keys{}, errors.New("must list at least one root")
	}
	var k Keys
	for i, entry := range strings.Split(env, ",") {
		id, raw, ok := strings.Cut(entry, ":")
		if !ok || !rootID.MatchString(id) {
			return Keys{}, fmt.Errorf("entry %d must be an id of 1 to 32 lowercase letters, "+
				"digits or hyphens, a colon and a key", i+1)
		}
		key, err := base64.StdEncoding.DecodeString(raw)
		if err != nil || len(key) != keyBytes {
			return Keys{}, fmt.Errorf("entry %d key must be standard base64 of 32 bytes", i+1)
		}
		if _, err := k.find(id); err == nil {
			return Keys{}, fmt.Errorf("lists %q more than once", id)
		}
		r, err := newRoot(id, key)
		if err != nil {
			return Keys{}, err
		}
		k.roots = append(k.roots, r)
	}
	return k, nil
}

func newRoot(id string, key []byte) (root, error) {
	r := root{id: id, key: key, purposes: map[string][]byte{}}
	for _, p := range Purposes {
		r.purposes[p] = Derive(key, p)
	}
	block, err := aes.NewCipher(r.purposes[purposeRef])
	if err != nil {
		return root{}, fmt.Errorf("ref cipher: %w", err)
	}
	if r.seal, err = cipher.NewGCM(block); err != nil {
		return root{}, fmt.Errorf("ref cipher: %w", err)
	}
	return r, nil
}

// NewEnv returns a fresh BUGNET_DATA_PLANE_KEYS value with one root, k1
func NewEnv() string {
	return "k1:" + base64.StdEncoding.EncodeToString(random(keyBytes))
}

// Current returns the newest root, the one new placements record
func (k Keys) Current() Root {
	return k.roots[0].public()
}

// Roots returns a copy of every root, newest first
func (k Keys) Roots() []Root {
	out := make([]Root, len(k.roots))
	for i, r := range k.roots {
		out[i] = r.public()
	}
	return out
}

func (r root) public() Root {
	return Root{ID: r.id, Key: slices.Clone(r.key)}
}

func (k Keys) find(id string) (root, error) {
	for _, r := range k.roots {
		if r.id == id {
			return r, nil
		}
	}
	return root{}, fmt.Errorf("%w %q", ErrUnknownRoot, id)
}

// Derive returns the purpose key HKDF-SHA256 gives for root, info "bugnet/<purpose>/v1"
func Derive(root []byte, purpose string) []byte {
	k, err := hkdf.Key(sha256.New, root, nil, "bugnet/"+purpose+"/v1", keyBytes)
	if err != nil {
		// Only FIPS 140-only mode refuses, and only a root shorter than any Parse accepts
		panic(err)
	}
	return k
}

// mac is HMAC-SHA256 under the purpose key over parts joined by zero bytes
func (r root) mac(purpose string, parts ...[]byte) []byte {
	h := hmac.New(sha256.New, r.purposes[purpose])
	h.Write(bytes.Join(parts, []byte{0}))
	return h.Sum(nil)
}

/* Previews and gates */

// PreviewToken derives a preview's bnp_ token under the root keyID names
func (k Keys) PreviewToken(keyID, previewID string, generation uint32) (string, error) {
	if previewID == "" {
		return "", errors.New("dpkeys: empty preview id")
	}
	r, err := k.find(keyID)
	if err != nil {
		return "", err
	}
	return r.previewToken(previewID, generation), nil
}

func (r root) previewToken(previewID string, generation uint32) string {
	gen := binary.BigEndian.AppendUint32(nil, generation)
	return previewPrefix + url64.EncodeToString(
		r.mac(purposePreview, []byte("preview"), []byte(previewID), gen))
}

// CheckPreviewToken reports whether token is the preview's token under any listed root
func (k Keys) CheckPreviewToken(previewID string, generation uint32, token string) bool {
	if previewID == "" {
		return false
	}
	for _, r := range k.roots {
		if hmac.Equal([]byte(r.previewToken(previewID, generation)), []byte(token)) {
			return true
		}
	}
	return false
}

// GateKey derives the key a placement's gate admits, under the root keyID names
func (k Keys) GateKey(keyID string, placement uuid.UUID) (string, error) {
	r, err := k.find(keyID)
	if err != nil {
		return "", err
	}
	return url64.EncodeToString(r.mac(purposeGate, []byte("gate"), placement[:])), nil
}

/* SSH grants */

// GrantID derives the g1. id of an SSH key's grant on a placement under the newest root
func (k Keys) GrantID(placement uuid.UUID, fingerprint string) string {
	r := k.roots[0]
	sum := r.mac(purposeGrant, placement[:], []byte(fingerprint))[:macBytes]
	return grantPrefix + url64.EncodeToString(slices.Concat(placement[:], []byte(fingerprint), sum))
}

// ParseGrantID returns the placement and fingerprint of a grant id any listed root made
func (k Keys) ParseGrantID(id string) (placement uuid.UUID, fingerprint string, err error) {
	raw, ok := decode(grantPrefix, id)
	if !ok || len(raw) <= len(placement)+macBytes {
		return uuid.Nil, "", ErrInvalid
	}
	end := len(raw) - macBytes
	copy(placement[:], raw)
	fp := raw[len(placement):end]
	for _, r := range k.roots {
		if hmac.Equal(r.mac(purposeGrant, placement[:], fp)[:macBytes], raw[end:]) {
			return placement, string(fp), nil
		}
	}
	return uuid.Nil, "", ErrInvalid
}

/* Transfers */

// TransferToken derives the bnx_ token of a keyed transfer call under the root keyID names
func (k Keys) TransferToken(keyID string, placement uuid.UUID, callKey string) (string, error) {
	if callKey == "" {
		return "", errors.New("dpkeys: empty call key")
	}
	r, err := k.find(keyID)
	if err != nil {
		return "", err
	}
	return transferPrefix + url64.EncodeToString(
		r.mac(purposeTransfer, placement[:], []byte(callKey))), nil
}

/* Secrets */

// LinkToken derives the bnsl_ token of a secret link request under the root keyID names
func (k Keys) LinkToken(keyID string, placement uuid.UUID, requestID string) (string, error) {
	if requestID == "" {
		return "", errors.New("dpkeys: empty request id")
	}
	r, err := k.find(keyID)
	if err != nil {
		return "", err
	}
	sum := r.mac(purposeLink, placement[:], []byte(requestID))[:macBytes]
	return linkPrefix + url64.EncodeToString(sum), nil
}

// PutGrant derives a bnsp_ grant for placement under the newest root, valid until expiry
func (k Keys) PutGrant(placement uuid.UUID, expiry time.Time) string {
	exp := binary.BigEndian.AppendUint64(nil, uint64(expiry.Unix()))
	sum := k.roots[0].putMAC(placement[:], exp)
	return putPrefix + url64.EncodeToString(slices.Concat(placement[:], exp, sum))
}

func (r root) putMAC(placement, expiry []byte) []byte {
	return r.mac(purposePut, []byte("put"), placement, expiry)[:macBytes]
}

// CheckPutGrant returns the placement of a grant any listed root made, unless it expired by now
func (k Keys) CheckPutGrant(grant string, now time.Time) (uuid.UUID, error) {
	var placement uuid.UUID
	raw, ok := decode(putPrefix, grant)
	if !ok || len(raw) != len(placement)+expiryBytes+macBytes {
		return uuid.Nil, ErrInvalid
	}
	copy(placement[:], raw)
	exp := raw[len(placement) : len(placement)+expiryBytes]
	valid := false
	for _, r := range k.roots {
		if hmac.Equal(r.putMAC(placement[:], exp), raw[len(placement)+expiryBytes:]) {
			valid = true
			break
		}
	}
	if !valid {
		return uuid.Nil, ErrInvalid
	}
	if !now.Before(time.Unix(int64(binary.BigEndian.Uint64(exp)), 0)) {
		return uuid.Nil, ErrExpired
	}
	return placement, nil
}

// PairingCode derives the 6 Crockford base32 characters a secret link shows, under keyID's root
func (k Keys) PairingCode(keyID string, linkTokenSHA256 []byte) (string, error) {
	if len(linkTokenSHA256) != sha256.Size {
		return "", errors.New("dpkeys: a link token hash is 32 bytes")
	}
	r, err := k.find(keyID)
	if err != nil {
		return "", err
	}
	// The first 6 characters of 4 bytes are exactly the first 30 bits
	return crockford.EncodeToString(r.mac(purposePair, linkTokenSHA256)[:4])[:pairChars], nil
}

// Placeholder derives the bnsec_ placeholder of a placement's secret name under keyID's root
func (k Keys) Placeholder(keyID string, placement uuid.UUID, name string) (string, error) {
	if name == "" {
		return "", errors.New("dpkeys: empty secret name")
	}
	r, err := k.find(keyID)
	if err != nil {
		return "", err
	}
	sum := r.mac(purposePlaceholder, placement[:], []byte(name))[:macBytes]
	return placeholderPrefix + strings.ToLower(b32.EncodeToString(sum)), nil
}

// NewTransferToken returns a fresh bnx_ token of 32 random bytes, for a call without a key
func NewTransferToken() string {
	return transferPrefix + url64.EncodeToString(random(keyBytes))
}

// NewTunnelToken returns a fresh bnt_ token of 32 random bytes
func NewTunnelToken() string {
	return tunnelPrefix + url64.EncodeToString(random(keyBytes))
}

// Binding is the sealing binding of a placement, upper case base32 of 52 characters
func Binding(placement uuid.UUID) string {
	sum := sha256.Sum256(slices.Concat([]byte("bugnet-secret-binding"), []byte{0}, placement[:]))
	return b32.EncodeToString(sum[:])
}

/* Provider refs */

// SealRef seals one provider credential of placement under the newest root
func (k Keys) SealRef(placement uuid.UUID, value string) string {
	r := k.roots[0]
	nonce := random(r.seal.NonceSize())
	sealed := r.seal.Seal(nonce, nonce, []byte(value), placement[:])
	return sealVersion + "." + r.id + "." + url64.EncodeToString(sealed)
}

// OpenRef opens a value SealRef sealed for placement under any listed root
func (k Keys) OpenRef(placement uuid.UUID, sealed string) (string, error) {
	version, rest, _ := strings.Cut(sealed, ".")
	id, body, ok := strings.Cut(rest, ".")
	if version != sealVersion || !ok {
		return "", ErrInvalid
	}
	r, err := k.find(id)
	if err != nil {
		return "", err
	}
	raw, ok := decode("", body)
	if !ok || len(raw) < r.seal.NonceSize()+r.seal.Overhead() {
		return "", ErrInvalid
	}
	n := r.seal.NonceSize()
	plain, err := r.seal.Open(nil, raw[:n], raw[n:], placement[:])
	if err != nil {
		return "", ErrInvalid
	}
	return string(plain), nil
}

/* Encoding */

// decode reads base64url after prefix, refusing any spelling but the one encoding gives
func decode(prefix, s string) ([]byte, bool) {
	body, ok := strings.CutPrefix(s, prefix)
	if !ok {
		return nil, false
	}
	raw, err := url64.DecodeString(body)
	if err != nil || url64.EncodeToString(raw) != body {
		return nil, false
	}
	return raw, true
}

// random returns n bytes from crypto/rand, which never fails to give them
func random(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}
