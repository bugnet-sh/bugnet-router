package hf

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// tokenLabel starts the message every sandbox token is the HMAC of, design 4.4
	tokenLabel = "bugnet/hf-sbx/v1"
	seedBytes  = 32
)

var (
	keyHex = regexp.MustCompile(`^[0-9a-f]{32}$`)
	// imageByDigest is an image ref naming its digest, the only kind a Job starts from
	imageByDigest = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/:-]*@sha256:[0-9a-f]{64}$`)
	sha256Hex     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// keptJob is the ref of the Job a key got in this process, and when it got it
type keptJob struct {
	ref provider.Ref
	at  time.Time
}

// jobKey is the 32 lowercase hex a Job's bugnet_key label and token derive from
func jobKey(idempotencyKey string) string {
	if keyHex.MatchString(idempotencyKey) {
		return idempotencyKey
	}
	sum := sha256.Sum256([]byte(idempotencyKey))
	return hex.EncodeToString(sum[:])[:32]
}

// freshKey is 32 random lowercase hex for a create without an idempotency key
func freshKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// sandboxToken derives the SBX_TOKEN of the Job carrying key, so any process can adopt the Job
func (p *Provider) sandboxToken(key string) string {
	mac := hmac.New(sha256.New, p.seed)
	mac.Write([]byte(tokenLabel))
	mac.Write([]byte{0})
	mac.Write([]byte(key))
	return hex.EncodeToString(mac.Sum(nil))
}
