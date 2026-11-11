package lium

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"strings"

	"golang.org/x/crypto/ssh"
)

const (
	// sshKeyLabel separates the pod key's derivation from any other use of the seed
	sshKeyLabel = "bugnet/lium-ssh/v1"
	seedBytes   = 32
	keyComment  = "bugnet"
)

// podPrivateKey derives the router's key for one pod from the seed and the pod name
func (p *Provider) podPrivateKey(podName string) ed25519.PrivateKey {
	mac := hmac.New(sha256.New, p.sshSeed)
	mac.Write([]byte(sshKeyLabel))
	mac.Write([]byte{0})
	mac.Write([]byte(podName))
	return ed25519.NewKeyFromSeed(mac.Sum(nil))
}

// podPublicKey is the pod key's authorized_keys line, the only part that leaves the router
func (p *Provider) podPublicKey(podName string) string {
	pub, err := ssh.NewPublicKey(p.podPrivateKey(podName).Public())
	if err != nil {
		// An ed25519 public key always converts
		panic(err)
	}
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub))) + " " + keyComment
}
