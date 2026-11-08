// Package sshkeystest makes SSH public keys of every kind for tests
package sshkeystest

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

// line is pub's authorized_keys line without a comment
func line(t testing.TB, pub ssh.PublicKey) string {
	t.Helper()
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub)))
}

func public(t testing.TB, k any) ssh.PublicKey {
	t.Helper()
	pub, err := ssh.NewPublicKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return pub
}

// ED25519 is a fresh ed25519 key's line
func ED25519(t testing.TB) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return line(t, public(t, pub))
}

// ECDSA is a fresh ECDSA key's line on curve c
func ECDSA(t testing.TB, c elliptic.Curve) string {
	t.Helper()
	k, err := ecdsa.GenerateKey(c, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return line(t, public(t, &k.PublicKey))
}

// RSA is a fresh RSA key's line of bits
func RSA(t testing.TB, bits int) string {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatal(err)
	}
	return line(t, public(t, &k.PublicKey))
}

// SK is a security key's line, ECDSA when ecdsaKind, which no Go generator makes
func SK(t testing.TB, ecdsaKind bool) string {
	t.Helper()
	var blob []byte
	if ecdsaKind {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		point, err := k.PublicKey.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		blob = ssh.Marshal(struct{ Name, Curve, Point, App string }{
			ssh.KeyAlgoSKECDSA256, "nistp256", string(point), "ssh:"})
	} else {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		blob = ssh.Marshal(struct{ Name, Key, App string }{
			ssh.KeyAlgoSKED25519, string(pub), "ssh:"})
	}
	pub, err := ssh.ParsePublicKey(blob)
	if err != nil {
		t.Fatal(err)
	}
	return line(t, pub)
}

// DSA is a DSA line, which x/crypto may no longer parse at all
func DSA() string {
	blob := ssh.Marshal(struct {
		Name       string
		P, Q, G, Y []byte
	}{"ssh-dss", []byte{0, 0xff}, []byte{1}, []byte{2}, []byte{3}})
	return "ssh-dss " + base64.StdEncoding.EncodeToString(blob)
}

// Cert is a user certificate's line
func Cert(t testing.TB) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, caPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(caPriv)
	if err != nil {
		t.Fatal(err)
	}
	cert := &ssh.Certificate{
		Key: public(t, pub), CertType: ssh.UserCert, ValidBefore: ssh.CertTimeInfinity,
	}
	if err := cert.SignCert(rand.Reader, signer); err != nil {
		t.Fatal(err)
	}
	return line(t, cert)
}

// Wire is a line's key in SSH wire format
func Wire(t testing.TB, l string) []byte {
	t.Helper()
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(l))
	if err != nil {
		t.Fatal(err)
	}
	return pub.Marshal()
}

// Fingerprint is a line's SHA256 fingerprint
func Fingerprint(t testing.TB, l string) string {
	t.Helper()
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(l))
	if err != nil {
		t.Fatal(err)
	}
	return ssh.FingerprintSHA256(pub)
}
