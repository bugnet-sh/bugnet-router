// Package sshkeys parses the SSH keys the router stores and builds what an agent needs to connect.
//
// A user key arrives as one authorized_keys line without options. Ed25519, ECDSA on the three NIST
// curves, the two security key kinds and RSA of 3072 bits or more are kept, as their wire bytes
// and SHA256 fingerprint, with the comment dropped. Certificates and DSA are refused. The gateway's
// own key for a placement is ed25519 alone, and goes into the sandbox's authorized_keys with
// options that allow a PTY and loopback forwards only. A HandOff builds the ssh command, the
// known_hosts lines and an ssh_config stanza for one alias. This is the only package of the router
// that imports golang.org/x/crypto/ssh.
package sshkeys

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base32"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh"
)

// ErrInvalid means a key is malformed, of a refused kind, or carries options
var ErrInvalid = errors.New("sshkeys: the key is not accepted")

// minRSABits is the smallest RSA key accepted
const minRSABits = 3072

// gatewayOptions bound what the gateway's key may do in the sandbox, as defence in depth
const gatewayOptions = `restrict,pty,port-forwarding,permitopen="127.0.0.1:*",` +
	`permitopen="[::1]:*"`

// accepted are the key types a user may add
var accepted = map[string]bool{
	ssh.KeyAlgoED25519:    true,
	ssh.KeyAlgoECDSA256:   true,
	ssh.KeyAlgoECDSA384:   true,
	ssh.KeyAlgoECDSA521:   true,
	ssh.KeyAlgoSKED25519:  true,
	ssh.KeyAlgoSKECDSA256: true,
	ssh.KeyAlgoRSA:        true,
}

// Key is one accepted public key
type Key struct {
	// Wire is the key in SSH wire format, with no comment
	Wire []byte
	// Fingerprint is the SHA256 form ssh-keygen -l prints
	Fingerprint string
}

// Parse reads one authorized_keys line without options
func Parse(line string) (Key, error) {
	pub, err := oneLine(line)
	if err != nil {
		return Key{}, err
	}
	return check(pub)
}

// ParseWire reads a key in SSH wire format, as the gateway sends it
func ParseWire(wire []byte) (Key, error) {
	pub, err := ssh.ParsePublicKey(wire)
	if err != nil {
		return Key{}, fmt.Errorf("%w: the wire bytes do not parse", ErrInvalid)
	}
	return check(pub)
}

// oneLine parses a single line that carries no options
func oneLine(line string) (ssh.PublicKey, error) {
	text := strings.TrimSpace(line)
	if text == "" || strings.ContainsAny(text, "\r\n") {
		return nil, fmt.Errorf("%w: want exactly one line", ErrInvalid)
	}
	pub, _, opts, _, err := ssh.ParseAuthorizedKey([]byte(text))
	switch {
	case err != nil:
		return nil, fmt.Errorf("%w: the line does not parse", ErrInvalid)
	case len(opts) > 0:
		return nil, fmt.Errorf("%w: the line carries options", ErrInvalid)
	}
	return pub, nil
}

// check refuses certificates, unlisted types and short RSA keys
func check(pub ssh.PublicKey) (Key, error) {
	if _, cert := pub.(*ssh.Certificate); cert || !accepted[pub.Type()] {
		return Key{}, fmt.Errorf("%w: type %s is refused", ErrInvalid, pub.Type())
	}
	if pub.Type() == ssh.KeyAlgoRSA {
		ck, ok := pub.(ssh.CryptoPublicKey)
		if !ok {
			return Key{}, fmt.Errorf("%w: the RSA key has no modulus", ErrInvalid)
		}
		rk, ok := ck.CryptoPublicKey().(*rsa.PublicKey)
		if !ok || rk.N.BitLen() < minRSABits {
			return Key{}, fmt.Errorf("%w: RSA keys need %d bits", ErrInvalid, minRSABits)
		}
	}
	return Key{Wire: pub.Marshal(), Fingerprint: ssh.FingerprintSHA256(pub)}, nil
}

// GatewayLine is the authorized_keys line of the gateway's key and its fingerprint, ed25519 only
func GatewayLine(wire []byte) (line, fingerprint string, err error) {
	pub, err := ssh.ParsePublicKey(wire)
	if err != nil {
		return "", "", fmt.Errorf("%w: the wire bytes do not parse", ErrInvalid)
	}
	if pub.Type() != ssh.KeyAlgoED25519 {
		return "", "", fmt.Errorf("%w: the gateway's key must be plain ed25519", ErrInvalid)
	}
	return gatewayOptions + " " + bare(pub) + " bn-gw", ssh.FingerprintSHA256(pub), nil
}

// Throwaway is the authorized_keys line of a gateway key made now and never kept
//
// The private half is dropped at once, so nothing can connect with the line. A base image check
// hands it to ssh-up to see the host key a fresh sandbox makes.
func Throwaway() (string, error) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", err
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return "", err
	}
	line, _, err := GatewayLine(sshPub.Marshal())
	return line, err
}

// HostKey checks a host key line and answers its type and base64, without a comment
func HostKey(line string) (string, error) {
	pub, err := oneLine(line)
	if err != nil {
		return "", err
	}
	if _, cert := pub.(*ssh.Certificate); cert {
		return "", fmt.Errorf("%w: a host certificate is refused", ErrInvalid)
	}
	return bare(pub), nil
}

// bare is a key's type and base64
func bare(pub ssh.PublicKey) string {
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub)))
}

/* Aliases */

// aliasBody is the 10 characters after bn-, 50 bits of lowercase base32
var aliasBody = regexp.MustCompile(`^bn-[a-z2-7]{10}$`)

var lower32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// NewAlias returns a fresh alias of bn- and 10 lowercase base32 characters
func NewAlias() string {
	b := make([]byte, 7)
	_, _ = rand.Read(b)
	return "bn-" + lower32.EncodeToString(b)[:10]
}

// ValidAlias reports whether s has an alias's shape
func ValidAlias(s string) bool { return aliasBody.MatchString(s) }

/* Hand-off */

// pathRule is DP 9.2's rule for identity_file and known_hosts_file
var pathRule = regexp.MustCompile(`^(~|\$TMPDIR)?[A-Za-z0-9._/-]{1,200}$`)

// ValidPath reports whether p may name a key or known_hosts file in the hand-off
func ValidPath(p string) bool {
	switch {
	case !pathRule.MatchString(p):
		return false
	case strings.HasPrefix(p, "~"):
		return strings.HasPrefix(p, "~/")
	case strings.HasPrefix(p, "$TMPDIR"):
		return strings.HasPrefix(p, "$TMPDIR/")
	}
	// ssh would read a leading dash as an option in some places
	return !strings.HasPrefix(p, "-")
}

// HandOff is what an agent needs to reach one alias through the gateway
type HandOff struct {
	Alias          string
	Host           string
	Port           int
	IdentityFile   string
	KnownHostsFile string
}

// Command is the ssh command line, with -p only when the port is not 22
func (h HandOff) Command() string {
	parts := []string{
		"ssh", "-i", h.IdentityFile, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + h.KnownHostsFile,
	}
	if h.Port != 22 {
		parts = append(parts, "-p", strconv.Itoa(h.Port))
	}
	return strings.Join(append(parts, h.Alias+"@"+h.Host), " ")
}

// KnownHosts answers one known_hosts line per gateway host key, host and port bracketed off 22
func (h HandOff) KnownHosts(hostKeys []string) []string {
	name := h.Host
	if h.Port != 22 {
		name = "[" + h.Host + "]:" + strconv.Itoa(h.Port)
	}
	out := make([]string, 0, len(hostKeys))
	for _, k := range hostKeys {
		f := strings.Fields(k)
		if len(f) >= 2 {
			out = append(out, name+" "+f[0]+" "+f[1])
		}
	}
	return out
}

// Config is an ssh_config stanza for the alias, its multiplexing lines last
func (h HandOff) Config() string {
	id, kh := configPath(h.IdentityFile), configPath(h.KnownHostsFile)
	lines := []string{
		"Host bugnet-" + h.Alias,
		"  HostName " + h.Host,
		"  Port " + strconv.Itoa(h.Port),
		"  User " + h.Alias,
		"  IdentityFile " + id,
		"  IdentitiesOnly yes",
		"  BatchMode yes",
		"  StrictHostKeyChecking yes",
		"  UserKnownHostsFile " + kh,
		"  ControlMaster auto",
		"  ControlPath " + path.Join(path.Dir(id), "bugnet-cm-%r"),
		"  ControlPersist 60",
	}
	return strings.Join(lines, "\n") + "\n"
}

// configPath writes $TMPDIR as ${TMPDIR}, the only form ssh_config expands
func configPath(p string) string {
	if rest, ok := strings.CutPrefix(p, "$TMPDIR"); ok {
		return "${TMPDIR}" + rest
	}
	return p
}
