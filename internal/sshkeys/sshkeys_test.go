package sshkeys_test

import (
	"crypto/elliptic"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/bugnet-sh/bugnet-router/internal/sshkeys"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys/sshkeystest"
)

func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func ed25519Line(t *testing.T) string { return sshkeystest.ED25519(t) + " me@laptop" }

func ecdsaLine(t *testing.T, c elliptic.Curve) string { return sshkeystest.ECDSA(t, c) }

func rsaLine(t *testing.T, bits int) string { return sshkeystest.RSA(t, bits) }

func skLine(t *testing.T, ecdsaKind bool) string { return sshkeystest.SK(t, ecdsaKind) }

func dsaLine(*testing.T) string { return sshkeystest.DSA() }

func certLine(t *testing.T) string { return sshkeystest.Cert(t) }

func TestParseAcceptsTheListedKinds(t *testing.T) {
	for name, l := range map[string]string{
		"ed25519": ed25519Line(t), "p256": ecdsaLine(t, elliptic.P256()),
		"p384": ecdsaLine(t, elliptic.P384()), "p521": ecdsaLine(t, elliptic.P521()),
		"sk-ed25519": skLine(t, false), "sk-ecdsa": skLine(t, true), "rsa3072": rsaLine(t, 3072),
	} {
		t.Run(name, func(t *testing.T) {
			k, err := sshkeys.Parse(l)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			eq(t, strings.HasPrefix(k.Fingerprint, "SHA256:"), true)
			again, err := sshkeys.ParseWire(k.Wire)
			if err != nil {
				t.Fatalf("ParseWire: %v", err)
			}
			eq(t, again, k)
		})
	}
}

func TestParseRefusals(t *testing.T) {
	ed := ed25519Line(t)
	for name, l := range map[string]string{
		"rsa2048": rsaLine(t, 2048), "dsa": dsaLine(t), "cert": certLine(t),
		"options": `command="x" ` + ed, "two lines": ed + "\n" + ed25519Line(t),
		"empty": "", "garbage": "ssh-ed25519 !!!", "private": "-----BEGIN OPENSSH PRIVATE KEY-----",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := sshkeys.Parse(l)
			eq(t, errors.Is(err, sshkeys.ErrInvalid), true)
		})
	}
}

func TestParseDropsTheComment(t *testing.T) {
	l := ed25519Line(t)
	k, err := sshkeys.Parse(l)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, strings.Contains(string(k.Wire), "me@laptop"), false)
	bare, err := sshkeys.Parse(strings.TrimSuffix(l, " me@laptop"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, bare, k)
}

func TestGatewayLine(t *testing.T) {
	k, err := sshkeys.Parse(ed25519Line(t))
	if err != nil {
		t.Fatal(err)
	}
	got, fp, err := sshkeys.GatewayLine(k.Wire)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, fp, k.Fingerprint)
	want := regexp.MustCompile(`^restrict,pty,port-forwarding,permitopen="127\.0\.0\.1:\*",` +
		`permitopen="\[::1\]:\*" ssh-ed25519 [A-Za-z0-9+/=]+ bn-gw$`)
	eq(t, want.MatchString(got), true)
	for _, l := range []string{ecdsaLine(t, elliptic.P256()), skLine(t, false), certLine(t)} {
		var wire []byte
		if k, err := sshkeys.Parse(l); err == nil {
			wire = k.Wire
		} else {
			pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(l))
			if err != nil {
				t.Fatal(err)
			}
			wire = pub.Marshal()
		}
		_, _, err := sshkeys.GatewayLine(wire)
		eq(t, errors.Is(err, sshkeys.ErrInvalid), true)
	}
}

func TestHostKey(t *testing.T) {
	l := ed25519Line(t)
	got, err := sshkeys.HostKey(l)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got, strings.TrimSuffix(l, " me@laptop"))
	for _, bad := range []string{"", "ssh-ed25519", `from="x" ` + l, certLine(t), l + "\n" + l} {
		_, err := sshkeys.HostKey(bad)
		eq(t, errors.Is(err, sshkeys.ErrInvalid), true)
	}
}

func TestAlias(t *testing.T) {
	seen := map[string]bool{}
	for range 100 {
		a := sshkeys.NewAlias()
		eq(t, regexp.MustCompile(`^bn-[a-z2-7]{10}$`).MatchString(a), true)
		eq(t, sshkeys.ValidAlias(a), true)
		eq(t, seen[a], false)
		seen[a] = true
	}
	for _, bad := range []string{"", "bn-", "bn-ABCDEFGHIJ", "bn-abcdefghi1", "xx-abcdefghij",
		"bn-abcdefghijk"} {
		eq(t, sshkeys.ValidAlias(bad), false)
	}
}

func TestIdentityFileRule(t *testing.T) {
	for _, ok := range []string{"~/.ssh/bugnet_bn-abc", "$TMPDIR/bugnet/k", "/tmp/k", "k",
		"~/a.b_c-d/e"} {
		eq(t, sshkeys.ValidPath(ok), true)
	}
	for _, bad := range []string{"~/.ssh/x;rm", "", "~", "$TMPDIR", "a b", "$HOME/k",
		"~/x\n", "${TMPDIR}/k", "~user/k", strings.Repeat("a", 201)} {
		eq(t, sshkeys.ValidPath(bad), false)
	}
}

const hostKeyLine = "ssh-ed25519 " +
	"AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"

func TestHandOffOnPort22(t *testing.T) {
	h := sshkeys.HandOff{
		Alias: "bn-k3fz2q7max", Host: "ssh.bugnet.sh", Port: 22,
		IdentityFile: "~/.ssh/bugnet_bn-k3fz2q7max", KnownHostsFile: "~/.ssh/bugnet_known_hosts",
	}
	eq(t, h.Command(), "ssh -i ~/.ssh/bugnet_bn-k3fz2q7max -o IdentitiesOnly=yes"+
		" -o BatchMode=yes -o StrictHostKeyChecking=yes"+
		" -o UserKnownHostsFile=~/.ssh/bugnet_known_hosts bn-k3fz2q7max@ssh.bugnet.sh")
	eq(t, h.KnownHosts([]string{hostKeyLine + " gateway@vps"}),
		[]string{"ssh.bugnet.sh " + hostKeyLine})
}

func TestKnownHostsOnPort2200(t *testing.T) {
	h := sshkeys.HandOff{
		Alias: "bn-k3fz2q7max", Host: "ssh.dev.bugnet.sh", Port: 2200,
		IdentityFile: "k", KnownHostsFile: "kh",
	}
	eq(t, h.KnownHosts([]string{hostKeyLine, hostKeyLine}),
		[]string{"[ssh.dev.bugnet.sh]:2200 " + hostKeyLine, "[ssh.dev.bugnet.sh]:2200 " +
			hostKeyLine})
	eq(t, h.Command(), "ssh -i k -o IdentitiesOnly=yes -o BatchMode=yes"+
		" -o StrictHostKeyChecking=yes -o UserKnownHostsFile=kh -p 2200"+
		" bn-k3fz2q7max@ssh.dev.bugnet.sh")
}

func TestConfigStanza(t *testing.T) {
	h := sshkeys.HandOff{
		Alias: "bn-k3fz2q7max", Host: "ssh.bugnet.sh", Port: 2222,
		IdentityFile: "$TMPDIR/bugnet/k", KnownHostsFile: "$TMPDIR/bugnet/kh",
	}
	eq(t, h.Config(), `Host bugnet-bn-k3fz2q7max
  HostName ssh.bugnet.sh
  Port 2222
  User bn-k3fz2q7max
  IdentityFile ${TMPDIR}/bugnet/k
  IdentitiesOnly yes
  BatchMode yes
  StrictHostKeyChecking yes
  UserKnownHostsFile ${TMPDIR}/bugnet/kh
  ControlMaster auto
  ControlPath ${TMPDIR}/bugnet/bugnet-cm-%r
  ControlPersist 60
`)
	eq(t, strings.Contains(h.Command(), "$TMPDIR/bugnet/k "), true)
}
