package lium

import (
	"bytes"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func TestLiumPodSSHKey(t *testing.T) {
	p, err := newProvider(testOptions(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	name := podName(testKey, placementLabels())
	mac := hmac.New(sha256.New, testSeed)
	mac.Write([]byte("bugnet/lium-ssh/v1\x00" + name))
	want := ed25519.NewKeyFromSeed(mac.Sum(nil))
	if !p.podPrivateKey(name).Equal(want) {
		t.Fatal("the pod key is not derived from the seed and the pod name")
	}
	line := p.podPublicKey(name)
	if !strings.HasPrefix(line, "ssh-ed25519 ") || !strings.HasSuffix(line, " bugnet") ||
		strings.ContainsAny(line, "\r\n") {
		t.Fatalf("authorized_keys line %q", line)
	}
	pub, comment, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	wantPub, err := ssh.NewPublicKey(want.Public())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pub.Marshal(), wantPub.Marshal()) || comment != "bugnet" {
		t.Fatalf("authorized_keys line %q names another key", line)
	}
	eq(t, p.podPublicKey(name), line)

	// Another pod and another seed each give another key
	if p.podPublicKey(podName(freshKey(), placementLabels())) == line {
		t.Fatal("two pod names gave one key")
	}
	opts := testOptions()
	opts.SSHSeed = bytes.Repeat([]byte{9}, 32)
	q, err := newProvider(opts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if q.podPublicKey(name) == line {
		t.Fatal("two seeds gave one key")
	}
}
