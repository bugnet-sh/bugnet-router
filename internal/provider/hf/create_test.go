package hf

import (
	"bytes"
	"testing"
)

// TestHFSandboxToken pins design 4.4's derivation to answers computed outside Go
func TestHFSandboxToken(t *testing.T) {
	p, err := newProvider(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.sandboxToken("0123456789abcdef0123456789abcdef"),
		"78477f9ed1deae7165c07faa3ed9d988fbf0bee8c0deb337d20e99bf9b8d0b0a")
	eq(t, p.sandboxToken("fedcba9876543210fedcba9876543210"),
		"cfe2b5bbc7b1cf36506190d955972588c081901d1577b2e58999fa855401f0e7")
	opts := testOptions()
	opts.SandboxSeed = bytes.Repeat([]byte{7}, 32)
	other, err := newProvider(opts)
	if err != nil {
		t.Fatal(err)
	}
	if other.sandboxToken("0123456789abcdef0123456789abcdef") ==
		p.sandboxToken("0123456789abcdef0123456789abcdef") {
		t.Fatal("two seeds gave one token")
	}
	// The provider keeps its own copy, so a caller clearing the seed changes nothing
	clear(opts.SandboxSeed)
	if other.sandboxToken("k") == p.sandboxToken("k") {
		t.Fatal("the provider shares the caller's seed")
	}
}

func TestHFJobKey(t *testing.T) {
	eq(t, jobKey("0123456789abcdef0123456789abcdef"), "0123456789abcdef0123456789abcdef")
	// The saga's keys are 32 hex already, any other key is hashed to that form
	eq(t, jobKey("order-1|1|hf"), "81e213554744e814d911849108067244")
	eq(t, jobKey("0123456789ABCDEF0123456789ABCDEF") == "0123456789ABCDEF0123456789ABCDEF",
		false)
	a, b := freshKey(), freshKey()
	if a == b || !keyHex.MatchString(a) || !keyHex.MatchString(b) {
		t.Fatalf("fresh keys %q and %q", a, b)
	}
}
