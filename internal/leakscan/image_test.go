package leakscan

import (
	"strings"
	"testing"
)

func TestScanImageNamesFail(t *testing.T) {
	a := newFixture(t).environ(1, "PATH=/usr/bin", "MY_API_TOKEN=x", "DB_PASSWORD=y").archive()
	f, err := ScanImage(a, nil, "e2b")
	eq(t, err, nil)
	eq(t, strings.Join(rules(f), "|"),
		"secret-like variable DB_PASSWORD environ|secret-like variable MY_API_TOKEN environ")
}

func TestScanImageAllowsOnlyProviderRowsAndCA(t *testing.T) {
	ca := "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
	a := newFixture(t).file("usr/local/share/ca-certificates/bugnet-egress-v1.crt", ca).
		environ(1, "E2B_ENVD_ACCESS_TOKEN=abc").
		file("etc/bugnet/egress.token", "bnt_"+strings.Repeat("a", 24)).archive()
	f, err := ScanImage(a, nil, "e2b")
	eq(t, err, nil)
	eq(t, strings.Join(rules(f), "|"), "shape tunnel etc/bugnet/egress.token")
}

func TestScanImageStockHostKeyIsAFinding(t *testing.T) {
	key := "-----BEGIN OPENSSH PRIVATE KEY-----\nAAAA\n-----END OPENSSH PRIVATE KEY-----\n"
	mk := func() *fixture { return newFixture(t).file("etc/ssh/ssh_host_ed25519_key", key) }
	f, err := ScanImage(mk().archive(), nil, "e2b")
	eq(t, err, nil)
	eq(t, len(f), 1)
	f, err = ScanImage(mk().archive(), nil, "boat")
	eq(t, err, nil)
	eq(t, len(f), 0)
	f, err = ScanImage(mk().file("root/id", key).archive(), nil, "boat")
	eq(t, err, nil)
	eq(t, len(f), 1)
}

func TestScanImageNeverPrintsValues(t *testing.T) {
	secret := "zz-secret-value-1234567890"
	a := newFixture(t).file("etc/x.conf", "k="+secret).
		environ(1, "API_TOKEN="+secret).archive()
	f, err := ScanImage(a, Needles{{"N", []byte(secret)}}, "koyeb")
	eq(t, err, nil)
	eq(t, len(f) >= 2, true)
	if out := FormatFindings(f); strings.Contains(out, secret) {
		t.Fatalf("a finding printed the value: %s", out)
	}
}
