package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
)

func TestStartRefusesSecretsWithoutCA(t *testing.T) {
	cfg := config.Config{DatabaseURL: "postgres://localhost:1/unused"}
	cfg.Secrets.Providers = []string{"e2b"}
	err := serve(t.Context(), discard, cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "secrets.providers") ||
		!strings.Contains(err.Error(), "secrets.ca_cert_file") {
		t.Fatalf("serve with secrets and no CA gave %v, want both settings named", err)
	}
}

// writeCA writes a self-signed certificate in PEM to a file of its own
func writeCA(t *testing.T) (string, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true,
		BasicConstraintsValid: true, NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return writeTemp(t, "ca.pem", pemBytes), pemBytes
}

func writeTemp(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBootSettingsCarryTheCAAndThePools(t *testing.T) {
	caFile, ca := writeCA(t)
	cfg := config.Config{}
	cfg.Previews.GatePort = 29417
	cfg.Names.PreviewDomain, cfg.Names.EgressHost = "preview.bugnet.sh", "egress.bugnet.sh"
	cfg.Secrets.Providers, cfg.Secrets.CACertFile, cfg.Secrets.CAVersion = []string{"e2b"},
		caFile, 2
	cfg.Secrets.EgressIPs = []netip.Addr{netip.MustParseAddr("203.0.113.7")}
	cfg.Hardening = config.Hardening{
		MailPorts: []int{25}, DenyRanges: []string{"10.0.0.0/8"},
		PoolList: writeTemp(t, "pools.txt", []byte("198.51.100.0/24\n")),
		Rules:    map[string]config.HardenRule{"boat": {Required: true}},
	}
	b, err := bootSettings(discard, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if string(b.CAPEM) != string(ca) || b.CAVersion != 2 || b.GatePort != 29417 ||
		b.PreviewDomain != "preview.bugnet.sh" || b.EgressHost != "egress.bugnet.sh" ||
		!reflect.DeepEqual(b.Pools, []string{"198.51.100.0/24"}) ||
		!reflect.DeepEqual(b.MailPorts, []int{25}) ||
		!reflect.DeepEqual(b.DenyRanges, []string{"10.0.0.0/8"}) ||
		!reflect.DeepEqual(b.Harden, cfg.Hardening.Rules) ||
		!reflect.DeepEqual(b.EgressIPs, cfg.Secrets.EgressIPs) {
		t.Fatalf("boot settings %+v", b)
	}

	cfg.Hardening.PoolList = filepath.Join(t.TempDir(), "missing.txt")
	if b, err := bootSettings(discard, cfg); err != nil || len(b.Pools) != 0 {
		t.Fatalf("a missing pool list: %v, %v, want a warning alone", b.Pools, err)
	}
	cfg.Hardening.PoolList = writeTemp(t, "bad.txt", []byte("pool.example.com\n"))
	if _, err := bootSettings(discard, cfg); err == nil || !strings.Contains(err.Error(),
		"line 1") {
		t.Fatalf("a malformed pool list: %v", err)
	}
	cfg.Hardening.PoolList = ""
	for name, content := range map[string]string{
		"key":   "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n",
		"text":  "not a certificate",
		"empty": "",
	} {
		cfg.Secrets.CACertFile = writeTemp(t, name+".pem", []byte(content))
		if _, err := bootSettings(discard, cfg); err == nil ||
			!strings.Contains(err.Error(), "secrets.ca_cert_file") {
			t.Fatalf("a CA file holding %s: %v", name, err)
		}
	}
}

func TestStartRefusesRestrictedOnE2BWithoutProxyIPv4(t *testing.T) {
	pkgs := func(extra string) *catalog.Catalog {
		t.Helper()
		c, err := catalog.Load(writeTemp(t, "packages.json", []byte(`{"packages": [{"id": `+
			`"locked", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10}, `+
			`"minutes": 10, "price_micros": 50000`+extra+`}]}`)))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	restricted, open := pkgs(`, "egress": {"allow": ["huggingface.co"]}`), pkgs("")
	v4, v6 := netip.MustParseAddr("203.0.113.7"), netip.MustParseAddr("2001:db8::7")
	for _, tc := range []struct {
		name      string
		providers []string
		cat       *catalog.Catalog
		ips       []netip.Addr
		refused   bool
	}{
		{"no proxy address", []string{"local", "e2b"}, restricted, nil, true},
		{"an IPv6 proxy alone", []string{"e2b"}, restricted, []netip.Addr{v6}, true},
		{"an IPv4 proxy", []string{"e2b"}, restricted, []netip.Addr{v6, v4}, false},
		{"no E2B", []string{"boxd"}, restricted, nil, false},
		{"no restricted package", []string{"e2b"}, open, nil, false},
	} {
		cfg := config.Config{Providers: tc.providers}
		cfg.Secrets.EgressIPs = tc.ips
		err := checkRestrictedEgress(cfg, tc.cat)
		if tc.refused != (err != nil) {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if err != nil && (!strings.Contains(err.Error(), "secrets.egress_ips") ||
			!strings.Contains(err.Error(), "locked")) {
			t.Fatalf("%s: %v, want the setting and the package named", tc.name, err)
		}
	}
}
