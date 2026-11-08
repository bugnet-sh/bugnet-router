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

	"github.com/oynozan/bugnet-router/internal/config"
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
