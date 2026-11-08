package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const day = 24 * time.Hour

// vpsFiles are the files mtls init writes under --out, with their modes
var vpsFiles = map[string]fs.FileMode{
	"etc/bugnet/internal-ca.pem":             0o644,
	"etc/bugnet/egress-x402-client.pem":      0o644,
	"etc/bugnet/keys/egress-x402-client.key": 0o600,
	"etc/bugnet/payments/x402-server.pem":    0o644,
	"payments.env.add":                       0o600,
}

// devFiles are the files mtls init --dev writes, with their modes
var devFiles = map[string]fs.FileMode{
	"internal-ca.pem":        0o644,
	"egress-x402-client.pem": 0o644,
	"egress_x402_client_key": 0o600,
	"x402-server.pem":        0o644,
	"payments.env.add":       0o600,
}

// mtlsInit runs mtls init into fresh out and state directories and returns them with stdout
func mtlsInit(t *testing.T, extra ...string) (string, string, string) {
	t.Helper()
	out, state := t.TempDir(), filepath.Join(t.TempDir(), "mtls")
	args := append([]string{"mtls", "init", "--out", out, "--state", state}, extra...)
	stdout, stderr, code := runCLI(t, env(nil), args...)
	if code != 0 {
		t.Fatalf("mtls init exited %d with %q", code, stderr)
	}
	return out, state, stdout
}

func readCert(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block, rest := pem.Decode(b)
	if block == nil || block.Type != "CERTIFICATE" || len(strings.TrimSpace(string(rest))) > 0 {
		t.Fatalf("%s is not one PEM certificate", path)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func readKey(t *testing.T, path string) *ecdsa.PrivateKey {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return parseKeyPEM(t, b)
}

func parseKeyPEM(t *testing.T, b []byte) *ecdsa.PrivateKey {
	t.Helper()
	block, _ := pem.Decode(b)
	if block == nil {
		t.Fatal("no PEM key")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	ec, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("key is %T, want ECDSA", key)
	}
	return ec
}

// envKey decodes the server key PEM from a payments.env.add file
func envKey(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := strings.CutPrefix(string(b), "BUGNET_X402_TLS_KEY=")
	if !ok || !strings.HasSuffix(value, "\n") || strings.Count(string(b), "\n") != 1 {
		t.Fatalf("payments.env.add is %q, want one BUGNET_X402_TLS_KEY line", b)
	}
	keyPEM, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(value, "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return keyPEM
}

func near(t *testing.T, what string, got, want time.Time) {
	t.Helper()
	if d := got.Sub(want); d > day || d < -day {
		t.Fatalf("%s is %v, want %v within a day", what, got, want)
	}
}

func checkP256(t *testing.T, what string, cert *x509.Certificate) {
	t.Helper()
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok || pub.Curve != elliptic.P256() {
		t.Fatalf("%s key is %T, want ECDSA P-256", what, cert.PublicKey)
	}
}

// checkLeaf checks a leaf's key, life, only SAN and use, and that it chains to the CA
func checkLeaf(t *testing.T, leaf, ca *x509.Certificate, san string, use x509.ExtKeyUsage) {
	t.Helper()
	checkP256(t, san, leaf)
	near(t, san+" NotAfter", leaf.NotAfter, leaf.NotBefore.AddDate(1, 0, 0))
	if !slices.Equal(leaf.DNSNames, []string{san}) || len(leaf.IPAddresses) > 0 ||
		len(leaf.URIs) > 0 || len(leaf.EmailAddresses) > 0 {
		t.Fatalf("%s SANs are %v %v %v %v", san, leaf.DNSNames, leaf.IPAddresses, leaf.URIs,
			leaf.EmailAddresses)
	}
	if !slices.Equal(leaf.ExtKeyUsage, []x509.ExtKeyUsage{use}) || leaf.IsCA {
		t.Fatalf("%s uses %v, CA %v", san, leaf.ExtKeyUsage, leaf.IsCA)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	_, err := leaf.Verify(x509.VerifyOptions{
		DNSName: san, Roots: roots, KeyUsages: []x509.ExtKeyUsage{use},
	})
	if err != nil {
		t.Fatalf("%s does not chain to the CA: %v", san, err)
	}
}

func TestMTLSInitShapes(t *testing.T) {
	out, state, _ := mtlsInit(t)
	now := time.Now()
	ca := readCert(t, filepath.Join(out, "etc/bugnet/internal-ca.pem"))
	checkP256(t, "CA", ca)
	if !ca.IsCA || !ca.BasicConstraintsValid || ca.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Fatalf("CA is CA %v with key usage %v", ca.IsCA, ca.KeyUsage)
	}
	if err := ca.CheckSignatureFrom(ca); err != nil {
		t.Fatalf("CA is not self-signed: %v", err)
	}
	near(t, "CA NotBefore", ca.NotBefore, now)
	near(t, "CA NotAfter", ca.NotAfter, now.AddDate(10, 0, 0))
	eq(t, readCert(t, filepath.Join(state, "internal-ca.pem")).Equal(ca), true)
	if !readKey(t, filepath.Join(state, "internal-ca.key")).PublicKey.Equal(ca.PublicKey) {
		t.Fatal("the kept CA key does not match the CA")
	}

	client := readCert(t, filepath.Join(out, "etc/bugnet/egress-x402-client.pem"))
	server := readCert(t, filepath.Join(out, "etc/bugnet/payments/x402-server.pem"))
	checkLeaf(t, client, ca, "bugnet-egress.internal", x509.ExtKeyUsageClientAuth)
	checkLeaf(t, server, ca, "bugnet-payments.internal", x509.ExtKeyUsageServerAuth)
	near(t, "leaf NotBefore", client.NotBefore, now)
	if client.SerialNumber.Cmp(server.SerialNumber) == 0 {
		t.Fatal("the two leaves share a serial")
	}
	clientKey := readKey(t, filepath.Join(out, "etc/bugnet/keys/egress-x402-client.key"))
	eq(t, clientKey.PublicKey.Equal(client.PublicKey), true)
	serverKey := parseKeyPEM(t, envKey(t, filepath.Join(out, "payments.env.add")))
	eq(t, serverKey.PublicKey.Equal(server.PublicKey), true)
}

func sameMap[V comparable](a, b map[string]V) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// modes lists every regular file under dir with its permission bits
func modes(t *testing.T, dir string) map[string]fs.FileMode {
	t.Helper()
	got := map[string]fs.FileMode{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		got[filepath.ToSlash(rel)] = info.Mode()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestMTLSInitModes(t *testing.T) {
	out, state, _ := mtlsInit(t)
	if got := modes(t, out); !sameMap(got, vpsFiles) {
		t.Fatalf("--out holds %v, want %v", got, vpsFiles)
	}
	wantState := map[string]fs.FileMode{"internal-ca.key": 0o600, "internal-ca.pem": 0o644}
	if got := modes(t, state); !sameMap(got, wantState) {
		t.Fatalf("--state holds %v, want %v", got, wantState)
	}
	for dir, want := range map[string]fs.FileMode{
		state: 0o700, filepath.Join(out, "etc/bugnet/keys"): 0o700,
	} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, info.Mode().Perm(), want)
	}
}

func TestMTLSInitDefaultState(t *testing.T) {
	home, out := t.TempDir(), t.TempDir()
	_, stderr, code := runCLI(t, env(map[string]string{"HOME": home}), "mtls", "init", "--out",
		out)
	if code != 0 {
		t.Fatalf("mtls init exited %d with %q", code, stderr)
	}
	kept := filepath.Join(home, ".local/state/bugnet-dev/mtls/internal-ca.key")
	if _, err := os.Stat(kept); err != nil {
		t.Fatal(err)
	}
}

// secretStrings are the forms of every private key a run wrote that must never be printed
func secretStrings(t *testing.T, keyFiles []string, envFile string) []string {
	t.Helper()
	envBytes, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatal(err)
	}
	value := strings.TrimSpace(strings.TrimPrefix(string(envBytes), "BUGNET_X402_TLS_KEY="))
	secrets := []string{"PRIVATE KEY", "BUGNET_X402_TLS_KEY=", value, value[:24]}
	keyPEMs := [][]byte{envKey(t, envFile)}
	for _, f := range keyFiles {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		keyPEMs = append(keyPEMs, b)
	}
	for _, b := range keyPEMs {
		block, _ := pem.Decode(b)
		body := base64.StdEncoding.EncodeToString(block.Bytes)
		secrets = append(secrets, body[:24], body[len(body)-24:])
	}
	return secrets
}

func TestMTLSInitPrintsNoKey(t *testing.T) {
	out, state, stdout := mtlsInit(t)
	secrets := secretStrings(t, []string{
		filepath.Join(state, "internal-ca.key"),
		filepath.Join(out, "etc/bugnet/keys/egress-x402-client.key"),
	}, filepath.Join(out, "payments.env.add"))
	for _, s := range secrets {
		if strings.Contains(stdout, s) {
			t.Fatalf("stdout holds %q:\n%s", s, stdout)
		}
	}
	for name := range vpsFiles {
		if !strings.Contains(stdout, "wrote="+filepath.Join(out, name)+"\n") {
			t.Fatalf("stdout does not name %s:\n%s", name, stdout)
		}
	}
	for _, name := range []string{"etc/bugnet/keys/egress-x402-client.key", "payments.env.add"} {
		if !strings.Contains(stdout, "delete_once_copied="+filepath.Join(out, name)+"\n") {
			t.Fatalf("stdout has no reminder for %s:\n%s", name, stdout)
		}
	}

	dev := t.TempDir()
	stdout, stderr, code := runCLI(t, env(nil), "mtls", "init", "--dev", "--out", dev)
	if code != 0 {
		t.Fatalf("mtls init --dev exited %d with %q", code, stderr)
	}
	for _, s := range secretStrings(t, []string{filepath.Join(dev, "egress_x402_client_key")},
		filepath.Join(dev, "payments.env.add")) {
		if strings.Contains(stdout, s) || strings.Contains(stderr, s) {
			t.Fatalf("--dev output holds %q:\n%s", s, stdout)
		}
	}
}

// snapshot reads every file under dir
func snapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	got := map[string]string{}
	for name := range modes(t, dir) {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		got[name] = string(b)
	}
	return got
}

func TestMTLSInitRefusesExistingCA(t *testing.T) {
	out, state, _ := mtlsInit(t)
	beforeOut, beforeState := snapshot(t, out), snapshot(t, state)
	other := t.TempDir()
	for _, dir := range []string{out, other} {
		_, stderr, code := runCLI(t, env(nil), "mtls", "init", "--out", dir, "--state", state)
		if code != 1 || !strings.Contains(stderr, state) || !strings.Contains(stderr, "--renew") {
			t.Fatalf("second init exited %d with %q, want 1 naming %s", code, stderr, state)
		}
	}
	eq(t, sameMap(snapshot(t, out), beforeOut), true)
	eq(t, sameMap(snapshot(t, state), beforeState), true)
	eq(t, len(snapshot(t, other)), 0)

	// a CA certificate alone, its key lost, still refuses
	if err := os.Remove(filepath.Join(state, "internal-ca.key")); err != nil {
		t.Fatal(err)
	}
	_, _, code := runCLI(t, env(nil), "mtls", "init", "--out", other, "--state", state)
	eq(t, code, 1)
}

// renew runs mtls init --renew from the two certificates under from, writing into out
func renew(t *testing.T, state, from, out string) (string, string, int) {
	t.Helper()
	certs := filepath.Join(from, "etc/bugnet/egress-x402-client.pem") + "," +
		filepath.Join(from, "etc/bugnet/payments/x402-server.pem")
	return runCLI(t, env(nil), "mtls", "init", "--renew", "--from", certs, "--out", out,
		"--state", state)
}

func TestMTLSRenewKeepsKeysAndSANs(t *testing.T) {
	out, state, _ := mtlsInit(t)
	before := snapshot(t, out)
	oldClient := readCert(t, filepath.Join(out, "etc/bugnet/egress-x402-client.pem"))
	oldServer := readCert(t, filepath.Join(out, "etc/bugnet/payments/x402-server.pem"))
	stateBefore := snapshot(t, state)

	stdout, stderr, code := renew(t, state, out, out)
	if code != 0 {
		t.Fatalf("renew exited %d with %q", code, stderr)
	}
	if strings.Contains(stdout, "PRIVATE KEY") || strings.Contains(stdout, "delete_once") {
		t.Fatalf("renew printed %q", stdout)
	}
	after := snapshot(t, out)
	changed := []string{}
	for name := range before {
		if before[name] != after[name] {
			changed = append(changed, name)
		}
	}
	slices.Sort(changed)
	eq(t, strings.Join(changed, " "),
		"etc/bugnet/egress-x402-client.pem etc/bugnet/payments/x402-server.pem")
	eq(t, len(after), len(before))
	eq(t, sameMap(snapshot(t, state), stateBefore), true)

	ca := readCert(t, filepath.Join(state, "internal-ca.pem"))
	for _, pair := range []struct {
		old  *x509.Certificate
		path string
		san  string
		use  x509.ExtKeyUsage
	}{
		{oldClient, "etc/bugnet/egress-x402-client.pem", "bugnet-egress.internal",
			x509.ExtKeyUsageClientAuth},
		{oldServer, "etc/bugnet/payments/x402-server.pem", "bugnet-payments.internal",
			x509.ExtKeyUsageServerAuth},
	} {
		got := readCert(t, filepath.Join(out, pair.path))
		checkLeaf(t, got, ca, pair.san, pair.use)
		if !got.PublicKey.(*ecdsa.PublicKey).Equal(pair.old.PublicKey) {
			t.Fatalf("%s has a new public key", pair.san)
		}
		if got.SerialNumber.Cmp(pair.old.SerialNumber) == 0 {
			t.Fatalf("%s kept its serial", pair.san)
		}
		near(t, pair.san+" NotBefore", got.NotBefore, time.Now())
	}

	// a fresh --out gets only the two certificates
	fresh := t.TempDir()
	if _, stderr, code := renew(t, state, out, fresh); code != 0 {
		t.Fatalf("renew into a fresh directory exited %d with %q", code, stderr)
	}
	got := sortedKeys(modes(t, fresh))
	eq(t, strings.Join(got, " "),
		"etc/bugnet/egress-x402-client.pem etc/bugnet/payments/x402-server.pem")
}

func TestMTLSRenewRefuses(t *testing.T) {
	out, state, _ := mtlsInit(t)
	dev := t.TempDir()
	if _, stderr, code := runCLI(t, env(nil), "mtls", "init", "--dev", "--out", dev); code != 0 {
		t.Fatalf("mtls init --dev exited %d with %q", code, stderr)
	}
	client := filepath.Join(out, "etc/bugnet/egress-x402-client.pem")
	server := filepath.Join(out, "etc/bugnet/payments/x402-server.pem")
	for name, from := range map[string]string{
		"swapped":      server + "," + client,
		"other CA":     filepath.Join(dev, "egress-x402-client.pem") + "," + server,
		"not a cert":   filepath.Join(out, "payments.env.add") + "," + server,
		"missing file": filepath.Join(out, "nope.pem") + "," + server,
	} {
		target := t.TempDir()
		_, stderr, code := runCLI(t, env(nil), "mtls", "init", "--renew", "--from", from,
			"--out", target, "--state", state)
		if code != 1 || len(snapshot(t, target)) != 0 {
			t.Fatalf("%s: renew exited %d with %q", name, code, stderr)
		}
	}
	_, _, code := runCLI(t, env(nil), "mtls", "init", "--renew", "--from", client+","+server,
		"--out", t.TempDir(), "--state", t.TempDir())
	eq(t, code, 1)
}

// replaceCA re-issues the kept CA certificate with the same key, subject and a new NotAfter
func replaceCA(t *testing.T, state string, notAfter time.Time) *x509.Certificate {
	t.Helper()
	old := readCert(t, filepath.Join(state, "internal-ca.pem"))
	key := readKey(t, filepath.Join(state, "internal-ca.key"))
	tmpl := *old
	tmpl.NotAfter = notAfter
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	b := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(filepath.Join(state, "internal-ca.pem"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	return readCert(t, filepath.Join(state, "internal-ca.pem"))
}

func TestMTLSRenewNeverPastTheCA(t *testing.T) {
	out, state, _ := mtlsInit(t)
	ca := replaceCA(t, state, time.Now().AddDate(0, 6, 0))
	if _, stderr, code := renew(t, state, out, out); code != 0 {
		t.Fatalf("renew exited %d with %q", code, stderr)
	}
	for _, name := range []string{
		"etc/bugnet/egress-x402-client.pem", "etc/bugnet/payments/x402-server.pem",
	} {
		eq(t, readCert(t, filepath.Join(out, name)).NotAfter, ca.NotAfter)
	}

	replaceCA(t, state, time.Now().Add(-time.Hour))
	target := t.TempDir()
	_, _, code := renew(t, state, out, target)
	eq(t, code, 1)
	eq(t, len(snapshot(t, target)), 0)
}

func TestMTLSDevIsASeparateCA(t *testing.T) {
	home := t.TempDir()
	out, state, _ := mtlsInit(t)
	dev := t.TempDir()
	vars := env(map[string]string{"HOME": home})
	if _, stderr, code := runCLI(t, vars, "mtls", "init", "--dev", "--out", dev); code != 0 {
		t.Fatalf("mtls init --dev exited %d with %q", code, stderr)
	}
	ca := readCert(t, filepath.Join(out, "etc/bugnet/internal-ca.pem"))
	devCA := readCert(t, filepath.Join(dev, "internal-ca.pem"))
	if devCA.PublicKey.(*ecdsa.PublicKey).Equal(ca.PublicKey) || devCA.Subject.String() ==
		ca.Subject.String() {
		t.Fatalf("dev CA %s shares the internal CA's key or name", devCA.Subject)
	}
	checkLeaf(t, readCert(t, filepath.Join(dev, "egress-x402-client.pem")), devCA,
		"bugnet-egress.internal", x509.ExtKeyUsageClientAuth)
	checkLeaf(t, readCert(t, filepath.Join(dev, "x402-server.pem")), devCA,
		"bugnet-payments.internal", x509.ExtKeyUsageServerAuth)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	_, err := readCert(t, filepath.Join(dev, "x402-server.pem")).Verify(x509.VerifyOptions{
		Roots: roots, DNSName: "bugnet-payments.internal",
	})
	if err == nil {
		t.Fatal("a dev leaf chains to the internal CA")
	}
	eq(t, len(snapshot(t, state)), 2)
	if _, err := os.Stat(filepath.Join(home, ".local")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("--dev touched the default state directory: %v", err)
	}

	again := t.TempDir()
	if _, stderr, code := runCLI(t, vars, "mtls", "init", "--dev", "--out", again); code != 0 {
		t.Fatalf("a second --dev exited %d with %q", code, stderr)
	}
	if readCert(t, filepath.Join(again, "internal-ca.pem")).Equal(devCA) {
		t.Fatal("two --dev runs share a CA")
	}
}

func TestMTLSDevLayout(t *testing.T) {
	dev := t.TempDir()
	stdout, stderr, code := runCLI(t, env(nil), "mtls", "init", "--dev", "--out", dev)
	if code != 0 {
		t.Fatalf("mtls init --dev exited %d with %q", code, stderr)
	}
	if got := modes(t, dev); !sameMap(got, devFiles) {
		t.Fatalf("--dev wrote %v, want %v", got, devFiles)
	}
	for name := range devFiles {
		if !strings.Contains(stdout, "wrote="+filepath.Join(dev, name)+"\n") {
			t.Fatalf("stdout does not name %s:\n%s", name, stdout)
		}
	}
	key := readKey(t, filepath.Join(dev, "egress_x402_client_key"))
	eq(t, key.PublicKey.Equal(readCert(t, filepath.Join(dev, "egress-x402-client.pem")).PublicKey),
		true)
	serverKey := parseKeyPEM(t, envKey(t, filepath.Join(dev, "payments.env.add")))
	eq(t, serverKey.PublicKey.Equal(readCert(t, filepath.Join(dev, "x402-server.pem")).PublicKey),
		true)

	// a second run over the same directory replaces the pair
	before := snapshot(t, dev)
	if _, stderr, code := runCLI(t, env(nil), "mtls", "init", "--dev", "--out", dev); code != 0 {
		t.Fatalf("a second --dev exited %d with %q", code, stderr)
	}
	after := snapshot(t, dev)
	eq(t, len(after), len(devFiles))
	eq(t, after["internal-ca.pem"] != before["internal-ca.pem"], true)
	if got := modes(t, dev); !sameMap(got, devFiles) {
		t.Fatalf("a second --dev left %v", got)
	}
}

func TestMTLSUsage(t *testing.T) {
	out := t.TempDir()
	for name, args := range map[string][]string{
		"no subcommand":     {"mtls"},
		"bad subcommand":    {"mtls", "rotate"},
		"no out":            {"mtls", "init", "--state", t.TempDir()},
		"dev with state":    {"mtls", "init", "--dev", "--out", out, "--state", t.TempDir()},
		"dev with renew":    {"mtls", "init", "--dev", "--renew", "--from", "a,b", "--out", out},
		"renew no from":     {"mtls", "init", "--renew", "--out", out},
		"from no renew":     {"mtls", "init", "--from", "a,b", "--out", out},
		"from one file":     {"mtls", "init", "--renew", "--from", "a", "--out", out},
		"from three files":  {"mtls", "init", "--renew", "--from", "a,b,c", "--out", out},
		"from empty member": {"mtls", "init", "--renew", "--from", "a,", "--out", out},
		"an argument":       {"mtls", "init", "--out", out, "more"},
	} {
		_, stderr, code := runCLI(t, env(nil), args...)
		if code != 2 {
			t.Fatalf("%s: exited %d with %q, want 2", name, code, stderr)
		}
	}
	eq(t, len(snapshot(t, out)), 0)
}

// loadPair loads a certificate and its PKCS 8 key PEM
func loadPair(t *testing.T, certPath string, keyPEM []byte) tls.Certificate {
	t.Helper()
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

// handshake runs a TLS 1.3 handshake over loopback TCP and returns the server's error
func handshake(t *testing.T, server, client *tls.Config) error {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	deadline := time.Now().Add(2 * time.Second)
	done := make(chan error, 1)
	go func() {
		conn, err := tls.DialWithDialer(&net.Dialer{Deadline: deadline}, "tcp",
			ln.Addr().String(), client)
		if err == nil {
			_ = conn.SetDeadline(deadline)
			// TLS 1.3 reports a refused client certificate on the first read
			_, err = conn.Read(make([]byte, 1))
			_ = conn.Close()
		}
		done <- err
	}()
	raw, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = raw.SetDeadline(deadline)
	srv := tls.Server(raw, server)
	err = srv.Handshake()
	if err == nil {
		_, err = srv.Write([]byte{1})
	}
	_ = srv.Close()
	if clientErr := <-done; err == nil && clientErr != nil {
		return clientErr
	}
	return err
}

func TestMTLSPairsHandshake(t *testing.T) {
	dev, other := t.TempDir(), t.TempDir()
	for _, dir := range []string{dev, other} {
		_, stderr, code := runCLI(t, env(nil), "mtls", "init", "--dev", "--out", dir)
		if code != 0 {
			t.Fatalf("mtls init --dev exited %d with %q", code, stderr)
		}
	}
	pool := func(dir string) *x509.CertPool {
		p := x509.NewCertPool()
		p.AddCert(readCert(t, filepath.Join(dir, "internal-ca.pem")))
		return p
	}
	keyFile := func(dir string) []byte {
		b, err := os.ReadFile(filepath.Join(dir, "egress_x402_client_key"))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	serverCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{
			loadPair(t, filepath.Join(dev, "x402-server.pem"),
				envKey(t, filepath.Join(dev, "payments.env.add"))),
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool(dev),
	}
	clientCfg := func(pairDir string) *tls.Config {
		cfg := &tls.Config{
			MinVersion: tls.VersionTLS13,
			ServerName: "bugnet-payments.internal",
			RootCAs:    pool(dev),
		}
		if pairDir != "" {
			cfg.Certificates = []tls.Certificate{loadPair(t,
				filepath.Join(pairDir, "egress-x402-client.pem"), keyFile(pairDir))}
		}
		return cfg
	}

	if err := handshake(t, serverCfg, clientCfg(dev)); err != nil {
		t.Fatalf("the client pair was refused: %v", err)
	}
	if err := handshake(t, serverCfg, clientCfg(other)); err == nil {
		t.Fatal("a client certificate from another CA was accepted")
	}
	if err := handshake(t, serverCfg, clientCfg("")); err == nil {
		t.Fatal("a client with no certificate was accepted")
	}
	wrongName := clientCfg(dev)
	wrongName.ServerName = "bugnet-egress.internal"
	if err := handshake(t, serverCfg, wrongName); err == nil {
		t.Fatal("the server pair passed for another name")
	}
}

func TestMTLSFilesReadByOpenssl(t *testing.T) {
	openssl, err := exec.LookPath("openssl")
	if err != nil {
		t.Skip("no openssl")
	}
	out, state, _ := mtlsInit(t)
	ca := filepath.Join(out, "etc/bugnet/internal-ca.pem")
	for _, name := range []string{
		ca, filepath.Join(state, "internal-ca.pem"),
		filepath.Join(out, "etc/bugnet/egress-x402-client.pem"),
		filepath.Join(out, "etc/bugnet/payments/x402-server.pem"),
	} {
		got, err := exec.Command(openssl, "x509", "-noout", "-subject", "-in", name).
			CombinedOutput()
		if err != nil || !strings.HasPrefix(string(got), "subject=") {
			t.Fatalf("openssl x509 on %s: %v %s", name, err, got)
		}
	}
	for cert, purpose := range map[string]string{
		"etc/bugnet/egress-x402-client.pem":   "sslclient",
		"etc/bugnet/payments/x402-server.pem": "sslserver",
	} {
		got, err := exec.Command(openssl, "verify", "-CAfile", ca, "-purpose", purpose,
			filepath.Join(out, cert)).CombinedOutput()
		if err != nil {
			t.Fatalf("openssl verify %s: %v %s", cert, err, got)
		}
	}
	got, err := exec.Command(openssl, "pkey", "-noout", "-in",
		filepath.Join(out, "etc/bugnet/keys/egress-x402-client.key")).CombinedOutput()
	if err != nil {
		t.Fatalf("openssl pkey: %v %s", err, got)
	}
}
