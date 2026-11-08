package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

/* Internal mTLS between egress and Payments */

const (
	// serverSAN is the name egress dials Payments' x402 listener by
	serverSAN = "bugnet-payments.internal"
	// clientSAN is the only client name Payments' x402 listener accepts
	clientSAN = "bugnet-egress.internal"
	caName    = "Bugnet internal CA"
	devCAName = "Bugnet development internal CA"
	caYears   = 10
	leafYears = 1
	// backdate keeps a new certificate valid on a host whose clock runs a little behind
	backdate = time.Hour
	// envKeyName holds Payments' server key PEM in base64
	envKeyName = "BUGNET_X402_TLS_KEY"
	caKeyFile  = "internal-ca.key"
	caCertFile = "internal-ca.pem"
)

// The files mtls init writes under --out, laid out as on the VPS
const (
	vpsCACert     = "etc/bugnet/internal-ca.pem"
	vpsClientCert = "etc/bugnet/egress-x402-client.pem"
	vpsKeysDir    = "etc/bugnet/keys"
	vpsClientKey  = "etc/bugnet/keys/egress-x402-client.key"
	vpsServerCert = "etc/bugnet/payments/x402-server.pem"
	envAddFile    = "payments.env.add"
)

// The files mtls init --dev writes flat, under the names egress and Payments read by default
const (
	devClientCert = "egress-x402-client.pem"
	devClientKey  = "egress_x402_client_key"
	devServerCert = "x402-server.pem"
)

// signer is a certificate and its private key
type signer struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

// pairs is a CA with Payments' server pair and egress's client pair
type pairs struct {
	ca, server, client signer
}

// outFile is one file to write, its name relative to the target directory
type outFile struct {
	name string
	body []byte
	mode fs.FileMode
}

// mtls runs mtls init
func (c *cli) mtls(args []string) error {
	if len(args) == 0 || args[0] != "init" {
		return fmt.Errorf("%w: mtls takes init", errUsage)
	}
	fs := newFlags("mtls init")
	out := fs.String("out", "", "directory to write the files into")
	state := fs.String("state", "", "directory that keeps the CA key")
	renew := fs.Bool("renew", false, "renew the two certificates for their existing keys")
	from := fs.String("from", "", "the client and server certificates to renew, comma separated")
	dev := fs.Bool("dev", false, "a development CA and both pairs, flat in --out, no CA kept")
	if err := parse(fs, args[1:], false); err != nil {
		return err
	}
	if err := required("out", *out); err != nil {
		return err
	}
	switch {
	case *dev && (*renew || isSet(fs, "state") || isSet(fs, "from")):
		return fmt.Errorf("%w: --dev takes only --out", errUsage)
	case *dev:
		return c.mtlsDev(*out)
	case !*renew && isSet(fs, "from"):
		return fmt.Errorf("%w: --from needs --renew", errUsage)
	}
	stateDir, err := c.mtlsState(*state)
	if err != nil {
		return err
	}
	if !*renew {
		return c.mtlsNew(stateDir, *out)
	}
	certs := strings.Split(*from, ",")
	if len(certs) != 2 || slices.Contains(certs, "") {
		return fmt.Errorf("%w: --from takes CLIENT_PEM,SERVER_PEM", errUsage)
	}
	return c.mtlsRenew(stateDir, certs[0], certs[1], *out)
}

// mtlsState is --state, or ~/.local/state/bugnet-dev/mtls when it is not given
func (c *cli) mtlsState(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	home := c.getenv("HOME")
	if home == "" {
		var err error
		if home, err = os.UserHomeDir(); err != nil {
			return "", err
		}
	}
	return filepath.Join(home, ".local", "state", "bugnet-dev", "mtls"), nil
}

// mtlsNew makes the internal CA once, keeps its key in state and writes the VPS's files
func (c *cli) mtlsNew(state, out string) error {
	for _, name := range []string{caKeyFile, caCertFile} {
		_, err := os.Lstat(filepath.Join(state, name))
		if err == nil {
			return fmt.Errorf("%s already holds a CA, renew its certificates with --renew", state)
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	p, err := newPairs(caName, time.Now())
	if err != nil {
		return err
	}
	files, err := p.files(vpsCACert, vpsClientCert, vpsClientKey, vpsServerCert)
	if err != nil {
		return err
	}
	caKey, err := keyPEM(p.ca.key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(out, vpsKeysDir), 0o700); err != nil {
		return err
	}
	wrote, err := writeFiles(out, files)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(state, 0o700); err != nil {
		return err
	}
	kept, err := writeFiles(state, []outFile{
		{caCertFile, certPEM(p.ca.cert), 0o644}, {caKeyFile, caKey, 0o600},
	})
	if err != nil {
		return err
	}
	lines := wrote
	lines = append(lines, kept...)
	for _, name := range []string{vpsClientKey, envAddFile} {
		lines = append(lines, [2]string{"delete_once_copied", outPath(out, name)})
	}
	return c.print(lines)
}

// mtlsDev writes a throwaway development CA and both pairs flat in out
func (c *cli) mtlsDev(out string) error {
	p, err := newPairs(devCAName, time.Now())
	if err != nil {
		return err
	}
	files, err := p.files(caCertFile, devClientCert, devClientKey, devServerCert)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	wrote, err := writeFiles(out, files)
	if err != nil {
		return err
	}
	return c.print(wrote)
}

// mtlsRenew issues new certificates for the public keys of the two current ones
func (c *cli) mtlsRenew(state, clientPath, serverPath, out string) error {
	ca, err := readCA(state)
	if err != nil {
		return err
	}
	now := time.Now()
	if !now.Before(ca.cert.NotAfter) {
		return fmt.Errorf("the CA in %s expired at %s", state,
			ca.cert.NotAfter.Format(time.RFC3339))
	}
	var files []outFile
	var notAfter time.Time
	for _, f := range []struct {
		path, san, name string
		use             x509.ExtKeyUsage
	}{
		{clientPath, clientSAN, vpsClientCert, x509.ExtKeyUsageClientAuth},
		{serverPath, serverSAN, vpsServerCert, x509.ExtKeyUsageServerAuth},
	} {
		old, err := readLeaf(f.path, f.san, ca.cert)
		if err != nil {
			return err
		}
		cert, err := issue(ca, old.PublicKey.(*ecdsa.PublicKey), f.san, f.use, now)
		if err != nil {
			return err
		}
		notAfter = cert.NotAfter
		files = append(files, outFile{f.name, certPEM(cert), 0o644})
	}
	wrote, err := writeFiles(out, files)
	if err != nil {
		return err
	}
	return c.print(append(wrote, [2]string{"not_after", notAfter.Format(time.RFC3339)}))
}

/* Certificates */

func newECKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

// newSerial is a random positive 127-bit serial
func newSerial() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, err
	}
	return n.Add(n, big.NewInt(1)), nil
}

func newCA(name string, now time.Time) (signer, error) {
	key, err := newECKey()
	if err != nil {
		return signer{}, err
	}
	serial, err := newSerial()
	if err != nil {
		return signer{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             now.Add(-backdate),
		NotAfter:              now.AddDate(caYears, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return signer{}, err
	}
	cert, err := x509.ParseCertificate(der)
	return signer{cert, key}, err
}

// issue signs a 1-year leaf for pub with one DNS name and one use, never past the CA
func issue(
	ca signer, pub *ecdsa.PublicKey, san string, use x509.ExtKeyUsage, now time.Time,
) (*x509.Certificate, error) {
	serial, err := newSerial()
	if err != nil {
		return nil, err
	}
	notAfter := now.AddDate(leafYears, 0, 0)
	if notAfter.After(ca.cert.NotAfter) {
		notAfter = ca.cert.NotAfter
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: san},
		DNSNames:              []string{san},
		NotBefore:             now.Add(-backdate),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{use},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(der)
}

// newLeaf makes a key and its leaf
func newLeaf(ca signer, san string, use x509.ExtKeyUsage, now time.Time) (signer, error) {
	key, err := newECKey()
	if err != nil {
		return signer{}, err
	}
	cert, err := issue(ca, &key.PublicKey, san, use, now)
	return signer{cert, key}, err
}

func newPairs(name string, now time.Time) (pairs, error) {
	ca, err := newCA(name, now)
	if err != nil {
		return pairs{}, err
	}
	server, err := newLeaf(ca, serverSAN, x509.ExtKeyUsageServerAuth, now)
	if err != nil {
		return pairs{}, err
	}
	client, err := newLeaf(ca, clientSAN, x509.ExtKeyUsageClientAuth, now)
	return pairs{ca, server, client}, err
}

// files are the CA certificate, both pairs and payments.env.add under the names given
func (p pairs) files(caCert, clientCert, clientKey, serverCert string) ([]outFile, error) {
	ck, err := keyPEM(p.client.key)
	if err != nil {
		return nil, err
	}
	sk, err := keyPEM(p.server.key)
	if err != nil {
		return nil, err
	}
	envLine := envKeyName + "=" + base64.StdEncoding.EncodeToString(sk) + "\n"
	return []outFile{
		{caCert, certPEM(p.ca.cert), 0o644},
		{clientCert, certPEM(p.client.cert), 0o644},
		{clientKey, ck, 0o600},
		{serverCert, certPEM(p.server.cert), 0o644},
		{envAddFile, []byte(envLine), 0o600},
	}, nil
}

func certPEM(cert *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}

// keyPEM is the key as PKCS 8 PEM, which Go, Node and openssl all read
func keyPEM(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// readCertFile reads a file holding exactly one PEM certificate
func readCertFile(path string) (*x509.Certificate, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(b)
	if block == nil || block.Type != "CERTIFICATE" || len(strings.TrimSpace(string(rest))) > 0 {
		return nil, fmt.Errorf("%s does not hold one PEM certificate", path)
	}
	return x509.ParseCertificate(block.Bytes)
}

// readCA reads the CA certificate and key kept in state and checks they match
func readCA(state string) (signer, error) {
	cert, err := readCertFile(filepath.Join(state, caCertFile))
	if errors.Is(err, fs.ErrNotExist) {
		return signer{}, fmt.Errorf("%s holds no CA, make one with mtls init", state)
	}
	if err != nil {
		return signer{}, err
	}
	keyPath := filepath.Join(state, caKeyFile)
	b, err := os.ReadFile(keyPath)
	if err != nil {
		return signer{}, err
	}
	block, _ := pem.Decode(b)
	if block == nil || block.Type != "PRIVATE KEY" {
		return signer{}, fmt.Errorf("%s does not hold a PKCS 8 PEM key", keyPath)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return signer{}, fmt.Errorf("%s does not hold a PKCS 8 PEM key", keyPath)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || !key.PublicKey.Equal(cert.PublicKey) {
		return signer{}, fmt.Errorf("the key in %s does not match the CA certificate", state)
	}
	return signer{cert, key}, nil
}

// readLeaf reads a current leaf and checks the CA issued it for san with a P-256 key
func readLeaf(path, san string, ca *x509.Certificate) (*x509.Certificate, error) {
	cert, err := readCertFile(path)
	if err != nil {
		return nil, err
	}
	if err := cert.CheckSignatureFrom(ca); err != nil {
		return nil, fmt.Errorf("%s was not issued by the kept CA", path)
	}
	if !slices.Equal(cert.DNSNames, []string{san}) {
		return nil, fmt.Errorf("%s is not the %s certificate", path, san)
	}
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok || pub.Curve != elliptic.P256() {
		return nil, fmt.Errorf("%s does not hold an ECDSA P-256 key", path)
	}
	return cert, nil
}

/* Files */

func outPath(dir, name string) string {
	return filepath.Join(dir, filepath.FromSlash(name))
}

// writeFiles writes each file whole with its exact mode and returns a wrote line for each
func writeFiles(dir string, files []outFile) ([][2]string, error) {
	var wrote [][2]string
	for _, f := range files {
		path := outPath(dir, f.name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return wrote, err
		}
		if err := writeFile(path, f.body, f.mode); err != nil {
			return wrote, err
		}
		wrote = append(wrote, [2]string{"wrote", path})
	}
	return wrote, nil
}

// writeFile replaces path by renaming a temporary file, so a key never sits under a wider mode
func writeFile(path string, body []byte, mode fs.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	_, err = tmp.Write(body)
	err = errors.Join(err, tmp.Chmod(mode), tmp.Close())
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
	}
	return err
}
