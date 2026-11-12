// Package image_test builds the Bugnet base image in Docker, CP 19.15.2
package image_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/image"
)

// platform is the one build.sh builds for and the payload runner runs on
const platform = "linux/amd64"

func eq[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func needDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("BUGNET_TEST_DOCKER") != "1" {
		t.Skip("set BUGNET_TEST_DOCKER=1 to build the image")
	}
}

// testCA writes a throwaway CA certificate named as a build expects and returns its path and
// its SHA-256, which the build takes from the environment
func testCA(t *testing.T, extra string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "bugnet test ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	b := append([]byte(extra), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	path := filepath.Join(t.TempDir(), "bugnet-egress-v1.crt")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return path, hex.EncodeToString(sum[:])
}

// hostCABundle returns the build host's CA bundle, which build.sh lends apt as apt-ca.pem
func hostCABundle(t *testing.T) string {
	t.Helper()
	for _, p := range []string{"/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/cert.pem"} {
		if fi, err := os.Stat(p); err == nil && fi.Size() > 0 {
			return p
		}
	}
	t.Fatal("this host has no CA bundle to lend apt")
	return ""
}

// pin returns the value of key in deploy/image/pins.conf
func pin(t *testing.T, key string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "deploy/image/pins.conf"))
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, key+"="); ok && v != "" {
			return v
		}
	}
	t.Fatalf("pins.conf has no %s", key)
	return ""
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// build runs build.sh with --no-push and returns the local tag and the combined output
func build(t *testing.T, extra string, env ...string) (string, string, error) {
	t.Helper()
	ca, sum := testCA(t, extra)
	cmd := exec.Command(filepath.Join(repoRoot(t), "deploy/image/build.sh"),
		"--channel", "dev", "--ca", ca, "--no-push")
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(), append([]string{"CA_SHA256_DEV_V1=" + sum}, env...)...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	return lines[len(lines)-1], out.String(), err
}

func buildImage(t *testing.T, env ...string) string {
	t.Helper()
	tag, out, err := build(t, "", env...)
	if err != nil {
		t.Fatalf("build.sh: %v\n%s", err, out)
	}
	return tag
}

func run(t *testing.T, tag, script string) string {
	t.Helper()
	out, err := exec.Command("docker", "run", "--rm", "--network", "none", "--entrypoint", "sh",
		tag, "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", script, err, out)
	}
	return strings.TrimSpace(string(out))
}

func manifestIn(t *testing.T, tag string) image.Manifest {
	t.Helper()
	m, err := image.Parse([]byte(run(t, tag, "cat "+image.ManifestPath)))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestImageBuildsAndPassesItsScan(t *testing.T) {
	needDocker(t)
	tag := buildImage(t)
	m := manifestIn(t, tag)
	eq(t, m.OS, "ubuntu-24.04")
	eq(t, m.Tools["python3"], "3.12")
	eq(t, run(t, tag, "locale -a | grep -x C.utf8"), "C.utf8")
	eq(t, run(t, tag, "ls /etc/ssh/ssh_host_* 2>/dev/null | wc -l"), "0")
	eq(t, run(t, tag, "printf 'é\\n' | column -t >/dev/null && echo ok"), "ok")
	// The OCI image leaves ssh disabled, not masked, since finish masks it after E2B's provisioning
	eq(t, run(t, tag, "ls /etc/systemd/system/ssh.service /etc/systemd/system/ssh.socket "+
		"/etc/systemd/system/multi-user.target.wants/ssh.service "+
		"/etc/systemd/system/sockets.target.wants/ssh.socket 2>/dev/null | wc -l"), "0")
	eq(t, run(t, tag, "nft --version >/dev/null && hf version >/dev/null && "+
		"pnpm --version >/dev/null && python3 --version >/dev/null && echo ok"), "ok")
	eq(t, run(t, tag, "find /root /tmp -type f | wc -l"), "0")
	eq(t, run(t, tag, "ls /var/lib/apt/lists | wc -l"), "0")
	eq(t, run(t, tag, `. /etc/bugnet/image.env; export LANG LC_ALL; `+
		`printf 'é\n' | column -t >/dev/null && echo ok`), "ok")
	// The same inputs give the same tools
	eq(t, manifestIn(t, buildImage(t)).Tools["node"], m.Tools["node"])
	again := manifestIn(t, buildImage(t))
	for tool, v := range m.Tools {
		eq(t, again.Tools[tool], v)
	}
}

func TestUbuntu2204BaseKeepsPython312(t *testing.T) {
	needDocker(t)
	tag := buildImage(t, "BUGNET_BASE_IMAGE=ubuntu:22.04")
	m := manifestIn(t, tag)
	eq(t, m.OS, "ubuntu-22.04")
	eq(t, m.Tools["python3"], "3.12")
}

func TestPlantedTokenFailsThePayloadScanWithoutItsValue(t *testing.T) {
	needDocker(t)
	value := "ghp_" + strings.Repeat("Xy7", 10)
	_, out, err := build(t, "# "+value+"\n")
	if err == nil {
		t.Fatal("the build passed with a token in its payload")
	}
	if strings.Contains(out, value) {
		t.Fatal("the build printed the planted value")
	}
	eq(t, strings.Count(out, "shape github"), 1)
}

func TestBoatTargetKeepsTheSSHUnits(t *testing.T) {
	needDocker(t)
	root := repoRoot(t)
	ca, sum := testCA(t, "")
	script := `set -e; mkdir /p; cp /payload/* /p/; echo CA_SHA256_DEV_V1=` + sum +
		` >> /p/pins.conf; /p/setup-image.sh build --target boat --channel dev --inputs ` +
		strings.Repeat("0", 64) + `; test ! -L /etc/systemd/system/ssh.service; echo kept`
	payload := t.TempDir()
	runner := filepath.Join(root, "..", "bugnet-run", "dist", "bugnet-run-linux-amd64")
	for src, dst := range map[string]string{
		filepath.Join(root, "deploy/image/setup-image.sh"): "setup-image.sh",
		filepath.Join(root, "deploy/image/pins.conf"):      "pins.conf",
		filepath.Join(root, "deploy/image/VERSION"):        "VERSION",
		runner:          "bugnet-run-linux-amd64",
		ca:              "bugnet-egress-v1.crt",
		hostCABundle(t): "apt-ca.pem",
	} {
		b, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(payload, dst), b, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	out, err := exec.Command("docker", "run", "--rm", "--platform", platform,
		"-v", payload+":/payload:ro", pin(t, "BASE_IMAGE"), "bash", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.HasSuffix(strings.TrimSpace(string(out)), "kept") {
		t.Fatalf("output %s", out)
	}
}
