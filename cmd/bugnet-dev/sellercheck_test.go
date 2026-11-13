package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testSecond is the check's and the fake seller's second in tests
const testSecond = 5 * time.Millisecond

// testPair makes a certificate for names, and the pool that trusts it
func testPair(t *testing.T, names ...string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: names[0]},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: names, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true,
		IsCA: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// fakeSeller answers the check's paths, one broken way at a time
type fakeSeller struct {
	mode  string
	free  atomic.Int64
	conns atomic.Int64
}

func (f *fakeSeller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	_ = rc.EnableFullDuplex()
	switch {
	case strings.HasPrefix(r.URL.Path, "/hold/"):
		n, _ := strconv.Atoi(strings.Trim(strings.TrimPrefix(r.URL.Path, "/hold/"), "/"))
		if f.mode == "cut140" {
			time.Sleep(140 * testSecond)
			conn, _, err := rc.Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		select {
		case <-time.After(time.Duration(n) * testSecond):
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "{}")
	case r.URL.Path == "/sse/":
		w.Header().Set("Content-Type", "text/event-stream")
		if f.mode != "bufferedSSE" {
			w.WriteHeader(http.StatusOK)
			_ = rc.Flush()
		}
		tick := time.NewTicker(5 * testSecond)
		defer tick.Stop()
		for i := 1; i <= 36; i++ {
			select {
			case <-tick.C:
			case <-r.Context().Done():
				return
			}
			_, _ = io.WriteString(w, "data: "+strconv.Itoa(i)+"\n\n")
			if f.mode != "bufferedSSE" {
				_ = rc.Flush()
			}
		}
	case r.URL.Path == "/cent/x":
		if f.mode == "late402" {
			_, _ = io.Copy(io.Discard, r.Body)
		} else {
			_, _ = io.CopyN(io.Discard, r.Body, 1024)
		}
		w.WriteHeader(http.StatusPaymentRequired)
		_ = rc.Flush()
	case r.URL.Path == "/free/":
		if f.mode == "refused32nd" && f.free.Add(1) == 32 {
			if conn, _, err := rc.Hijack(); err == nil {
				_ = conn.Close()
			}
			return
		}
		_, _ = io.WriteString(w, "{}")
	default:
		http.NotFound(w, r)
	}
}

// startSeller serves the fake over TLS with cert and returns its address
func startSeller(t *testing.T, f *fakeSeller, cert tls.Certificate) string {
	t.Helper()
	srv := httptest.NewUnstartedServer(f)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			f.conns.Add(1)
		}
	}
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// runCheck runs seller-check against the fake, answering a.test and b.test with addr
func runCheck(t *testing.T, addr string, roots *x509.CertPool, ip string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	c := &cli{
		ctx: context.Background(), stdout: &out, stderr: io.Discard,
		seller: &sellerCheck{
			// minute and slack cover TLS work under -race, which does not shrink with testSecond
			second: testSecond, minute: 10 * time.Second, slack: 400 * time.Millisecond,
			roots: roots,
			lookup: func(context.Context, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr(ip)}, nil
			},
			addrOf: func(string, netip.Addr) string { return addr },
		},
	}
	err := c.x402SellerCheck([]string{"--names", "a.test,b.test"})
	return out.String(), err
}

func TestSellerCheckPasses(t *testing.T) {
	cert, roots := testPair(t, "a.test", "b.test")
	addr := startSeller(t, &fakeSeller{}, cert)
	out, err := runCheck(t, addr, roots, "127.0.0.1")
	if err != nil {
		t.Fatalf("check failed: %v\n%s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 5 {
		t.Fatalf("got %d lines, want one per check:\n%s", len(lines), out)
	}
	for i, l := range lines {
		if !strings.HasPrefix(l, "ok ") {
			t.Fatalf("line %d %q", i, l)
		}
	}
}

func TestSellerCheckFails(t *testing.T) {
	cases := []struct {
		mode string
		want string
	}{
		{"cut140", "seller check 2 "},
		{"bufferedSSE", "seller check 3 "},
		{"late402", "seller check 4 "},
		{"refused32nd", "seller check 5 "},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			t.Parallel()
			cert, roots := testPair(t, "a.test", "b.test")
			addr := startSeller(t, &fakeSeller{mode: tc.mode}, cert)
			_, err := runCheck(t, addr, roots, "127.0.0.1")
			if err == nil || !strings.Contains(err.Error(), tc.want) || errors.Is(err, errUsage) {
				t.Fatalf("got %v, want a failure of %q", err, tc.want)
			}
		})
	}
	t.Run("wrongName", func(t *testing.T) {
		t.Parallel()
		cert, roots := testPair(t, "other.test")
		addr := startSeller(t, &fakeSeller{}, cert)
		_, err := runCheck(t, addr, roots, "127.0.0.1")
		if err == nil || !strings.Contains(err.Error(), "seller check 1 ") {
			t.Fatalf("got %v, want check 1 to fail", err)
		}
	})
	t.Run("tailnetAddress", func(t *testing.T) {
		t.Parallel()
		cert, roots := testPair(t, "a.test", "b.test")
		f := &fakeSeller{}
		addr := startSeller(t, f, cert)
		_, err := runCheck(t, addr, roots, "100.100.1.1")
		if err == nil || !strings.Contains(err.Error(), "seller check 1 ") ||
			!strings.Contains(err.Error(), "Tailscale") {
			t.Fatalf("got %v, want check 1 to refuse a Tailscale address", err)
		}
		if f.conns.Load() != 0 {
			t.Fatalf("%d connections were made", f.conns.Load())
		}
	})
}

func TestSellerCheckNeedsTwoNames(t *testing.T) {
	c := &cli{ctx: context.Background(), stdout: io.Discard, stderr: io.Discard}
	for _, names := range []string{"a.test", "a.test,a.test", "a.test,b.test,c.test"} {
		if err := c.x402SellerCheck([]string{"--names", names}); !errors.Is(err, errUsage) {
			t.Errorf("%q: got %v, want a usage error", names, err)
		}
	}
}
