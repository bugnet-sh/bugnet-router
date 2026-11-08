package livetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// opTimeout bounds one create, command or small file call of a live test
	opTimeout = 5 * time.Minute
	// roundTripTimeout bounds one round trip, 101 MiB from a slow uplink included
	roundTripTimeout = 20 * time.Minute
	writeChunk       = 256 << 10
)

// RoundTripSizes are the file sizes every adapter's live file rows send up and back
var RoundTripSizes = []int64{0, 1, 1<<20 + 1, 5<<20 + 1, 101 << 20}

// Sandbox creates a ready sandbox under a fresh label and destroys all it labelled at the end
func Sandbox(t *testing.T, p provider.Provider) provider.Ref {
	t.Helper()
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	labels := map[string]string{"bugnet-live": hex.EncodeToString(b)}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		defer cancel()
		owned, err := p.ListOwned(ctx, labels)
		if err != nil {
			t.Errorf("list sandboxes left by the live run: %v", err)
		}
		for _, s := range owned {
			if err := p.Destroy(ctx, s.Ref); err != nil {
				t.Errorf("destroy %s: %v", s.Ref.ID, err)
			}
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), opTimeout)
	defer cancel()
	size, _ := p.SizeFor(provider.Resources{})
	ref, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: labels["bugnet-live"], Size: size, Labels: labels,
		Lifetime: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.WaitReady(ctx, ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

// Run runs argv in the sandbox, failing the test unless it exits 0, and returns its stdout
func Run(ctx context.Context, t *testing.T, p provider.Provider, ref provider.Ref,
	argv ...string,
) string {
	t.Helper()
	stream, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: argv})
	if err != nil {
		t.Fatalf("%v: %v", argv, err)
	}
	defer func() { _ = stream.Close() }()
	var out bytes.Buffer
	_, err = io.Copy(&out, stream)
	code, werr := stream.Wait()
	if err != nil || werr != nil || code != 0 {
		t.Fatalf("%v gave exit %d, %v, %v, output %q", argv, code, err, werr, out.String())
	}
	return out.String()
}

// SHA256 is the hex digest sha256sum computes for name inside the sandbox
func SHA256(ctx context.Context, t *testing.T, p provider.Provider, ref provider.Ref,
	name string,
) string {
	t.Helper()
	sum, _, _ := strings.Cut(Run(ctx, t, p, ref, "sha256sum", name), " ")
	return sum
}

// RoundTrip streams size random bytes up and back, comparing SHA-256 at every step
func RoundTrip(t *testing.T, p provider.Provider, ref provider.Ref, name string, size int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), roundTripTimeout)
	defer cancel()
	var seed [32]byte
	_, _ = rand.Read(seed[:])
	src := mrand.NewChaCha8(seed)

	begin := time.Now()
	w, err := p.OpenWriter(ctx, ref, name, provider.WriteSpec{Size: size, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	sent := sha256.New()
	buf := make([]byte, writeChunk)
	for left := size; left > 0; {
		chunk := buf[:min(int64(len(buf)), left)]
		_, _ = src.Read(chunk)
		sent.Write(chunk)
		if _, err := w.Write(chunk); err != nil {
			_ = w.Abort()
			t.Fatalf("write with %d of %d bytes left: %v", left, size, err)
		}
		left -= int64(len(chunk))
	}
	sum := sent.Sum(nil)
	entry, err := w.Commit(ctx, sum)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	up := time.Since(begin)
	if entry.Size != size || !entry.Regular {
		t.Errorf("Commit described %+v, want a regular file of %d bytes", entry, size)
	}
	if disk := SHA256(ctx, t, p, ref, name); disk != hex.EncodeToString(sum) {
		t.Fatalf("sha256sum in the sandbox gave %s, want %x", disk, sum)
	}

	begin = time.Now()
	r, err := p.OpenReader(ctx, ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := sha256.New()
	n, err := io.Copy(got, r)
	_ = r.Close()
	if err != nil || n != size {
		t.Fatalf("read back %d of %d bytes, %v", n, size, err)
	}
	if !bytes.Equal(got.Sum(nil), sum) {
		t.Fatalf("read back bytes whose SHA-256 is %x, want %x", got.Sum(nil), sum)
	}
	t.Logf("%d bytes up in %v (%s) and down in %v (%s)", size, up.Round(time.Millisecond),
		Rate(size, up), time.Since(begin).Round(time.Millisecond), Rate(size, time.Since(begin)))
}

// RoundTrips runs RoundTrip for every size of RoundTripSizes inside dir
func RoundTrips(t *testing.T, p provider.Provider, ref provider.Ref, dir string) {
	for _, size := range RoundTripSizes {
		t.Run(fmt.Sprintf("RoundTrip%dBytes", size), func(t *testing.T) {
			RoundTrip(t, p, ref, fmt.Sprintf("%s/round-%d.bin", dir, size), size)
		})
	}
}

// FilesystemTypes logs the filesystem of /tmp and home, where a tmpfs would hold staging in RAM
func FilesystemTypes(t *testing.T, p provider.Provider, ref provider.Ref, home string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), opTimeout)
	defer cancel()
	for _, dir := range []string{"/tmp", home} {
		kind := strings.TrimSpace(Run(ctx, t, p, ref, "stat", "-f", "-c", "%T", dir))
		t.Logf("stat -f -c %%T %s = %s", dir, kind)
		if kind == "" {
			t.Errorf("stat -f named no filesystem for %s", dir)
		}
	}
}

// NotRegular checks that name stats as no regular file and never opens for reading
func NotRegular(t *testing.T, p provider.Provider, ref provider.Ref, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), opTimeout)
	defer cancel()
	e, err := p.StatFile(ctx, ref, name)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s stats as %+v", name, e)
	if e.Regular {
		t.Errorf("%s stats as a regular file", name)
	}
	if r, err := p.OpenReader(ctx, ref, name, 0); err == nil {
		_ = r.Close()
		t.Errorf("OpenReader opened %s", name)
	}
}

// Rate formats size bytes over d as MB/s
func Rate(size int64, d time.Duration) string {
	if d <= 0 {
		return "instant"
	}
	return fmt.Sprintf("%.2f MB/s", float64(size)/1e6/d.Seconds())
}
