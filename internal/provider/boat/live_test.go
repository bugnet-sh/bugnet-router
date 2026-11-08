package boat

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat/api"
	"github.com/bugnet-sh/bugnet-router/internal/provider/contract"
	"github.com/bugnet-sh/bugnet-router/internal/provider/livetest"
)

const liveTimeout = 3 * time.Minute

// liveProvider talks to the real Boat with the config/dev.toml settings, skipping without a key
func liveProvider(t *testing.T) *Provider {
	t.Helper()
	key := os.Getenv("BOAT_API_KEY")
	if key == "" {
		t.Skip("BOAT_API_KEY is not set")
	}
	p, err := New(Options{APIKey: key, TypeBySize: livetest.Settings(t).Boat.Types})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestBoatLiveContract(t *testing.T) {
	p := liveProvider(t)
	contract.Run(t, func(*testing.T) provider.Provider { return p })
}

func TestBoatLiveArch(t *testing.T) {
	p := liveProvider(t)
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	labels := map[string]string{"bugnet-live": hex.EncodeToString(b)}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), liveTimeout)
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

	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	size, _ := p.SizeFor(provider.Resources{})
	ref, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: labels["bugnet-live"], Size: size, Labels: labels,
		Lifetime: 15 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), liveTimeout)
		defer cancel()
		if err := p.Destroy(ctx, ref); err != nil {
			t.Errorf("destroy %s: %v", ref.ID, err)
		}
	})
	started := time.Now()
	if err := p.WaitReady(ctx, ref); err != nil {
		t.Fatal(err)
	}
	t.Logf("boat sandbox %s ready in %v", ref.ID, time.Since(started).Round(time.Millisecond))
	out, code, err := runExec(ctx, p, ref, provider.ExecSpec{Argv: []string{"uname", "-m"}})
	if err != nil || code != 0 {
		t.Fatalf("uname -m gave exit %d, %v", code, err)
	}
	machine := strings.TrimSpace(string(out))
	t.Logf("boat sandbox architecture: uname -m = %s, Ref.Meta[arch] = %s", machine,
		ref.Meta["arch"])
	arch := map[string]string{"x86_64": "amd64", "aarch64": "arm64"}[machine]
	if arch != ref.Meta["arch"] {
		t.Errorf("uname -m is %q but the ref says arch %q", machine, ref.Meta["arch"])
	}
}

const (
	liveHome = "/home/user"
	// liveShortTTL is the signed URL lifetime BOAT7 outlives in the middle of a transfer
	liveShortTTL = 2
	livePause    = 5 * time.Second
)

func TestLiveBoatFileRows(t *testing.T) {
	p := liveProvider(t)
	ref := livetest.Sandbox(t, p)
	dir := liveHome + "/bugnet-streams"
	t.Run("Contract", func(t *testing.T) { contract.Streams(t, p, ref, dir) })
	livetest.RoundTrips(t, p, ref, dir)
	t.Run("NotRegular", func(t *testing.T) { liveNotRegular(t, p, ref) })
	t.Run("FilesystemTypes", func(t *testing.T) { livetest.FilesystemTypes(t, p, ref, liveHome) })
	t.Run("BOAT7", func(t *testing.T) {
		t.Run("RangeOnDownloadURLs", func(t *testing.T) {
			liveRange(t, p, ref, dir+"/range.bin")
		})
		t.Run("DownloadURLPastExpiresAt", func(t *testing.T) {
			liveExpiredDownload(t, p, ref, dir+"/expiry-down.bin")
		})
		t.Run("UploadURLPastExpiresAt", func(t *testing.T) {
			liveExpiredUpload(t, p, ref, dir+"/expiry-up.bin")
		})
	})
}

// liveNotRegular checks a FIFO in /tmp, logging Boat's info for it, and /dev/urandom refused
func liveNotRegular(t *testing.T, p *Provider, ref provider.Ref) {
	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	fifo := "/tmp/bugnet-live-fifo"
	livetest.Run(ctx, t, p, ref, "mkfifo", fifo)
	params := &api.GetFileInfoParams{Path: fifo}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.GetFileInfo(ctx, ref.ID, params)
	})
	t.Logf("boat's file info for a FIFO answers %s, %v", raw, err)
	livetest.NotRegular(t, p, ref, fifo)
	if _, err := p.StatFile(ctx, ref, "/dev/urandom"); !isCode(err, provider.CodeInvalid) {
		t.Errorf("StatFile of /dev/urandom gave %v, want invalid before any call", err)
	}
}

// liveFile makes size random bytes at name inside the sandbox and returns their SHA-256
func liveFile(ctx context.Context, t *testing.T, p *Provider, ref provider.Ref, name string,
	size int,
) string {
	t.Helper()
	livetest.Run(ctx, t, p, ref, "sh", "-c",
		"mkdir -p \"$(dirname \"$1\")\" && head -c "+strconv.Itoa(size)+" /dev/urandom > \"$1\"",
		"sh", name)
	return livetest.SHA256(ctx, t, p, ref, name)
}

// liveSignedURL asks for a signed URL of ttl seconds, which the test never logs
func liveSignedURL(ctx context.Context, t *testing.T, p *Provider, ref provider.Ref,
	name string, ttl int, upload bool,
) string {
	t.Helper()
	req := api.FileUrlRequest{Path: name, TtlSeconds: new(ttl)}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		if upload {
			return p.api.CreateUploadUrl(ctx, ref.ID, req)
		}
		return p.api.CreateDownloadUrl(ctx, ref.ID, req)
	})
	if err != nil {
		t.Fatal(err)
	}
	var grant uploadReply
	if err := decode(raw, &grant); err != nil || grant.URL == "" {
		t.Fatal("the signed URL reply holds no URL")
	}
	return grant.URL
}

// liveRange asks a signed download URL for 100 bytes and logs whether Boat honours Range
func liveRange(t *testing.T, p *Provider, ref provider.Ref, name string) {
	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	liveFile(ctx, t, p, ref, name, 1<<20)
	r, err := p.OpenReader(ctx, ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	whole, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		liveSignedURL(ctx, t, p, ref, name, liveShortTTL*30, false), nil)
	if err != nil {
		t.Fatal("the signed download URL does not parse")
	}
	req.Header.Set("Range", "bytes=100-199")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := p.streams.Do(req)
	if err != nil {
		t.Fatalf("GET with Range: %v", redact(err))
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	t.Logf("a ranged GET of a signed URL answered %d, Accept-Ranges %q, Content-Range %q, "+
		"%d bytes", resp.StatusCode, resp.Header.Get("Accept-Ranges"),
		resp.Header.Get("Content-Range"), len(body))
	switch {
	case err != nil:
		t.Fatalf("read the ranged answer: %v", redact(err))
	case resp.StatusCode == http.StatusPartialContent:
		if !bytes.Equal(body, whole[100:200]) {
			t.Error("the 206 answer holds other bytes than 100 to 199")
		}
	case resp.StatusCode == http.StatusOK:
		t.Log("boat ignores Range on signed URLs, so reads from an offset skip bytes")
		if !bytes.Equal(body, whole) {
			t.Error("the 200 answer holds other bytes than the file")
		}
	default:
		t.Errorf("a ranged GET answered %d", resp.StatusCode)
	}
}

// liveExpiredDownload reads a signed URL of 2 s across a pause past its expiry
func liveExpiredDownload(t *testing.T, p *Provider, ref provider.Ref, name string) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	const size = 32 << 20
	want := liveFile(ctx, t, p, ref, name, size)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		liveSignedURL(ctx, t, p, ref, name, liveShortTTL, false), nil)
	if err != nil {
		t.Fatal("the signed download URL does not parse")
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := p.streams.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", redact(err))
	}
	defer func() { _ = resp.Body.Close() }()
	h := sha256.New()
	first, err := io.CopyN(h, resp.Body, 1<<20)
	if err != nil {
		t.Fatalf("the first MiB: %v", redact(err))
	}
	time.Sleep(livePause)
	rest, err := io.Copy(h, resp.Body)
	t.Logf("a download URL of %d s read %d bytes, paused %v, then %d more, %v", liveShortTTL,
		first, livePause, rest, err)
	if err != nil || first+rest != size || hex.EncodeToString(h.Sum(nil)) != want {
		t.Errorf("a download URL stopped working past its expiresAt mid-transfer, %d of %d "+
			"bytes", first+rest, size)
	}
}

// pausedReader stops once for pause after its first part
type pausedReader struct {
	r      io.Reader
	after  int64
	pause  time.Duration
	read   int64
	paused bool
}

func (p *pausedReader) Read(b []byte) (int, error) {
	if !p.paused && p.read >= p.after {
		time.Sleep(p.pause)
		p.paused = true
	}
	if !p.paused {
		b = b[:min(int64(len(b)), p.after-p.read)]
	}
	n, err := p.r.Read(b)
	p.read += int64(n)
	return n, err
}

// liveExpiredUpload PUTs to a signed URL of 2 s across a pause past its expiry
func liveExpiredUpload(t *testing.T, p *Provider, ref provider.Ref, name string) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	data := make([]byte, 4<<20)
	_, _ = rand.Read(data)
	body := &pausedReader{r: bytes.NewReader(data), after: 1 << 20, pause: livePause}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		liveSignedURL(ctx, t, p, ref, name, liveShortTTL, true), body)
	if err != nil {
		t.Fatal("the signed upload URL does not parse")
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := p.streams.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", redact(err))
	}
	reply, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	_ = resp.Body.Close()
	t.Logf("an upload URL of %d s paused %v after 1 MiB answered %d", liveShortTTL, livePause,
		resp.StatusCode)
	sum := sha256.Sum256(data)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("an upload URL stopped working past its expiresAt mid-transfer, %d %q",
			resp.StatusCode, reply)
	}
	if got := livetest.SHA256(ctx, t, p, ref, name); got != hex.EncodeToString(sum[:]) {
		t.Errorf("the uploaded file's SHA-256 is %s, want %x", got, sum)
	}
}
