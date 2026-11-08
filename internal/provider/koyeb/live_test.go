package koyeb

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/contract"
	"github.com/bugnet-sh/bugnet-router/internal/provider/livetest"
)

const liveTimeout = 5 * time.Minute

// liveProvider talks to the real Koyeb with the config/dev.toml settings, skipping without a token
func liveProvider(t *testing.T) *Provider {
	t.Helper()
	token := os.Getenv("KOYEB_API_TOKEN")
	if token == "" {
		t.Skip("KOYEB_API_TOKEN is not set")
	}
	settings := livetest.Settings(t)
	cfg := settings.Koyeb
	p, err := New(Options{
		APIToken: token, Region: cfg.Region, InstanceTypeBySize: cfg.InstanceTypes,
		PartBytes: settings.Transfers.KoyebPart, BufferTotal: settings.Transfers.KoyebBufferTotal,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestKoyebLiveContract(t *testing.T) {
	p := liveProvider(t)
	contract.Run(t, func(*testing.T) provider.Provider { return p })
}

func TestKoyebLiveArch(t *testing.T) {
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
	t.Logf("koyeb sandbox %s ready in %v at %s", ref.ID, time.Since(started).Round(time.Second),
		ref.Meta["executor_url"])
	out, code, err := runExec(ctx, p, ref, provider.ExecSpec{Argv: []string{"uname", "-m"}})
	if err != nil || code != 0 {
		t.Fatalf("uname -m gave exit %d, %v", code, err)
	}
	machine := strings.TrimSpace(string(out))
	t.Logf("koyeb sandbox architecture: uname -m = %s, Ref.Meta[arch] = %s", machine,
		ref.Meta["arch"])
	arch := map[string]string{"x86_64": "amd64", "aarch64": "arm64"}[machine]
	if arch != ref.Meta["arch"] {
		t.Errorf("uname -m is %q but the ref says arch %q", machine, ref.Meta["arch"])
	}
}

const (
	liveHome = "/root"
	// liveGiB is the download KOYEB7 reads through Koyeb's cuts
	liveGiB = 1 << 30
	// liveSlowRate is the upload speed KOYEB7 sends parts at, 0.3 MB/s
	liveSlowRate = 300_000
)

func TestLiveKoyebFileRows(t *testing.T) {
	p := liveProvider(t)
	ref := livetest.Sandbox(t, p)
	dir := liveHome + "/bugnet-streams"
	t.Run("Contract", func(t *testing.T) { contract.Streams(t, p, ref, dir) })
	livetest.RoundTrips(t, p, ref, dir)
	t.Run("DevUrandom", func(t *testing.T) { livetest.NotRegular(t, p, ref, "/dev/urandom") })
	t.Run("FilesystemTypes", func(t *testing.T) { livetest.FilesystemTypes(t, p, ref, liveHome) })
	t.Run("KOYEB7", func(t *testing.T) {
		t.Run("FilesRoutes", func(t *testing.T) { liveFilesRoutes(t, p, ref, dir) })
		t.Run("GiBDownloadThroughReissues", func(t *testing.T) {
			liveGiBDownload(t, p, ref, dir+"/gib.bin")
		})
		t.Run("SlowPartsDrawNo524", func(t *testing.T) {
			liveSlowParts(t, p, ref, dir+"/slow.bin", livetest.Settings(t).Transfers.KoyebPart)
		})
	})
}

// liveFilesRoutes checks the executor serves GET /files/stat and the upload sessions
func liveFilesRoutes(t *testing.T, p *Provider, ref provider.Ref, dir string) {
	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	tg, err := p.target(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	root, err := p.statFollow(ctx, ref, tg, "/")
	if err != nil || !root.IsDir {
		t.Fatalf("GET /files/stat of / gave %+v, %v", root, err)
	}
	u := &partUpload{
		p: p, ctx: ctx, cancel: func() {}, ref: ref, t: tg, path: dir + "/probe",
		spec: provider.WriteSpec{Size: 1, Mode: 0o644}, part: 1,
	}
	if err := u.open(ctx); err != nil {
		t.Fatalf("POST /files/uploads: %v", err)
	}
	if err := u.Discard(ctx); err != nil {
		t.Fatalf("DELETE /files/uploads/{id}: %v", err)
	}
}

// countingTransport counts the GETs of /files that pass through it
type countingTransport struct {
	base http.RoundTripper
	gets atomic.Int64
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/files") {
		c.gets.Add(1)
	}
	return c.base.RoundTrip(r)
}

// liveGiBDownload reads a GiB made in the sandbox, counting the GETs Koyeb's cuts take
func liveGiBDownload(t *testing.T, p *Provider, ref provider.Ref, name string) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	livetest.Run(ctx, t, p, ref, "sh", "-c",
		"head -c "+strconv.Itoa(liveGiB)+" /dev/urandom > "+name)
	want := livetest.SHA256(ctx, t, p, ref, name)
	counter := &countingTransport{base: p.streams.Transport}
	p.streams.Transport = counter
	defer func() { p.streams.Transport = counter.base }()

	begin := time.Now()
	r, err := p.OpenReader(ctx, ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	n, err := io.Copy(h, r)
	_ = r.Close()
	took := time.Since(begin)
	t.Logf("1 GiB read %d bytes in %v (%s) over %d GETs", n, took.Round(time.Second),
		livetest.Rate(n, took), counter.gets.Load())
	if err != nil || n != liveGiB {
		t.Fatalf("read %d of %d bytes, %v", n, liveGiB, err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		t.Fatalf("read bytes whose SHA-256 is %s, want %s", got, want)
	}
}

// slowReader hands out its bytes at perSecond, as a slow uplink would send them
type slowReader struct {
	r         io.Reader
	perSecond int
	start     time.Time
	sent      int
}

func (s *slowReader) Read(b []byte) (int, error) {
	if s.start.IsZero() {
		s.start = time.Now()
	}
	due := s.start.Add(time.Duration(float64(s.sent) / float64(s.perSecond) * float64(time.Second)))
	time.Sleep(time.Until(due))
	n, err := s.r.Read(b[:min(len(b), s.perSecond/10)])
	s.sent += n
	return n, err
}

// liveSlowParts sends two parts of part bytes at 0.3 MB/s, each of which must answer 204
func liveSlowParts(t *testing.T, p *Provider, ref provider.Ref, name string, part int64) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	tg, err := p.target(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	u := &partUpload{
		p: p, ctx: ctx, cancel: func() {}, ref: ref, t: tg, path: name,
		spec: provider.WriteSpec{Size: 2 * part, Mode: 0o644}, part: part,
	}
	if err := u.open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = u.Discard(context.WithoutCancel(ctx)) }()
	whole := sha256.New()
	for i := range int64(2) {
		data := make([]byte, part)
		_, _ = rand.Read(data)
		whole.Write(data)
		sum := sha256.Sum256(data)
		q := url.Values{"offset": {strconv.FormatInt(i*part, 10)}}
		req, err := tg.request(ctx, http.MethodPut, "/files/uploads/"+url.PathEscape(u.id), q,
			&slowReader{r: bytes.NewReader(data), perSecond: liveSlowRate})
		if err != nil {
			t.Fatal(err)
		}
		req.ContentLength = part
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Content-SHA256", hex.EncodeToString(sum[:]))
		begin := time.Now()
		resp, err := p.streams.Do(req)
		took := time.Since(begin)
		if err != nil {
			t.Fatalf("part %d of %d bytes at 0.3 MB/s failed after %v: %v", i, part,
				took.Round(time.Second), err)
		}
		reply, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
		_ = resp.Body.Close()
		t.Logf("part %d of %d bytes at 0.3 MB/s answered %d in %v", i, part, resp.StatusCode,
			took.Round(time.Second))
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("part %d answered %d %q, transfers.koyeb_part stays only if no part draws "+
				"a 524", i, resp.StatusCode, reply)
		}
	}
	var done struct {
		Size int64 `json:"size"`
	}
	route := "/files/uploads/" + url.PathEscape(u.id) + "/complete"
	if err := u.call(ctx, http.MethodPost, route, nil, &done); err != nil {
		t.Fatal(err)
	}
	u.id = ""
	if got := livetest.SHA256(ctx, t, p, ref, name); got != hex.EncodeToString(whole.Sum(nil)) {
		t.Fatalf("the completed file's SHA-256 is %s, want %x", got, whole.Sum(nil))
	}
}
