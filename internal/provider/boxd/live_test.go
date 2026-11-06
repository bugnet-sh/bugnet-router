package boxd

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/boxd/boxdpb"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
	"github.com/oynozan/bugnet-router/internal/provider/livetest"
)

const (
	liveTimeout = 3 * time.Minute
	// liveProxyWatch is how long the default proxy is asked, auto-detect needs a moment to react
	liveProxyWatch = 45 * time.Second
)

// liveProvider talks to the real boxd with the config/dev.toml settings, skipping without a key
func liveProvider(t *testing.T) *Provider {
	t.Helper()
	key := os.Getenv("BOXD_API_KEY")
	if key == "" {
		t.Skip("BOXD_API_KEY is not set")
	}
	cfg := livetest.Settings(t).Boxd
	p, err := New(Options{APIKey: key, Region: cfg.Region, Endpoint: cfg.Endpoint})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// liveMachine creates a ready machine under a fresh label and destroys it when the test ends
func liveMachine(ctx context.Context, t *testing.T, p *Provider) provider.Ref {
	t.Helper()
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	labels := map[string]string{"bugnet-live": hex.EncodeToString(b)}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), liveTimeout)
		defer cancel()
		owned, err := p.ListOwned(ctx, labels)
		if err != nil {
			t.Errorf("list machines left by the live run: %v", err)
		}
		for _, s := range owned {
			if err := p.Destroy(ctx, s.Ref); err != nil {
				t.Errorf("destroy %s: %v", s.Ref.ID, err)
			}
		}
	})
	size, _ := p.SizeFor(provider.Resources{})
	ref, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: labels["bugnet-live"], Size: size, Labels: labels,
		Lifetime: 15 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.WaitReady(ctx, ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestBoxdLiveContract(t *testing.T) {
	p := liveProvider(t)
	contract.Run(t, func(*testing.T) provider.Provider { return p })
}

func TestBoxdLiveArch(t *testing.T) {
	p := liveProvider(t)
	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	ref := liveMachine(ctx, t, p)
	out, code, err := runExec(ctx, p, ref, provider.ExecSpec{Argv: []string{"uname", "-m"}})
	if err != nil || code != 0 {
		t.Fatalf("uname -m gave exit %d, %v", code, err)
	}
	machine := strings.TrimSpace(string(out))
	t.Logf("boxd machine architecture: uname -m = %s, Ref.Meta[arch] = %s", machine,
		ref.Meta["arch"])
	arch := map[string]string{"x86_64": "amd64", "aarch64": "arm64"}[machine]
	if arch != ref.Meta["arch"] {
		t.Errorf("uname -m is %q but the ref says arch %q", machine, ref.Meta["arch"])
	}
}

func TestLiveBoxdDefaultProxyIsClosed(t *testing.T) {
	p := liveProvider(t)
	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	ref := liveMachine(ctx, t, p)
	vm, err := p.api.GetVm(ctx, &boxdpb.GetVmRequest{VmId: ref.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ssh_disabled %v, auto hibernate %d s, hibernate_off %q, bot protection %q",
		vm.GetSshDisabled(), vm.GetAutoHibernateTimeoutSecs(), ref.Meta[hibernateOffMeta],
		vm.GetBotProtectionMode())
	if !vm.GetSshDisabled() {
		t.Error("boxd reports SSH on for a machine Create returned")
	}
	if ref.Meta[hibernateOffMeta] != "1" || vm.GetAutoHibernateTimeoutSecs() != 0 {
		t.Errorf("hibernate_off %q with auto hibernate %d s, want 1 and 0 from Create",
			ref.Meta[hibernateOffMeta], vm.GetAutoHibernateTimeoutSecs())
	}
	proxies, err := p.api.ListProxies(ctx, &boxdpb.ListProxiesRequest{Vm: ref.ID})
	if err != nil {
		t.Fatal(err)
	}
	pinned := false
	for _, px := range proxies.GetProxies() {
		t.Logf("proxy %s default %v mode %q effective port %d", px.GetName(), px.GetIsDefault(),
			px.GetPortMode(), px.GetEffectivePort())
		pinned = pinned || px.GetIsDefault() && px.GetPortMode() == "locked" &&
			px.GetEffectivePort() == provider.DefaultGatePort
	}
	if !pinned {
		t.Errorf("no default proxy locked to port %d", provider.DefaultGatePort)
	}

	// A bot challenge would hide an open proxy, so the check runs with it off
	_, err = p.api.SetBotProtection(ctx, &boxdpb.SetBotProtectionRequest{
		VmId: ref.ID, Mode: "off",
	})
	if err != nil {
		t.Fatalf("turn bot protection off: %v", err)
	}
	serve := "setsid nohup python3 -m http.server 8000 >/dev/null 2>&1 </dev/null &"
	if _, code, err := runExec(ctx, p, ref, provider.ExecSpec{
		Argv: []string{"sh", "-c", serve},
	}); err != nil || code != 0 {
		t.Fatalf("start python3 -m http.server 8000 gave exit %d, %v", code, err)
	}
	probe := "import urllib.request as u; u.urlopen('http://127.0.0.1:8000/', timeout=2)"
	for attempt := 0; ; attempt++ {
		_, code, err := runExec(ctx, p, ref, provider.ExecSpec{
			Argv: []string{"python3", "-c", probe},
		})
		if err == nil && code == 0 {
			break
		}
		if attempt == 20 {
			t.Fatalf("the app never listened on port 8000, last exit %d, %v", code, err)
		}
		time.Sleep(time.Second)
	}

	url := "https://" + vm.GetName() + "." + cmp.Or(vm.GetAccessDomain(), homeHost) + "/"
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: provider.NoRedirect}
	var seen []string
	for end := time.Now().Add(liveProxyWatch); time.Now().Before(end); time.Sleep(3 * time.Second) {
		code, body, err := liveGet(ctx, client, url)
		if err != nil {
			seen = append(seen, "error")
			t.Logf("GET %s: %v", url, err)
			continue
		}
		seen = append(seen, strconv.Itoa(code))
		if code == http.StatusOK && strings.Contains(body, "Directory listing for /") {
			t.Fatalf("the default proxy serves the app on port 8000 at %s", url)
		}
	}
	t.Logf("the default proxy answered %v", seen)
	if !slices.ContainsFunc(seen, func(s string) bool { return s != "error" }) {
		t.Errorf("the default proxy at %s never answered, so the test proves nothing", url)
	}
}

// liveGet fetches url and returns the status and at most 64 KiB of the body
func liveGet(ctx context.Context, client *http.Client, url string) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return resp.StatusCode, string(body), err
}

// liveHome is the exec user's home on boxd
const liveHome = "/home/boxd"

func TestLiveBoxdFileRows(t *testing.T) {
	p := liveProvider(t)
	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	ref := liveMachine(ctx, t, p)
	cancel()
	dir := liveHome + "/bugnet-streams"
	t.Run("Contract", func(t *testing.T) { contract.Streams(t, p, ref, dir) })
	livetest.RoundTrips(t, p, ref, dir)
	t.Run("DevUrandom", func(t *testing.T) { livetest.NotRegular(t, p, ref, "/dev/urandom") })
	t.Run("FilesystemTypes", func(t *testing.T) { livetest.FilesystemTypes(t, p, ref, liveHome) })
	t.Run("BOXD5", func(t *testing.T) {
		t.Run("CancelledUploadKeepsTheOldFile", func(t *testing.T) {
			liveCancelledUpload(t, p, ref, dir+"/cancelled")
		})
		t.Run("AcceptsMiBChunks", func(t *testing.T) {
			liveOneWrite(t, p, ref, dir+"/chunks", 4<<20)
		})
		t.Run("RenameReplacesAHardLinkedTarget", func(t *testing.T) {
			liveReplace(t, p, ref, dir, "linked", "printf old > linked && ln -f linked other")
			ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
			defer cancel()
			if got := livetest.Run(ctx, t, p, ref, "cat", dir+"/other"); got != "old" {
				t.Errorf("the other hard link holds %q, want the old bytes it kept", got)
			}
		})
		t.Run("RenameReplacesARootOwnedTarget", func(t *testing.T) {
			liveReplace(t, p, ref, dir, "rooted", "sudo -n sh -c "+
				"'printf old > rooted && chown root:root rooted && chmod 0644 rooted'")
			ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
			defer cancel()
			owner := livetest.Run(ctx, t, p, ref, "stat", "-c", "%U", dir+"/rooted")
			t.Logf("after the rename the file belongs to %s", strings.TrimSpace(owner))
		})
	})
}

// liveCancelledUpload ends a writer's context halfway and checks the old file and no temp stay
func liveCancelledUpload(t *testing.T, p *Provider, ref provider.Ref, name string) {
	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	old := make([]byte, 1<<20)
	_, _ = rand.Read(old)
	w, err := p.OpenWriter(ctx, ref, name, provider.WriteSpec{Size: int64(len(old)), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(old); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(ctx, nil); err != nil {
		t.Fatal(err)
	}
	before := livetest.SHA256(ctx, t, p, ref, name)

	wctx, stop := context.WithCancel(ctx)
	w, err = p.OpenWriter(wctx, ref, name, provider.WriteSpec{Size: 2 << 20, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(make([]byte, 1<<20)); err != nil {
		t.Fatal(err)
	}
	stop()
	if _, err := w.Commit(ctx, nil); err == nil {
		t.Fatal("a writer whose context ended committed")
	}
	if after := livetest.SHA256(ctx, t, p, ref, name); after != before {
		t.Errorf("the target changed from %s to %s after a cancelled upload", before, after)
	}
	liveNoTemp(ctx, t, p, ref, path.Dir(name))
}

// liveNoTemp waits a while for every temp file in dir to go and fails if one stays
func liveNoTemp(ctx context.Context, t *testing.T, p *Provider, ref provider.Ref, dir string) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(time.Second) {
		listing := provider.NewListing(1000)
		if err := p.ListFiles(ctx, ref, dir, 1, listing); err != nil {
			t.Fatal(err)
		}
		var temps []string
		for _, e := range listing.Entries {
			if base := path.Base(e.Path); strings.Contains(base, ".bugnet-") &&
				strings.HasSuffix(base, ".part") {
				temps = append(temps, base)
			}
		}
		if len(temps) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("temp files left in %s: %v", dir, temps)
		}
	}
}

// liveOneWrite sends size bytes in one Write, which the adapter splits into 1 MiB chunks
func liveOneWrite(t *testing.T, p *Provider, ref provider.Ref, name string, size int) {
	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	data := make([]byte, size)
	_, _ = rand.Read(data)
	w, err := p.OpenWriter(ctx, ref, name, provider.WriteSpec{Size: int64(size), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("a write boxd took in 1 MiB chunks failed: %v", err)
	}
	sum := sha256.Sum256(data)
	if _, err := w.Commit(ctx, sum[:]); err != nil {
		t.Fatal(err)
	}
	if got := livetest.SHA256(ctx, t, p, ref, name); got != hex.EncodeToString(sum[:]) {
		t.Errorf("sha256sum gave %s, want %x", got, sum)
	}
}

// liveReplace makes the target with setup, then streams new bytes over it through FsRename
func liveReplace(t *testing.T, p *Provider, ref provider.Ref, dir, base, setup string) {
	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	livetest.Run(ctx, t, p, ref, "mkdir", "-p", dir)
	livetest.Run(ctx, t, p, ref, "sh", "-c", "cd "+dir+" && "+setup)
	name := dir + "/" + base
	w, err := p.OpenWriter(ctx, ref, name, provider.WriteSpec{Size: 3, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if got := livetest.Run(ctx, t, p, ref, "cat", name); got != "old" {
		t.Errorf("before Commit %s holds %q, want the old bytes", name, got)
	}
	if _, err := w.Commit(ctx, nil); err != nil {
		t.Fatalf("Commit over %s: %v", name, err)
	}
	if got := livetest.Run(ctx, t, p, ref, "cat", name); got != "new" {
		t.Errorf("after Commit %s holds %q, want the new bytes", name, got)
	}
}
