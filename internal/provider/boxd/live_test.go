package boxd

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"os"
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
