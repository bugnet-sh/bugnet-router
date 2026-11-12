package providers_test

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// fakeNet records the calls of the local server that stands in for every host
type fakeNet struct {
	mu   sync.Mutex
	seen []string
}

// calls are the requests answered so far, each as method, host, path and whether it held a key
func (f *fakeNet) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.seen)
}

// swapNet answers every call with answer on a local server until the test ends, recording each
func swapNet(t *testing.T, answer http.HandlerFunc) *fakeNet {
	t.Helper()
	f := &fakeNet{}
	testenv.LocalInternet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Authorization") != "" || r.Header.Get("X-API-Key") != ""
		f.mu.Lock()
		f.seen = append(f.seen, fmt.Sprintf("%s %s%s key=%t", r.Method, r.Host, r.URL.Path, key))
		f.mu.Unlock()
		answer(w, r)
	}))
	return f
}

// logLines collects the default logger's output from every goroutine until the test ends
type logLines struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logLines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logLines) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func captureLogs(t *testing.T) *logLines {
	t.Helper()
	l := &logLines{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(l, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return l
}

// closeOnCleanup stops the background reads p started once the test ends
func closeOnCleanup(t *testing.T, p provider.Provider) {
	t.Helper()
	t.Cleanup(func() { _ = p.(io.Closer).Close() })
}

// seeds stands in for the 32-byte seeds the environment holds
func seeds(names ...string) map[string][]byte {
	out := map[string][]byte{}
	for i, name := range names {
		out[name] = bytes.Repeat([]byte{byte('a' + i)}, 32)
	}
	return out
}

// liumSettings lists lium with one RTX 3090 size at a $0.35 cap
const liumSettings = `providers = ["lium"]
[limits_poll]
account_every = "10m"
[ssh]
providers = ["lium"]
[lium]
max_lifetime = "90m"
start_billed = "6m"
max_concurrent = 3
[[lium.sizes]]
size = "lium-rtx3090"
model = "NVIDIA RTX 3090"
match = ["NVIDIA GeForce RTX 3090"]
gpus = 1
gpu_memory_gib = 24
vcpu = 8
memory_mib = 28672
disk_gib = 200
max_hourly_micros = 350000
`

// liumAnswers is a Lium account with 10 cents and no pods, and a node listing with no node
func liumAnswers(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Host != "lium.io":
		http.Error(w, "not lium", http.StatusTeapot)
	case r.URL.Path == "/api/users/me":
		_, _ = io.WriteString(w, `{"balance": 0.1, "balance_warning": null}`)
	default:
		_, _ = io.WriteString(w, `[]`)
	}
}

func TestBuildLium(t *testing.T) {
	web := swapNet(t, liumAnswers)
	cfg := load(t, liumSettings, map[string]string{"lium": "lium_test"})
	cfg.ProviderSeeds = seeds("lium")
	reg, err := providers.Build(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := reg.Get("lium")
	if !ok {
		t.Fatalf("registry %v lacks lium", reg.Ordered())
	}
	closeOnCleanup(t, p)
	caps := p.Capabilities()
	if !caps.GPU || !caps.NeedsRunnerUpload || caps.MaxLifetime != 90*time.Minute ||
		!reg.Capabilities(p).SSH || !reg.NativeSSH(p) {
		t.Fatalf("lium capabilities %+v, ssh %v and native %v", caps, reg.Capabilities(p).SSH,
			reg.NativeSSH(p))
	}
	sizer, _ := provider.As[provider.Sizer](p)
	sizes := sizer.Sizes()
	if len(sizes) != 1 || sizes[0].Name != "lium-rtx3090" ||
		sizes[0].Resources.GPUModel != gpu.RTX3090 {
		t.Fatalf("lium sizes %+v, want lium-rtx3090", sizes)
	}
	sc, _ := provider.As[provider.StartCoster](p)
	start, ok := sc.StartCostMicros(sizes[0])
	if p.HourlyCostMicros(sizes[0]) != 350_000 || !ok || start != 35_000 {
		t.Fatalf("lium costs %d an hour and %d, %v to start, want the cap and 6 minutes of it",
			p.HourlyCostMicros(sizes[0]), start, ok)
	}
	// 3 slots at the $0.35 cap for account_every and a rent's 15 minutes is $0.4375
	checker, _ := provider.As[provider.AccountChecker](p)
	acc, err := checker.CheckAccount(t.Context())
	if err != nil || acc.Block != provider.BlockUnfunded ||
		!strings.Contains(acc.Detail, "under the headroom $0.44") {
		t.Fatalf("account %+v, %v, want unfunded under the headroom of 3 slots and 10 minutes",
			acc, err)
	}
	for _, call := range web.calls() {
		keyed := strings.HasPrefix(call, "GET lium.io/api/users/me ") ||
			strings.HasPrefix(call, "GET lium.io/api/pods ")
		if !strings.HasPrefix(call, "GET lium.io/api/") || strings.HasSuffix(call, "key=true") !=
			keyed {
			t.Errorf("call %q, want Lium's API with the key only on the account's routes", call)
		}
	}
}

func TestBuildLiumRefusesBadConfig(t *testing.T) {
	swapNet(t, liumAnswers)
	for name, c := range map[string]struct {
		settings string
		keys     map[string]string
		seeds    map[string][]byte
		want     string
	}{
		"no key":  {liumSettings, nil, seeds("lium"), "LIUM_API_KEY"},
		"no seed": {liumSettings, map[string]string{"lium": "lium_test"}, nil, "LIUM_SSH_SEED"},
		"a match of another model": {
			strings.Replace(liumSettings, `["NVIDIA GeForce RTX 3090"]`,
				`["NVIDIA GeForce RTX 4090"]`, 1),
			map[string]string{"lium": "lium_test"}, seeds("lium"), "lium: size",
		},
	} {
		cfg := load(t, c.settings, c.keys)
		cfg.ProviderSeeds = c.seeds
		reg, err := providers.Build(cfg, nil, nil)
		if err == nil {
			p, _ := reg.Get("lium")
			closeOnCleanup(t, p)
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: build gave %v, want an error naming %s", name, err, c.want)
		}
	}
}

// hfSettings lists hf selling t4-small alone
const hfSettings = `providers = ["hf"]
[hf]
namespace = "bugnet-test"
flavors = ["t4-small"]
max_lifetime = "90m"
`

// hfHardware answers HF's hardware list with t4-small at 2 cents a minute, three times the table's
func hfHardware(w http.ResponseWriter, r *http.Request) {
	if r.Host != "huggingface.co" || r.URL.Path != "/api/jobs/hardware" {
		http.Error(w, "not the hardware list", http.StatusTeapot)
		return
	}
	_, _ = io.WriteString(w, `[{"name": "cpu-basic", "cpu": "2 vCPU", "ram": "16 GB",
		"ephemeralStorage": "50 GB", "accelerator": null, "unitCostMicroUSD": 167,
		"unitLabel": "minute"},
		{"name": "t4-small", "cpu": "4 vCPU", "ram": "15 GB", "ephemeralStorage": "50 GB",
		"accelerator": {"type": "gpu", "model": "T4", "quantity": "1", "vram": "16 GB"},
		"unitCostMicroUSD": 20000, "unitLabel": "minute"}]`)
}

// t4Package is a 10-minute T4 package, which t4-small covers
func t4Package(price int64) catalog.Package {
	return catalog.Package{ID: "gpu-t4-10m", Minutes: 10, PriceMicros: price, GPUModel: gpu.T4,
		Resources: provider.Resources{
			VCPU: 3, MemoryMiB: 12 << 10, DiskGiB: 40, GPUCount: 1, GPUMemoryGiB: 16,
			GPUModel: gpu.T4,
		}}
}

// buildHFRead builds hf selling pkgs, then waits for its first hardware read and stops the reads
func buildHFRead(t *testing.T, pkgs []catalog.Package) (provider.Provider, *fakeNet) {
	t.Helper()
	web := swapNet(t, hfHardware)
	cfg := load(t, hfSettings, map[string]string{"hf": "hf_test"})
	cfg.ProviderSeeds = seeds("hf")
	reg, err := providers.Build(cfg, nil, nil, providers.WithPackages(pkgs))
	if err != nil {
		t.Fatal(err)
	}
	p, ok := reg.Get("hf")
	if !ok {
		t.Fatalf("registry %v lacks hf", reg.Ordered())
	}
	closeOnCleanup(t, p)
	size := provider.Size{Name: "t4-small"}
	for end := time.Now().Add(10 * time.Second); p.HourlyCostMicros(size) != 1_210_000; {
		if time.Now().After(end) {
			t.Fatalf("t4-small costs %d an hour, the hardware list was never read",
				p.HourlyCostMicros(size))
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Close returns once the read and the margin check after it have ended
	_ = p.(io.Closer).Close()
	return p, web
}

func TestBuildHF(t *testing.T) {
	logs := captureLogs(t)
	// The table's $0.41 an hour costs 88,838 for 10 minutes and 3 start minutes, now 262,167
	p, web := buildHFRead(t, []catalog.Package{t4Package(180_000)})
	caps := p.Capabilities()
	if !caps.GPU || !caps.NeedsRunnerUpload || !caps.LossyExecText ||
		caps.MaxLifetime != 90*time.Minute {
		t.Fatalf("hf capabilities %+v", caps)
	}
	size, ok := p.SizeFor(t4Package(0).Resources)
	if !ok || size.Name != "t4-small" || p.BillingIncrement() != time.Minute {
		t.Fatalf("size for a T4 package %+v, %v, increment %v", size, ok, p.BillingIncrement())
	}
	if _, ok := p.SizeFor(provider.Resources{GPUCount: 1, GPUModel: gpu.L4}); ok {
		t.Errorf("hf sold an L4 while hf.flavors names t4-small alone")
	}
	if got := web.calls(); !slices.Equal(got, []string{
		"GET huggingface.co/api/jobs/hardware key=false",
	}) {
		t.Errorf("calls %q, want one keyless read of the hardware list", got)
	}
	got := logs.String()
	if !strings.Contains(got, "level=ERROR") || !strings.Contains(got, "margin_floor") ||
		!strings.Contains(got, `\"gpu-t4-10m\"`) {
		t.Errorf("log %q, want an error naming the package the new price puts under margin", got)
	}
	if strings.Contains(got, "hf_test") {
		t.Errorf("log %q holds the Jobs token", got)
	}
}

func TestBuildHFLogsNoMarginKept(t *testing.T) {
	logs := captureLogs(t)
	buildHFRead(t, []catalog.Package{t4Package(600_000)})
	if got := logs.String(); strings.Contains(got, "level=ERROR") {
		t.Errorf("log %q, want no error while the package keeps its margin", got)
	}
}

func TestBuildHFRefusesBadConfig(t *testing.T) {
	swapNet(t, hfHardware)
	keys := map[string]string{"hf": "hf_test"}
	for name, c := range map[string]struct {
		settings string
		keys     map[string]string
		seeds    map[string][]byte
		want     string
	}{
		"no token": {hfSettings, nil, seeds("hf"), "HF_JOBS_TOKEN"},
		"no seed":  {hfSettings, keys, nil, "HF_SANDBOX_SEED"},
		// The adapter fetches sbx-server at each boot, no image holds it yet
		"baked": {hfSettings + "sbx_server = \"baked\"\n", keys, seeds("hf"), "hf.sbx_server"},
		"a namespace hf refuses": {
			strings.Replace(hfSettings, "bugnet-test", "bugnet..test", 1), keys, seeds("hf"),
			"hf: the namespace",
		},
	} {
		cfg := load(t, c.settings, c.keys)
		cfg.ProviderSeeds = c.seeds
		reg, err := providers.Build(cfg, nil, nil)
		if err == nil {
			p, _ := reg.Get("hf")
			closeOnCleanup(t, p)
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: build gave %v, want an error naming %s", name, err, c.want)
		}
	}
}
