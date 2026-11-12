package providers

import (
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd"
	"github.com/bugnet-sh/bugnet-router/internal/provider/hf"
	"github.com/bugnet-sh/bugnet-router/internal/provider/koyeb"
	"github.com/bugnet-sh/bugnet-router/internal/provider/lium"
)

func TestKoyebOptionsCarryTheTransferParts(t *testing.T) {
	var cfg config.Config
	cfg.Koyeb.Region = "par"
	cfg.Koyeb.InstanceTypes = map[string]string{"cpu": "large"}
	cfg.Transfers.KoyebPart = 4 << 20
	cfg.Transfers.KoyebBufferTotal = 64 << 20
	cfg.Koyeb.PoolIdleHourlyMicros = 120
	got := koyebOptions(cfg, "koyeb_test")
	want := koyeb.Options{
		APIToken: "koyeb_test", Region: "par", InstanceTypeBySize: cfg.Koyeb.InstanceTypes,
		PartBytes: 4 << 20, BufferTotal: 64 << 20, PoolIdleHourlyMicros: 120,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("koyeb options %+v, want %+v", got, want)
	}
}

func TestBoxdOptionsCarryTheGateSettings(t *testing.T) {
	var cfg config.Config
	cfg.Boxd = config.Boxd{Region: "nyc", Endpoint: "boxd.example:9443", GateRoute: "default"}
	cfg.Previews.GatePort = 29418
	got := boxdOptions(cfg, "boxd_test")
	want := boxd.Options{
		APIKey: "boxd_test", Endpoint: "boxd.example:9443", Region: "nyc", GatePort: 29418,
		GateRoute: "default",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("boxd options %+v, want %+v", got, want)
	}
}

func TestBuildKoyebNamesTheSettingItRefuses(t *testing.T) {
	var cfg config.Config
	cfg.Transfers.KoyebPart = 200 << 20
	cfg.Transfers.KoyebBufferTotal = 1 << 30
	_, err := buildKoyeb(cfg, "koyeb_test")
	if err == nil || !strings.Contains(err.Error(), "transfers.koyeb_part") ||
		strings.Contains(err.Error(), "instance_types") {
		t.Fatalf("a part over 100 MiB gave %v, want it to name transfers.koyeb_part", err)
	}
	cfg = config.Config{}
	cfg.Koyeb.InstanceTypes = map[string]string{"cpu": "nano"}
	_, err = buildKoyeb(cfg, "koyeb_test")
	if err == nil || !strings.Contains(err.Error(), "koyeb.instance_types") {
		t.Fatalf("an unknown instance type gave %v, want it to name koyeb.instance_types", err)
	}
}

func TestE2BOptionsCarryTheDenyRanges(t *testing.T) {
	var cfg config.Config
	cfg.E2B.Domain = "e2b-juliett.dev"
	cfg.Hardening.DenyRanges = []string{"10.0.0.0/8", "fe80::/10"}
	got := e2bOptions(cfg, "e2b_test")
	want := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fe80::/10")}
	if !reflect.DeepEqual(got.DenyOut, want) || got.Domain != "e2b-juliett.dev" ||
		got.APIKey != "e2b_test" {
		t.Fatalf("e2b options %+v, want the deny ranges, domain and key", got)
	}
}

func TestImageArtifactsReachEachAdapter(t *testing.T) {
	var cfg config.Config
	digest := "ghcr.io/bugnet-sh/base@sha256:" + strings.Repeat("a", 64)
	cfg.Images.Koyeb = config.ProviderImage{Version: "2026.11.1", Ref: digest}
	cfg.Images.Boxd = config.ProviderImage{
		Version: "2026.11.1", Refs: map[string]string{"2x8": "bugnet-dev-2026-11-1-2x8"},
	}
	if got := koyebOptions(cfg, "k").Image; got != digest {
		t.Errorf("koyeb image %q, want the digest", got)
	}
	if got := boxdOptions(cfg, "b").Snapshots["2x8"]; got != "bugnet-dev-2026-11-1-2x8" {
		t.Errorf("boxd snapshots %v", boxdOptions(cfg, "b").Snapshots)
	}
	p, err := buildBoat(config.Boat{}, "boat_test", nil, "", 0, "bugnet-dev-2026-11-1")
	if err != nil || p == nil {
		t.Fatalf("buildBoat gave %v", err)
	}
}

func TestLiumOptionsCarryEverySetting(t *testing.T) {
	var cfg config.Config
	cfg.Lium = config.Lium{
		Countries: []string{"DE", "NL"}, OtherCountries: true, BlockedCountries: []string{"RU"},
		MinDownloadMbps: 500, MaxLifetime: 3 * time.Hour, StartBilled: 8 * time.Minute,
		TemplateVolume: "/root", Termination: "lifetime", OpsPerSecond: 4,
		Sizes: []config.LiumSize{{
			Size: "lium-rtx3090", Model: gpu.RTX3090, Match: []string{"NVIDIA GeForce RTX 3090"},
			GPUs: 1, GPUMemoryGiB: 24, VCPU: 8, MemoryMiB: 28672, DiskGiB: 200,
			MaxHourlyMicros: 350_000,
		}},
	}
	// The account buckets are the users' operations, never the API pace
	cfg.ProviderOps = map[string]int{"lium": 40, "hf": 30}
	cfg.CreatesPerSecond = map[string]int{"lium": 2, "hf": 1}
	cfg.MaxConcurrent = map[string]int{"lium": 3, "hf": 7}
	cfg.LimitsPoll.AccountEvery = 7 * time.Minute
	digest := "ghcr.io/bugnet-sh/bugnet-base@sha256:" + strings.Repeat("b", 64)
	cfg.Images.Lium = config.ProviderImage{Version: "2026.11.3", Ref: digest}
	seed := []byte(strings.Repeat("s", 32))
	got := liumOptions(cfg, "lium_test", seed)
	want := lium.Options{
		APIKey: "lium_test", SSHSeed: seed,
		Sizes: []lium.SizeSpec{{
			Name: "lium-rtx3090", Model: gpu.RTX3090, Match: []string{"NVIDIA GeForce RTX 3090"},
			GPUs: 1, GPUMemoryGiB: 24, VCPU: 8, MemoryMiB: 28672, DiskGiB: 200,
			MaxHourlyMicros: 350_000,
		}},
		Countries: []string{"DE", "NL"}, OtherCountries: true, BlockedCountries: []string{"RU"},
		MinDownloadMbps: 500, MaxLifetime: 3 * time.Hour, StartBilled: 8 * time.Minute,
		OpsPerSecond: 4, CreatesPerSecond: 2, Image: digest, ImageVersion: "2026.11.3",
		TemplateVolume: "/root", Termination: "lifetime", MaxConcurrent: 3,
		AccountEvery: 7 * time.Minute,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lium options %+v, want %+v", got, want)
	}
	// With no lium.max_concurrent the headroom keeps room for the default slots
	delete(cfg.MaxConcurrent, "lium")
	got = liumOptions(cfg, "lium_test", seed)
	if got.MaxConcurrent != config.DefaultMaxConcurrent {
		t.Errorf("max concurrent %d without a setting, want %d", got.MaxConcurrent,
			config.DefaultMaxConcurrent)
	}
}

func TestHFOptionsCarryEverySetting(t *testing.T) {
	var cfg config.Config
	cfg.HF = config.HF{
		Namespace: "bugnet-test", Flavors: []string{"t4-small", "l4x1"},
		MaxLifetime: 90 * time.Minute, ExtendRoom: 30 * time.Minute,
		TimeoutMargin: 5 * time.Minute, StartBilled: 2 * time.Minute, PollEvery: 2 * time.Second,
		SBXServer: "fetch", SBXServerVersion: "0.7.0", SBXServerSHA256: strings.Repeat("c", 64),
		TokenPlan: "pro", OpsPerSecond: 8,
	}
	cfg.ProviderOps = map[string]int{"lium": 40, "hf": 80}
	digest := "ghcr.io/bugnet-sh/bugnet-base@sha256:" + strings.Repeat("d", 64)
	cfg.Images.HF = config.ProviderImage{Version: "2026.11.3", Ref: digest}
	seed := []byte(strings.Repeat("h", 32))
	got := hfOptions(cfg, "hf_test", seed)
	want := hf.Options{
		Token: "hf_test", Namespace: "bugnet-test", Flavors: []string{"t4-small", "l4x1"},
		MaxLifetime: 90 * time.Minute, StartBilled: 2 * time.Minute, OpsPerSecond: 8,
		SandboxSeed: seed, Image: digest, ExtendRoom: 30 * time.Minute,
		TimeoutMargin: 5 * time.Minute, PollEvery: 2 * time.Second,
		SBXServerSHA256: strings.Repeat("c", 64),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("hf options %+v, want %+v", got, want)
	}
	cfg.HF.SBXServer = "baked"
	want.SBXServerBaked = true
	if got := hfOptions(cfg, "hf_test", seed); !reflect.DeepEqual(got, want) {
		t.Fatalf("baked hf options %+v, want %+v", got, want)
	}
}
