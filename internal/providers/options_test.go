package providers

import (
	"net/netip"
	"reflect"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd"
	"github.com/bugnet-sh/bugnet-router/internal/provider/koyeb"
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
	cfg.Boxd = config.Boxd{Region: "nyc", Endpoint: "boxd.example:9443", GateRoute: "default",
		USDPerEUR: 1.2}
	cfg.Previews.GatePort = 29418
	got := boxdOptions(cfg, "boxd_test")
	want := boxd.Options{
		APIKey: "boxd_test", Endpoint: "boxd.example:9443", Region: "nyc", GatePort: 29418,
		GateRoute: "default", USDPerEUR: 1.2,
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
