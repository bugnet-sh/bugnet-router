package providers

import (
	"reflect"
	"strings"
	"testing"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/provider/boxd"
	"github.com/oynozan/bugnet-router/internal/provider/koyeb"
)

func TestKoyebOptionsCarryTheTransferParts(t *testing.T) {
	var cfg config.Config
	cfg.Koyeb.Region = "par"
	cfg.Koyeb.InstanceTypes = map[string]string{"cpu": "large"}
	cfg.Transfers.KoyebPart = 4 << 20
	cfg.Transfers.KoyebBufferTotal = 64 << 20
	got := koyebOptions(cfg, "koyeb_test")
	want := koyeb.Options{
		APIToken: "koyeb_test", Region: "par", InstanceTypeBySize: cfg.Koyeb.InstanceTypes,
		PartBytes: 4 << 20, BufferTotal: 64 << 20,
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
