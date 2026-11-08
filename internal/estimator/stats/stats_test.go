package stats_test

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/stats"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var size1x4 = provider.Size{Name: "1x4", Resources: provider.Resources{
	VCPU: 1, MemoryMiB: 4096, DiskGiB: 10}}

func TestStatsDropped(t *testing.T) {
	for _, raw := range []string{`{"v":1,"mem_peak_mib":-1}`, `{"v":1,"mem_peak_mib":999999}`,
		`{"v":1,"cpu_seconds":1.5}`, `{"v":1,"extra":1}`, strings.Repeat(" ", 5<<10),
		`{"v":2}`, `{"v":1,"mem_peak_mib":1e2}`, `{"v":1} {"v":1}`, `{"v":1,"disk_peak_mib":10241}`,
		`{"v":1,"gpu_mem_peak_mib":1}`, `{"v":1,"busy_p95_millicores":1001}`,
		`{"v":1,"cpu_seconds":601}`, `{"v":1,"oom_kills":1000001}`, `{"v":1,"samples":-3}`,
		`not json`, ``,
	} {
		_, err := stats.Check([]byte(raw), size1x4, 10*time.Minute)
		if !errors.Is(err, stats.ErrInvalid) {
			t.Errorf("%q: got %v, want ErrInvalid", raw, err)
		}
	}
}

func TestStatsKept(t *testing.T) {
	raw := `{"v":1,"mem_peak_mib":4096,"swap_peak_mib":0,"disk_peak_mib":10240,"cpu_seconds":600,` +
		`"busy_p95_millicores":1000,"gpu_mem_peak_mib":0,"oom_kills":2,"samples":50,` +
		`"updated_at":"2026-11-08T10:00:00Z"}`
	got, err := stats.Check([]byte(raw), size1x4, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got.MemPeakMiB != 4096 || got.DiskPeakMiB != 10240 || got.CPUSeconds != 600 ||
		got.OOMKills != 2 || got.Samples != 50 {
		t.Fatalf("got %+v", got)
	}
}

func TestStatsLimitFollowsTheSetting(t *testing.T) {
	raw := []byte(`{"v":1}` + strings.Repeat(" ", 100))
	if _, err := stats.CheckMax(raw, 64, size1x4, time.Minute); err == nil {
		t.Fatal("a file over the setting was kept")
	}
	if _, err := stats.CheckMax(raw, 4096, size1x4, time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestStatsOnTheLocalSize(t *testing.T) {
	huge := provider.Size{Name: "local", Resources: provider.Resources{
		VCPU: math.MaxInt32, MemoryMiB: math.MaxInt32, DiskGiB: math.MaxInt32,
		GPUCount: math.MaxInt32, GPUMemoryGiB: math.MaxInt32}}
	raw := `{"v":1,"mem_peak_mib":4000,"disk_peak_mib":9000,"cpu_seconds":50,"gpu_mem_peak_mib":7}`
	if _, err := stats.Check([]byte(raw), huge, time.Hour); err != nil {
		t.Fatal(err)
	}
}
