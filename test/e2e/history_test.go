package e2e

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/estimatortest"
)

const historyCatalog = `{"packages": [
	{"id": "hist-2g-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000},
	{"id": "hist-4g-10m", "resources": {"vcpu": 2, "memory_mib": 4096, "disk_gib": 10},
		"minutes": 10, "price_micros": 60000},
	{"id": "hist-8g-10m", "resources": {"vcpu": 2, "memory_mib": 8192, "disk_gib": 10},
		"minutes": 10, "price_micros": 70000}
]}`

// Three payers that are valid Solana addresses
var historyPayers = []string{e2ePayer, "SysvarC1ock11111111111111111111111111111111",
	"ComputeBudget111111111111111111111111111111"}

// pauseSampler stops the only sandbox's sampler, so it cannot write over the file a test puts
// in its place, and returns that file's path. The destroy at the stop kills the sampler.
func pauseSampler(t *testing.T, root string) string {
	t.Helper()
	const pidSuffix = "/fs/run/bugnet/sample.pid"
	var pidFiles []string
	// The sampler starts a moment after the placement is ready
	for deadline := time.Now().Add(10 * time.Second); len(pidFiles) != 1; {
		pidFiles = nil
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(p, pidSuffix) {
				pidFiles = append(pidFiles, p)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(pidFiles) != 1 && time.Now().After(deadline) {
			t.Fatalf("sampler pid files %v", pidFiles)
		}
		time.Sleep(50 * time.Millisecond)
	}
	b, err := os.ReadFile(pidFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 1 {
		t.Fatalf("sampler pid file %q", b)
	}
	// A pid file left behind by a reused pid must not stop another process
	cmd, err := exec.Command("ps", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	if err != nil || !strings.Contains(string(cmd), " sample") {
		t.Fatalf("pid %d is not the sampler: %q, %v", pid, cmd, err)
	}
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	file := strings.TrimSuffix(pidFiles[0], "/run/bugnet/sample.pid") + "/tmp/bugnet/stats.json"
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestE2EHistoryMovesTheQuote(t *testing.T) {
	t.Parallel()
	gh := estimatortest.NewGitHub(t, &estimatortest.Repo{Owner: "acme", Name: "app",
		Default: "main", Branches: map[string]string{"main": strings.Repeat("a", 40)},
		Commits: map[string]map[string]string{strings.Repeat("a", 40): {
			"Dockerfile": "FROM node:20\n"}}})
	s := startStackWith(t, stackOptions{catalog: historyCatalog, extra: fmt.Sprintf(`
[wallets]
use_payer = true
emit_events = true
max_concurrent = 5
facilitator_addresses = [%q]

[estimator]
github_api = %q
github_raw = %q
github_git = %q
history_payer_min_age = "0s"
history_payer_min_placements = 1
`, e2eFacilitator, gh.URL(), gh.URL(), gh.URL())})
	api := s.client(t, s.a)
	quote := func() *routerv1.EstimateResponse {
		t.Helper()
		res, err := api.Estimate(t.Context(), connect.NewRequest(&routerv1.EstimateRequest{
			Source: "github.com/acme/app", Workload: routerv1.Workload_WORKLOAD_TESTS,
		}))
		if err != nil {
			t.Fatalf("Estimate: %v", err)
		}
		return res.Msg
	}
	first := quote()
	if first.GetBasis() == routerv1.EstimateBasis_ESTIMATE_BASIS_HISTORY ||
		first.GetFloor().GetResources().GetMemoryMib() != 2048 || first.GetRecommendedPackageId() != "hist-2g-10m" {
		t.Fatalf("first quote %v", first)
	}
	// Five placements from three payers, each ending with a peak above the floor and in the size
	for i := range 5 {
		est := quote().GetEstimateId()
		if est == "" {
			t.Fatal("a quote without an estimate id")
		}
		out := parseKV(s.devOK(t, s.a, "create", "--package", "hist-2g-10m", "--estimate-id", est,
			"--payer", historyPayers[i%3], "--fee-payer", e2eFacilitator))
		if out["state"] != "ready" {
			t.Fatalf("create %d printed %v", i+1, out)
		}
		file := pauseSampler(t, s.root)
		stats := `{"v":1,"mem_peak_mib":3000,"swap_peak_mib":0,"disk_peak_mib":2000,` +
			`"cpu_seconds":5,"busy_p95_millicores":500,"gpu_mem_peak_mib":0,"oom_kills":0,` +
			`"samples":20,"updated_at":"2026-11-08T10:00:00Z"}`
		if err := os.WriteFile(file, []byte(stats), 0o644); err != nil {
			t.Fatal(err)
		}
		s.devOK(t, s.a, "stop", "--handle", out["handle"])
	}
	conn, err := pgx.Connect(t.Context(), s.db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	var rows, payers, peak int
	err = conn.QueryRow(t.Context(), `
		select count(*), count(distinct p.payer), max(o.peak_memory_mib)
		from usage_observations o join placements p on p.id = o.placement_id`).
		Scan(&rows, &payers, &peak)
	if err != nil || rows != 5 || payers != 3 || peak != 3000 {
		t.Fatalf("observations %d from %d payers peaking at %d, %v", rows, payers, peak, err)
	}
	// 3000 MiB with 10% headroom is 3300, rounded to 3328, inside the one step up to 4096
	last := quote()
	if last.GetBasis() != routerv1.EstimateBasis_ESTIMATE_BASIS_HISTORY ||
		last.GetFloor().GetResources().GetMemoryMib() != 3328 || last.GetRecommendedPackageId() != "hist-4g-10m" {
		t.Fatalf("quote after five runs: basis %v floor %v package %s", last.GetBasis(),
			last.GetFloor(), last.GetRecommendedPackageId())
	}
}
