package lium

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func ptr[T any](v T) *T { return &v }

// parsed reads a fake node the way the listing does
func parsed(t *testing.T, n fakeNode) node {
	t.Helper()
	raw, err := json.Marshal(n.json())
	if err != nil {
		t.Fatal(err)
	}
	got, ok := parseNode(raw)
	if !ok {
		t.Fatalf("node %+v did not parse", n)
	}
	return got
}

func ids(nodes []node) []string {
	out := []string{}
	for _, n := range nodes {
		out = append(out, n.id)
	}
	return out
}

func TestLiumNodeListingRequest(t *testing.T) {
	f := newFake(t)
	healthy, flaky := euNode("healthy"), euNode("flaky")
	flaky.unreliable = true
	f.setNodes(healthy, flaky)
	p := f.provider(t, testOptions(), time.Now)
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs := f.requestsTo("GET", "/executors")
	eq(t, len(reqs), 1)
	// The listing is keyless, so the key never travels where it is not needed
	eq(t, reqs[0].header.Values("X-API-Key"), []string(nil))
	eq(t, reqs[0].query.Get("reliable_only"), "true")
	eq(t, reqs[0].query.Get("size"), "1000")
	eq(t, reqs[0].query.Get("page"), "1")
	eq(t, reqs[0].query.Has("min_download_mbps"), false)
	nodes, _ := p.nodes.listing()
	eq(t, ids(nodes), []string{"healthy"})

	slow := euNode("slow")
	slow.downloadMbps = 200
	f.setNodes(healthy, slow)
	opts := testOptions()
	opts.MinDownloadMbps = 500
	p = f.provider(t, opts, time.Now)
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs = f.requestsTo("GET", "/executors")
	eq(t, reqs[len(reqs)-1].query.Get("min_download_mbps"), "500")
	nodes, _ = p.nodes.listing()
	eq(t, ids(nodes), []string{"healthy"})
}

func TestLiumNodeListingPages(t *testing.T) {
	f := newFake(t)
	var all []fakeNode
	for _, id := range []string{"n1", "n2", "n3", "n4", "n5"} {
		all = append(all, euNode(id))
	}
	f.setNodes(all...)
	p := f.provider(t, testOptions(), time.Now)
	p.nodes.pageSize = 2
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	nodes, _ := p.nodes.listing()
	eq(t, ids(nodes), []string{"n1", "n2", "n3", "n4", "n5"})
	var pages []string
	for _, r := range f.requestsTo("GET", "/executors") {
		pages = append(pages, r.query.Get("page"))
	}
	eq(t, pages, []string{"1", "2", "3"})

	// A server that ignores page answers the same nodes again, which ends the paging
	f.unpaged = true
	f.setNodes(all[:2]...)
	p.nodes.pageSize = 1
	before := len(f.requestsTo("GET", "/executors"))
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	nodes, _ = p.nodes.listing()
	eq(t, ids(nodes), []string{"n1", "n2"})
	eq(t, len(f.requestsTo("GET", "/executors"))-before, 2)

	// A listing longer than every page reads at most maxNodePages of them
	f.unpaged = false
	many := make([]fakeNode, maxNodePages+3)
	for i := range many {
		many[i] = euNode(fmt.Sprintf("m%02d", i))
	}
	f.setNodes(many...)
	before = len(f.requestsTo("GET", "/executors"))
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	nodes, _ = p.nodes.listing()
	eq(t, len(nodes), maxNodePages)
	eq(t, len(f.requestsTo("GET", "/executors"))-before, maxNodePages)
}

func TestLiumNodeRecordsBothNames(t *testing.T) {
	f := newFake(t)
	nvl := euNode("nvl")
	nvl.gpuClass, nvl.machine = "NVIDIA H200", "NVIDIA H200 NVL"
	maxQ := euNode("maxq")
	maxQ.gpuClass = "NVIDIA RTX PRO 6000 Blackwell Server Edition"
	maxQ.machine = "NVIDIA RTX PRO 6000 Blackwell Max-Q Workstation Edition"
	nameless := euNode("nameless")
	nameless.noMachine = true
	f.setNodes(nvl, maxQ, nameless)
	p := f.provider(t, testOptions(), time.Now)
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	nodes, _ := p.nodes.listing()
	eq(t, nodes[0].gpuClass, "NVIDIA H200")
	eq(t, nodes[0].machineName, "NVIDIA H200 NVL")
	eq(t, nodes[2].machineName, "")
	for machine, want := range map[string]string{
		"NVIDIA H200 NVL": "NVIDIA H200",
		"NVIDIA RTX PRO 6000 Blackwell Max-Q Workstation Edition": "NVIDIA RTX PRO 6000 " +
			"Blackwell Server Edition",
	} {
		got, ok := p.nodes.classOf(machine)
		if !ok || got != want {
			t.Errorf("classOf(%q) = %q, %v, want %q", machine, got, ok, want)
		}
	}
	// A class seen once stays known after its node leaves the listing
	f.setNodes()
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, ok := p.nodes.classOf("NVIDIA H200 NVL"); !ok || got != "NVIDIA H200" {
		t.Errorf("classOf after the node left = %q, %v", got, ok)
	}
	if _, ok := p.nodes.classOf("NVIDIA B300 SXM6 PC"); ok {
		t.Error("a machine name never listed has a class")
	}
}

func TestLiumNodeParsesLiumUnits(t *testing.T) {
	// specs.ram.total and specs.hard_disk.total are KiB, as the public feed's GB values show
	n := euNode("units")
	n.ramGiB, n.diskGiB, n.vcpu = 31.25, 14900, 31
	got := parsed(t, n)
	eq(t, got.memoryMiB, int64(32000))
	eq(t, got.diskGiB, int64(14900))
	eq(t, got.vcpu, 31)
	eq(t, got.hourlyMicros, int64(280_000))
	eq(t, got.country, "DE")

	two := euNode("two")
	two.gpus, two.free, two.price, two.pending = 2, 2, 0.35, ptr(0.4)
	got = parsed(t, two)
	eq(t, got.hourlyMicros, int64(700_000))
	eq(t, got.pendingMicros, int64(800_000))

	for name, raw := range map[string]string{
		"not an object": `"node"`, "a count of another type": `{"id": "x", "gpu_count": "1"}`,
		"no id": `{"gpu_count": 1}`,
	} {
		if _, ok := parseNode(json.RawMessage(raw)); ok {
			t.Errorf("%s parsed as a node", name)
		}
	}
}

func TestLiumNodeEligibility(t *testing.T) {
	opts := testOptions()
	// A Grace class could only be matched by a size that names it, which the check still refuses
	opts.Sizes = append(opts.Sizes, SizeSpec{
		Name: "lium-b200", Model: gpu.B200, Match: []string{"NVIDIA B200", "NVIDIA GB200"},
		GPUs: 1, GPUMemoryGiB: 180, VCPU: 16, MemoryMiB: 204800, DiskGiB: 400,
		MaxHourlyMicros: 9_500_000,
	}, SizeSpec{
		Name: "lium-rtx3090x2", Model: gpu.RTX3090, Match: []string{"NVIDIA GeForce RTX 3090"},
		GPUs: 2, GPUMemoryGiB: 48, VCPU: 8, MemoryMiB: 28672, DiskGiB: 200,
		MaxHourlyMicros: 700_000,
	})
	p, err := newProvider(opts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	rows := []struct {
		name   string
		size   string
		change func(*fakeNode)
		fits   bool
	}{
		{"every rule met", "lium-rtx3090", func(*fakeNode) {}, true},
		{"spot tier", "lium-rtx3090", func(n *fakeNode) { n.tier = "spot" }, false},
		{"no tier", "lium-rtx3090", func(n *fakeNode) { n.tier = "" }, false},
		{"more GPUs than the size", "lium-rtx3090", func(n *fakeNode) { n.gpus, n.free = 2, 2 },
			false},
		{"whole 2-GPU node", "lium-rtx3090x2", func(n *fakeNode) { n.gpus, n.free = 2, 2 },
			true},
		{"split with another renter", "lium-rtx3090x2",
			func(n *fakeNode) { n.gpus, n.free = 2, 1 }, false},
		{"rented", "lium-rtx3090", func(n *fakeNode) { n.free = 0 }, false},
		{"another class", "lium-rtx3090", func(n *fakeNode) {
			n.gpuClass, n.machine = "NVIDIA GeForce RTX 4090", "NVIDIA GeForce RTX 4090"
		}, false},
		{"no class", "lium-rtx3090", func(n *fakeNode) { n.gpuClass = "" }, false},
		{"a class matched under another machine name", "lium-h200", func(n *fakeNode) {
			n.gpuClass, n.machine, n.price = "NVIDIA H200", "NVIDIA H200 NVL", 3.85
			n.vcpu, n.ramGiB, n.diskGiB = 23, 240, 2747
		}, true},
		{"vCPUs under the floor", "lium-rtx3090", func(n *fakeNode) { n.vcpu = 7 }, false},
		{"vCPUs at the floor", "lium-rtx3090", func(n *fakeNode) { n.vcpu = 8 }, true},
		{"memory under the floor", "lium-rtx3090", func(n *fakeNode) { n.ramGiB = 27.99 }, false},
		{"memory at the floor", "lium-rtx3090", func(n *fakeNode) { n.ramGiB = 28 }, true},
		{"disk under the floor", "lium-rtx3090", func(n *fakeNode) { n.diskGiB = 199.9 }, false},
		{"disk at the floor", "lium-rtx3090", func(n *fakeNode) { n.diskGiB = 200 }, true},
		{"price over the cap", "lium-rtx3090", func(n *fakeNode) { n.price = 0.351 }, false},
		{"price at the cap", "lium-rtx3090", func(n *fakeNode) { n.price = 0.35 }, true},
		{"node price over the cap", "lium-rtx3090x2", func(n *fakeNode) {
			n.gpus, n.free, n.price = 2, 2, 0.36
		}, false},
		{"no price", "lium-rtx3090", func(n *fakeNode) { n.unpriced = true }, false},
		{"pending price over the cap", "lium-rtx3090",
			func(n *fakeNode) { n.pending = ptr(0.36) }, false},
		{"pending price at the cap", "lium-rtx3090",
			func(n *fakeNode) { n.pending = ptr(0.35) }, true},
		{"pending price below", "lium-rtx3090", func(n *fakeNode) { n.pending = ptr(0.2) },
			true},
		{"scheduled notice", "lium-rtx3090", func(n *fakeNode) { n.notice = true }, false},
		{"removal at", "lium-rtx3090", func(n *fakeNode) { n.removal = true }, false},
		{"Grace class", "lium-b200", func(n *fakeNode) {
			n.gpuClass, n.machine, n.price = "NVIDIA GB200", "NVIDIA GB200", 8
			n.vcpu, n.ramGiB, n.diskGiB = 72, 480, 4000
		}, false},
		{"Grace host", "lium-rtx3090", func(n *fakeNode) { n.cpuModel = "NVIDIA Grace CPU" },
			false},
		{"Arm host", "lium-rtx3090", func(n *fakeNode) { n.cpuModel = "Neoverse-V2" }, false},
		{"amd64 B200", "lium-b200", func(n *fakeNode) {
			n.gpuClass, n.machine, n.price = "NVIDIA B200", "NVIDIA B200", 8.88
			n.vcpu, n.ramGiB, n.diskGiB = 19, 220, 464
		}, true},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			n := euNode("node")
			tc.change(&n)
			s, ok := p.size(tc.size)
			if !ok {
				t.Fatalf("no size %s", tc.size)
			}
			eq(t, s.fits(parsed(t, n)), tc.fits)
		})
	}
}

func TestLiumCountryRule(t *testing.T) {
	at := func(id, country string, price float64) fakeNode {
		n := euNode(id)
		n.country, n.price = country, price
		return n
	}
	spot := at("eu-spot", "FR", 0.1)
	spot.tier = "spot"
	rows := []struct {
		name  string
		nodes []fakeNode
		other bool
		want  []string
	}{
		{"an EU node wins over a cheaper one elsewhere", []fakeNode{
			at("us", "US", 0.2), at("de", "DE", 0.34),
		}, true, []string{"de"}},
		{"EU nodes go cheapest first", []fakeNode{
			at("de", "DE", 0.3), at("fr", "FR", 0.25), at("ro", "RO", 0.3),
		}, true, []string{"fr", "de", "ro"}},
		{"elsewhere only when no EU node fits", []fakeNode{
			spot, at("us", "US", 0.3), at("ca", "CA", 0.25),
		}, true, []string{"ca", "us"}},
		{"blocked countries never", []fakeNode{
			at("ru", "RU", 0.1), at("by", "BY", 0.1), at("us", "US", 0.3),
		}, true, []string{"us"}},
		{"only blocked countries", []fakeNode{at("ir", "IR", 0.1)}, true, []string{}},
		{"elsewhere is off", []fakeNode{at("us", "US", 0.2)}, false, []string{}},
		{"no country", []fakeNode{at("unknown", "", 0.2)}, true, []string{}},
		{"a lowercase code", []fakeNode{at("us", "US", 0.2), at("de", "de", 0.3)}, true,
			[]string{"de"}},
		{"no node", nil, true, []string{}},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			opts := testOptions()
			opts.OtherCountries = tc.other
			p, err := newProvider(opts, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			var nodes []node
			for _, n := range tc.nodes {
				nodes = append(nodes, parsed(t, n))
			}
			s, _ := p.size("lium-rtx3090")
			eq(t, ids(p.candidates(s, nodes)), tc.want)
		})
	}

	// A country in both lists is blocked
	opts := testOptions()
	opts.BlockedCountries = append(opts.BlockedCountries, "DE")
	p, err := newProvider(opts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := p.size("lium-rtx3090")
	eq(t, ids(p.candidates(s, []node{parsed(t, at("de", "DE", 0.3))})), []string{})
}

func TestLiumCandidatesBreakPriceTiesByID(t *testing.T) {
	p, err := newProvider(testOptions(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	var nodes []node
	for _, id := range []string{"c", "a", "b"} {
		nodes = append(nodes, parsed(t, euNode(id)))
	}
	s, _ := p.size("lium-rtx3090")
	got := ids(p.candidates(s, nodes))
	eq(t, got, []string{"a", "b", "c"})
	eq(t, ids(nodes), []string{"c", "a", "b"})
	if !slices.IsSorted(got) {
		t.Fatal("ties are not in id order")
	}
}

func TestLiumFailedRefreshKeepsTheListing(t *testing.T) {
	f := newFake(t)
	f.setNodes(euNode("n1"))
	clk := newClock()
	p := f.provider(t, testOptions(), clk.now)
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	read := clk.now()
	clk.add(time.Minute)
	f.inject("GET /executors", fault{status: 503, code: "upstream_unavailable", message: "x"})
	if err := p.nodes.refresh(context.Background()); err == nil {
		t.Fatal("a refused listing refreshed")
	}
	nodes, at := p.nodes.listing()
	eq(t, ids(nodes), []string{"n1"})
	eq(t, at, read)

	f.inject("GET /executors", fault{status: 200, raw: `{"nodes": []}`})
	if err := p.nodes.refresh(context.Background()); err == nil {
		t.Fatal("a listing that is not an array refreshed")
	}
	_, at = p.nodes.listing()
	eq(t, at, read)
}

func TestLiumListingSkipsNodesItCannotRead(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	good, err := json.Marshal(euNode("good").json())
	if err != nil {
		t.Fatal(err)
	}
	f.inject("GET /executors", fault{status: 200,
		raw: `[{"id": "bad", "gpu_count": "one"}, ` + string(good) + `, 7]`})
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	nodes, _ := p.nodes.listing()
	eq(t, ids(nodes), []string{"good"})
}

func TestLiumNewKeepsTheListingFresh(t *testing.T) {
	f := newFake(t)
	f.setNodes(euNode("n3090"))
	var logs syncBuffer
	opts := testOptions()
	opts.BaseURL, opts.NodesEvery = f.srv.URL, 20*time.Millisecond
	opts.Log = slog.New(slog.NewTextHandler(&logs, nil))
	f.inject("GET /executors", fault{status: 503, code: "upstream_unavailable",
		message: "down", times: 2})
	p, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	floor := provider.Resources{GPUCount: 1}
	waitFor(t, func() bool {
		_, ok := p.SizeFor(floor)
		return ok && len(f.requestsTo("GET", "/executors")) >= 5
	})
	if !strings.Contains(logs.String(), "level=WARN") ||
		!strings.Contains(logs.String(), "upstream_unavailable") {
		t.Fatalf("a failed listing was not logged: %q", logs.String())
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	calls := len(f.requestsTo("GET", "/executors"))
	time.Sleep(100 * time.Millisecond)
	eq(t, len(f.requestsTo("GET", "/executors")), calls)
	if err := p.Close(); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
	var _ io.Closer = p
}

func TestLiumListingEvery30Seconds(t *testing.T) {
	opts := testOptions()
	opts.BaseURL = "http://127.0.0.1:1"
	p, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	eq(t, p.nodesEvery, 30*time.Second)
	opts.NodesEvery = -time.Second
	q, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	eq(t, q.nodesEvery, 30*time.Second)
}

// waitFor polls cond for up to 5 seconds
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("the condition never held")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// syncBuffer is a log sink the listing loop and the test may share
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
