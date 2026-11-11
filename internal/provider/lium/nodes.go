package lium

import (
	"cmp"
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	nodesEvery = 30 * time.Second
	// listingStale is the age past which no size is offered, so an outage moves orders on
	listingStale = 2 * time.Minute
	listTimeout  = 20 * time.Second
	// nodesPageSize is the largest page GET /executors serves
	nodesPageSize = 1000
	maxNodePages  = 10
	// maxClasses bounds the machine names remembered, every GPU type Lium lists fits many times
	maxClasses = 1000
	kib        = 1024
)

var (
	// graceName is a GPU type that runs on Grace (ARM) hosts, which our amd64 image cannot use
	graceName = regexp.MustCompile(`(?i)\bGB[23]00\b`)
	armCPU    = regexp.MustCompile(`(?i)\b(grace|neoverse|aarch64|arm64)\b`)
)

// nodeRecord holds the fields of one GET /executors entry that eligibility reads
type nodeRecord struct {
	ID                  string   `json:"id"`
	MachineName         string   `json:"machine_name"`
	GPUClass            string   `json:"gpu_class"`
	GPUCount            int      `json:"gpu_count"`
	AvailableGPUCount   int      `json:"available_gpu_count"`
	PricePerGPU         *float64 `json:"price_per_gpu"`
	PendingPricePerHour *float64 `json:"pending_price_per_hour"`
	Tier                string   `json:"tier"`
	Location            struct {
		CountryCode string `json:"country_code"`
	} `json:"location"`
	ScheduledNotice json.RawMessage `json:"scheduled_notice"`
	RemovalAt       json.RawMessage `json:"removal_at"`
	Specs           struct {
		CPU struct {
			Count int    `json:"count"`
			Model string `json:"model"`
		} `json:"cpu"`
		// RAM and HardDisk totals are KiB, as the public feed's GB values of the same nodes show
		RAM struct {
			Total float64 `json:"total"`
		} `json:"ram"`
		HardDisk struct {
			Total float64 `json:"total"`
		} `json:"hard_disk"`
	} `json:"specs"`
}

// node is one listed node as eligibility reads it
type node struct {
	id, gpuClass, machineName, tier, country, cpuModel string
	gpus, freeGPUs, vcpu                               int
	memoryMiB, diskGiB                                 int64
	// hourlyMicros is price_per_gpu times gpu_count, 0 when the node has no price
	hourlyMicros int64
	// pendingMicros is a coming price_per_hour times gpu_count, 0 when none is coming
	pendingMicros   int64
	notice, removal bool
}

// parseNode reads one listing entry, false for an entry any of whose fields it cannot read
func parseNode(raw json.RawMessage) (node, bool) {
	var r nodeRecord
	if err := json.Unmarshal(raw, &r); err != nil || r.ID == "" {
		return node{}, false
	}
	n := node{
		id: r.ID, gpuClass: r.GPUClass, machineName: r.MachineName, tier: r.Tier,
		country:  strings.ToUpper(strings.TrimSpace(r.Location.CountryCode)),
		cpuModel: r.Specs.CPU.Model, gpus: r.GPUCount, freeGPUs: r.AvailableGPUCount,
		vcpu: r.Specs.CPU.Count, memoryMiB: int64(r.Specs.RAM.Total / kib),
		diskGiB: int64(r.Specs.HardDisk.Total / (kib * kib)),
		notice:  present(r.ScheduledNotice), removal: present(r.RemovalAt),
	}
	if r.PricePerGPU != nil && *r.PricePerGPU > 0 {
		n.hourlyMicros = micros(*r.PricePerGPU) * int64(n.gpus)
	}
	// A pending price per hour is read per GPU, which can only ask more of a larger node
	if r.PendingPricePerHour != nil {
		n.pendingMicros = micros(*r.PendingPricePerHour) * int64(max(n.gpus, 1))
	}
	return n, true
}

// present reports whether a nullable field holds a value
func present(raw json.RawMessage) bool { return len(raw) > 0 && string(raw) != "null" }

// micros converts USD to micro-USD
func micros(usd float64) int64 { return int64(math.Round(usd * 1e6)) }

// grace reports a node on a Grace (ARM) host, by its GPU type or its CPU
func (n node) grace() bool {
	return graceName.MatchString(n.gpuClass) || graceName.MatchString(n.machineName) ||
		armCPU.MatchString(n.cpuModel)
}

// fits reports whether s may rent n by every rule of design 3.5 but the country rule
func (s size) fits(n node) bool {
	return n.tier == "secure" && n.gpus == s.GPUs && n.freeGPUs == n.gpus &&
		slices.Contains(s.Match, n.gpuClass) && n.vcpu >= s.VCPU &&
		n.memoryMiB >= int64(s.MemoryMiB) && n.diskGiB >= int64(s.DiskGiB) &&
		n.hourlyMicros > 0 && n.hourlyMicros <= s.MaxHourlyMicros &&
		n.pendingMicros <= s.MaxHourlyMicros && !n.notice && !n.removal && !n.grace()
}

// candidates are the nodes s may rent, cheapest first, from the first countries alone unless
// none there fits, then from any country that is not blocked when other countries are allowed
func (p *Provider) candidates(s size, nodes []node) []node {
	first, other := []node{}, []node{}
	for _, n := range nodes {
		switch {
		case !s.fits(n) || n.country == "" || p.blocked[n.country]:
		case p.countries[n.country]:
			first = append(first, n)
		case p.otherCountries:
			other = append(other, n)
		}
	}
	out := first
	if len(out) == 0 {
		out = other
	}
	slices.SortFunc(out, func(a, b node) int {
		return cmp.Or(cmp.Compare(a.hourlyMicros, b.hourlyMicros), strings.Compare(a.id, b.id))
	})
	return out
}

// nodeCache keeps the newest node listing and the gpu_class of every machine name seen
type nodeCache struct {
	api      *api
	log      *slog.Logger
	now      func() time.Time
	query    url.Values
	pageSize int

	mu      sync.Mutex
	nodes   []node
	at      time.Time // when the newest listing was asked for, zero before the first
	classes map[string]string
}

func newNodeCache(a *api, log *slog.Logger, now func() time.Time, minDownload int) *nodeCache {
	q := url.Values{"reliable_only": {"true"}}
	if minDownload > 0 {
		q.Set("min_download_mbps", strconv.Itoa(minDownload))
	}
	return &nodeCache{
		api: a, log: log, now: now, query: q, pageSize: nodesPageSize,
		classes: map[string]string{},
	}
}

// listing returns the newest listing and when it was asked for
func (c *nodeCache) listing() ([]node, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes, c.at
}

// fresh returns the newest listing, false when there is none under listingStale old
func (c *nodeCache) fresh() ([]node, bool) {
	nodes, at := c.listing()
	return nodes, !at.IsZero() && c.now().Sub(at) <= listingStale
}

// classOf returns the gpu_class a node with this machine name was listed under
func (c *nodeCache) classOf(machineName string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	class, ok := c.classes[machineName]
	return class, ok
}

// refresh reads the whole listing and keeps it, the old one staying when a page fails
func (c *nodeCache) refresh(ctx context.Context) error {
	at := c.now()
	var nodes []node
	seen := map[string]bool{}
	for page := 1; page <= maxNodePages; page++ {
		q := maps.Clone(c.query)
		q.Set("size", strconv.Itoa(c.pageSize))
		q.Set("page", strconv.Itoa(page))
		var raw []json.RawMessage
		_, err := c.api.do(ctx, call{
			method: http.MethodGet, route: "/executors", query: q, public: true,
		}, &raw)
		if err != nil {
			return err
		}
		added := false
		for _, r := range raw {
			n, ok := parseNode(r)
			if ok && !seen[n.id] {
				seen[n.id], added = true, true
				nodes = append(nodes, n)
			}
		}
		// A short page is the last, and a page of nothing new means paging is ignored
		if len(raw) < c.pageSize || !added {
			break
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes, c.at = nodes, at
	for _, n := range nodes {
		if _, known := c.classes[n.machineName]; n.machineName == "" || n.gpuClass == "" ||
			!known && len(c.classes) >= maxClasses {
			continue
		}
		c.classes[n.machineName] = n.gpuClass
	}
	return nil
}
