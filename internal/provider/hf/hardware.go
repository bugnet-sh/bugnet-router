package hf

import (
	"cmp"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// portsHourlyMicros is HF's flat fee for a Job's exposed ports, $0.01 an hour (jobs-pricing)
	portsHourlyMicros = 10_000
	// maxMinuteMicros bounds a minute's price read from the list, $100
	maxMinuteMicros = 100_000_000
	// maxAmount bounds any count or GB figure read from the list
	maxAmount = 10_000_000
	mib       = 1 << 20
	gib       = 1 << 30
	// hardwareEvery spaces the hardware list reads, which HF's prices rarely outpace
	hardwareEvery = time.Hour
	// hardwareRetry spaces the reads after one that failed
	hardwareRetry   = 5 * time.Minute
	hardwareTimeout = 20 * time.Second
)

// compiledHardware is GET /api/jobs/hardware as read on 2026-11-12, design 4.5's table
//
//go:embed hardware.json
var compiledHardware []byte

// compiled holds the compiled list's GPU flavors by name, sold until a live list is read
var compiled = mustReadCompiled()

// flavor is one GPU flavor, its resources and minute price as HF's hardware list states them
type flavor struct {
	name      string
	resources provider.Resources
	// minuteMicros is unitCostMicroUSD, what a minute of the flavor costs without the ports
	minuteMicros int64
}

// hourlyMicros is an hour of the flavor with the exposed ports' fee
func (f flavor) hourlyMicros() int64 { return f.minuteMicros*60 + portsHourlyMicros }

// hwEntry is the part of one GET /api/jobs/hardware entry the adapter reads
type hwEntry struct {
	Name        string `json:"name"`
	CPU         string `json:"cpu"`
	RAM         string `json:"ram"`
	Storage     string `json:"ephemeralStorage"`
	Accelerator *struct {
		Type  string `json:"type"`
		Model string `json:"model"`
		// Quantity is a string in the list read, a number is taken too
		Quantity json.RawMessage `json:"quantity"`
		VRAM     string          `json:"vram"`
	} `json:"accelerator"`
	UnitCostMicroUSD json.Number `json:"unitCostMicroUSD"`
	UnitLabel        string      `json:"unitLabel"`
}

// flavor reads the entry as a GPU flavor, its GB taken as 10^9 bytes so a size never overstates
func (e hwEntry) flavor() (flavor, error) {
	a := e.Accelerator
	if a == nil || !strings.EqualFold(a.Type, "gpu") {
		return flavor{}, errors.New("it has no GPU accelerator")
	}
	model, ok := gpu.FromHF(a.Model)
	if !ok {
		return flavor{}, fmt.Errorf("its accelerator model %.40q is not a model the router names",
			a.Model)
	}
	gpus, err := count(a.Quantity)
	if err != nil {
		return flavor{}, fmt.Errorf("its accelerator quantity: %w", err)
	}
	var vram, vcpu, ram, disk float64
	for _, v := range []struct {
		field, raw, unit string
		into             *float64
	}{
		{"vram", a.VRAM, "GB", &vram}, {"cpu", e.CPU, "vCPU", &vcpu}, {"ram", e.RAM, "GB", &ram},
		{"ephemeralStorage", e.Storage, "GB", &disk},
	} {
		if *v.into, err = amount(v.raw, v.unit); err != nil {
			return flavor{}, fmt.Errorf("its %s: %w", v.field, err)
		}
	}
	if e.UnitLabel != "minute" {
		return flavor{}, fmt.Errorf("its unitLabel %.20q is not minute", e.UnitLabel)
	}
	price, err := strconv.ParseFloat(e.UnitCostMicroUSD.String(), 64)
	if err != nil || !(price > 0 && price <= maxMinuteMicros) {
		return flavor{}, fmt.Errorf("its unitCostMicroUSD is not a price above 0 and at most %d",
			maxMinuteMicros)
	}
	r := provider.Resources{
		VCPU: int(vcpu), MemoryMiB: int(ram * 1e9 / mib), DiskGiB: int(disk * 1e9 / gib),
		GPUCount: gpus, GPUMemoryGiB: int(vram), GPUModel: model,
	}
	if min(r.VCPU, r.MemoryMiB, r.DiskGiB, r.GPUMemoryGiB) < 1 {
		return flavor{}, errors.New("it states a resource below one whole unit")
	}
	// A price in part of a micro-USD is rounded up, so the cost never understates
	return flavor{name: e.Name, resources: r, minuteMicros: int64(math.Ceil(price))}, nil
}

// count reads a whole number of at least 1, written as a JSON number or string
func count(raw json.RawMessage) (int, error) {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		s = string(raw)
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 1 || n > maxAmount {
		return 0, fmt.Errorf("%.20q is not a whole number from 1 to %d", s, maxAmount)
	}
	return n, nil
}

// amount reads a figure followed by its unit, such as "15 GB"
func amount(s, unit string) (float64, error) {
	f := strings.Fields(s)
	if len(f) != 2 || !strings.EqualFold(f[1], unit) {
		return 0, fmt.Errorf("%.40q is not a figure in %s", s, unit)
	}
	v, err := strconv.ParseFloat(f[0], 64)
	if err != nil || !(v > 0 && v <= maxAmount) {
		return 0, fmt.Errorf("%.40q is not a figure above 0 and at most %d", s, maxAmount)
	}
	return v, nil
}

// mustReadCompiled reads the compiled list, which holds every GPU flavor in readable form
func mustReadCompiled() map[string]flavor {
	var entries []hwEntry
	if err := json.Unmarshal(compiledHardware, &entries); err != nil {
		panic("hf: the compiled hardware list: " + err.Error())
	}
	out := map[string]flavor{}
	for _, e := range entries {
		if !gpu.HFFlavor(e.Name) {
			continue
		}
		f, err := e.flavor()
		if err != nil {
			panic(fmt.Sprintf("hf: the compiled hardware list's %s: %v", e.Name, err))
		}
		out[e.Name] = f
	}
	if len(out) != len(gpu.HFFlavors()) {
		panic("hf: the compiled hardware list lacks a GPU flavor")
	}
	return out
}

// hardware holds the flavors sold, read from the newest hardware list or the compiled one
type hardware struct {
	// sells are the configured flavors
	sells map[string]bool

	mu sync.RWMutex
	// flavors are the configured flavors of the newest list, cheapest first, replaced whole
	flavors []flavor
	// known holds each configured flavor's last readable entry, the compiled one at first
	known map[string]flavor
}

// newHardware sells the named flavors, every GPU flavor when none, at the compiled prices
func newHardware(names []string) *hardware {
	if len(names) == 0 {
		names = gpu.HFFlavors()
	}
	h := &hardware{sells: map[string]bool{}, known: maps.Clone(compiled)}
	for _, n := range names {
		h.sells[n] = true
	}
	h.flavors = h.ordered(compiled)
	return h
}

// update reads a hardware list, keeping an unreadable flavor's last reading, and reports a change
func (h *hardware) update(entries []json.RawMessage, log *slog.Logger) (bool, error) {
	read, gpus := map[string]flavor{}, 0
	for _, raw := range entries {
		var e hwEntry
		err := json.Unmarshal(raw, &e)
		if !gpu.HFFlavor(e.Name) {
			continue
		}
		gpus++
		if !h.sells[e.Name] {
			continue
		}
		var f flavor
		if err == nil {
			f, err = e.flavor()
		}
		if err != nil {
			log.Error("hf: a flavor of the hardware list is unreadable, its last reading stays",
				"flavor", e.Name, "err", err)
			f = h.last(e.Name)
		}
		read[e.Name] = f
	}
	// A list that names no GPU flavor is taken as a broken answer, never as none sold
	if gpus == 0 {
		return false, errors.New("hf: the hardware list names no GPU flavor, the last " +
			"prices stay")
	}
	next := h.ordered(read)
	h.mu.Lock()
	prev := h.flavors
	h.flavors = next
	maps.Copy(h.known, read)
	h.mu.Unlock()
	logChanges(prev, next, log)
	return !slices.Equal(prev, next), nil
}

// last is the named flavor's last readable entry
func (h *hardware) last(name string) flavor {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.known[name]
}

// logChanges logs each flavor that left the list, came back, or changed its price or resources
func logChanges(prev, next []flavor, log *slog.Logger) {
	for _, p := range prev {
		if !slices.ContainsFunc(next, func(n flavor) bool { return n.name == p.name }) {
			log.Warn("hf: a flavor left the hardware list and is not sold", "flavor", p.name)
		}
	}
	for _, n := range next {
		i := slices.IndexFunc(prev, func(p flavor) bool { return p.name == n.name })
		switch {
		case i < 0:
			log.Info("hf: a flavor is back on the hardware list and sold", "flavor", n.name)
		case prev[i].minuteMicros != n.minuteMicros:
			log.Info("hf: a flavor's price changed", "flavor", n.name,
				"hourly_micros_was", prev[i].hourlyMicros(), "hourly_micros", n.hourlyMicros())
		case prev[i].resources != n.resources:
			log.Info("hf: a flavor's resources changed", "flavor", n.name,
				"was", prev[i].resources, "now", n.resources)
		}
	}
}

// readHardware loads the hardware list at once and then every hwEvery, sooner after a failure
func (p *Provider) readHardware(ctx context.Context) {
	for {
		next := p.hwEvery
		rctx, cancel := context.WithTimeout(ctx, hardwareTimeout)
		err := p.loadHardware(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.log.Warn("hf: the hardware list was not read, the last prices stay", "err", err)
			next = min(next, p.hwRetry)
		}
		if sleep(ctx, next) != nil {
			return
		}
	}
}

// loadHardware reads GET /api/jobs/hardware into the flavors sold
func (p *Provider) loadHardware(ctx context.Context) error {
	var entries []json.RawMessage
	_, err := p.jobs.do(ctx, call{
		method: http.MethodGet, url: jobsURL(nil, "hardware"), public: true,
	}, &entries)
	if err != nil {
		return err
	}
	changed, err := p.hw.update(entries, p.log)
	if err == nil && changed && p.onChange != nil {
		p.onChange()
	}
	return err
}

// ordered keeps the configured flavors of read, cheapest first, equal prices in HF's list order
func (h *hardware) ordered(read map[string]flavor) []flavor {
	var out []flavor
	for _, name := range gpu.HFFlavors() {
		if f, ok := read[name]; ok && h.sells[name] {
			out = append(out, f)
		}
	}
	slices.SortStableFunc(out, func(a, b flavor) int {
		return cmp.Compare(a.minuteMicros, b.minuteMicros)
	})
	return out
}

// sold returns the flavors sold now, cheapest first
func (h *hardware) sold() []flavor {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.flavors
}

// flavor returns the named flavor while it is sold
func (h *hardware) flavor(name string) (flavor, bool) {
	sold := h.sold()
	i := slices.IndexFunc(sold, func(f flavor) bool { return f.name == name })
	if i < 0 {
		return flavor{}, false
	}
	return sold[i], true
}
