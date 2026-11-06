// Package catalog loads the priced packages the router sells
package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"

	"github.com/oynozan/bugnet-router/internal/bytesize"
	"github.com/oynozan/bugnet-router/internal/hostname"
	"github.com/oynozan/bugnet-router/internal/provider"
)

// Package is one purchasable bundle of resources and time
type Package struct {
	ID          string
	Resources   provider.Resources
	Minutes     int
	PriceMicros int64
	ExtensionOf string
	// Edge is the package's own edge budget, which wins over the router's settings
	Edge Edge
	// EgressAllow lists the only hosts a restricted package reaches, unrestricted when empty
	EgressAllow []string
}

// Edge is a package's own edge budget, a zero field keeps the router's setting
type Edge struct {
	// Bytes is the edge bytes both ways for the package's whole time
	Bytes                  int64
	BytesPerSecond         int64
	RequestsPerSecond      int
	RequestsBurst          int
	QueuedRequests         int
	WebSockets             int
	TransferMaxBytes       int64
	EgressBytes            int64
	EgressBytesPerSecond   int64
	EgressRequestsInFlight int
}

// Catalog is an immutable set of packages
type Catalog struct {
	byID map[string]Package
	all  []Package
}

type fileResources struct {
	VCPU         int `json:"vcpu"`
	MemoryMiB    int `json:"memory_mib"`
	DiskGiB      int `json:"disk_gib"`
	GPUCount     int `json:"gpu_count"`
	GPUMemoryGiB int `json:"gpu_memory_gib"`
}

type filePackage struct {
	ID          string        `json:"id"`
	Resources   fileResources `json:"resources"`
	Minutes     int           `json:"minutes"`
	PriceMicros int64         `json:"price_micros"`
	ExtensionOf string        `json:"extension_of"`
	Edge        *fileEdge     `json:"edge"`
	Egress      *fileEgress   `json:"egress"`
}

// fileEdge holds pointers so a field given as zero is told apart from one left out
type fileEdge struct {
	Bytes                  *string `json:"bytes"`
	BytesPerSecond         *string `json:"bytes_per_second"`
	RequestsPerSecond      *int    `json:"requests_per_second"`
	RequestsBurst          *int    `json:"requests_burst"`
	QueuedRequests         *int    `json:"queued_requests"`
	WebSockets             *int    `json:"websockets"`
	TransferMaxBytes       *string `json:"transfer_max_bytes"`
	EgressBytes            *string `json:"egress_bytes"`
	EgressBytesPerSecond   *string `json:"egress_bytes_per_second"`
	EgressRequestsInFlight *int    `json:"egress_requests_in_flight"`
}

type fileEgress struct {
	Allow []string `json:"allow"`
}

type file struct {
	Packages []filePackage `json:"packages"`
}

// Load reads and validates a catalog file
func Load(path string) (*Catalog, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	var f file
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("catalog: parse %s: %w", path, err)
	}
	c := &Catalog{byID: make(map[string]Package, len(f.Packages))}
	for _, fp := range f.Packages {
		p := Package{
			ID: fp.ID,
			Resources: provider.Resources{
				VCPU: fp.Resources.VCPU, MemoryMiB: fp.Resources.MemoryMiB,
				DiskGiB: fp.Resources.DiskGiB, GPUCount: fp.Resources.GPUCount,
				GPUMemoryGiB: fp.Resources.GPUMemoryGiB,
			},
			Minutes: fp.Minutes, PriceMicros: fp.PriceMicros, ExtensionOf: fp.ExtensionOf,
		}
		if err := validate(p); err != nil {
			return nil, err
		}
		if p.Edge, err = fp.Edge.read(p.ID); err != nil {
			return nil, err
		}
		if p.EgressAllow, err = fp.Egress.read(p.ID); err != nil {
			return nil, err
		}
		if _, dup := c.byID[p.ID]; dup {
			return nil, fmt.Errorf("catalog: duplicate package id %q", p.ID)
		}
		c.byID[p.ID] = p
		c.all = append(c.all, p)
	}
	for _, p := range c.all {
		if p.ExtensionOf == "" {
			continue
		}
		target, ok := c.byID[p.ExtensionOf]
		switch {
		case !ok:
			return nil, fmt.Errorf(
				"catalog: package %q extends unknown package %q", p.ID, p.ExtensionOf)
		case target.ExtensionOf != "":
			return nil, fmt.Errorf(
				"catalog: package %q extends %q, which is itself an extension", p.ID, target.ID)
		case target.Resources != p.Resources:
			return nil, fmt.Errorf(
				"catalog: package %q has different resources than %q it extends", p.ID, target.ID)
		case !sameHosts(target.EgressAllow, p.EgressAllow):
			// The sandbox keeps the egress rules it was created with
			return nil, fmt.Errorf("catalog: package %q must carry the same egress.allow as %q "+
				"it extends", p.ID, target.ID)
		}
	}
	sort.Slice(c.all, func(i, j int) bool {
		if c.all[i].PriceMicros != c.all[j].PriceMicros {
			return c.all[i].PriceMicros < c.all[j].PriceMicros
		}
		return c.all[i].ID < c.all[j].ID
	})
	return c, nil
}

func validate(p Package) error {
	switch {
	case p.ID == "":
		return fmt.Errorf("catalog: empty package id")
	case p.Minutes <= 0:
		return fmt.Errorf("catalog: package %q needs positive minutes", p.ID)
	case p.Resources.VCPU <= 0 || p.Resources.MemoryMiB <= 0:
		return fmt.Errorf("catalog: package %q needs positive vcpu and memory", p.ID)
	case p.Resources.DiskGiB < 0 || p.Resources.GPUCount < 0 || p.Resources.GPUMemoryGiB < 0:
		return fmt.Errorf("catalog: package %q has negative disk or gpu resources", p.ID)
	case p.Resources.GPUMemoryGiB > 0 && p.Resources.GPUCount == 0:
		return fmt.Errorf("catalog: package %q has gpu memory but no gpu", p.ID)
	case p.ExtensionOf == p.ID && p.ExtensionOf != "":
		return fmt.Errorf("catalog: package %q extends itself", p.ID)
	case p.PriceMicros < 0:
		return fmt.Errorf("catalog: package %q has a negative price", p.ID)
	}
	return nil
}

// read checks each override given and returns them, every field zero when edge is absent
func (fe *fileEdge) read(id string) (Edge, error) {
	var e Edge
	if fe == nil {
		return e, nil
	}
	for _, z := range []struct {
		key  string
		raw  *string
		into *int64
	}{
		{"bytes", fe.Bytes, &e.Bytes},
		{"bytes_per_second", fe.BytesPerSecond, &e.BytesPerSecond},
		{"transfer_max_bytes", fe.TransferMaxBytes, &e.TransferMaxBytes},
		{"egress_bytes", fe.EgressBytes, &e.EgressBytes},
		{"egress_bytes_per_second", fe.EgressBytesPerSecond, &e.EgressBytesPerSecond},
	} {
		if z.raw == nil {
			continue
		}
		n, ok := bytesize.Parse(*z.raw)
		if !ok {
			return Edge{}, fmt.Errorf("catalog: package %q edge.%s must be 1 byte to 1 TiB, "+
				"a whole number in a string optionally ending in KiB, MiB or GiB", id, z.key)
		}
		*z.into = n
	}
	for _, n := range []struct {
		key  string
		raw  *int
		into *int
	}{
		{"requests_per_second", fe.RequestsPerSecond, &e.RequestsPerSecond},
		{"requests_burst", fe.RequestsBurst, &e.RequestsBurst},
		{"queued_requests", fe.QueuedRequests, &e.QueuedRequests},
		{"websockets", fe.WebSockets, &e.WebSockets},
		{"egress_requests_in_flight", fe.EgressRequestsInFlight, &e.EgressRequestsInFlight},
	} {
		if n.raw == nil {
			continue
		}
		if *n.raw < 1 {
			return Edge{}, fmt.Errorf("catalog: package %q edge.%s must be a positive whole "+
				"number", id, n.key)
		}
		*n.into = *n.raw
	}
	return e, nil
}

// read checks the hosts of a restricted package, exact lowercase names only
func (fe *fileEgress) read(id string) ([]string, error) {
	if fe == nil {
		return nil, nil
	}
	if len(fe.Allow) == 0 {
		return nil, fmt.Errorf("catalog: package %q egress.allow must name a host", id)
	}
	for i, h := range fe.Allow {
		if !hostname.Valid(h) {
			return nil, fmt.Errorf("catalog: package %q egress.allow entry %d must be a "+
				"lowercase host name with no wildcard or final dot", id, i+1)
		}
		if slices.Contains(fe.Allow[:i], h) {
			return nil, fmt.Errorf("catalog: package %q egress.allow lists %q more than once",
				id, h)
		}
	}
	return slices.Clone(fe.Allow), nil
}

// sameHosts says whether a and b hold the same hosts in any order
func sameHosts(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// Get returns the package with the given id
func (c *Catalog) Get(id string) (Package, bool) {
	p, ok := c.byID[id]
	return p, ok
}

// All returns every package sorted by price then id
func (c *Catalog) All() []Package {
	return append([]Package(nil), c.all...)
}
