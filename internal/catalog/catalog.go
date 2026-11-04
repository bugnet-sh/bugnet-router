// Package catalog loads the priced packages the router sells
package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/oynozan/bugnet-router/internal/provider"
)

// Package is one purchasable bundle of resources and time
type Package struct {
	ID          string
	Resources   provider.Resources
	Minutes     int
	PriceMicros int64
	ExtensionOf string
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

// Get returns the package with the given id
func (c *Catalog) Get(id string) (Package, bool) {
	p, ok := c.byID[id]
	return p, ok
}

// All returns every package sorted by price then id
func (c *Catalog) All() []Package {
	return append([]Package(nil), c.all...)
}
