package provider

import (
	"fmt"
	"slices"
)

// Registry holds the configured providers by name and in preference order
type Registry struct {
	order  []Provider
	byName map[string]Provider
}

// NewRegistry panics when two providers share a name, since refs are routed by name
func NewRegistry(order []Provider) *Registry {
	r := &Registry{order: slices.Clone(order), byName: make(map[string]Provider, len(order))}
	for _, p := range order {
		if _, dup := r.byName[p.Name()]; dup {
			panic(fmt.Sprintf("provider: two providers named %q", p.Name()))
		}
		r.byName[p.Name()] = p
	}
	return r
}

// Get returns the provider with the given name
func (r *Registry) Get(name string) (Provider, bool) {
	p, ok := r.byName[name]
	return p, ok
}

// Ordered returns the providers most preferred first
func (r *Registry) Ordered() []Provider {
	return slices.Clone(r.order)
}
