package provider

import (
	"fmt"
	"maps"
	"slices"
)

// Features lists the providers whose live tests passed for each data-plane feature
type Features struct {
	Preview, SSH, Secrets []string
	// BoatNativeSSH reaches Boat through its own SSH server instead of the gate
	BoatNativeSSH bool
	// Images is the base image version each provider's creates start from, absent for none
	Images map[string]string
	// ImageExempt lists providers placed on their own image while images.accept is set
	ImageExempt []string
}

// Registry holds the configured providers by name and in the order they were listed
type Registry struct {
	order  []Provider
	byName map[string]Provider
	flags  map[string]flags
	images map[string]string
	exempt []string
}

// flags are the data-plane features the registry computed for one provider at start
type flags struct{ preview, ssh, secrets, native bool }

// NewRegistry refuses two providers with one name and a listed provider lacking what it needs
func NewRegistry(order []Provider, f Features) (*Registry, error) {
	r := &Registry{
		order:  slices.Clone(order),
		byName: make(map[string]Provider, len(order)),
		flags:  make(map[string]flags, len(order)),
		images: maps.Clone(f.Images),
		exempt: slices.Clone(f.ImageExempt),
	}
	for _, p := range order {
		name := p.Name()
		if _, dup := r.byName[name]; dup {
			return nil, fmt.Errorf("provider: two providers named %q", name)
		}
		r.byName[name] = p
		fl, err := features(p, f)
		if err != nil {
			return nil, err
		}
		r.flags[name] = fl
	}
	return r, nil
}

// features computes p's flags from the lists, and refuses a listing p cannot serve
func features(p Provider, f Features) (flags, error) {
	name := p.Name()
	_, gater := p.(Gater)
	fl := flags{secrets: slices.Contains(f.Secrets, name)}
	if slices.Contains(f.Preview, name) {
		if !gater {
			return flags{}, fmt.Errorf("provider: %s is listed for previews but has no gate route",
				name)
		}
		fl.preview = true
	}
	if slices.Contains(f.SSH, name) {
		_, native := p.(NativeSSHer)
		switch {
		case f.BoatNativeSSH && name == "boat":
			if !native {
				return flags{}, fmt.Errorf("provider: %s is listed for ssh on its native path "+
					"but has no SSH server of its own", name)
			}
			fl.native = true
		case !gater:
			return flags{}, fmt.Errorf("provider: %s is listed for ssh but has no gate route", name)
		}
		fl.ssh = true
	}
	return fl, nil
}

// Get returns the provider with the given name
func (r *Registry) Get(name string) (Provider, bool) {
	p, ok := r.byName[name]
	return p, ok
}

// Ordered returns the providers in the order they were configured
func (r *Registry) Ordered() []Provider {
	return slices.Clone(r.order)
}

// Capabilities is what p reports, with SSH, Preview and Secrets as computed at start
func (r *Registry) Capabilities(p Provider) Capabilities {
	c := p.Capabilities()
	fl := r.flags[p.Name()]
	c.SSH, c.Preview, c.Secrets = fl.ssh, fl.preview, fl.secrets
	return c
}

// NativeSSH reports whether p's SSH runs through its own server instead of the gate
func (r *Registry) NativeSSH(p Provider) bool {
	return r.flags[p.Name()].native
}

// ImageVersion is the base image version p's creates start from, empty for none
func (r *Registry) ImageVersion(p Provider) string {
	return r.images[p.Name()]
}

// ImageAccepted reports whether p may host placements when accept lists the allowed versions
func (r *Registry) ImageAccepted(p Provider, accept []string) bool {
	if len(accept) == 0 || slices.Contains(r.exempt, p.Name()) {
		return true
	}
	v := r.images[p.Name()]
	return v != "" && slices.Contains(accept, v)
}

// ImageExempt reports whether p is placed on its own image while images.accept is set
func (r *Registry) ImageExempt(p Provider) bool {
	return slices.Contains(r.exempt, p.Name())
}
