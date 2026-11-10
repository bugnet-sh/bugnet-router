package config

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/image"
)

// Images is the [images] table
type Images struct {
	Accept  []string
	Exempt  []string
	Channel string
	E2B     ProviderImage
	Boxd    ProviderImage
	Koyeb   ProviderImage
	Boat    ProviderImage
}

// ProviderImage is one [images.<provider>] table, what that provider's creates start from
type ProviderImage struct {
	// Version is empty for the provider's own image
	Version string
	// Ref is one artifact for every size, Koyeb and Boat
	Ref string
	// Refs maps a size to its artifact, boxd
	Refs  map[string]string
	OS    string
	Tools map[string]string
}

// Of returns the table of the named provider, false for one without a table
func (i Images) Of(name string) (ProviderImage, bool) {
	switch name {
	case "e2b":
		return i.E2B, true
	case "boxd":
		return i.Boxd, true
	case "koyeb":
		return i.Koyeb, true
	case "boat":
		return i.Boat, true
	}
	return ProviderImage{}, false
}

// Versions maps each listed provider that has an image version to it
func (i Images) Versions(providers []string) map[string]string {
	out := map[string]string{}
	for _, name := range providers {
		if p, ok := i.Of(name); ok && p.Version != "" {
			out[name] = p.Version
		}
	}
	return out
}

// imagesFile is the [images] table as written
type imagesFile struct {
	Accept  []string  `toml:"accept"`
	Exempt  []string  `toml:"exempt"`
	Channel string    `toml:"channel"`
	E2B     imageFile `toml:"e2b"`
	Boat    imageFile `toml:"boat"`
	Boxd    imageFile `toml:"boxd"`
	Koyeb   imageFile `toml:"koyeb"`
}

type imageFile struct {
	Version string            `toml:"version"`
	Ref     string            `toml:"ref"`
	Refs    map[string]string `toml:"refs"`
	OS      string            `toml:"os"`
	Tools   map[string]string `toml:"tools"`
}

func defaultImages() imagesFile {
	return imagesFile{Accept: []string{}, Exempt: []string{"local"}, Channel: "prod"}
}

// read checks the image settings against the listed providers and returns them with the five
// tools the estimator's rules read for each listed provider
func (f imagesFile) read(
	providers []string, templates int,
) (Images, map[string]ImageTools, error) {
	out := Images{Accept: slices.Clone(f.Accept), Exempt: slices.Clone(f.Exempt),
		Channel: f.Channel}
	if out.Channel != "prod" && out.Channel != "dev" {
		return out, nil, errors.New("images.channel must be prod or dev")
	}
	for _, list := range []struct {
		key string
		v   []string
	}{{"images.accept", out.Accept}, {"images.exempt", out.Exempt}} {
		for _, v := range list.v {
			if list.key == "images.accept" && !image.VersionPattern.MatchString(v) {
				return out, nil, fmt.Errorf("%s must hold versions such as 2026.11.1", list.key)
			}
			if list.key == "images.exempt" && !knownProviders[v] {
				return out, nil, fmt.Errorf("%s names a provider that does not exist", list.key)
			}
		}
	}
	tools := map[string]ImageTools{}
	for _, p := range []struct {
		name string
		file imageFile
		dst  *ProviderImage
	}{{"boat", f.Boat, &out.Boat}, {"boxd", f.Boxd, &out.Boxd}, {"e2b", f.E2B, &out.E2B},
		{"koyeb", f.Koyeb, &out.Koyeb}} {
		pi, err := p.file.read("images."+p.name, p.name, templates)
		if err != nil {
			return out, nil, err
		}
		*p.dst = pi
		if slices.Contains(providers, p.name) {
			t := pi.Tools
			tools[p.name] = ImageTools{Git: t["git"], Tar: t["tar"], Python3: t["python3"],
				Node: t["node"], Docker: t["docker"]}
		}
	}
	if len(out.Accept) > 0 && !anyAccepted(out, providers) {
		return out, nil, errors.New("images.accept lists no version that a provider in " +
			"providers has, and images.exempt names none of them")
	}
	return out, tools, nil
}

// anyAccepted reports whether some listed provider is exempt or on a version accept lists
func anyAccepted(i Images, providers []string) bool {
	for _, name := range providers {
		p, _ := i.Of(name)
		if slices.Contains(i.Exempt, name) ||
			p.Version != "" && slices.Contains(i.Accept, p.Version) {
			return true
		}
	}
	return false
}

func (f imageFile) read(key, name string, templates int) (ProviderImage, error) {
	p := ProviderImage(f)
	if p.Version != "" && !image.VersionPattern.MatchString(p.Version) {
		return p, fmt.Errorf("%s.version must look like 2026.11.1", key)
	}
	if p.Ref != "" && len(p.Refs) > 0 {
		return p, fmt.Errorf("%s sets both ref and refs", key)
	}
	for _, ref := range p.Refs {
		if ref == "" {
			return p, fmt.Errorf("%s.refs has an empty artifact", key)
		}
	}
	switch name {
	case "boxd":
		if p.Ref != "" {
			return p, fmt.Errorf("%s.ref is for one artifact, boxd takes refs by size", key)
		}
	case "e2b":
		if p.Ref != "" || len(p.Refs) > 0 {
			return p, fmt.Errorf("%s names no ref, e2b starts from [[e2b.templates]]", key)
		}
	default:
		if len(p.Refs) > 0 {
			return p, fmt.Errorf("%s.refs is for boxd, %s takes ref", key, name)
		}
	}
	if p.Version != "" {
		var missing bool
		switch name {
		case "boxd":
			missing = len(p.Refs) == 0
		case "e2b":
			missing = templates == 0
		default:
			missing = p.Ref == ""
		}
		if missing {
			return p, fmt.Errorf("%s has a version and no artifact to start from", key)
		}
	}
	if name == "koyeb" && p.Ref != "" && !strings.Contains(p.Ref, "@sha256:") {
		return p, fmt.Errorf("%s.ref must name an image by digest, such as name@sha256:hex", key)
	}
	// A tool left empty is one nobody measured, which a missing key says too
	p.Tools = maps.Clone(p.Tools)
	for tool, v := range p.Tools {
		if !slices.Contains(image.Tools, tool) {
			return p, fmt.Errorf("%s.tools has a key that is not a tool the image lists", key)
		}
		if v == "" {
			delete(p.Tools, tool)
		}
	}
	if len(p.Tools) == 0 {
		p.Tools = nil
	}
	if len(p.Refs) == 0 {
		p.Refs = nil
	}
	return p, nil
}
