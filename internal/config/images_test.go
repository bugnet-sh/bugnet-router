package config

import (
	"strings"
	"testing"
)

func TestImagesDefaults(t *testing.T) {
	for name, c := range map[string]Config{"example": loadExample(t), "empty": loadEmpty(t)} {
		t.Run(name, func(t *testing.T) {
			eq(t, len(c.Images.Accept), 0)
			eq(t, c.Images.Exempt, []string{"local"})
			eq(t, c.Images.Channel, "prod")
			eq(t, c.Images.Koyeb.Version, "")
			eq(t, c.Images.Versions(c.Providers), map[string]string{})
		})
	}
	eq(t, loadDev(t).Images.Channel, "dev")
}

func loadEmpty(t *testing.T) Config {
	t.Helper()
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const e2bTemplates = `[[e2b.templates]]
size = "2x2"
template = "bugnet-2026-11-1-2x2"
vcpu = 2
memory_mib = 2048
disk_gib = 10
`

func TestImagesSettingsRead(t *testing.T) {
	c, err := LoadFile(writeFile(t, `providers = ["koyeb", "boat", "boxd", "e2b"]

[images]
accept = ["2026.11.1"]
channel = "dev"

[images.koyeb]
version = "2026.11.1"
ref = "ghcr.io/x/bugnet-base@sha256:`+strings.Repeat("a", 64)+`"
os = "ubuntu-24.04"
tools = { git = "2.43", node = "24.22", pnpm = "12.18" }

[images.boxd]
version = "2026.11.1"
refs = { 2x2 = "bugnet-dev-2026-11-1-2x2" }

[images.boat]
version = "2026.11.1"
ref = "bugnet-dev-2026-11-1"

[images.e2b]
version = "2026.11.1"

`+e2bTemplates))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Images.Versions(c.Providers), map[string]string{
		"koyeb": "2026.11.1", "boxd": "2026.11.1", "boat": "2026.11.1", "e2b": "2026.11.1"})
	eq(t, c.ImageTools["koyeb"], ImageTools{Git: "2.43", Node: "24.22"})
	eq(t, c.Images.Koyeb.Tools["pnpm"], "12.18")
}

func TestImagesSettingsStrict(t *testing.T) {
	digest := "ghcr.io/x/b@sha256:" + strings.Repeat("a", 64)
	cases := []struct{ name, toml, want string }{
		{"accept version", "[images]\naccept = [\"2026-11-1\"]", "images.accept"},
		{"provider version", "[images.koyeb]\nversion = \"v1\"\nref = \"" + digest + "\"",
			"images.koyeb.version"},
		{"koyeb no ref", "[images.koyeb]\nversion = \"2026.11.1\"", "images.koyeb"},
		{"boat no ref", "[images.boat]\nversion = \"2026.11.1\"", "images.boat"},
		{"boxd no refs", "[images.boxd]\nversion = \"2026.11.1\"", "images.boxd"},
		{"e2b no templates", "[images.e2b]\nversion = \"2026.11.1\"", "images.e2b"},
		{"ref and refs", "[images.boxd]\nref = \"x\"\nrefs = { a = \"b\" }", "images.boxd"},
		{"koyeb ref without digest",
			"[images.koyeb]\nversion = \"2026.11.1\"\nref = \"ghcr.io/x/b:1\"",
			"images.koyeb.ref"},
		{"lium ref without digest", "[images.lium]\nref = \"ghcr.io/x/b:1\"",
			"images.lium.ref must name an image by digest"},
		{"hf ref without digest", "[images.hf]\nversion = \"2026.11.1\"\nref = \"ghcr.io/x/b\"",
			"images.hf.ref must name an image by digest"},
		{"lium no ref", "[images.lium]\nversion = \"2026.11.1\"", "images.lium"},
		{"hf refs", "[images.hf]\nrefs = { a = \"" + digest + "\" }", "images.hf.refs"},
		{"unknown tool", "[images.e2b]\ntools = { rust = \"1\" }", "images.e2b.tools"},
		{"channel", "[images]\nchannel = \"stage\"", "images.channel"},
		{"accept with no provider",
			"providers = [\"koyeb\"]\n[images]\naccept = [\"2026.11.1\"]", "images.accept"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := LoadFile(writeFile(t, c.toml))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want one naming %s", err, c.want)
			}
		})
	}
	// An exempt provider keeps accept valid
	_, err := LoadFile(writeFile(t,
		"providers = [\"koyeb\"]\n[images]\naccept = [\"2026.11.1\"]\nexempt = [\"koyeb\"]"))
	eq(t, err, nil)
}

func TestImagesGPUTables(t *testing.T) {
	digest := "ghcr.io/bugnet-sh/bugnet-base@sha256:" + strings.Repeat("b", 64)
	c, err := LoadFile(writeFile(t, `providers = ["lium", "hf", "e2b"]

[images.lium]
version = "2026.11.3"
ref = "`+digest+`"
tools = { git = "2.43" }

[images.hf]
version = "2026.11.3"
ref = "`+digest+`"
os = "ubuntu-24.04"
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Images.Lium, ProviderImage{
		Version: "2026.11.3", Ref: digest, Tools: map[string]string{"git": "2.43"},
	})
	eq(t, c.Images.HF, ProviderImage{Version: "2026.11.3", Ref: digest, OS: "ubuntu-24.04"})
	for _, name := range []string{"lium", "hf"} {
		got, ok := c.Images.Of(name)
		eq(t, ok, true)
		eq(t, got.Ref, digest)
	}
	eq(t, c.Images.Versions(c.Providers), map[string]string{"lium": "2026.11.3", "hf": "2026.11.3"})
	eq(t, c.ImageTools["lium"], ImageTools{Git: "2.43"})
	eq(t, c.ImageTools["hf"], ImageTools{})
	empty := loadEmpty(t)
	eq(t, empty.Images.Lium, ProviderImage{})
	eq(t, empty.Images.HF, ProviderImage{})
}
