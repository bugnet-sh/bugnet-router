package catalog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oynozan/bugnet-router/internal/provider"
)

func TestCatalogLoad(t *testing.T) {
	c, err := Load("testdata/packages.json")
	if err != nil {
		t.Fatal(err)
	}
	all := c.All()
	if len(all) != 3 {
		t.Fatalf("got %d packages", len(all))
	}
	if all[0].ID != "cpu-small-10m" || all[2].ID != "gpu-small-30m" {
		t.Fatalf("not sorted by price: %+v", all)
	}
	g, ok := c.Get("gpu-small-30m")
	want := provider.Resources{
		VCPU: 4, MemoryMiB: 16384, DiskGiB: 50, GPUCount: 1, GPUMemoryGiB: 20,
	}
	if !ok || g.Resources != want || g.Minutes != 30 || g.PriceMicros != 1500000 {
		t.Fatalf("gpu package: %+v %v", g, ok)
	}
	if _, ok := c.Get("nope"); ok {
		t.Fatal("unknown id found")
	}
}

func TestCatalogRejects(t *testing.T) {
	res := `"resources":{"vcpu":1,"memory_mib":512,"disk_gib":1}`
	pkg := func(id string, extra string) string {
		return `{"id":"` + id + `",` + res + `,"minutes":5,"price_micros":10` + extra + `}`
	}
	cases := map[string]string{
		"duplicate":   pkg("a", "") + "," + pkg("a", ""),
		"empty id":    pkg("", ""),
		"bad minutes": strings.Replace(pkg("a", ""), `"minutes":5`, `"minutes":0`, 1),
		"bad vcpu":    strings.Replace(pkg("a", ""), `"vcpu":1`, `"vcpu":0`, 1),
		"bad memory":  strings.Replace(pkg("a", ""), `"memory_mib":512`, `"memory_mib":0`, 1),
		"neg price":   strings.Replace(pkg("a", ""), `"price_micros":10`, `"price_micros":-1`, 1),
		"unknown ext": pkg("a", `,"extension_of":"zzz"`),
	}
	for name, body := range cases {
		p := filepath.Join(t.TempDir(), "p.json")
		if err := os.WriteFile(p, []byte(`{"packages":[`+body+`]}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(p); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Error("missing file accepted")
	}
}

func TestCatalogExtension(t *testing.T) {
	body := `{"packages":[
		{"id":"a","resources":{"vcpu":1,"memory_mib":512,"disk_gib":1},"minutes":5,
		"price_micros":10},
		{"id":"a-ext","resources":{"vcpu":1,"memory_mib":512,"disk_gib":1},"minutes":5,
		"price_micros":5,"extension_of":"a"}]}`
	p := filepath.Join(t.TempDir(), "p.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := c.Get("a-ext"); e.ExtensionOf != "a" {
		t.Fatalf("%+v", e)
	}
	if c.All()[0].ID != "a-ext" {
		t.Fatal("expected price ordering")
	}
}
