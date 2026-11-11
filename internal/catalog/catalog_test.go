package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
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
		GPUModel: "NVIDIA RTX 4000 SFF Ada",
	}
	if !ok || g.Resources != want || g.Minutes != 30 || g.PriceMicros != 1500000 ||
		g.GPUModel != "NVIDIA RTX 4000 SFF Ada" {
		t.Fatalf("gpu package: %+v %v", g, ok)
	}
	cpu, _ := c.Get("cpu-small-10m")
	if cpu.Resources != (provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10}) ||
		cpu.GPUModel != "" {
		t.Fatalf("cpu package: %+v", cpu)
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
	gpu := func(id, model, extra string) string {
		return strings.Replace(pkg(id, extra), `"disk_gib":1`,
			`"disk_gib":1,"gpu_count":1,"gpu_memory_gib":24,"gpu_model":`+model, 1)
	}
	cases := map[string]string{
		"duplicate":   pkg("a", "") + "," + pkg("a", ""),
		"empty id":    pkg("", ""),
		"bad minutes": strings.Replace(pkg("a", ""), `"minutes":5`, `"minutes":0`, 1),
		"bad vcpu":    strings.Replace(pkg("a", ""), `"vcpu":1`, `"vcpu":0`, 1),
		"bad memory":  strings.Replace(pkg("a", ""), `"memory_mib":512`, `"memory_mib":0`, 1),
		"neg price":   strings.Replace(pkg("a", ""), `"price_micros":10`, `"price_micros":-1`, 1),
		"unknown ext": pkg("a", `,"extension_of":"zzz"`),
		"neg disk":    strings.Replace(pkg("a", ""), `"disk_gib":1`, `"disk_gib":-1`, 1),
		"neg gpu": strings.Replace(
			pkg("a", ""), `"disk_gib":1`, `"disk_gib":1,"gpu_count":-1`, 1),
		"neg gpu mem": strings.Replace(
			pkg("a", ""), `"disk_gib":1`, `"disk_gib":1,"gpu_memory_gib":-1`, 1),
		"gpu mem without gpu": strings.Replace(
			pkg("a", ""), `"disk_gib":1`, `"disk_gib":1,"gpu_memory_gib":8`, 1),
		"self ext": pkg("a", `,"extension_of":"a"`),
		"ext of ext": pkg("a", "") + "," + pkg("b", `,"extension_of":"a"`) + "," +
			pkg("c", `,"extension_of":"b"`),
		"ext resizes": pkg("a", "") + "," + strings.Replace(
			pkg("b", `,"extension_of":"a"`), `"vcpu":1`, `"vcpu":2`, 1),
		"ext differs in gpu": pkg("a", "") + "," + strings.Replace(
			pkg("b", `,"extension_of":"a"`), `"disk_gib":1`, `"disk_gib":1,"gpu_count":1`, 1),
		"gpu without model": strings.Replace(
			pkg("a", ""), `"disk_gib":1`, `"disk_gib":1,"gpu_count":1`, 1),
		"model without gpu": strings.Replace(
			pkg("a", ""), `"disk_gib":1`, `"disk_gib":1,"gpu_model":"NVIDIA L4"`, 1),
		"blank model":   gpu("a", `"  "`, ""),
		"padded model":  gpu("a", `" NVIDIA L4"`, ""),
		"control model": gpu("a", `"NVIDIA\u0007L4"`, ""),
		"long model":    gpu("a", `"`+strings.Repeat("x", 65)+`"`, ""),
		"ext differs in gpu model": gpu("a", `"NVIDIA L4"`, "") + "," +
			gpu("b", `"NVIDIA A10G"`, `,"extension_of":"a"`),
		"unknown model":    gpu("a", `"NVIDIA RTX 9090"`, ""),
		"lowercase model":  gpu("a", `"nvidia l4"`, ""),
		"provider's model": gpu("a", `"A100-SXM4-80GB"`, ""),
		"vague model":      gpu("a", `"NVIDIA A100"`, ""),
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

func TestCatalogGPUModel(t *testing.T) {
	body := `{"packages":[
		{"id":"g","resources":{"vcpu":4,"memory_mib":16384,"disk_gib":50,"gpu_count":2,
		"gpu_memory_gib":24,"gpu_model":"NVIDIA L4"},"minutes":30,"price_micros":10},
		{"id":"g-ext","resources":{"vcpu":4,"memory_mib":16384,"disk_gib":50,"gpu_count":2,
		"gpu_memory_gib":24,"gpu_model":"NVIDIA L4"},"minutes":30,"price_micros":5,
		"extension_of":"g"}]}`
	p := filepath.Join(t.TempDir(), "p.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"g", "g-ext"} {
		if g, _ := c.Get(id); g.GPUModel != "NVIDIA L4" || g.Resources.GPUModel != "NVIDIA L4" ||
			g.Resources.GPUCount != 2 {
			t.Fatalf("%s: %+v", id, g)
		}
	}
}

// TestCatalogKnowsEveryModel loads one package per canonical model
func TestCatalogKnowsEveryModel(t *testing.T) {
	var pkgs []string
	for i, m := range gpu.Models() {
		pkgs = append(pkgs, fmt.Sprintf(`{"id":"g%d","resources":{"vcpu":4,"memory_mib":16384,`+
			`"disk_gib":50,"gpu_count":1,"gpu_memory_gib":24,"gpu_model":%q},"minutes":10,`+
			`"price_micros":10}`, i, m))
	}
	p := filepath.Join(t.TempDir(), "p.json")
	body := `{"packages":[` + strings.Join(pkgs, ",") + `]}`
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range gpu.Models() {
		if g, _ := c.Get(fmt.Sprintf("g%d", i)); g.Resources.GPUModel != m {
			t.Errorf("package for %q carries %q", m, g.Resources.GPUModel)
		}
	}
}
