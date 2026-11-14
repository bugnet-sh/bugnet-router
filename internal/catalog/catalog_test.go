package catalog

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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
		t.Fatalf("not sorted by resources: %+v", all)
	}
	g, ok := c.Get("gpu-small-30m")
	want := provider.Resources{
		VCPU: 4, MemoryMiB: 16384, DiskGiB: 50, GPUCount: 1, GPUMemoryGiB: 20,
	}
	if !ok || g.Resources != want || g.Minutes != 30 || g.PriceMicros != 1500000 ||
		g.GPUModel != "NVIDIA RTX 4000 SFF Ada" {
		t.Fatalf("gpu package: %+v %v", g, ok)
	}
	if cpu, _ := c.Get("cpu-small-10m"); cpu.GPUModel != "" {
		t.Fatalf("cpu package has a gpu model %q", cpu.GPUModel)
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
	if c.All()[0].ID != "a" {
		t.Fatal("expected the same resources and minutes to order by id")
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
		if g, _ := c.Get(id); g.GPUModel != "NVIDIA L4" || g.Resources.GPUCount != 2 {
			t.Fatalf("%s: %+v", id, g)
		}
	}
}

func writeCatalog(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "p.json")
	if err := os.WriteFile(p, []byte(`{"packages":[`+body+`]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const smallRes = `"resources":{"vcpu":1,"memory_mib":512,"disk_gib":1}`

func TestLivePackageHasNoPrice(t *testing.T) {
	c, err := Load(writeCatalog(t, `{"id":"a",`+smallRes+`,"minutes":5}`))
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := c.Get("a"); p.FixedPrice || p.PriceMicros != 0 {
		t.Fatalf("%+v", p)
	}
}

func TestFixedZeroIsFree(t *testing.T) {
	c, err := Load(writeCatalog(t, `{"id":"a",`+smallRes+`,"minutes":5,"price_micros":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := c.Get("a"); !p.FixedPrice || p.PriceMicros != 0 {
		t.Fatalf("%+v", p)
	}
}

func TestMixedExtensionRefused(t *testing.T) {
	live := `{"id":"a",` + smallRes + `,"minutes":5}`
	fixed := `{"id":"a",` + smallRes + `,"minutes":5,"price_micros":10}`
	liveExt := `{"id":"b",` + smallRes + `,"minutes":5,"extension_of":"a"}`
	fixedExt := `{"id":"b",` + smallRes + `,"minutes":5,"price_micros":10,"extension_of":"a"}`
	for name, body := range map[string]string{
		"live start fixed extension": live + "," + fixedExt,
		"fixed start live extension": fixed + "," + liveExt,
	} {
		if _, err := Load(writeCatalog(t, body)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	for name, body := range map[string]string{
		"live": live + "," + liveExt, "fixed": fixed + "," + fixedExt,
	} {
		if _, err := Load(writeCatalog(t, body)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestAllOrderIgnoresPrice(t *testing.T) {
	big := `{"id":"a","resources":{"vcpu":4,"memory_mib":512,"disk_gib":1},` +
		`"minutes":5,"price_micros":1}`
	smallLong := `{"id":"b",` + smallRes + `,"minutes":20,"price_micros":1}`
	smallShort := `{"id":"c",` + smallRes + `,"minutes":5,"price_micros":900}`
	smallShortB := `{"id":"d",` + smallRes + `,"minutes":5,"price_micros":10}`
	c, err := Load(writeCatalog(t, big+","+smallLong+","+smallShort+","+smallShortB))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range c.All() {
		got = append(got, p.ID)
	}
	if want := []string{"c", "d", "b", "a"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
