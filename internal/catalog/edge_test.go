package catalog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// load writes a catalog of the given packages and loads it
func load(t *testing.T, packages string) (*Catalog, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "p.json")
	if err := os.WriteFile(p, []byte(`{"packages":[`+packages+`]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

// small is a package with extra JSON fields appended
func small(id, extra string) string {
	return `{"id":"` + id + `","resources":{"vcpu":1,"memory_mib":512,"disk_gib":1},` +
		`"minutes":10,"price_micros":50000` + extra + `}`
}

func TestCatalogEdgeOverrides(t *testing.T) {
	c, err := load(t, small("small-10m", `,"edge": {"bytes": "4GiB",
		"bytes_per_second": "10MiB", "requests_per_second": 200, "requests_burst": 400,
		"queued_requests": 128, "websockets": 64, "transfer_max_bytes": "4GiB",
		"egress_bytes": "1GiB", "egress_bytes_per_second": "4MiB",
		"egress_requests_in_flight": 64}`)+","+small("plain", ""))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := c.Get("small-10m")
	want := Edge{
		Bytes: 4 << 30, BytesPerSecond: 10 << 20, RequestsPerSecond: 200, RequestsBurst: 400,
		QueuedRequests: 128, WebSockets: 64, TransferMaxBytes: 4 << 30, EgressBytes: 1 << 30,
		EgressBytesPerSecond: 4 << 20, EgressRequestsInFlight: 64,
	}
	if p.Edge != want {
		t.Fatalf("edge %+v, want %+v", p.Edge, want)
	}
	if plain, _ := c.Get("plain"); plain.Edge != (Edge{}) || plain.EgressAllow != nil {
		t.Fatalf("a package without overrides got %+v", plain)
	}
	partial, err := load(t, small("p", `,"edge":{"bytes":"1GiB"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := partial.Get("p"); got.Edge != (Edge{Bytes: 1 << 30}) {
		t.Fatalf("one override gave %+v, want the others left to the settings", got.Edge)
	}

	for name, c := range map[string]struct{ extra, want string }{
		"unknown key inside edge": {`,"edge":{"bytez":"1GiB"}`, `unknown field "bytez"`},
		"bytes in GB":             {`,"edge":{"bytes":"4GB"}`, `"small" edge.bytes must be`},
		"zero pace":               {`,"edge":{"bytes_per_second":"0"}`, "edge.bytes_per_second"},
		"bytes as a number":       {`,"edge":{"bytes":4096}`, "edge.bytes"},
		"zero requests": {`,"edge":{"requests_per_second":0}`,
			"edge.requests_per_second"},
		"negative burst":  {`,"edge":{"requests_burst":-1}`, "edge.requests_burst"},
		"zero queue":      {`,"edge":{"queued_requests":0}`, "edge.queued_requests"},
		"zero websockets": {`,"edge":{"websockets":0}`, "edge.websockets"},
		"zero transfer cap": {`,"edge":{"transfer_max_bytes":"0"}`,
			"edge.transfer_max_bytes"},
		"zero egress bytes": {`,"edge":{"egress_bytes":"0MiB"}`, "edge.egress_bytes"},
		"zero egress pace": {`,"edge":{"egress_bytes_per_second":"0"}`,
			"edge.egress_bytes_per_second"},
		"zero egress in flight": {`,"edge":{"egress_requests_in_flight":0}`,
			"edge.egress_requests_in_flight"},
	} {
		_, err := load(t, small("small", c.extra))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error with %q", name, err, c.want)
		}
	}
}

func TestCatalogEgressAllow(t *testing.T) {
	c, err := load(t, small("locked", `,"egress":{"allow":["huggingface.co","api.github.com"]}`))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := c.Get("locked")
	if strings.Join(p.EgressAllow, ",") != "huggingface.co,api.github.com" {
		t.Fatalf("egress allow %v", p.EgressAllow)
	}
	for name, c := range map[string]struct{ packages, want string }{
		"empty allow":    {small("a", `,"egress":{"allow":[]}`), "egress.allow must name a host"},
		"no allow":       {small("a", `,"egress":{}`), "egress.allow must name a host"},
		"unknown key":    {small("a", `,"egress":{"deny":["x.example"]}`), `unknown field "deny"`},
		"capital letter": {small("a", `,"egress":{"allow":["HF.co"]}`), "egress.allow entry 1"},
		"final dot":      {small("a", `,"egress":{"allow":["hf.co."]}`), "egress.allow entry 1"},
		"wildcard":       {small("a", `,"egress":{"allow":["*.hf.co"]}`), "egress.allow entry 1"},
		"address": {small("a", `,"egress":{"allow":["hf.co","10.0.0.1"]}`),
			"egress.allow entry 2"},
		"twice": {small("a", `,"egress":{"allow":["hf.co","hf.co"]}`),
			`egress.allow lists "hf.co" more than once`},
		"extension unlike its package": {small("a", `,"egress":{"allow":["hf.co"]}`) + "," +
			small("b", `,"extension_of":"a"`), "the same egress.allow as \"a\""},
	} {
		_, err := load(t, c.packages)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error with %q", name, err, c.want)
		}
	}
	_, err = load(t, small("a", `,"egress":{"allow":["b.example","a.example"]}`)+","+
		small("b", `,"extension_of":"a","egress":{"allow":["a.example","b.example"]}`))
	if err != nil {
		t.Fatalf("an extension with its package's hosts in another order: %v", err)
	}
}
