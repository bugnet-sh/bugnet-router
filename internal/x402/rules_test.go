package x402

import (
	"bytes"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/hostrules"
)

// payTo is a canonical base58 key of 32 bytes
const payTo = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

const maxPrice = 500_000

// base58 encodes b for a test value, with one leading 1 for each leading zero byte
func base58(b []byte) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	n, mod, base := new(big.Int).SetBytes(b), new(big.Int), big.NewInt(58)
	var out []byte
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		out = append([]byte{alphabet[mod.Int64()]}, out...)
	}
	for i := 0; i < len(b) && b[i] == 0; i++ {
		out = append([]byte{'1'}, out...)
	}
	return string(out)
}

func hostRules() hostrules.Rules {
	return hostrules.Rules{
		BugnetNames:     []string{"bugnet.sh"},
		ProviderDomains: []string{"e2b.app", "boxd.sh"},
		SharedSuffixes:  []string{"workers.dev", "onrender.com"},
		AllowedShared:   []string{"api.workers.dev"},
		Blocked:         []string{"bad.example.com"},
	}
}

func good() Spec {
	return Spec{
		Host: "api.example.com", Prefix: "/search/", Method: "GET", MaxPriceMicros: 10_000,
		PayTo: payTo, PaidPath: "/search",
	}
}

// reason is the Refusal's reason in err, empty when err is nil
func reason(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		return ""
	}
	var r Refusal
	if !errors.As(err, &r) {
		t.Fatalf("%v is not a Refusal", err)
	}
	eq(t, err.Error(), r.Reason)
	return r.Reason
}

func TestServiceIDVectors(t *testing.T) {
	eq(t, ServiceID("POST", "agent402.tools", "/v1/auto/"), "xs_w5a4htopmpzmmhos")
	var file struct {
		V     int `json:"v"`
		Cases []struct {
			Method string `json:"method"`
			Host   string `json:"host"`
			Prefix string `json:"prefix"`
			ID     string `json:"id"`
		} `json:"cases"`
	}
	vector(t, "service-ids.json", &file)
	eq(t, file.V, 1)
	if len(file.Cases) == 0 {
		t.Fatal("service-ids.json holds no case")
	}
	for _, c := range file.Cases {
		eq(t, ServiceID(c.Method, c.Host, c.Prefix), c.ID)
	}
}

func TestServiceIDShape(t *testing.T) {
	shape := regexp.MustCompile(`^xs_[a-z2-7]{16}$`)
	ids := map[string]bool{}
	for _, in := range [][3]string{
		{"GET", "a.com", "/x/"}, {"POST", "a.com", "/x/"}, {"GET", "b.com", "/x/"},
		{"GET", "a.com", "/y/"}, {"GET", "a.com/", "x/"}, {"GET", "a.co", "m/x/"},
	} {
		id := ServiceID(in[0], in[1], in[2])
		if !shape.MatchString(id) {
			t.Fatalf("%q does not match the id shape", id)
		}
		ids[id] = true
	}
	eq(t, len(ids), 6)
}

func TestCheckSpecPasses(t *testing.T) {
	eq(t, reason(t, CheckSpec(good(), maxPrice, hostRules())), "")
	indexed := good()
	indexed.Indexed, indexed.PaidPath, indexed.Method = true, "", "POST"
	eq(t, reason(t, CheckSpec(indexed, maxPrice, hostRules())), "")
	edge := good()
	edge.MaxPriceMicros = maxPrice
	edge.Prefix = "/" + strings.Repeat("a", 198) + "/"
	edge.PaidPath = edge.Prefix + strings.Repeat("b", 400-len(edge.Prefix))
	edge.Host = "api.workers.dev"
	eq(t, len(edge.Prefix), 200)
	eq(t, len(edge.PaidPath), 400)
	eq(t, reason(t, CheckSpec(edge, maxPrice, hostRules())), "")
	low := good()
	low.MaxPriceMicros = 1
	eq(t, reason(t, CheckSpec(low, 1, hostRules())), "")
}

func TestCheckSpecRefusals(t *testing.T) {
	short := base58(bytes.Repeat([]byte{0x9c}, 31))
	cases := []struct {
		name   string
		change func(*Spec)
		want   string
	}{
		{"prefix only a slash", func(s *Spec) { s.Prefix = "/" }, "PREFIX_REFUSED"},
		{"prefix with a double slash", func(s *Spec) { s.Prefix = "/v1//"; s.PaidPath = "/v1" },
			"PREFIX_REFUSED"},
		{"prefix of 201", func(s *Spec) {
			s.Prefix = "/" + strings.Repeat("a", 199) + "/"
			s.PaidPath = s.Prefix + "x"
		}, "PREFIX_REFUSED"},
		{"prefix with no final slash", func(s *Spec) { s.Prefix = "/search" }, "PREFIX_REFUSED"},
		{"prefix with no first slash", func(s *Spec) { s.Prefix = "search/" }, "PREFIX_REFUSED"},
		{"empty prefix", func(s *Spec) { s.Prefix = "" }, "PREFIX_REFUSED"},
		{"prefix with a dot segment", func(s *Spec) {
			s.Prefix = "/a/../search/"
			s.PaidPath = "/a/../search/x"
		}, "PREFIX_REFUSED"},
		{"prefix with an encoded dot", func(s *Spec) {
			s.Prefix = "/%2e/"
			s.PaidPath = "/%2e/x"
		}, "PREFIX_REFUSED"},
		{"unlisted with no paid path", func(s *Spec) { s.PaidPath = "" }, "PREFIX_REFUSED"},
		{"indexed with a paid path", func(s *Spec) { s.Indexed = true }, "PREFIX_REFUSED"},
		{"paid path of 401", func(s *Spec) {
			s.PaidPath = "/search/" + strings.Repeat("b", 393)
		}, "PREFIX_REFUSED"},
		{"paid path with a semicolon", func(s *Spec) { s.PaidPath = "/search/a;b" },
			"PREFIX_REFUSED"},
		{"paid path with a query byte", func(s *Spec) { s.PaidPath = "/search/a b" },
			"PREFIX_REFUSED"},
		{"method PUT", func(s *Spec) { s.Method = "PUT" }, "METHOD_REFUSED"},
		{"method get", func(s *Spec) { s.Method = "get" }, "METHOD_REFUSED"},
		{"method HEAD", func(s *Spec) { s.Method = "HEAD" }, "METHOD_REFUSED"},
		{"no method", func(s *Spec) { s.Method = "" }, "METHOD_REFUSED"},
		{"price 0", func(s *Spec) { s.MaxPriceMicros = 0 }, "PRICE_REFUSED"},
		{"price below 0", func(s *Spec) { s.MaxPriceMicros = -1 }, "PRICE_REFUSED"},
		{"price 500001", func(s *Spec) { s.MaxPriceMicros = 500_001 }, "PRICE_REFUSED"},
		{"pay_to of 31 bytes", func(s *Spec) { s.PayTo = short }, "PAYTO_REFUSED"},
		{"pay_to with a leading 1", func(s *Spec) { s.PayTo = "1" + payTo }, "PAYTO_REFUSED"},
		{"pay_to of 64 bytes", func(s *Spec) {
			s.PayTo = base58(bytes.Repeat([]byte{0x9c}, 64))
		}, "PAYTO_REFUSED"},
		{"pay_to with a 0", func(s *Spec) { s.PayTo = "0" + payTo[1:] }, "PAYTO_REFUSED"},
		{"empty pay_to", func(s *Spec) { s.PayTo = "" }, "PAYTO_REFUSED"},
		{"host x.workers.dev", func(s *Spec) { s.Host = "x.workers.dev" }, "HOST_REFUSED"},
		{"blocked host", func(s *Spec) { s.Host = "bad.example.com" }, "HOST_REFUSED"},
		{"upper-case host", func(s *Spec) { s.Host = "API.example.com" }, "HOST_REFUSED"},
		{"Bugnet's own name", func(s *Spec) { s.Host = "egress.bugnet.sh" }, "HOST_REFUSED"},
		{"a provider domain", func(s *Spec) { s.Host = "x.e2b.app" }, "HOST_REFUSED"},
		{"a public suffix", func(s *Spec) { s.Host = "co.uk" }, "HOST_REFUSED"},
	}
	for _, c := range cases {
		s := good()
		c.change(&s)
		if got := reason(t, CheckSpec(s, maxPrice, hostRules())); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
	s := good()
	s.MaxPriceMicros = 1001
	eq(t, reason(t, CheckSpec(s, 1000, hostRules())), "PRICE_REFUSED")
}

func TestPrefixAloneMustPassThePathRules(t *testing.T) {
	for _, p := range []string{
		"/v1//", "/a//", "search/", "/a/../b/", "/./", "/%2e/", "/%2E%2e/", "/a;b/", "/a b/",
		"/a\\b/", "/a%2/", "/a%25/", "/é/", "/a\tb/",
	} {
		s := good()
		s.Prefix, s.Indexed, s.PaidPath = p, true, ""
		if got := reason(t, CheckSpec(s, maxPrice, hostRules())); got != "PREFIX_REFUSED" {
			t.Errorf("indexed prefix %q: got %q, want PREFIX_REFUSED", p, got)
		}
	}
	s := good()
	s.Prefix, s.PaidPath = "/a//", "/a/"
	eq(t, PathOK(s.PaidPath), true)
	eq(t, strings.HasPrefix(s.PaidPath+"/", s.Prefix), true)
	eq(t, reason(t, CheckSpec(s, maxPrice, hostRules())), "PREFIX_REFUSED")
}

func TestCheckSpecAnswersTheFirstFailingCheck(t *testing.T) {
	bad := Spec{
		Host: "x.workers.dev", Prefix: "/", Method: "PUT", MaxPriceMicros: 0, PayTo: "x",
	}
	for _, want := range []string{
		"HOST_REFUSED", "PREFIX_REFUSED", "PREFIX_REFUSED", "METHOD_REFUSED", "PRICE_REFUSED",
		"PAYTO_REFUSED", "",
	} {
		eq(t, reason(t, CheckSpec(bad, maxPrice, hostRules())), want)
		switch {
		case bad.Host != "api.example.com":
			bad.Host = "api.example.com"
		case bad.Prefix != "/search/":
			bad.Prefix = "/search/"
		case bad.PaidPath == "":
			bad.PaidPath = "/search"
		case bad.Method != "GET":
			bad.Method = "GET"
		case bad.MaxPriceMicros == 0:
			bad.MaxPriceMicros = 1
		default:
			bad.PayTo = payTo
		}
	}
}

func TestCheckSpecNeverQuotesTheSpec(t *testing.T) {
	s := good()
	s.Host = "secret-looking.workers.dev"
	err := CheckSpec(s, maxPrice, hostRules())
	if err == nil || strings.Contains(err.Error(), "secret-looking") {
		t.Fatalf("got %v", err)
	}
}

func TestPaidPathUnderPrefix(t *testing.T) {
	for _, c := range []struct{ prefix, paid, want string }{
		{"/search/", "/search", ""},
		{"/search/", "/search/", ""},
		{"/search/", "/search/web", ""},
		{"/search/", "/search/web/", ""},
		{"/v1/auto/", "/v1/auto/chat/completions", ""},
		{"/search/", "/other", "PREFIX_REFUSED"},
		{"/search/", "/searchx", "PREFIX_REFUSED"},
		{"/search/", "/searc", "PREFIX_REFUSED"},
		{"/search/", "/", "PREFIX_REFUSED"},
		{"/search/", "/x/search", "PREFIX_REFUSED"},
		{"/search/", "/search/../admin", "PREFIX_REFUSED"},
		{"/search/", "/search//x", "PREFIX_REFUSED"},
	} {
		s := good()
		s.Prefix, s.PaidPath = c.prefix, c.paid
		if got := reason(t, CheckSpec(s, maxPrice, hostRules())); got != c.want {
			t.Errorf("prefix %q paid %q: got %q, want %q", c.prefix, c.paid, got, c.want)
		}
	}
}

func TestBlocked(t *testing.T) {
	s := config.X402{
		SharedHostingSuffixes: []string{"workers.dev", "up.railway.app"},
		AllowedSharedHosts:    []string{"api.workers.dev", "x.up.railway.app"},
		BlockedHosts:          []string{"bad.example.com", "x.up.railway.app"},
	}
	for _, c := range []struct {
		host string
		want bool
	}{
		{"bad.example.com", true},
		{"other.workers.dev", true},
		{"a.api.workers.dev", true},
		{"workers.dev", true},
		{"x.up.railway.app", true},
		{"api.workers.dev", false},
		{"api.example.com", false},
		{"sub.bad.example.com", false},
		{"myworkers.dev", false},
	} {
		if got := Blocked(c.host, s); got != c.want {
			t.Errorf("Blocked(%q) is %v, want %v", c.host, got, c.want)
		}
	}
	eq(t, Blocked("api.workers.dev", config.X402{}), false)
}

func TestSharedListEqualsVectorFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "router.toml")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		V        int      `json:"v"`
		Suffixes []string `json:"suffixes"`
	}
	vector(t, "shared-hosting-suffixes.json", &file)
	eq(t, file.V, 1)
	eq(t, c.X402.SharedHostingSuffixes, file.Suffixes)
	eq(t, len(file.Suffixes), 16)
}
