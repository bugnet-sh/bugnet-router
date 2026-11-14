package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

func TestStartRefusesThePayerClaimWithoutPayments(t *testing.T) {
	for _, c := range []struct {
		name, token string
		events      bool
		want        string
	}{
		{"no token", "", true, "BUGNET_PAYMENTS_ROUTER_TOKEN"},
		{"no events", "payments-token-0123456789abcdef0123456789", false, "wallets.emit_events"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := config.Config{DatabaseURL: "postgres://localhost:1/unused",
				PaymentsToken: c.token}
			cfg.Wallets.UsePayer, cfg.Wallets.EmitEvents = true, c.events
			err := serve(t.Context(), discard, cfg, nil)
			if err == nil || !strings.Contains(err.Error(), "wallets.use_payer") ||
				!strings.Contains(err.Error(), c.want) {
				t.Fatalf("serve gave %v, want wallets.use_payer and %s named", err, c.want)
			}
		})
	}
	cfg := config.Config{PaymentsToken: "payments-token-0123456789abcdef0123456789"}
	cfg.Wallets.UsePayer, cfg.Wallets.EmitEvents = true, true
	if err := checkWallets(discard, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestStartRefusesSecretsWithoutCA(t *testing.T) {
	cfg := config.Config{DatabaseURL: "postgres://localhost:1/unused"}
	cfg.Secrets.Providers = []string{"e2b"}
	err := serve(t.Context(), discard, cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "secrets.providers") ||
		!strings.Contains(err.Error(), "secrets.ca_cert_file") {
		t.Fatalf("serve with secrets and no CA gave %v, want both settings named", err)
	}
}

// writeCA writes a self-signed certificate in PEM to a file of its own
func writeCA(t *testing.T) (string, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true,
		BasicConstraintsValid: true, NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return writeTemp(t, "ca.pem", pemBytes), pemBytes
}

func writeTemp(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBootSettingsCarryTheCAAndThePools(t *testing.T) {
	caFile, ca := writeCA(t)
	cfg := config.Config{}
	cfg.Previews.GatePort = 29417
	cfg.Names.PreviewDomain, cfg.Names.EgressHost = "preview.bugnet.sh", "egress.bugnet.sh"
	cfg.Secrets.Providers, cfg.Secrets.CACertFile, cfg.Secrets.CAVersion = []string{"e2b"},
		caFile, 2
	cfg.Secrets.EgressIPs = []netip.Addr{netip.MustParseAddr("203.0.113.7")}
	cfg.Hardening = config.Hardening{
		MailPorts: []int{25}, DenyRanges: []string{"10.0.0.0/8"},
		PoolList: writeTemp(t, "pools.txt", []byte("198.51.100.0/24\n")),
		Rules:    map[string]config.HardenRule{"boat": {Required: true}},
	}
	b, err := bootSettings(discard, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if string(b.CAPEM) != string(ca) || b.CAVersion != 2 || b.GatePort != 29417 ||
		b.PreviewDomain != "preview.bugnet.sh" || b.EgressHost != "egress.bugnet.sh" ||
		!reflect.DeepEqual(b.Pools, []string{"198.51.100.0/24"}) ||
		!reflect.DeepEqual(b.MailPorts, []int{25}) ||
		!reflect.DeepEqual(b.DenyRanges, []string{"10.0.0.0/8"}) ||
		!reflect.DeepEqual(b.Harden, cfg.Hardening.Rules) ||
		!reflect.DeepEqual(b.EgressIPs, cfg.Secrets.EgressIPs) {
		t.Fatalf("boot settings %+v", b)
	}

	cfg.Hardening.PoolList = filepath.Join(t.TempDir(), "missing.txt")
	if b, err := bootSettings(discard, cfg); err != nil || len(b.Pools) != 0 {
		t.Fatalf("a missing pool list: %v, %v, want a warning alone", b.Pools, err)
	}
	cfg.Hardening.PoolList = writeTemp(t, "bad.txt", []byte("pool.example.com\n"))
	if _, err := bootSettings(discard, cfg); err == nil || !strings.Contains(err.Error(),
		"line 1") {
		t.Fatalf("a malformed pool list: %v", err)
	}
	cfg.Hardening.PoolList = ""
	for name, content := range map[string]string{
		"key":   "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n",
		"text":  "not a certificate",
		"empty": "",
	} {
		cfg.Secrets.CACertFile = writeTemp(t, name+".pem", []byte(content))
		if _, err := bootSettings(discard, cfg); err == nil ||
			!strings.Contains(err.Error(), "secrets.ca_cert_file") {
			t.Fatalf("a CA file holding %s: %v", name, err)
		}
	}
}

func TestStartRefusesRestrictedOnE2BWithoutProxyIPv4(t *testing.T) {
	pkgs := func(extra string) *catalog.Catalog {
		t.Helper()
		c, err := catalog.Load(writeTemp(t, "packages.json", []byte(`{"packages": [{"id": `+
			`"locked", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10}, `+
			`"minutes": 10, "price_micros": 50000`+extra+`}]}`)))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	restricted, open := pkgs(`, "egress": {"allow": ["huggingface.co"]}`), pkgs("")
	v4, v6 := netip.MustParseAddr("203.0.113.7"), netip.MustParseAddr("2001:db8::7")
	for _, tc := range []struct {
		name      string
		providers []string
		cat       *catalog.Catalog
		ips       []netip.Addr
		refused   bool
	}{
		{"no proxy address", []string{"local", "e2b"}, restricted, nil, true},
		{"an IPv6 proxy alone", []string{"e2b"}, restricted, []netip.Addr{v6}, true},
		{"an IPv4 proxy", []string{"e2b"}, restricted, []netip.Addr{v6, v4}, false},
		{"no E2B", []string{"boxd"}, restricted, nil, false},
		{"no restricted package", []string{"e2b"}, open, nil, false},
	} {
		cfg := config.Config{Providers: tc.providers}
		cfg.Secrets.EgressIPs = tc.ips
		err := checkRestrictedEgress(cfg, tc.cat)
		if tc.refused != (err != nil) {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if err != nil && (!strings.Contains(err.Error(), "secrets.egress_ips") ||
			!strings.Contains(err.Error(), "locked")) {
			t.Fatalf("%s: %v, want the setting and the package named", tc.name, err)
		}
	}
}

// plain hides everything of a local provider but the port, so it can neither park nor list sizes
type plain struct{ provider.Provider }

func TestStartRefusesBadPools(t *testing.T) {
	loc := local.New(local.Options{
		Name: "local", Root: t.TempDir(), Sizes: []provider.Size{{Name: "small"}},
	})
	named := func(name string) provider.Provider {
		return plain{local.New(local.Options{Name: name, Root: t.TempDir()})}
	}
	reg := func(ps ...provider.Provider) *provider.Registry {
		r, err := provider.NewRegistry(ps, provider.Features{})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	pools := func(p, size string) config.Config {
		var cfg config.Config
		cfg.Pools.Pool = []config.Pool{{Provider: p, Size: size, Min: 1, Max: 2,
			MaxAge: time.Hour}}
		return cfg
	}
	for _, tc := range []struct {
		name string
		reg  *provider.Registry
		cfg  config.Config
		want string
	}{
		{"a parked pool", reg(loc), pools("local", "small"), ""},
		{"no pools", reg(loc), config.Config{}, ""},
		{"boat", reg(named("boat")), pools("boat", "small"), "boat"},
		{"a size not sold", reg(loc), pools("local", "huge"), `"huge"`},
		{"no parker and no claimer", reg(named("e2b")), pools("e2b", "small"), "keeps no pools"},
		{"a provider not listed", reg(loc), pools("boxd", "small"), "not in providers"},
	} {
		err := checkPools(tc.reg, tc.cfg)
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: %v", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: got %v, want an error naming %s", tc.name, err, tc.want)
		}
	}
}

func TestStartRefusesAcceptWithNoProvider(t *testing.T) {
	named := func(name string) provider.Provider {
		return plain{local.New(local.Options{Name: name, Root: t.TempDir()})}
	}
	reg := func(f provider.Features, ps ...provider.Provider) *provider.Registry {
		r, err := provider.NewRegistry(ps, f)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	accepting := config.Config{Images: config.Images{Accept: []string{"2026.11.1"}}}
	on := reg(provider.Features{Images: map[string]string{"a": "2026.11.1"}}, named("a"))
	off := reg(provider.Features{Images: map[string]string{"a": "2026.10.1"}}, named("a"))
	exempt := reg(provider.Features{ImageExempt: []string{"a"}}, named("a"))
	for name, err := range map[string]error{
		"accepted": checkImages(on, accepting), "exempt": checkImages(exempt, accepting),
		"no accept": checkImages(off, config.Config{}),
	} {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	err := checkImages(off, accepting)
	if err == nil || !strings.Contains(err.Error(), "images.accept") {
		t.Fatalf("error %v, want one naming images.accept", err)
	}
}

func TestStartRefusesBadBoxdRefs(t *testing.T) {
	sizes := []provider.Size{
		{Name: "1x4", Resources: provider.Resources{VCPU: 1, MemoryMiB: 4096, DiskGiB: 20}},
		{Name: "2x8", Resources: provider.Resources{VCPU: 2, MemoryMiB: 8192, DiskGiB: 20}},
	}
	boxd := local.New(local.Options{Name: "boxd", Root: t.TempDir(), Sizes: sizes})
	r, err := provider.NewRegistry([]provider.Provider{boxd}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Load(writeTemp(t, "packages.json", []byte(`{"packages": [{"id": "big", `+
		`"resources": {"vcpu": 2, "memory_mib": 6000, "disk_gib": 10}, "minutes": 10, `+
		`"price_micros": 50000}]}`)))
	if err != nil {
		t.Fatal(err)
	}
	with := func(refs map[string]string) config.Config {
		var cfg config.Config
		cfg.Images.Boxd.Refs = refs
		return cfg
	}
	for _, tc := range []struct {
		name string
		refs map[string]string
		want string
	}{
		{"no refs", nil, ""},
		{"all sizes", map[string]string{"1x4": "a", "2x8": "b"}, ""},
		{"only the landing size", map[string]string{"2x8": "b"}, ""},
		{"unknown size", map[string]string{"99x99": "x", "2x8": "b"}, `"99x99"`},
		{"missing size", map[string]string{"1x4": "a"}, `"2x8"`},
	} {
		err := checkBoxdRefs(r, with(tc.refs), cat)
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: %v", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: got %v, want an error naming %s", tc.name, err, tc.want)
		}
	}
}

// downHealth reports every provider as down
type downHealth struct{}

func (downHealth) Healthy(string) bool           { return false }
func (downHealth) ReadyP95(string) time.Duration { return 0 }

func TestStartLogsEachPackageNoProviderPlaces(t *testing.T) {
	// 10 minutes cost 3,000, so a fixed price of 5,000 holds a markup up to 66%
	p := cheapProvider(t)
	reg, err := provider.NewRegistry([]provider.Provider{p}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	pkg := func(id string, vcpu int, price int64, extends string) string {
		return fmt.Sprintf(`{"id": %q, "resources": {"vcpu": %d, "memory_mib": 2048, `+
			`"disk_gib": 10}, "minutes": 10, "price_micros": %d, "extension_of": %q}`,
			id, vcpu, price, extends)
	}
	cat, err := catalog.Load(writeTemp(t, "packages.json", []byte(`{"packages": [`+
		strings.Join([]string{
			pkg("fits", 2, 50000, ""), pkg("fits-more", 2, 10, "fits"),
			pkg("thin", 2, 5000, ""), pkg("huge", 64, 50000, ""),
		}, ", ")+`]}`)))
	if err != nil {
		t.Fatal(err)
	}
	// cheap runs the image 2026.10.1 in this registry
	aged, err := provider.NewRegistry([]provider.Provider{p}, provider.Features{
		Images: map[string]string{"cheap": "2026.10.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The provider is down, which a boot check looks past
	e, agedE := placement.New(reg, downHealth{}, nil), placement.New(aged, downHealth{}, nil)
	// A minute of overhead takes thin's cost to 3,300, which 60% prices at 5,280
	slow := map[string]time.Duration{"cheap": time.Minute}
	for _, tc := range []struct {
		e      *placement.Engine
		rule   pricing.Rule
		accept []string
		want   []string
	}{
		{e, pricing.Rule{MarkupBP: 10000}, nil, []string{"huge", "thin"}},
		{e, pricing.Rule{MarkupBP: 1000}, nil, []string{"huge"}},
		{e, pricing.Rule{MarkupBP: 6000}, nil, []string{"huge"}},
		{e, pricing.Rule{MarkupBP: 6000, Overhead: slow}, nil, []string{"huge", "thin"}},
		{agedE, pricing.Rule{MarkupBP: 1000}, []string{"2026.10.1"}, []string{"huge"}},
		{agedE, pricing.Rule{MarkupBP: 1000}, []string{"2026.11.1"},
			[]string{"fits", "huge", "thin"}},
	} {
		named := checked(t, tc.e, cat, tc.rule, tc.accept)
		if !reflect.DeepEqual(named, tc.want) {
			t.Errorf("rule %+v accept %v named %v, want %v", tc.rule, tc.accept, named, tc.want)
		}
	}
}

func TestStartLogsALivePackageThatLandsNowhere(t *testing.T) {
	reg, err := provider.NewRegistry([]provider.Provider{cheapProvider(t)}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	e := placement.New(reg, downHealth{}, nil)
	live := func(id string, vcpu int) *catalog.Catalog {
		cat, err := catalog.Load(writeTemp(t, "packages.json", fmt.Appendf(nil,
			`{"packages": [{"id": %q, "resources": {"vcpu": %d, "memory_mib": 2048, `+
				`"disk_gib": 10}, "minutes": 10}]}`, id, vcpu)))
		if err != nil {
			t.Fatal(err)
		}
		return cat
	}
	// A live package lands at any cost, so only a missing size keeps it off cheap
	rule := pricing.Rule{MarkupBP: 100000}
	if named := checked(t, e, live("lands", 2), rule, nil); len(named) != 0 {
		t.Errorf("a live package that lands logged %v", named)
	}
	if named := checked(t, e, live("huge", 64), rule, nil); !slices.Equal(named, []string{"huge"}) {
		t.Errorf("a live package that lands nowhere logged %v, want huge", named)
	}
}

// cheapProvider is a provider named cheap that sells a small size at 18,000 an hour
func cheapProvider(t *testing.T) provider.Provider {
	t.Helper()
	small := provider.Size{Name: "small", Resources: provider.Resources{
		VCPU: 2, MemoryMiB: 2048, DiskGiB: 10,
	}}
	return local.New(local.Options{Name: "cheap", Root: t.TempDir(),
		Sizes: []provider.Size{small}, HourlyCostMicros: 18000, Increment: time.Second})
}

// checked runs the boot check of the catalog and names each package it logged, sorted
func checked(
	t *testing.T, e *placement.Engine, cat *catalog.Catalog, rule pricing.Rule, accept []string,
) []string {
	t.Helper()
	var logs bytes.Buffer
	var cfg config.Config
	cfg.Pricing.Rule = rule
	cfg.Images.Accept = accept
	checkPackages(slog.New(slog.NewTextHandler(&logs, nil)), e, cat, cfg)
	var named []string
	for line := range strings.Lines(logs.String()) {
		if !strings.Contains(line, "level=ERROR") {
			t.Errorf("rule %+v accept %v logged %q", rule, accept, line)
		}
		_, id, _ := strings.Cut(line, "package=")
		named = append(named, strings.Fields(id)[0])
	}
	slices.Sort(named)
	return named
}
