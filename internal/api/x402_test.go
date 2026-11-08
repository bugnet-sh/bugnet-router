package api_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/x402"
)

// payTo is a canonical base58 key of 32 bytes
const payTo = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

// lockedPkg is a package whose egress.allow names three hosts
const lockedPkg = "locked-10m"

// lockedCatalog is catalogJSON with lockedPkg added
var lockedCatalog = strings.Replace(catalogJSON, `{"packages": [`, `{"packages": [
	{"id": "locked-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000,
		"egress": {"allow": ["huggingface.co", "api.example.com", "api2.example.com"]}},`, 1)

const (
	xApplying = routerv1.X402ServiceState_X402_SERVICE_STATE_APPLYING
	xActive   = routerv1.X402ServiceState_X402_SERVICE_STATE_ACTIVE
	xFailed   = routerv1.X402ServiceState_X402_SERVICE_STATE_FAILED
	xDisabled = routerv1.X402ServiceState_X402_SERVICE_STATE_DISABLED
)

// x402Of is the API server's [x402] settings, the switch off unless opts turn it on
func x402Of(opts options) config.X402 {
	c := config.X402{
		MaxServices: 5, MaxPriceMicros: 500_000, SharedHostingSuffixes: []string{"workers.dev"},
	}
	if opts.x402 != nil {
		opts.x402(&c)
	}
	return c
}

func x402On(c *config.X402) { c.Enabled = true }

// svc is a GET service on host and prefix that is not indexed, its paid path the bare prefix
func svc(host, prefix string) *routerv1.X402ServiceSpec {
	return &routerv1.X402ServiceSpec{
		Host: host, Prefix: prefix, Method: "GET", MaxPriceMicros: 10_000, PayTo: payTo,
		PaidPath: strings.TrimSuffix(prefix, "/"),
	}
}

func idOf(s *routerv1.X402ServiceSpec) string {
	return x402.ServiceID(s.GetMethod(), s.GetHost(), s.GetPrefix())
}

func (e *env) checkX402(
	t *testing.T, h string, add []*routerv1.X402ServiceSpec, disable ...string,
) error {
	t.Helper()
	_, err := e.client.CheckX402(t.Context(), connect.NewRequest(&routerv1.CheckX402Request{
		Handle: h, Add: add, Disable: disable,
	}))
	return err
}

func (e *env) mustCheckX402(
	t *testing.T, h string, add []*routerv1.X402ServiceSpec, disable ...string,
) {
	t.Helper()
	if err := e.checkX402(t, h, add, disable...); err != nil {
		t.Fatalf("CheckX402: %v", err)
	}
}

func (e *env) listX402(t *testing.T, h string) (*routerv1.ListX402Response, error) {
	t.Helper()
	res, err := e.client.ListX402(t.Context(), connect.NewRequest(
		&routerv1.ListX402Request{Handle: h}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (e *env) mustListX402(t *testing.T, h string) *routerv1.ListX402Response {
	t.Helper()
	r, err := e.listX402(t, h)
	if err != nil {
		t.Fatalf("ListX402: %v", err)
	}
	return r
}

// placingWith stores a placing placement that no saga runs, needs_secrets as given
func (e *env) placingWith(t *testing.T, needsSecrets bool) (string, store.Placement) {
	t.Helper()
	h, sha := handle.New()
	p, _, err := e.store.CreatePlacement(t.Context(), store.NewPlacement{
		HandleSHA256: sha, OrderID: "x402-" + rand.Text(), PackageID: smallPkg,
		NeedsSecrets: needsSecrets,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h, p
}

// putRow stores s as a row of the placement in state, failed rows with PORT_443_TAKEN
func (e *env) putRow(t *testing.T, id string, s *routerv1.X402ServiceSpec, state string) {
	t.Helper()
	pid := uuid.MustParse(id)
	err := e.store.PutX402Service(t.Context(), store.X402Service{
		PlacementID: pid, ServiceID: idOf(s), Spec: x402.Spec{
			Host: s.GetHost(), Prefix: s.GetPrefix(), Method: s.GetMethod(),
			MaxPriceMicros: s.GetMaxPriceMicros(), PayTo: s.GetPayTo(), Indexed: s.GetIndexed(),
			PaidPath: s.GetPaidPath(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.pool.Exec(t.Context(), `
		update x402_services set state = $3,
			failure_reason = case when $3 = 'failed' then 'PORT_443_TAKEN' end
		where placement_id = $1 and service_id = $2`, pid, idOf(s), state)
	if err != nil {
		t.Fatal(err)
	}
}

// putSecretHosts stores secrets on hosts, ten hosts to a secret
func (e *env) putSecretHosts(t *testing.T, h string, hosts []string) {
	t.Helper()
	for i := 0; i < len(hosts); i += 10 {
		e.mustPutSecret(t, h, &routerv1.SecretSpec{
			Name: fmt.Sprintf("KEY_%d", i), Hosts: hosts[i:min(i+10, len(hosts))],
		})
	}
}

// hostsNamed is n hosts named prefix and a number under domain
func hostsNamed(prefix, domain string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%02d.%s", prefix, i, domain)
	}
	return out
}

// specsOn is one service on each host
func specsOn(hosts ...string) []*routerv1.X402ServiceSpec {
	out := make([]*routerv1.X402ServiceSpec, len(hosts))
	for i, h := range hosts {
		out[i] = svc(h, "/v1/")
	}
	return out
}

// errInfo is err's google.rpc.ErrorInfo
func errInfo(t *testing.T, err error) *errdetails.ErrorInfo {
	t.Helper()
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("error %v is not a Connect error", err)
	}
	for _, d := range ce.Details() {
		v, derr := d.Value()
		if derr != nil {
			t.Fatalf("decode error detail: %v", derr)
		}
		if info, ok := v.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	t.Fatalf("error %v has no ErrorInfo detail", err)
	return nil
}

// wantEntry checks err's code and reason and that its metadata names s
func wantEntry(
	t *testing.T, err error, code connect.Code, reason string, s *routerv1.X402ServiceSpec,
) {
	t.Helper()
	mustCode(t, err, code, reason)
	eq(t, errInfo(t, err).GetMetadata(), map[string]string{
		"service_id": idOf(s), "host": s.GetHost(),
	})
}

// wantPlacementWide checks err's code and reason and that its ErrorInfo has no metadata
func wantPlacementWide(t *testing.T, err error, code connect.Code, reason string) {
	t.Helper()
	mustCode(t, err, code, reason)
	if md := errInfo(t, err).GetMetadata(); len(md) != 0 {
		t.Fatalf("%s carries metadata %v, want none", reason, md)
	}
}

func TestCheckX402WithoutHandle(t *testing.T) {
	e := newEnv(t, options{x402: x402On})
	good := svc("api.example.com", "/search/")
	e.mustCheckX402(t, "", []*routerv1.X402ServiceSpec{good})
	wantEntry(t, e.checkX402(t, "", []*routerv1.X402ServiceSpec{good, good}),
		connect.CodeInvalidArgument, "SERVICE_DUPLICATE", good)
	six := specsOn(hostsNamed("api", "example.com", 6)...)
	wantPlacementWide(t, e.checkX402(t, "", six), connect.CodeResourceExhausted,
		"TOO_MANY_SERVICES")
	e.mustCheckX402(t, "", six[:5])
	err := e.checkX402(t, "", nil, idOf(good))
	mustCode(t, err, connect.CodeNotFound, "SERVICE_UNKNOWN")
	eq(t, errInfo(t, err).GetMetadata(), map[string]string{"service_id": idOf(good)})
	// A per-service check, the host rules here, runs without a handle
	wantEntry(t, e.checkX402(t, "", specsOn("x.workers.dev")), connect.CodeInvalidArgument,
		"HOST_REFUSED", specsOn("x.workers.dev")[0])
}

func TestCheckX402SwitchApartFromCapability(t *testing.T) {
	off := newEnv(t, options{secrets: true})
	h, _ := off.readyPlacement(t)
	wantPlacementWide(t, off.checkX402(t, h, specsOn("api.example.com")),
		connect.CodeFailedPrecondition, "X402_DISABLED")
	wantPlacementWide(t, off.checkX402(t, "", specsOn("api.example.com")),
		connect.CodeFailedPrecondition, "X402_DISABLED")

	bare := newEnv(t, options{x402: x402On})
	h, _ = bare.readyPlacement(t)
	wantPlacementWide(t, bare.checkX402(t, h, specsOn("api.example.com")),
		connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")
}

func TestCheckX402WhilePlacing(t *testing.T) {
	e := newEnv(t, options{x402: x402On})
	without, p := e.placingWith(t, false)
	eq(t, p.State, store.PlacementPlacing)
	eq(t, p.ProviderRef == nil, true)
	wantPlacementWide(t, e.checkX402(t, without, specsOn("api.example.com")),
		connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")
	wantPlacementWide(t, e.checkX402(t, without, nil), connect.CodeFailedPrecondition,
		"FEATURE_UNAVAILABLE")
	with, _ := e.placingWith(t, true)
	e.mustCheckX402(t, with, specsOn("api.example.com"))
	e.mustCheckX402(t, with, nil)
}

func TestCheckX402HandleNoServices(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402On})
	h, p := e.readyPlacement(t)
	// Six live rows are over max_services, which a call with no services never counts
	for _, s := range specsOn(hostsNamed("api", "example.com", 6)...) {
		e.putRow(t, p.GetPlacementId(), s, store.X402Active)
	}
	e.mustCheckX402(t, h, nil)
	_ = e.stop(t, h)
	wantPlacementWide(t, e.checkX402(t, h, nil), connect.CodeFailedPrecondition, "NOT_READY")

	off := newEnv(t, options{secrets: true})
	h, _ = off.readyPlacement(t)
	wantPlacementWide(t, off.checkX402(t, h, nil), connect.CodeFailedPrecondition,
		"X402_DISABLED")

	bare := newEnv(t, options{x402: x402On})
	h, _ = bare.readyPlacement(t)
	wantPlacementWide(t, bare.checkX402(t, h, nil), connect.CodeFailedPrecondition,
		"FEATURE_UNAVAILABLE")
}

func TestCheckX402NotReady(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402On})
	ctx := t.Context()
	terminating, p := e.readyPlacement(t)
	if _, err := e.store.MarkPlacementTerminating(ctx, uuid.MustParse(p.GetPlacementId()),
		"test", time.Now()); err != nil {
		t.Fatal(err)
	}
	terminated, _ := e.readyPlacement(t)
	_ = e.stop(t, terminated)
	failed, sha := handle.New()
	if _, _, err := e.store.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: sha, OrderID: "x402-failed", PackageID: smallPkg,
		FailureReason: "no_candidate", NeedsSecrets: true,
	}); err != nil {
		t.Fatal(err)
	}
	for name, h := range map[string]string{
		"terminating": terminating, "terminated": terminated, "failed": failed,
	} {
		t.Run(name, func(t *testing.T) {
			wantPlacementWide(t, e.checkX402(t, h, specsOn("api.example.com")),
				connect.CodeFailedPrecondition, "NOT_READY")
		})
	}
}

func TestHostInUseByASecret(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402On})
	h, p := e.readyPlacement(t)
	e.mustPutSecret(t, h, hf())
	used := svc("huggingface.co", "/api/")
	wantEntry(t, e.checkX402(t, h, []*routerv1.X402ServiceSpec{used}),
		connect.CodeFailedPrecondition, "HOST_IN_USE", used)
	// Without a handle no secret is known, so the host passes
	e.mustCheckX402(t, "", []*routerv1.X402ServiceSpec{used})
	// A fetch_only secret's host is in use too
	e.mustPutSecret(t, h, &routerv1.SecretSpec{
		Name: "FETCH_KEY", Hosts: []string{"git.example.com"}, FetchOnly: true,
	})
	git := svc("git.example.com", "/v1/")
	wantEntry(t, e.checkX402(t, h, []*routerv1.X402ServiceSpec{git}),
		connect.CodeFailedPrecondition, "HOST_IN_USE", git)
	e.mustCheckX402(t, h, specsOn("api.example.com"))
	eq(t, e.applyJobs(t, p.GetPlacementId()), 1)
}

func TestHostNotAllowedOnRestrictedPackage(t *testing.T) {
	e := newEnv(t, options{
		secrets: true, x402: x402On, catalog: lockedCatalog, egressControl: true,
	})
	resp, err := e.createWith(t, e.startToken(t, lockedPkg, 50000))
	if err != nil {
		t.Fatal(err)
	}
	h := resp.Msg.GetHandle()
	e.waitReady(t, h)
	outside := svc("other.example.com", "/v1/")
	wantEntry(t, e.checkX402(t, h, []*routerv1.X402ServiceSpec{outside}),
		connect.CodeFailedPrecondition, "HOST_NOT_ALLOWED", outside)
	e.mustCheckX402(t, h, specsOn("api.example.com", "api2.example.com"))
	// Without a handle the package is unknown, so the host passes
	e.mustCheckX402(t, "", []*routerv1.X402ServiceSpec{outside})
	// The unrestricted package takes any host
	free, _ := e.readyPlacement(t)
	e.mustCheckX402(t, free, []*routerv1.X402ServiceSpec{outside})
}

func TestNoCatalogPackageLimitsEgress(t *testing.T) {
	files := []string{filepath.Join("..", "..", "config", "dev-packages.json")}
	if f := os.Getenv("BUGNET_TEST_PACKAGES_FILE"); f != "" {
		files = append(files, f)
	}
	for _, f := range files {
		cat, err := catalog.Load(f)
		if err != nil {
			t.Fatalf("load %s: %v", f, err)
		}
		for _, p := range cat.All() {
			if len(p.EgressAllow) > 0 {
				t.Errorf("package %s of %s sets egress.allow, so CheckX402 must gain "+
					"package_id (XS 10.1) before that package ships, or a handle-less check "+
					"passes a host the package refuses", p.ID, f)
			}
		}
	}
}

func TestThirtyTwoHostsWithSecrets(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402On})
	h, _ := e.readyPlacement(t)
	e.putSecretHosts(t, h, hostsNamed("s", "example.org", 28))
	paid := hostsNamed("x", "example.net", 5)
	e.mustCheckX402(t, h, specsOn(paid[:4]...))
	wantPlacementWide(t, e.checkX402(t, h, specsOn(paid...)), connect.CodeResourceExhausted,
		"TOO_MANY_SECRETS")
	// Two services on one host count it once
	shared := append(specsOn(paid[:4]...), svc(paid[0], "/v2/"))
	e.mustCheckX402(t, h, shared)
}

func TestFiveServicesCap(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402On})
	h, p := e.readyPlacement(t)
	live := specsOn(hostsNamed("api", "example.com", 5)...)
	for _, s := range live {
		e.putRow(t, p.GetPlacementId(), s, store.X402Active)
	}
	more := specsOn("more.example.com")
	wantPlacementWide(t, e.checkX402(t, h, more), connect.CodeResourceExhausted,
		"TOO_MANY_SERVICES")
	// Re-adding a live row adds nothing, and a disable makes room in the same change
	e.mustCheckX402(t, h, live[:1])
	e.mustCheckX402(t, h, more, idOf(live[0]))
	e.putRow(t, p.GetPlacementId(), live[0], store.X402Disabled)
	e.mustCheckX402(t, h, more)
	// Re-enabling the disabled row as well is six again
	wantPlacementWide(t, e.checkX402(t, h, append(more, live[0])),
		connect.CodeResourceExhausted, "TOO_MANY_SERVICES")
	e.putRow(t, p.GetPlacementId(), live[0], store.X402Failed)
	wantPlacementWide(t, e.checkX402(t, h, more), connect.CodeResourceExhausted,
		"TOO_MANY_SERVICES")
}

func TestCountsRefuseOnlyAChangeThatGrows(t *testing.T) {
	e := newEnv(t, options{secrets: true,
		x402: func(c *config.X402) { c.Enabled, c.MaxServices = true, 2 }})
	h, p := e.readyPlacement(t)
	live := specsOn(hostsNamed("api", "example.com", 4)...)
	for _, s := range live {
		e.putRow(t, p.GetPlacementId(), s, store.X402Active)
	}
	// Four live rows over a lowered cap may still disable one, or re-add one
	e.mustCheckX402(t, h, nil, idOf(live[0]))
	e.mustCheckX402(t, h, live[1:2])
	wantPlacementWide(t, e.checkX402(t, h, specsOn("more.example.com"), idOf(live[0])),
		connect.CodeResourceExhausted, "TOO_MANY_SERVICES")
}

func TestEveryValidationOfSixThree(t *testing.T) {
	e := newEnv(t, options{
		secrets: true, catalog: lockedCatalog, egressControl: true,
		x402: func(c *config.X402) {
			c.Enabled = true
			c.BlockedHosts = []string{"agent402.tools"}
			c.AllowedSharedHosts = []string{"api.workers.dev"}
		},
	})
	h, p := e.readyPlacement(t)
	e.mustPutSecret(t, h, hf())
	good := svc("api.example.com", "/search/")
	entry := func(t *testing.T, code connect.Code, reason string, s *routerv1.X402ServiceSpec) {
		t.Helper()
		wantEntry(t, e.checkX402(t, h, []*routerv1.X402ServiceSpec{s}), code, reason, s)
		wantEntry(t, e.checkX402(t, "", []*routerv1.X402ServiceSpec{s}), code, reason, s)
	}
	with := func(f func(s *routerv1.X402ServiceSpec)) *routerv1.X402ServiceSpec {
		s := svc("api.example.com", "/search/")
		f(s)
		return s
	}
	t.Run("the switch", func(t *testing.T) {
		off := newEnv(t, options{secrets: true})
		oh, _ := off.readyPlacement(t)
		wantPlacementWide(t, off.checkX402(t, oh, []*routerv1.X402ServiceSpec{good}),
			connect.CodeFailedPrecondition, "X402_DISABLED")
	})
	t.Run("the capability", func(t *testing.T) {
		placing, _ := e.placingWith(t, false)
		wantPlacementWide(t, e.checkX402(t, placing, []*routerv1.X402ServiceSpec{good}),
			connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")
		bare := newEnv(t, options{x402: x402On})
		bh, _ := bare.readyPlacement(t)
		wantPlacementWide(t, bare.checkX402(t, bh, []*routerv1.X402ServiceSpec{good}),
			connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")
	})
	t.Run("the state", func(t *testing.T) {
		ended, _ := e.readyPlacement(t)
		_ = e.stop(t, ended)
		wantPlacementWide(t, e.checkX402(t, ended, []*routerv1.X402ServiceSpec{good}),
			connect.CodeFailedPrecondition, "NOT_READY")
	})
	t.Run("host rules", func(t *testing.T) {
		for _, host := range []string{
			"bugnet.sh", "x.preview.bugnet.sh", "203.0.113.7", "*.example.com", "com",
			"API.example.com", "api.example.com.", "x.e2b.app", "x.boxd.sh", "x.workers.dev",
			"agent402.tools", "co.uk", "",
		} {
			entry(t, connect.CodeInvalidArgument, "HOST_REFUSED", svc(host, "/v1/"))
		}
		e.mustCheckX402(t, h, specsOn("api.workers.dev"))
	})
	t.Run("egress allow", func(t *testing.T) {
		resp, err := e.createWith(t, e.startToken(t, lockedPkg, 50000))
		if err != nil {
			t.Fatal(err)
		}
		lh := resp.Msg.GetHandle()
		e.waitReady(t, lh)
		s := svc("other.example.com", "/v1/")
		wantEntry(t, e.checkX402(t, lh, []*routerv1.X402ServiceSpec{s}),
			connect.CodeFailedPrecondition, "HOST_NOT_ALLOWED", s)
	})
	t.Run("prefix", func(t *testing.T) {
		for _, prefix := range []string{
			"", "/", "//", "v1/", "/v1", "/" + strings.Repeat("a", 199) + "/", "/a?b/",
			"/a#b/", "/a/../b/", "/a/./b/", "/a//b/", "/a b/", "/a%2fb/", "/a;b/",
		} {
			s := svc("api.example.com", prefix)
			s.Indexed, s.PaidPath = true, ""
			entry(t, connect.CodeInvalidArgument, "PREFIX_REFUSED", s)
		}
		s := svc("api.example.com", "/"+strings.Repeat("a", 198)+"/")
		s.Indexed, s.PaidPath = true, ""
		e.mustCheckX402(t, h, []*routerv1.X402ServiceSpec{s})
	})
	t.Run("paid path", func(t *testing.T) {
		for name, s := range map[string]*routerv1.X402ServiceSpec{
			"empty unindexed": with(func(s *routerv1.X402ServiceSpec) { s.PaidPath = "" }),
			"set indexed":     with(func(s *routerv1.X402ServiceSpec) { s.Indexed = true }),
			"outside":         with(func(s *routerv1.X402ServiceSpec) { s.PaidPath = "/other" }),
			"sibling":         with(func(s *routerv1.X402ServiceSpec) { s.PaidPath = "/searchx" }),
			"query":           with(func(s *routerv1.X402ServiceSpec) { s.PaidPath = "/search?q" }),
			"fragment":        with(func(s *routerv1.X402ServiceSpec) { s.PaidPath = "/search#a" }),
			"dot segment": with(func(s *routerv1.X402ServiceSpec) {
				s.PaidPath = "/search/../x"
			}),
		} {
			t.Run(name, func(t *testing.T) {
				entry(t, connect.CodeInvalidArgument, "PREFIX_REFUSED", s)
			})
		}
		e.mustCheckX402(t, h, []*routerv1.X402ServiceSpec{
			good, with(func(s *routerv1.X402ServiceSpec) {
				s.Prefix, s.PaidPath = "/run/", "/run/job"
			}),
		})
	})
	t.Run("duplicate", func(t *testing.T) {
		wantEntry(t, e.checkX402(t, h, []*routerv1.X402ServiceSpec{good, good}),
			connect.CodeInvalidArgument, "SERVICE_DUPLICATE", good)
		e.putRow(t, p.GetPlacementId(), good, store.X402Active)
		wantEntry(t, e.checkX402(t, h, []*routerv1.X402ServiceSpec{good}, idOf(good)),
			connect.CodeInvalidArgument, "SERVICE_DUPLICATE", good)
	})
	t.Run("method", func(t *testing.T) {
		for _, m := range []string{"", "PUT", "get", "HEAD", "DELETE"} {
			entry(t, connect.CodeInvalidArgument, "METHOD_REFUSED",
				with(func(s *routerv1.X402ServiceSpec) { s.Method = m }))
		}
		e.mustCheckX402(t, h, []*routerv1.X402ServiceSpec{
			with(func(s *routerv1.X402ServiceSpec) { s.Method = "POST" }),
		})
	})
	t.Run("price", func(t *testing.T) {
		for _, n := range []int64{0, -1, 500_001} {
			entry(t, connect.CodeInvalidArgument, "PRICE_REFUSED",
				with(func(s *routerv1.X402ServiceSpec) { s.MaxPriceMicros = n }))
		}
		e.mustCheckX402(t, h, []*routerv1.X402ServiceSpec{
			with(func(s *routerv1.X402ServiceSpec) { s.MaxPriceMicros = 500_000 }),
		})
	})
	t.Run("pay_to", func(t *testing.T) {
		for _, to := range []string{
			"", "abc", "0PjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", payTo + "1", " " + payTo,
		} {
			entry(t, connect.CodeInvalidArgument, "PAYTO_REFUSED",
				with(func(s *routerv1.X402ServiceSpec) { s.PayTo = to }))
		}
	})
	t.Run("secret host", func(t *testing.T) {
		s := svc("huggingface.co", "/api/")
		wantEntry(t, e.checkX402(t, h, []*routerv1.X402ServiceSpec{s}),
			connect.CodeFailedPrecondition, "HOST_IN_USE", s)
	})
	t.Run("counts", func(t *testing.T) {
		ch, _ := e.readyPlacement(t)
		wantPlacementWide(t, e.checkX402(t, ch, specsOn(hostsNamed("api", "example.com", 6)...)),
			connect.CodeResourceExhausted, "TOO_MANY_SERVICES")
		e.putSecretHosts(t, ch, hostsNamed("s", "example.org", 30))
		wantPlacementWide(t, e.checkX402(t, ch, specsOn(hostsNamed("x", "example.net", 3)...)),
			connect.CodeResourceExhausted, "TOO_MANY_SECRETS")
	})
	t.Run("unknown disable", func(t *testing.T) {
		err := e.checkX402(t, h, nil, "xs_aaaaaaaaaaaaaaaa")
		mustCode(t, err, connect.CodeNotFound, "SERVICE_UNKNOWN")
	})
	t.Run("operation bucket", func(t *testing.T) {
		b := newEnv(t, options{secrets: true, x402: x402On, opsPerSecond: 1, opsBurst: 1})
		bh, _ := b.readyPlacement(t)
		drain(t, b, bh)
		b.mustCheckX402(t, bh, []*routerv1.X402ServiceSpec{good})
	})
}

// drain spends the placement's operation bucket until an Exec is refused
func drain(t *testing.T, e *env, h string) {
	t.Helper()
	for range 10 {
		_, err := e.client.Exec(t.Context(), connect.NewRequest(sh(h, "true")))
		if connect.CodeOf(err) == connect.CodeResourceExhausted {
			return
		}
	}
	t.Fatal("the operation bucket never ran out")
}

func TestPerServiceRefusalNamesTheEntry(t *testing.T) {
	e := newEnv(t, options{
		secrets: true, x402: x402On, catalog: lockedCatalog, egressControl: true,
	})
	resp, err := e.createWith(t, e.startToken(t, lockedPkg, 50000))
	if err != nil {
		t.Fatal(err)
	}
	h := resp.Msg.GetHandle()
	e.waitReady(t, h)
	e.mustPutSecret(t, h, hf())
	first := svc("api.example.com", "/search/")
	second := func(f func(s *routerv1.X402ServiceSpec)) *routerv1.X402ServiceSpec {
		s := svc("api2.example.com", "/v1/")
		f(s)
		return s
	}
	for _, c := range []struct {
		reason string
		code   connect.Code
		s      *routerv1.X402ServiceSpec
	}{
		{"HOST_REFUSED", connect.CodeInvalidArgument,
			second(func(s *routerv1.X402ServiceSpec) { s.Host = "x.workers.dev" })},
		{"PREFIX_REFUSED", connect.CodeInvalidArgument,
			second(func(s *routerv1.X402ServiceSpec) { s.Prefix = "/v1" })},
		{"METHOD_REFUSED", connect.CodeInvalidArgument,
			second(func(s *routerv1.X402ServiceSpec) { s.Method = "PUT" })},
		{"PRICE_REFUSED", connect.CodeInvalidArgument,
			second(func(s *routerv1.X402ServiceSpec) { s.MaxPriceMicros = 0 })},
		{"PAYTO_REFUSED", connect.CodeInvalidArgument,
			second(func(s *routerv1.X402ServiceSpec) { s.PayTo = "abc" })},
		{"SERVICE_DUPLICATE", connect.CodeInvalidArgument, first},
		{"HOST_IN_USE", connect.CodeFailedPrecondition,
			second(func(s *routerv1.X402ServiceSpec) { s.Host = "huggingface.co" })},
		{"HOST_NOT_ALLOWED", connect.CodeFailedPrecondition,
			second(func(s *routerv1.X402ServiceSpec) { s.Host = "other.example.com" })},
	} {
		t.Run(c.reason, func(t *testing.T) {
			err := e.checkX402(t, h, []*routerv1.X402ServiceSpec{first, c.s})
			wantEntry(t, err, c.code, c.reason, c.s)
			quotesNone(t, err, h)
		})
	}
	used := svc("huggingface.co", "/api/")
	bad := second(func(s *routerv1.X402ServiceSpec) { s.Prefix = "/v1" })
	wantEntry(t, e.checkX402(t, h, []*routerv1.X402ServiceSpec{used, bad}),
		connect.CodeFailedPrecondition, "HOST_IN_USE", used)
}

func TestPlacementWideRefusalHasNoMetadata(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402On})
	h, _ := e.readyPlacement(t)
	placing, _ := e.placingWith(t, false)
	ended, _ := e.readyPlacement(t)
	_ = e.stop(t, ended)
	full, _ := e.readyPlacement(t)
	e.putSecretHosts(t, full, hostsNamed("s", "example.org", 32))
	unknown, _ := handle.New()
	off := newEnv(t, options{secrets: true})
	oh, _ := off.readyPlacement(t)
	one := specsOn("api.example.com")
	for _, c := range []struct {
		reason string
		code   connect.Code
		err    error
	}{
		{"X402_DISABLED", connect.CodeFailedPrecondition, off.checkX402(t, oh, one)},
		{"FEATURE_UNAVAILABLE", connect.CodeFailedPrecondition, e.checkX402(t, placing, one)},
		{"NOT_READY", connect.CodeFailedPrecondition, e.checkX402(t, ended, one)},
		{"TOO_MANY_SERVICES", connect.CodeResourceExhausted,
			e.checkX402(t, h, specsOn(hostsNamed("api", "example.com", 6)...))},
		{"TOO_MANY_SECRETS", connect.CodeResourceExhausted, e.checkX402(t, full, one)},
		{"HANDLE_UNKNOWN", connect.CodeNotFound, e.checkX402(t, unknown, one)},
	} {
		t.Run(c.reason, func(t *testing.T) {
			wantPlacementWide(t, c.err, c.code, c.reason)
		})
	}
}

func TestServiceUnknownNamesTheID(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402On})
	h, p := e.readyPlacement(t)
	known := svc("api.example.com", "/search/")
	e.putRow(t, p.GetPlacementId(), known, store.X402Disabled)
	one, two := idOf(svc("one.example.com", "/v1/")), idOf(svc("two.example.com", "/v1/"))
	err := e.checkX402(t, h, nil, idOf(known), one, two)
	mustCode(t, err, connect.CodeNotFound, "SERVICE_UNKNOWN")
	eq(t, errInfo(t, err).GetMetadata(), map[string]string{"service_id": one})
	e.mustCheckX402(t, h, nil, idOf(known))
	err = e.checkX402(t, "", nil, one, two)
	mustCode(t, err, connect.CodeNotFound, "SERVICE_UNKNOWN")
	eq(t, errInfo(t, err).GetMetadata(), map[string]string{"service_id": one})
}

func TestCheckAndListSpendNoToken(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402On, opsPerSecond: 1, opsBurst: 1})
	h, _ := e.readyPlacement(t)
	for range 5 {
		e.mustCheckX402(t, h, specsOn("api.example.com"))
		e.mustListX402(t, h)
	}
	// The bucket is still full, so the first operation goes through
	e.exec(t, sh(h, "true"))
	drain(t, e, h)
	e.mustCheckX402(t, h, specsOn("api.example.com"))
	e.mustListX402(t, h)
}

func TestListX402AnyState(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402On})
	h, p := e.readyPlacement(t)
	s := svc("api.example.com", "/search/")
	e.putRow(t, p.GetPlacementId(), s, store.X402Active)
	_, err := e.pool.Exec(t.Context(), `
		update x402_services set requests = 7, paid = 3, uncertain = 1 where placement_id = $1`,
		p.GetPlacementId())
	if err != nil {
		t.Fatal(err)
	}
	e.refusal(t, p.GetPlacementId(), "path_refused", 2, time.Now())
	_ = e.stop(t, h)
	r := e.mustListX402(t, h)
	eq(t, len(r.GetServices()), 1)
	got := r.GetServices()[0]
	eq(t, got.GetServiceId(), idOf(s))
	eq(t, got.GetState(), xActive)
	eq(t, got.GetSpec().GetPaidPath(), "/search")
	eq(t, []uint32{got.GetRequests(), got.GetPaid(), got.GetUncertain()}, []uint32{7, 3, 1})
	eq(t, len(r.GetRefusals()), 1)
	eq(t, r.GetRefusals()[0].GetReason(), "path_refused")
	eq(t, r.GetRefusals()[0].GetCount(), uint32(2))

	placing, _ := e.placingWith(t, true)
	eq(t, len(e.mustListX402(t, placing).GetServices()), 0)
}

// refusal stores a refusal count of the placement
func (e *env) refusal(t *testing.T, id, reason string, count int, at time.Time) {
	t.Helper()
	_, err := e.pool.Exec(t.Context(), `
		insert into x402_refusals (placement_id, reason, count, last_at) values ($1, $2, $3, $4)`,
		id, reason, count, at)
	if err != nil {
		t.Fatal(err)
	}
}

func TestListX402HostBlocked(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: func(c *config.X402) {
		c.Enabled, c.BlockedHosts = true, []string{"agent402.tools"}
	}})
	h, p := e.readyPlacement(t)
	blocked := svc("agent402.tools", "/v1/auto/")
	off := svc("agent402.tools", "/v2/")
	fine := svc("api.example.com", "/search/")
	e.putRow(t, p.GetPlacementId(), blocked, store.X402Active)
	e.putRow(t, p.GetPlacementId(), off, store.X402Disabled)
	e.putRow(t, p.GetPlacementId(), fine, store.X402Active)
	states := map[string]*routerv1.X402ServiceInfo{}
	for _, s := range e.mustListX402(t, h).GetServices() {
		states[s.GetServiceId()] = s
	}
	eq(t, states[idOf(blocked)].GetState(), xFailed)
	eq(t, states[idOf(blocked)].GetFailureReason(), "HOST_BLOCKED")
	eq(t, states[idOf(off)].GetState(), xDisabled)
	eq(t, states[idOf(off)].GetFailureReason(), "")
	eq(t, states[idOf(fine)].GetState(), xActive)
	rows, err := e.store.X402Services(t.Context(), uuid.MustParse(p.GetPlacementId()))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.ServiceID == idOf(blocked) {
			eq(t, r.State, store.X402Active)
			eq(t, r.FailureReason, "")
		}
	}
}

func TestListX402EnabledFlag(t *testing.T) {
	for _, on := range []bool{true, false} {
		t.Run(fmt.Sprint(on), func(t *testing.T) {
			e := newEnv(t, options{secrets: true,
				x402: func(c *config.X402) { c.Enabled = on }})
			h, p := e.readyPlacement(t)
			s := svc("api.example.com", "/search/")
			e.putRow(t, p.GetPlacementId(), s, store.X402Applying)
			e.putRow(t, p.GetPlacementId(), svc("api2.example.com", "/v1/"), store.X402Failed)
			r := e.mustListX402(t, h)
			eq(t, r.GetEnabled(), on)
			eq(t, len(r.GetServices()), 2)
			states := map[string]routerv1.X402ServiceState{}
			reasons := map[string]string{}
			for _, s := range r.GetServices() {
				states[s.GetSpec().GetHost()] = s.GetState()
				reasons[s.GetSpec().GetHost()] = s.GetFailureReason()
			}
			eq(t, states, map[string]routerv1.X402ServiceState{
				"api.example.com": xApplying, "api2.example.com": xFailed,
			})
			eq(t, reasons, map[string]string{
				"api.example.com": "", "api2.example.com": "PORT_443_TAKEN",
			})
		})
	}
}

func TestRefusalsCarryLastAt(t *testing.T) {
	e := newEnv(t, options{secrets: true, x402: x402On})
	h, p := e.readyPlacement(t)
	first := time.Date(2026, 11, 8, 9, 0, 0, 0, time.UTC)
	last := time.Date(2026, 11, 8, 9, 5, 30, 0, time.UTC)
	e.refusal(t, p.GetPlacementId(), "price_over_cap", 1, first)
	e.refusal(t, p.GetPlacementId(), "path_refused", 4, last)
	r := e.mustListX402(t, h).GetRefusals()
	eq(t, len(r), 2)
	got := map[string]time.Time{}
	counts := map[string]uint32{}
	for _, x := range r {
		got[x.GetReason()] = x.GetLastAt().AsTime()
		counts[x.GetReason()] = x.GetCount()
	}
	eq(t, got, map[string]time.Time{"price_over_cap": first, "path_refused": last})
	eq(t, counts, map[string]uint32{"price_over_cap": 1, "path_refused": 4})
}
