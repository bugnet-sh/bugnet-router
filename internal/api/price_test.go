package api_test

import (
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// failingEngine answers every candidates call with err and quotes no server
type failingEngine struct{ err error }

func (f failingEngine) Candidates(placement.Inputs) ([]placement.Candidate, error) {
	return nil, f.err
}

func (failingEngine) Quote(placement.Inputs) (placement.Candidate, bool) {
	return placement.Candidate{}, false
}

// Boat small's and E2B's hourly rates for the small size, both billed by the second
const (
	boatRate = 18_000
	e2bRate  = 133_200
)

// tenPercent is the production markup with each provider's measured overhead
var tenPercent = pricing.Rule{MarkupBP: 1000, Overhead: map[string]time.Duration{
	"boat": 10 * time.Second, "boxd": 10 * time.Second, "e2b": 15 * time.Second,
}}

// liveCatalog is catalogJSON with every package quoted live
const liveCatalog = `{"packages": [
	{"id": "gpu-10m", "resources": {"vcpu": 4, "memory_mib": 8192, "disk_gib": 20,
		"gpu_count": 1, "gpu_memory_gib": 24, "gpu_model": "NVIDIA L4"}, "minutes": 10},
	{"id": "small-30m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 30},
	{"id": "small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "extension_of": "small-10m"},
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10}
]}`

// metered is a local provider whose hourly rate a test may move
type metered struct {
	*local.Provider
	hourly *atomic.Int64
}

func (m metered) HourlyCostMicros(provider.Size) int64 { return m.hourly.Load() }

// rate names one server and what it bills an hour
type rate struct {
	name   string
	hourly int64
}

// servers are the API server's own providers, each with the small size, and their health
type servers struct {
	engine  *placement.Engine
	tracker *placement.Tracker
	hourly  map[string]*atomic.Int64
}

// newServers lists one provider per rate, in the order given
func newServers(t *testing.T, rates ...rate) *servers {
	t.Helper()
	s := &servers{tracker: placement.NewTracker(time.Now), hourly: map[string]*atomic.Int64{}}
	var ps []provider.Provider
	for _, r := range rates {
		h := &atomic.Int64{}
		h.Store(r.hourly)
		s.hourly[r.name] = h
		ps = append(ps, metered{Provider: local.New(local.Options{
			Name: r.name, Root: t.TempDir(), Sizes: []provider.Size{small}, Increment: time.Second,
		}), hourly: h})
	}
	reg, err := provider.NewRegistry(ps, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	s.engine = placement.New(reg, s.tracker, nil)
	return s
}

// down marks a provider unhealthy
func (s *servers) down(name string) {
	for range 3 {
		s.tracker.RecordFailure(name)
	}
}

// liveOptions give the API server the live catalog on s at 10%, an estimator and a 45 m hold
func liveOptions(s *servers) options {
	return options{
		apiCatalog: liveCatalog, apiEngine: s.engine, apiRule: &tenPercent,
		quoteHold: 45 * time.Minute, estimator: true,
	}
}

// lockedPrice prices a start package under the quotes of an estimate
func (e *env) lockedPrice(t *testing.T, id, estimateID string) *routerv1.PackageOption {
	t.Helper()
	resp, err := e.client.PricePackage(t.Context(), connect.NewRequest(
		&routerv1.PricePackageRequest{PackageId: id, EstimateId: estimateID}))
	if err != nil {
		t.Fatalf("price %s under %q: %v", id, estimateID, err)
	}
	return resp.Msg.GetOption()
}

// quotes reads the stored quotes of an estimate by package, without their times
func (e *env) quotes(t *testing.T, estimateID string) map[string]store.Quote {
	t.Helper()
	rows, err := e.pool.Query(t.Context(), `select package_id, provider, size_name, cost_micros,
		price_micros from price_quotes where estimate_id = $1`, uuid.MustParse(estimateID))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]store.Quote{}
	for rows.Next() {
		var q store.Quote
		if err := rows.Scan(&q.PackageID, &q.Provider, &q.SizeName, &q.CostMicros,
			&q.PriceMicros); err != nil {
			t.Fatal(err)
		}
		out[q.PackageID] = q
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// ageQuotes moves the quotes of an estimate back to age ago
func (e *env) ageQuotes(t *testing.T, estimateID string, age time.Duration) {
	t.Helper()
	_, err := e.pool.Exec(t.Context(), `update price_quotes set quoted_at = now() - $2::interval
		where estimate_id = $1`, uuid.MustParse(estimateID), age)
	if err != nil {
		t.Fatal(err)
	}
}

// byPackage keys options by their package
func byPackage(opts []*routerv1.PackageOption) map[string]*routerv1.PackageOption {
	out := map[string]*routerv1.PackageOption{}
	for _, o := range opts {
		out[o.GetPackageId()] = o
	}
	return out
}

// offer is what an option says of its price and availability
func offer(o *routerv1.PackageOption) []any {
	return []any{o.GetPriceMicros(), o.GetAvailable(), o.GetUnavailableReason()}
}

func (e *env) estimate(t *testing.T, r *routerv1.Requirements) []*routerv1.PackageOption {
	t.Helper()
	resp, err := e.client.Estimate(t.Context(), connect.NewRequest(
		&routerv1.EstimateRequest{Requirements: r}))
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	return resp.Msg.GetOptions()
}

func (e *env) pricePackage(
	t *testing.T, id string, r *routerv1.Requirements,
) (*routerv1.PackageOption, error) {
	t.Helper()
	resp, err := e.client.PricePackage(t.Context(), connect.NewRequest(
		&routerv1.PricePackageRequest{PackageId: id, Requirements: r}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetOption(), nil
}

func (e *env) price(t *testing.T, id string, r *routerv1.Requirements) *routerv1.PackageOption {
	t.Helper()
	opt, err := e.pricePackage(t, id, r)
	if err != nil {
		t.Fatalf("price %s: %v", id, err)
	}
	return opt
}

// reasons maps each option's package to its unavailable_reason, empty when it is available
func reasons(t *testing.T, opts []*routerv1.PackageOption) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, o := range opts {
		if o.GetAvailable() != (o.GetUnavailableReason() == "") {
			t.Fatalf("option %v, want a reason exactly when it is unavailable", o)
		}
		out[o.GetPackageId()] = o.GetUnavailableReason()
	}
	return out
}

// sameAsEstimate checks that Estimate lists starts packages and PricePackage answers each the same
func (e *env) sameAsEstimate(t *testing.T, starts int) {
	t.Helper()
	for _, r := range []*routerv1.Requirements{
		nil, {Ssh: true}, {Preview: true}, {Gpu: true}, {Secrets: true},
	} {
		opts := e.estimate(t, r)
		eq(t, len(opts), starts)
		for _, want := range opts {
			if got := e.price(t, want.GetPackageId(), r); !proto.Equal(got, want) {
				t.Fatalf("requirements %v, PricePackage %v, Estimate %v", r, got, want)
			}
		}
	}
}

func TestPricePackageMatchesEstimate(t *testing.T) {
	e := newEnv(t, options{})
	e.sameAsEstimate(t, 3)
	small := e.price(t, smallPkg, nil)
	eq(t, small.GetAvailable(), true)
	eq(t, small.GetPriceMicros(), int64(smallPrice))
	eq(t, small.GetMinutes(), uint32(10))
	eq(t, small.GetResources().GetVcpu(), uint32(2))
	eq(t, small.GetFits(), false)
	eq(t, small.GetExtensionsNeeded(), uint32(0))
	eq(t, small.GetShortOn(), []routerv1.Dimension(nil))
	gpu := e.price(t, gpuPkg, nil)
	eq(t, gpu.GetAvailable(), false)
	eq(t, gpu.GetPriceMicros(), int64(gpuPrice))
	for range 3 {
		e.tracker.RecordFailure("local")
	}
	eq(t, e.price(t, smallPkg, nil).GetUnavailableReason(), "no_candidate")
	e.sameAsEstimate(t, 3)
}

const thinPkg = "thin-10m"

// thinCatalog adds a start package priced over the local provider's 167 micro cost but under
// twice it, so only the markup keeps a provider from taking it
var thinCatalog = strings.TrimSuffix(catalogJSON, "\n]}") + `,
	{"id": "thin-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 200}
]}`

func TestPricePackageKeepsTheMarkup(t *testing.T) {
	e := newEnv(t, options{apiCatalog: thinCatalog})
	eq(t, reasons(t, e.estimate(t, nil)), map[string]string{
		thinPkg: "no_candidate", smallPkg: "", longPkg: "", gpuPkg: "feature_unavailable",
	})
	thin := e.price(t, thinPkg, nil)
	eq(t, thin.GetUnavailableReason(), "no_candidate")
	eq(t, thin.GetPriceMicros(), int64(200))
	e.sameAsEstimate(t, 4)
}

func TestUnavailableReasonCodes(t *testing.T) {
	e := newEnv(t, options{noPreviews: true})
	// The local provider here offers no SSH, previews, secrets or GPU
	for _, r := range []*routerv1.Requirements{{Ssh: true}, {Preview: true}, {Secrets: true}} {
		eq(t, reasons(t, e.estimate(t, r)), map[string]string{
			smallPkg: "feature_unavailable", longPkg: "feature_unavailable",
			gpuPkg: "feature_unavailable",
		})
		eq(t, e.price(t, longPkg, r).GetUnavailableReason(), "feature_unavailable")
	}
	eq(t, reasons(t, e.estimate(t, nil)), map[string]string{
		smallPkg: "", longPkg: "", gpuPkg: "feature_unavailable",
	})

	for range 3 {
		e.tracker.RecordFailure("local")
	}
	eq(t, reasons(t, e.estimate(t, nil)), map[string]string{
		smallPkg: "no_candidate", longPkg: "no_candidate", gpuPkg: "feature_unavailable",
	})
	eq(t, e.price(t, smallPkg, nil).GetUnavailableReason(), "no_candidate")
	ssh := &routerv1.Requirements{Ssh: true}
	eq(t, e.price(t, smallPkg, ssh).GetUnavailableReason(), "feature_unavailable")
}

func TestSecretsNeedAProviderReportingThem(t *testing.T) {
	need := &routerv1.Requirements{Secrets: true}
	none := newEnv(t, options{})
	eq(t, reasons(t, none.estimate(t, need)), map[string]string{
		smallPkg: "feature_unavailable", longPkg: "feature_unavailable",
		gpuPkg: "feature_unavailable",
	})
	for _, id := range []string{smallPkg, longPkg, gpuPkg} {
		eq(t, none.price(t, id, need).GetUnavailableReason(), "feature_unavailable")
	}

	offered := newEnv(t, options{secrets: true})
	eq(t, reasons(t, offered.estimate(t, need)), map[string]string{
		smallPkg: "", longPkg: "", gpuPkg: "feature_unavailable",
	})
	eq(t, offered.price(t, smallPkg, need).GetAvailable(), true)
	eq(t, offered.price(t, longPkg, need).GetAvailable(), true)
	offered.sameAsEstimate(t, 3)
}

func TestCreatePlacementNeedingAMissingFeatureFindsNoCandidate(t *testing.T) {
	e := newEnv(t, options{})
	for _, r := range []*routerv1.Requirements{{Ssh: true}, {Secrets: true}} {
		order, tok := e.startOrder(t, smallPkg, smallPrice)
		_, err := e.createFor(t, tok, r)
		mustCode(t, err, connect.CodeResourceExhausted, "")
		h, _ := handle.Derive(e.handleKey, order)
		p := e.get(t, h, 0, 0)
		eq(t, p.GetState(), routerv1.PlacementState_PLACEMENT_STATE_FAILED)
		eq(t, p.GetFailureReason(), "no_candidate")
	}
}

func TestEngineErrorIsInternalAndLogged(t *testing.T) {
	var logs syncBuffer
	detail := "dial tcp 10.9.8.7:443 for provider boat"
	e := newEnv(t, options{
		log:       slog.New(slog.NewTextHandler(&logs, nil)),
		apiEngine: failingEngine{err: errors.New(detail)},
	})
	opts := e.estimate(t, nil)
	eq(t, reasons(t, opts), map[string]string{
		smallPkg: "internal", longPkg: "internal", gpuPkg: "internal",
	})
	priced := e.price(t, smallPkg, nil)
	eq(t, priced.GetUnavailableReason(), "internal")
	for _, m := range append(opts, priced) {
		if wire := protojson.Format(m); strings.Contains(wire, "10.9.8.7") {
			t.Fatalf("the answer carries the engine's error text:\n%s", wire)
		}
	}
	logged := map[string]int{}
	for line := range strings.Lines(logs.String()) {
		_, rest, ok := strings.Cut(line, "package=")
		if ok && strings.Contains(line, "level=ERROR") && strings.Contains(line, detail) {
			pkg, _, _ := strings.Cut(rest, " ")
			logged[pkg]++
		}
	}
	eq(t, logged, map[string]int{smallPkg: 2, longPkg: 1, gpuPkg: 1})
}

func TestPricePackageRefusals(t *testing.T) {
	e := newEnv(t, options{})
	for name, c := range map[string]struct{ id, reason string }{
		"unknown":   {"large-10m", "PACKAGE_UNKNOWN"},
		"empty":     {"", "PACKAGE_UNKNOWN"},
		"extension": {morePkg, "NOT_START_PACKAGE"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := e.pricePackage(t, c.id, nil)
			mustCode(t, err, connect.CodeInvalidArgument, c.reason)
		})
	}
	stranger := e.clientWith(t, "Bearer wrong-token-0123456789abcdef0123456789", true)
	_, err := stranger.PricePackage(t.Context(), connect.NewRequest(
		&routerv1.PricePackageRequest{PackageId: smallPkg}))
	mustCode(t, err, connect.CodeUnauthenticated, "")
}

func TestPricePackageWritesNothing(t *testing.T) {
	e := newEnv(t, options{})
	rows, keys := e.rowCounts(t), e.keys(t)
	for range 30 {
		e.price(t, smallPkg, nil)
		e.price(t, gpuPkg, &routerv1.Requirements{Ssh: true})
		_, err := e.pricePackage(t, morePkg, nil)
		mustCode(t, err, connect.CodeInvalidArgument, "NOT_START_PACKAGE")
	}
	eq(t, e.rowCounts(t), rows)
	eq(t, e.keys(t), keys)
}

// rowCounts counts the rows of every table in the test database, keyed by schema and table
func (e *env) rowCounts(t *testing.T) map[string]int64 {
	t.Helper()
	ctx := t.Context()
	rows, err := e.pool.Query(ctx, `SELECT table_schema, table_name
		FROM information_schema.tables WHERE table_type = 'BASE TABLE'
		AND table_schema NOT IN ('pg_catalog', 'information_schema')`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []pgx.Identifier
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, pgx.Identifier{schema, table})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(tables) == 0 {
		t.Fatal("the test database has no tables")
	}
	out := map[string]int64{}
	for _, id := range tables {
		var n int64
		if err := e.pool.QueryRow(ctx, "SELECT count(*) FROM "+id.Sanitize()).Scan(&n); err != nil {
			t.Fatal(err)
		}
		out[id.Sanitize()] = n
	}
	return out
}

// keys lists the Valkey keys under the test's prefix
func (e *env) keys(t *testing.T) []string {
	t.Helper()
	keys, err := e.vk.Do(t.Context(), e.vk.B().Keys().Pattern(e.prefix+"*").Build()).
		AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

func TestPricePackageLocked(t *testing.T) {
	s := newServers(t, rate{"boat", boatRate}, rate{"e2b", e2bRate})
	e := newEnv(t, liveOptions(s))
	q, err := e.quote(t, &routerv1.EstimateRequest{})
	eq(t, err, nil)
	id := q.GetEstimateId()
	// boat moves to 36,000 an hour, so 610 s cost 6,100, priced at 6,710
	s.hourly["boat"].Store(2 * boatRate)
	eq(t, offer(e.lockedPrice(t, smallPkg, id)), []any{int64(3355), true, ""})
	eq(t, offer(e.price(t, smallPkg, nil)), []any{int64(6710), true, ""})
	for _, other := range []string{"", "garbage", uuid.NewString()} {
		eq(t, offer(e.lockedPrice(t, smallPkg, other)), []any{int64(6710), true, ""})
	}
	// 1,810 s were 9,050 priced at 9,955 and are now 18,100 priced at 19,910
	eq(t, e.lockedPrice(t, longPkg, id).GetPriceMicros(), int64(9955))
	eq(t, e.price(t, longPkg, nil).GetPriceMicros(), int64(19910))
	// With boat down nothing takes the start at the locked price, as e2b's is 25,031
	s.down("boat")
	eq(t, offer(e.lockedPrice(t, smallPkg, id)), []any{int64(3355), false, "no_candidate"})
	eq(t, offer(e.price(t, smallPkg, nil)), []any{int64(25031), true, ""})
}

func TestPricePackageLockExpires(t *testing.T) {
	s := newServers(t, rate{"boat", boatRate})
	e := newEnv(t, liveOptions(s))
	q, err := e.quote(t, &routerv1.EstimateRequest{})
	eq(t, err, nil)
	id := q.GetEstimateId()
	s.hourly["boat"].Store(2 * boatRate)
	e.ageQuotes(t, id, 44*time.Minute)
	eq(t, e.lockedPrice(t, smallPkg, id).GetPriceMicros(), int64(3355))
	e.ageQuotes(t, id, 46*time.Minute)
	eq(t, e.lockedPrice(t, smallPkg, id).GetPriceMicros(), int64(6710))
}

func TestPricePackageUnavailableKeepsAPrice(t *testing.T) {
	s := newServers(t, rate{"boat", boatRate}, rate{"e2b", e2bRate})
	e := newEnv(t, liveOptions(s))
	up, err := e.quote(t, &routerv1.EstimateRequest{})
	eq(t, err, nil)
	s.down("boat")
	s.down("e2b")
	// No server takes a start now, and each package keeps its cheapest landing's price
	q, err := e.quote(t, &routerv1.EstimateRequest{})
	eq(t, err, nil)
	eq(t, reasons(t, q.GetOptions()), map[string]string{
		smallPkg: "no_candidate", longPkg: "no_candidate", gpuPkg: "feature_unavailable",
	})
	opts := byPackage(q.GetOptions())
	eq(t, offer(opts[smallPkg]), []any{int64(3355), false, "no_candidate"})
	eq(t, offer(opts[longPkg]), []any{int64(9955), false, "no_candidate"})
	eq(t, offer(opts[gpuPkg]), []any{int64(0), false, "feature_unavailable"})
	eq(t, e.quotes(t, q.GetEstimateId())[smallPkg].Provider, "boat")
	for _, id := range []string{"", up.GetEstimateId(), q.GetEstimateId()} {
		eq(t, offer(e.lockedPrice(t, smallPkg, id)), []any{int64(3355), false, "no_candidate"})
		eq(t, offer(e.lockedPrice(t, gpuPkg, id)), []any{int64(0), false, "feature_unavailable"})
	}
}
