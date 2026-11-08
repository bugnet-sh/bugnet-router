package api_test

import (
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/oynozan/bugnet-router/internal/handle"
	"github.com/oynozan/bugnet-router/internal/placement"
)

// failingEngine answers every candidates call with err
type failingEngine struct{ err error }

func (f failingEngine) Candidates(placement.Inputs) ([]placement.Candidate, error) {
	return nil, f.err
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

// thinCatalog adds a start package priced over the local provider's 167 micro cost but under the
// 0.5 margin floor, so only the floor keeps a provider from taking it
var thinCatalog = strings.TrimSuffix(catalogJSON, "\n]}") + `,
	{"id": "thin-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 200}
]}`

func TestPricePackageKeepsTheMarginFloor(t *testing.T) {
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
