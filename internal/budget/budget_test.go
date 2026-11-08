package budget_test

import (
	"math"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/budget"
	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// defaults are the [edge_budget] settings of an empty settings file
var defaults = config.EdgeBudget{
	BytesPer10Minutes: 2 << 30, BytesPerSecond: 5 << 20, RequestsPerSecond: 100,
	RequestsBurst: 200, QueuedRequests: 64, WebSockets: 32, EgressBytesPer10Minutes: 2 << 30,
	EgressBytesPerSecond: 2 << 20, EgressRequestsInFlight: 32,
}

func pkg(minutes int) catalog.Package {
	return catalog.Package{ID: "p", Minutes: minutes}
}

func pkgWithEdgeBytes(minutes int, size string) catalog.Package {
	p := pkg(minutes)
	n := map[string]int64{"4GiB": 4 << 30, "1GiB": 1 << 30}[size]
	p.Edge.Bytes, p.Edge.EgressBytes = n, n/4
	return p
}

type meter struct {
	*budget.Meter
	t      *testing.T
	vk     valkey.Client
	prefix string
}

func newMeter(t *testing.T) meter {
	vk, prefix := testenv.Valkey(t)
	return meter{Meter: budget.New(vk, prefix, defaults), t: t, vk: vk, prefix: prefix}
}

func limit(p uuid.UUID, bytes int64) budget.Limit {
	return budget.Limit{Placement: p, Budget: bytes, Deadline: time.Now().Add(10 * time.Minute)}
}

// report sends one placement's bytes out and returns the answers and the placements just spent
func (m meter) report(
	kind budget.Kind, by budget.Carrier, l budget.Limit, out uint64,
) (map[uuid.UUID]budget.Answer, []uuid.UUID) {
	m.t.Helper()
	a, spent, err := m.Report(m.t.Context(), kind, by,
		[]budget.Usage{{Placement: l.Placement, Out: out}},
		map[uuid.UUID]budget.Limit{l.Placement: l})
	if err != nil {
		m.t.Fatalf("report: %v", err)
	}
	return a, spent
}

func (m meter) pttl(key string) time.Duration {
	m.t.Helper()
	ms, err := m.vk.Do(m.t.Context(), m.vk.B().Pttl().Key(m.prefix+key).Build()).AsInt64()
	if err != nil {
		m.t.Fatal(err)
	}
	return time.Duration(ms) * time.Millisecond
}

func TestBudgetAtReady(t *testing.T) {
	eq(t, budget.BytesFor(pkg(10), defaults, budget.Edge), int64(2<<30))
	eq(t, budget.BytesFor(pkg(30), defaults, budget.Edge), int64(6<<30))
	eq(t, budget.BytesFor(pkgWithEdgeBytes(10, "4GiB"), defaults, budget.Edge), int64(4<<30))
	eq(t, budget.BytesFor(pkg(5), defaults, budget.Egress), int64(1<<30))
	eq(t, budget.BytesFor(pkgWithEdgeBytes(10, "4GiB"), defaults, budget.Egress), int64(1<<30))
	eq(t, budget.PaceFor(pkg(10), defaults, budget.Edge), int64(5<<20))
	eq(t, budget.PaceFor(pkg(10), defaults, budget.Egress), int64(2<<20))
	fast := pkg(10)
	fast.Edge.BytesPerSecond, fast.Edge.EgressBytesPerSecond = 10<<20, 4<<20
	eq(t, budget.PaceFor(fast, defaults, budget.Edge), int64(10<<20))
	eq(t, budget.PaceFor(fast, defaults, budget.Egress), int64(4<<20))
}

func TestReportAddsAndAnswersWhatIsLeft(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 10<<20)
	a, spent, err := m.Report(t.Context(), budget.Edge, budget.ByEdge, []budget.Usage{
		{Placement: p, In: 1 << 20, Out: 2 << 20}, {Placement: p, Out: 1 << 20},
	}, map[uuid.UUID]budget.Limit{p: l})
	eq(t, err, nil)
	eq(t, len(spent), 0)
	eq(t, a[p].Remaining, uint64(6<<20))
	got, err := m.Remaining(t.Context(), budget.Edge, l)
	eq(t, err == nil && got == 6<<20, true)
	untilExpiry := time.Until(l.Deadline.Add(time.Hour))
	// Valkey keeps whole milliseconds, so the expiry may round up past untilExpiry
	if ttl := m.pttl("edge:used:" + p.String()); ttl < untilExpiry-5*time.Second ||
		ttl > untilExpiry+time.Second {
		t.Fatalf("edge:used expires in %v, want about %v", ttl, untilExpiry)
	}
}

func TestReportForAPlacementWithoutALimitAnswersNothing(t *testing.T) {
	m := newMeter(t)
	known, gone := uuid.New(), uuid.New()
	a, spent, err := m.Report(t.Context(), budget.Edge, budget.ByEdge, []budget.Usage{
		{Placement: known, Out: 1}, {Placement: gone, Out: 1 << 30},
	}, map[uuid.UUID]budget.Limit{known: limit(known, 10)})
	eq(t, err, nil)
	eq(t, len(spent), 0)
	_, has := a[gone]
	eq(t, has, false)
	eq(t, len(a), 1)
	n, err := m.vk.Do(t.Context(), m.vk.B().Exists().Key(m.prefix+"edge:used:"+gone.String()).
		Build()).AsInt64()
	eq(t, err == nil && n == 0, true)
}

// reportConcurrently sends u from n meters at once and counts the meters that saw it spent
func reportConcurrently(t *testing.T, n int, u budget.Usage, l budget.Limit) int {
	t.Helper()
	vk, prefix := testenv.Valkey(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	spent := 0
	start := make(chan struct{})
	for range n {
		m := budget.New(vk, prefix, defaults)
		wg.Go(func() {
			<-start
			_, s, err := m.Report(t.Context(), budget.Edge, budget.ByEdge, []budget.Usage{u},
				map[uuid.UUID]budget.Limit{u.Placement: l})
			if err != nil {
				t.Errorf("report: %v", err)
			}
			mu.Lock()
			spent += len(s)
			mu.Unlock()
		})
	}
	close(start)
	wg.Wait()
	return spent
}

func TestBudgetSpentOnceAcrossReplicas(t *testing.T) {
	p := uuid.New()
	for range 5 {
		spent := reportConcurrently(t, 2, budget.Usage{Placement: p, Out: 600 << 20},
			limit(p, 1<<30))
		eq(t, spent, 1) // two reports of 600 MiB cross a 1 GiB budget, one sees the crossing
	}
}

func TestReportAnswersZeroOnceSpentAndSpendsOnce(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 1<<20)
	a, spent := m.report(budget.Edge, budget.ByEdge, l, 2<<20)
	eq(t, a[p].Remaining, uint64(0))
	eq(t, spent, []uuid.UUID{p})
	a, spent = m.report(budget.Edge, budget.BySSH, l, 1)
	eq(t, a[p].Remaining, uint64(0))
	eq(t, len(spent), 0)
	ttl := m.pttl("edge:spent:" + p.String())
	if ttl < 69*time.Minute || ttl > 70*time.Minute+time.Second {
		t.Fatalf("edge:spent expires in %v, want the deadline plus 1h", ttl)
	}
}

func TestExtensionClearsSpentAndCountsOnFromWhatWasUsed(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 1<<20)
	_, spent := m.report(budget.Edge, budget.ByEdge, l, 3<<20/2)
	eq(t, spent, []uuid.UUID{p})
	m.report(budget.Egress, budget.ByEdge, l, 2<<20)
	l.Budget = 2 << 20
	if err := m.Grow(t.Context(), p, 2<<20, 2<<20, l.Deadline); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []budget.Kind{budget.Edge, budget.Egress} {
		n, err := m.vk.Do(t.Context(), m.vk.B().Exists().
			Key(m.prefix+string(kind)+":spent:"+p.String()).Build()).AsInt64()
		eq(t, err == nil && n == 0, true)
	}
	left, err := m.Remaining(t.Context(), budget.Edge, l)
	eq(t, err == nil && left == 1<<19, true) // remaining = new budget - used
	a, spent := m.report(budget.Edge, budget.ByEdge, l, 1<<20)
	eq(t, a[p].Remaining, uint64(0))
	eq(t, spent, []uuid.UUID{p})
}

func TestAReportComparesAgainstTheBudgetAnExtensionWrote(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	stale := limit(p, 1<<20)
	if err := m.Grow(t.Context(), p, 2<<20, 2<<20, stale.Deadline); err != nil {
		t.Fatal(err)
	}
	if err := m.Grow(t.Context(), p, 1<<20, 1<<20, stale.Deadline); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []budget.Kind{budget.Edge, budget.Egress} {
		key := string(kind) + ":budget:" + p.String()
		got, err := m.vk.Do(t.Context(), m.vk.B().Get().Key(m.prefix+key).Build()).ToString()
		eq(t, err == nil && got == strconv.Itoa(2<<20), true)
		if ttl := m.pttl(key); ttl < 69*time.Minute || ttl > 70*time.Minute+time.Second {
			t.Fatalf("%s expires in %v, want the deadline plus 1h", key, ttl)
		}
		a, spent := m.report(kind, budget.ByEdge, stale, 3<<20/2)
		eq(t, len(spent), 0)
		eq(t, a[p].Remaining, uint64(1<<19))
		gone, err := m.Spent(t.Context(), kind, p)
		eq(t, err == nil && !gone, true)
		a, spent = m.report(kind, budget.ByEdge, stale, 1<<19)
		eq(t, spent, []uuid.UUID{p})
		eq(t, a[p].Remaining, uint64(0))
	}
}

func TestSeenLastsThirtySecondsPastTheLastReportWithBytes(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	m.report(budget.Edge, budget.ByEdge, limit(p, 1<<30), 1)
	if ttl := m.pttl("edge:seen:" + p.String()); ttl <= 29*time.Second || ttl > 30*time.Second {
		t.Fatalf("edge:seen expires in %v, want 30 s", ttl)
	}
}

// expiresAt is the key's expiry in Unix milliseconds, -2 when the key is absent
func (m meter) expiresAt(key string) int64 {
	m.t.Helper()
	at, err := m.vk.Do(m.t.Context(), m.vk.B().Pexpiretime().Key(m.prefix+key).Build()).AsInt64()
	if err != nil {
		m.t.Fatal(err)
	}
	return at
}

func TestMoveDeadlineKeepsBothCountersAnHourPastIt(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 10<<20)
	m.report(budget.Edge, budget.ByEdge, l, 1<<20)
	m.report(budget.Egress, budget.ByEdge, l, 1<<20)
	later := l.Deadline.Add(30 * time.Minute)
	if err := m.MoveDeadline(t.Context(), p, later); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"edge:used:", "egress:used:"} {
		eq(t, m.expiresAt(key+p.String()), later.Add(time.Hour).UnixMilli())
	}
	if err := m.MoveDeadline(t.Context(), p, l.Deadline); err != nil {
		t.Fatal(err)
	}
	eq(t, m.expiresAt("egress:used:"+p.String()), later.Add(time.Hour).UnixMilli())

	// A placement that reported nothing gets no counters
	q := uuid.New()
	if err := m.MoveDeadline(t.Context(), q, later); err != nil {
		t.Fatal(err)
	}
	eq(t, m.expiresAt("edge:used:"+q.String()), int64(-2))
	eq(t, m.expiresAt("egress:used:"+q.String()), int64(-2))
}

func TestAReportNeverShortensTheExpiry(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 10<<20)
	later := l
	later.Deadline = l.Deadline.Add(30 * time.Minute)
	m.report(budget.Edge, budget.ByEdge, later, 1<<20)
	m.report(budget.Edge, budget.ByEdge, l, 1<<20)
	eq(t, m.expiresAt("edge:used:"+p.String()), later.Deadline.Add(time.Hour).UnixMilli())
}

func TestEdgeAndEgressCountApart(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 10<<20)
	m.report(budget.Edge, budget.ByEdge, l, 4<<20)
	a, _ := m.report(budget.Egress, budget.ByEdge, l, 1<<20)
	eq(t, a[p].Remaining, uint64(9<<20))
	eq(t, a[p].Pace, uint64(2<<20))
	left, err := m.RemainingAll(t.Context(), budget.Edge, []budget.Limit{l, limit(uuid.New(), 5)})
	eq(t, err, nil)
	eq(t, left[p], uint64(6<<20))
	eq(t, len(left), 2)
}

func TestPaceHalvesWhenBothCarry(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 1<<30)
	a, _ := m.report(budget.Edge, budget.ByEdge, l, 1<<20)
	eq(t, a[p].Pace, uint64(5<<20))
	a, _ = m.report(budget.Edge, budget.BySSH, l, 1<<20)
	eq(t, a[p].Pace, uint64(5<<20)/2)
	a, _ = m.report(budget.Edge, budget.ByEdge, l, 0)
	eq(t, a[p].Pace, uint64(5<<20)/2)
	if ttl := m.pttl("edge:seen:" + p.String()); ttl <= 29*time.Second || ttl > 30*time.Second {
		t.Fatalf("edge:seen expires in %v, want 30s", ttl)
	}
}

func TestPaceIsFullWhenTheOtherCarrierIsQuiet(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 1<<30)
	l.BytesPerSecond = 8 << 20
	// SSH carried bytes 11 s ago, longer than the 10 s that make it count
	now, err := m.vk.Do(t.Context(), m.vk.B().Time().Build()).AsIntSlice()
	if err != nil {
		t.Fatal(err)
	}
	old := now[0]*1000 + now[1]/1000 - 11_000
	err = m.vk.Do(t.Context(), m.vk.B().Hset().Key(m.prefix+"edge:seen:"+p.String()).
		FieldValue().FieldValue("ssh", strconv.FormatInt(old, 10)).Build()).Error()
	if err != nil {
		t.Fatal(err)
	}
	a, _ := m.report(budget.Edge, budget.ByEdge, l, 1<<20)
	eq(t, a[p].Pace, uint64(8<<20))
	// A report of no bytes does not count as carrying
	m.report(budget.Edge, budget.BySSH, l, 0)
	a, _ = m.report(budget.Edge, budget.ByEdge, l, 1)
	eq(t, a[p].Pace, uint64(8<<20))
}

func TestHugeReportsNeitherOverflowNorFail(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, math.MaxInt64)
	a, spent, err := m.Report(t.Context(), budget.Edge, budget.ByEdge, []budget.Usage{
		{Placement: p, In: math.MaxUint64, Out: math.MaxUint64},
	}, map[uuid.UUID]budget.Limit{p: l})
	eq(t, err, nil)
	eq(t, len(spent), 0)
	eq(t, a[p].Remaining > 0, true)
}

func TestSpentListFollowsTheReportOrder(t *testing.T) {
	m := newMeter(t)
	ps := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	var usage []budget.Usage
	limits := map[uuid.UUID]budget.Limit{}
	for _, p := range ps {
		usage = append(usage, budget.Usage{Placement: p, Out: 2})
		limits[p] = limit(p, 1)
	}
	_, spent, err := m.Report(t.Context(), budget.Edge, budget.ByEdge, usage, limits)
	eq(t, err, nil)
	eq(t, slices.Equal(spent, ps), true)
}

// unpaid reports one placement's unpaid use by host and answers the fields dropped
func (m meter) unpaid(l budget.Limit, hosts map[string]budget.X402Unpaid) int {
	m.t.Helper()
	dropped, err := m.ReportX402Unpaid(m.t.Context(),
		map[uuid.UUID]map[string]budget.X402Unpaid{l.Placement: hosts},
		map[uuid.UUID]budget.Limit{l.Placement: l})
	if err != nil {
		m.t.Fatalf("report unpaid: %v", err)
	}
	return dropped[l.Placement]
}

// unpaidUse reads back the placement's unpaid totals
func (m meter) unpaidUse(p uuid.UUID) map[string]budget.X402Unpaid {
	m.t.Helper()
	got, err := m.X402Unpaid(m.t.Context(), p)
	if err != nil {
		m.t.Fatalf("read unpaid: %v", err)
	}
	return got
}

func TestX402UnpaidDeltasAdd(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 10<<20)
	eq(t, m.unpaidUse(p), map[string]budget.X402Unpaid{})
	eq(t, m.unpaid(l, map[string]budget.X402Unpaid{"agent402.tools": {3, 1000}}), 0)
	eq(t, m.unpaid(l, map[string]budget.X402Unpaid{
		"agent402.tools": {2, 500}, "api.example.com": {0, 7},
	}), 0)
	eq(t, m.unpaidUse(p), map[string]budget.X402Unpaid{
		"agent402.tools": {5, 1500}, "api.example.com": {0, 7},
	})
	n, err := m.vk.Do(t.Context(), m.vk.B().Hlen().Key(m.prefix+"egress:x402:"+p.String()).
		Build()).AsInt64()
	eq(t, err == nil && n == 4, true)
}

func TestX402UnpaidAcrossReplicas(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	meters := []*budget.Meter{
		budget.New(vk, prefix, defaults), budget.New(vk, prefix, defaults),
	}
	p := uuid.New()
	l := limit(p, 10<<20)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 50 {
		m := meters[i%2]
		wg.Go(func() {
			<-start
			_, err := m.ReportX402Unpaid(t.Context(), map[uuid.UUID]map[string]budget.X402Unpaid{
				p: {"agent402.tools": {1, 10}},
			}, map[uuid.UUID]budget.Limit{p: l})
			if err != nil {
				t.Errorf("report unpaid: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	got, err := meters[1].X402Unpaid(t.Context(), p)
	eq(t, err, nil)
	eq(t, got, map[string]budget.X402Unpaid{"agent402.tools": {50, 500}})
}

func TestX402UnpaidExpiryNeverEarlier(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 10<<20)
	key := "egress:x402:" + p.String()
	m.unpaid(l, map[string]budget.X402Unpaid{"agent402.tools": {1, 1}})
	eq(t, m.expiresAt(key), l.Deadline.Add(time.Hour).UnixMilli())
	older := l
	older.Deadline = l.Deadline.Add(-5 * time.Minute)
	m.unpaid(older, map[string]budget.X402Unpaid{"agent402.tools": {1, 1}})
	eq(t, m.expiresAt(key), l.Deadline.Add(time.Hour).UnixMilli())
	later := l
	later.Deadline = l.Deadline.Add(5 * time.Minute)
	m.unpaid(later, map[string]budget.X402Unpaid{"agent402.tools": {1, 1}})
	eq(t, m.expiresAt(key), later.Deadline.Add(time.Hour).UnixMilli())
}

func TestX402UnpaidFieldCap(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 10<<20)
	hosts := map[string]budget.X402Unpaid{}
	for i := range 33 {
		hosts["h"+strconv.Itoa(100+i)+".example.com"] = budget.X402Unpaid{Requests: 1, Bytes: 1}
	}
	eq(t, m.unpaid(l, hosts), 2)
	n, err := m.vk.Do(t.Context(), m.vk.B().Hlen().Key(m.prefix+"egress:x402:"+p.String()).
		Build()).AsInt64()
	eq(t, err == nil && n == 64, true)
	got := m.unpaidUse(p)
	eq(t, len(got), 32)
	// Hosts are added in sorted order, so the last one is the one dropped
	_, has := got["h132.example.com"]
	eq(t, has, false)
	eq(t, m.unpaid(l, map[string]budget.X402Unpaid{
		"h100.example.com": {1, 1}, "zz.example.com": {1, 1},
	}), 2)
	eq(t, m.unpaidUse(p)["h100.example.com"], budget.X402Unpaid{Requests: 2, Bytes: 2})
}

func TestX402UnpaidSkipsUnknownPlacementsBadHostsAndClampsDeltas(t *testing.T) {
	m := newMeter(t)
	known, gone := uuid.New(), uuid.New()
	l := limit(known, 10<<20)
	dropped, err := m.ReportX402Unpaid(t.Context(), map[uuid.UUID]map[string]budget.X402Unpaid{
		known: {
			"agent402.tools": {math.MaxUint64, math.MaxUint64}, "Bad.Host": {1, 1},
			"127.0.0.1": {1, 1}, "a:b": {1, 1}, "quiet.example.com": {},
		},
		gone: {"agent402.tools": {1, 1}},
	}, map[uuid.UUID]budget.Limit{known: l})
	eq(t, err, nil)
	eq(t, len(dropped), 0)
	eq(t, m.unpaidUse(known), map[string]budget.X402Unpaid{
		"agent402.tools": {1 << 40, 1 << 40},
	})
	eq(t, m.expiresAt("egress:x402:"+gone.String()), int64(-2))
}

func TestMoveDeadlineMovesX402Key(t *testing.T) {
	m := newMeter(t)
	p := uuid.New()
	l := limit(p, 10<<20)
	m.unpaid(l, map[string]budget.X402Unpaid{"agent402.tools": {1, 1}})
	later := l.Deadline.Add(30 * time.Minute)
	if err := m.MoveDeadline(t.Context(), p, later); err != nil {
		t.Fatal(err)
	}
	eq(t, m.expiresAt("egress:x402:"+p.String()), later.Add(time.Hour).UnixMilli())
	if err := m.MoveDeadline(t.Context(), p, l.Deadline); err != nil {
		t.Fatal(err)
	}
	eq(t, m.expiresAt("egress:x402:"+p.String()), later.Add(time.Hour).UnixMilli())

	// A placement with no unpaid use gets no hash
	q := uuid.New()
	if err := m.MoveDeadline(t.Context(), q, later); err != nil {
		t.Fatal(err)
	}
	eq(t, m.expiresAt("egress:x402:"+q.String()), int64(-2))
}
