package history_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/history"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

func eq[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

var epoch = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func settings() history.Settings {
	return history.Settings{
		Window: 2160 * time.Hour, MinObservations: 5, MinPayers: 3, LowerObservations: 10,
		LowerPayers: 5, MayLower: true, OOMPayers: 2, PayerMinAge: 168 * time.Hour,
		PayerMinPlacements: 3, HeadroomPercent: 10,
		PackageMemoryMiB: map[string]int{"small": 2048, "std": 4096},
		Now:              func() time.Time { return epoch.Add(24 * time.Hour) },
	}
}

type rig struct {
	pool *pgxpool.Pool
	n    int
}

func newRig(t *testing.T) *rig {
	t.Helper()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(t.Context(), url); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &rig{pool: pool}
}

func (r *rig) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := r.pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// wallet adds a payer first seen age before the epoch with n paid placements
func (r *rig) wallet(t *testing.T, payer string, age time.Duration, paid int) {
	t.Helper()
	r.exec(t, `insert into wallets (payer, first_seen, last_seen) values ($1, $2, $2)`,
		payer, epoch.Add(-age))
	for range paid {
		r.placement(t, payer)
	}
}

func (r *rig) placement(t *testing.T, payer string) uuid.UUID {
	t.Helper()
	r.n++
	id := uuid.New()
	r.exec(t, `insert into placements (id, handle_sha256, order_id, package_id, state, payer)
		values ($1, $2, $3, 'std', 'terminated', $4)`, id, []byte(fmt.Sprint("h", r.n)),
		fmt.Sprint("o", r.n), payer)
	return id
}

type obs struct {
	payer, source, revision, lock string
	pkg                           string
	mem, disk, active, oom        int
	age                           time.Duration
	flagged                       bool
}

func (r *rig) observe(t *testing.T, o obs) {
	t.Helper()
	id := r.placement(t, o.payer)
	r.exec(t, `insert into usage_observations (placement_id, source_key, revision, lock_key,
			workload, package_id, peak_memory_mib, peak_disk_mib, active_seconds, oom_kills,
			flagged, recorded_at)
		values ($1, $2, nullif($3, ''), nullif($4, ''), 'tests', coalesce(nullif($5, ''), 'std'),
			$6, $7, $8, $9, $10, $11)`, id, o.source, o.revision, o.lock, o.pkg, o.mem, o.disk,
		o.active, o.oom, o.flagged, epoch.Add(-o.age))
}

func (r *rig) load(t *testing.T, key history.Key, s history.Settings) rules.History {
	t.Helper()
	h, err := history.Load(t.Context(), store.New(r.pool).Querier(), key, "tests", s)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

const src = "github.com/acme/app"

// established makes three payers who count under the default settings
func (r *rig) established(t *testing.T, payers ...string) {
	for _, p := range payers {
		r.wallet(t, p, 30*24*time.Hour, 3)
	}
}

func TestHistoryThresholds(t *testing.T) {
	r := newRig(t)
	r.established(t, "a", "b", "c")
	key := history.Key{Source: src}
	// Four rows are too few, however many payers
	for i, p := range []string{"a", "b", "c", "a"} {
		r.observe(t, obs{payer: p, source: src, mem: 1000 + 100*i, disk: 1000, active: 120})
	}
	eq(t, r.load(t, key, settings()).Observations, 0)
	// Five rows from two payers are too few payers
	r2 := newRig(t)
	r2.established(t, "a", "b")
	for _, p := range []string{"a", "b", "a", "b", "a"} {
		r2.observe(t, obs{payer: p, source: src, mem: 1000, disk: 1000})
	}
	eq(t, r2.load(t, key, settings()).Observations, 0)
	// A fifth row from a third payer's second run reaches both
	r.observe(t, obs{payer: "c", source: src, mem: 1400, disk: 3000, active: 61})
	h := r.load(t, key, settings())
	eq(t, h.Observations, 5)
	eq(t, h.Payers, 3)
	// Per-payer maxima are a: 1300, b: 1100, c: 1400, and percentile_disc(0.9) of three is the top
	eq(t, h.MemoryMiB, 1400)
	eq(t, h.DiskMiB, 3000)
	eq(t, h.ActiveMinutes, 2)
	eq(t, h.HeadroomPercent, 10)
	eq(t, h.Lower, false)
}

func TestHistoryPercentileIsARealObservation(t *testing.T) {
	r := newRig(t)
	for i := range 10 {
		p := fmt.Sprint("p", i)
		r.wallet(t, p, 30*24*time.Hour, 3)
		r.observe(t, obs{payer: p, source: src, mem: 100 * (i + 1)})
	}
	h := r.load(t, history.Key{Source: src}, settings())
	// percentile_disc(0.9) of 100..1000 is the ninth value
	eq(t, h.MemoryMiB, 900)
	eq(t, h.Lower, true)
}

func TestOOMCensoring(t *testing.T) {
	r := newRig(t)
	r.established(t, "a", "b", "c")
	for _, p := range []string{"a", "b", "c"} {
		r.observe(t, obs{payer: p, source: src, mem: 900})
	}
	// Two payers push on the 2048 MiB package, one never does
	r.observe(t, obs{payer: "a", source: src, pkg: "small", mem: 1500, oom: 1})
	r.observe(t, obs{payer: "b", source: src, pkg: "small", mem: 2000, oom: 2})
	r.observe(t, obs{payer: "c", source: src, pkg: "std", mem: 3000, oom: 0})
	h := r.load(t, history.Key{Source: src}, settings())
	// 1.25 x 2048 = 2560 for both of them, so the 90th percentile of 2560, 2560, 3000 is 3000
	eq(t, h.MemoryMiB, 3000)
	eq(t, fmt.Sprint(h.OOMPackages), "[small]")
	// One wallet's OOMs, however many, push nothing
	r2 := newRig(t)
	r2.established(t, "a", "b", "c")
	for _, p := range []string{"a", "b", "c"} {
		r2.observe(t, obs{payer: p, source: src, mem: 900})
	}
	for range 3 {
		r2.observe(t, obs{payer: "a", source: src, pkg: "small", mem: 1500, oom: 1})
	}
	h = r2.load(t, history.Key{Source: src}, settings())
	eq(t, len(h.OOMPackages), 0)
	eq(t, h.MemoryMiB, 2560)
}

func TestPayersThatDoNotCount(t *testing.T) {
	r := newRig(t)
	r.established(t, "a", "b", "c")
	r.wallet(t, "young", 24*time.Hour, 3)      // under 7 days
	r.wallet(t, "few", 30*24*time.Hour, 0)     // under 3 placements, its two rows add two
	r.wallet(t, "blocked", 30*24*time.Hour, 3) // ever blocked
	r.exec(t, `insert into wallet_events (id, payer, kind, actor)
		values ($1, 'blocked', 'block', 'router')`, uuid.New())
	for _, p := range []string{"young", "blocked"} {
		for range 3 {
			r.observe(t, obs{payer: p, source: src, mem: 99999})
		}
	}
	for range 2 {
		r.observe(t, obs{payer: "few", source: src, mem: 99999})
	}
	// An observation with no payer, a flagged one and one outside the window never count
	r.observe(t, obs{payer: "", source: src, mem: 99999})
	r.observe(t, obs{payer: "a", source: src, mem: 99999, flagged: true})
	r.observe(t, obs{payer: "a", source: src, mem: 99999, age: 91 * 24 * time.Hour})
	for _, p := range []string{"a", "b", "c", "a", "b"} {
		r.observe(t, obs{payer: p, source: src, mem: 1000})
	}
	h := r.load(t, history.Key{Source: src}, settings())
	eq(t, h.Observations, 5)
	eq(t, h.MemoryMiB, 1000)
}

func TestHistoryLevels(t *testing.T) {
	r := newRig(t)
	r.established(t, "a", "b", "c")
	// Two rows on the revision, five on the lock key, six on the source
	for _, p := range []string{"a", "b"} {
		r.observe(t, obs{payer: p, source: src, revision: "r1", lock: "l1", mem: 1000})
	}
	for _, p := range []string{"a", "b", "c"} {
		r.observe(t, obs{payer: p, source: src, revision: "r0", lock: "l1", mem: 2000})
	}
	r.observe(t, obs{payer: "c", source: src, revision: "r2", lock: "l2", mem: 5000})
	key := history.Key{Source: src, Revision: "r1", Lock: "l1"}
	h := r.load(t, key, settings())
	// The revision has too few, the lock key has five rows and three payers
	eq(t, h.Observations, 5)
	eq(t, h.MemoryMiB, 2000)
	// Another lock key falls through to the source
	key.Lock = "l9"
	h = r.load(t, key, settings())
	eq(t, h.Observations, 6)
	eq(t, h.MemoryMiB, 5000)
	// No source key, no history
	eq(t, r.load(t, history.Key{}, settings()).Observations, 0)
}

func TestHistoryHonoursItsSettings(t *testing.T) {
	r := newRig(t)
	for i := range 5 {
		p := fmt.Sprint("p", i)
		r.wallet(t, p, 30*24*time.Hour, 3)
		r.observe(t, obs{payer: p, source: src, mem: 1000})
	}
	s := settings()
	eq(t, r.load(t, history.Key{Source: src}, s).Lower, false) // five payers, five rows
	s.LowerObservations, s.LowerPayers = 5, 5
	eq(t, r.load(t, history.Key{Source: src}, s).Lower, true)
	s.MayLower = false
	eq(t, r.load(t, history.Key{Source: src}, s).Lower, false)
}
