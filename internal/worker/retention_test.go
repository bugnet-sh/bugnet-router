package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
)

const day = 24 * time.Hour

func count(t *testing.T, e *env, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func sqlRun(t *testing.T, e *env, query string, args ...any) {
	t.Helper()
	if _, err := e.pool.Exec(t.Context(), query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// seedEstimate stores an estimate served ago, with its catalog, and returns its id
func seedEstimate(t *testing.T, e *env, ago time.Duration, source string) uuid.UUID {
	t.Helper()
	sqlRun(t, e, `insert into catalogs (sha256, bytes) values ('\x01', '\x01')
		on conflict do nothing`)
	id := uuid.New()
	sqlRun(t, e, `
		insert into estimates (id, created_at, served_at, estimator_version, inputs_sha256,
			inputs, output, availability, catalog_sha256, source_key, revision)
		values ($1::uuid, now() - $2::interval, now() - $2::interval, 'v',
			decode(md5($1::text), 'hex'), '\x01',
			'\x01', '\x01', '\x01', nullif($3, ''), nullif($3, ''))`,
		id, ago.String(), source)
	return id
}

func seedFacts(t *testing.T, e *env, ago time.Duration, key string) {
	t.Helper()
	sqlRun(t, e, `
		insert into source_facts (source_key, revision, extractor_version, facts, facts_sha256,
			fetched_at, used_at)
		values ($1, $1, 'x', '\x01', '\x01', now() - $2::interval, now() - $2::interval)`,
		key, ago.String())
}

func TestRetentionAges(t *testing.T) {
	e := newEnv(t, options{})
	ctx := t.Context()
	now := time.Now()

	// start samples live 30 days
	addSample(t, e, "local", store.SamplePlacement, "ok", 31*day)
	addSample(t, e, "local", store.SampleCanary, "ok", 29*day)
	// benches live 90 days
	for id, age := range map[string]time.Duration{"b_old": 91 * day, "b_new": 89 * day} {
		sqlRun(t, e, `insert into benches (id, provider, size_name, runs, concurrency, actor,
			created_at) values ($1, 'local', 's', 1, 1, 'x', now() - $2::interval)`,
			id, age.String())
	}
	// transfer_index rows live 30 days
	p1, _ := e.placeReady(t, "order-1")
	p2, _ := e.placeReady(t, "order-2")
	for _, c := range []struct {
		hash string
		age  time.Duration
	}{{"01", 31 * day}, {"02", 29 * day}} {
		sqlRun(t, e, `insert into transfer_index (token_sha256, placement_id, created_at)
			values (decode(repeat($1, 32), 'hex'), $2, now() - $3::interval)`,
			c.hash, p1.ID, c.age.String())
	}
	// usage observations live 90 days
	for _, c := range []struct {
		id  uuid.UUID
		age time.Duration
	}{{p1.ID, 91 * day}, {p2.ID, 89 * day}} {
		sqlRun(t, e, `insert into usage_observations (placement_id, package_id, recorded_at)
			values ($1, 'p', now() - $2::interval)`, c.id, c.age.String())
	}
	// probe slots live 10 minutes and an old one gives its capacity back
	if err := e.store.SetCapacity(ctx, "local", 5); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Duration{11 * time.Minute, 9 * time.Minute} {
		ok, err := e.store.ReserveProbeSlot(ctx, "local", uuid.New(), now.Add(-at))
		if err != nil || !ok {
			t.Fatalf("reserve a probe slot: %v %v", ok, err)
		}
	}
	inUse := func() int {
		return count(t, e, `select in_use from provider_capacity where provider = 'local'`)
	}
	held := inUse()

	// estimates live 7 days unless a placement or an observation refers to them
	gone := seedEstimate(t, e, 8*day, "")
	seedEstimate(t, e, 6*day, "")
	byPlacement := seedEstimate(t, e, 8*day, "")
	sqlRun(t, e, `update placements set estimate_id = $1 where id = $2`, byPlacement, p1.ID)
	byObservation := seedEstimate(t, e, 8*day, "")
	sqlRun(t, e, `update usage_observations set estimate_id = $1 where placement_id = $2`,
		byObservation, p2.ID)
	// source facts live 7 days unless a kept estimate refers to them
	seedFacts(t, e, 8*day, "github.com/o/old")
	seedFacts(t, e, 6*day, "github.com/o/new")
	seedFacts(t, e, 8*day, "github.com/o/kept")
	seedEstimate(t, e, time.Hour, "github.com/o/kept")
	// wallets and catalogs stay
	sqlRun(t, e, `insert into wallets (payer, first_seen, last_seen)
		values ('w', now() - interval '400 days', now() - interval '400 days')`)

	eq(t, e.worker.Retain(ctx), false)

	eq(t, count(t, e, `select count(*) from start_samples
		where started_at < now() - interval '30 days'`), 0)
	eq(t, count(t, e, `select count(*) from start_samples where kind = 'canary'`), 1)
	eq(t, count(t, e, `select count(*) from benches where id = 'b_new'`), 1)
	eq(t, count(t, e, `select count(*) from benches`), 1)
	eq(t, count(t, e, `select count(*) from transfer_index`), 1)
	eq(t, count(t, e, `select count(*) from usage_observations where placement_id = $1`, p2.ID), 1)
	eq(t, count(t, e, `select count(*) from usage_observations`), 1)
	eq(t, count(t, e, `select count(*) from probe_slots`), 1)
	eq(t, inUse(), held-1)
	eq(t, count(t, e, `select count(*) from estimates where id = $1`, gone), 0)
	// the 6 day one, the two referred to, and the one served an hour ago
	eq(t, count(t, e, `select count(*) from estimates`), 4)
	eq(t, count(t, e, `select count(*) from source_facts`), 2)
	eq(t, count(t, e, `select count(*) from source_facts where source_key = 'github.com/o/old'`),
		0)
	eq(t, count(t, e, `select count(*) from wallets`), 1)
	eq(t, count(t, e, `select count(*) from catalogs`), 1)
}

func TestRetentionDeletesInBatches(t *testing.T) {
	e := newEnv(t, options{})
	sqlRun(t, e, `
		insert into start_samples (kind, provider, started_at, outcome)
		select 'canary', 'local', now() - interval '40 days', 'ok' from generate_series(1, 25000)`)
	// One pass deletes batches of 10,000 until a short batch ends the table
	eq(t, e.worker.Retain(context.Background()), false)
	eq(t, count(t, e, `select count(*) from start_samples`), 0)
}

func TestRetentionRunsDailyAtFourUTC(t *testing.T) {
	at := func(h, m int) time.Time { return time.Date(2026, 11, 9, h, m, 0, 0, time.UTC) }
	eq(t, worker.RetentionNext(at(3, 59)), at(4, 0))
	eq(t, worker.RetentionNext(at(4, 0)), at(4, 0).AddDate(0, 0, 1))
	eq(t, worker.RetentionNext(at(23, 0)), at(4, 0).AddDate(0, 0, 1))
	// Another zone's wall clock does not move it
	zone := time.FixedZone("x", 5*3600)
	eq(t, worker.RetentionNext(at(3, 0).In(zone)).UTC(), at(4, 0))
}
