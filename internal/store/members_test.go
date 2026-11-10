package store_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/migrations"
)

// fillWarm makes n warm members of a pool, each with a slot, as pool_fill leaves them
func fillWarm(t *testing.T, s *store.Store, provider, size string, n int) []store.Member {
	t.Helper()
	return fillWarmWith(t, s, provider, size, n, nil)
}

// fillWarmWith is fillWarm for sandboxes whose ref carries meta
func fillWarmWith(
	t *testing.T, s *store.Store, name, size string, n int, meta map[string]string,
) []store.Member {
	t.Helper()
	ctx := context.Background()
	var out []store.Member
	for range n {
		m, err := s.StartFill(ctx, name, size, 1, nil)
		if err != nil {
			t.Fatalf("start fill: %v", err)
		}
		ref := provider.Ref{Provider: name, ID: "sb-" + m.ID.String()[:8], Meta: meta}
		now := time.Now()
		if err := s.MarkMemberCreated(ctx, m.ID, ref, now); err != nil {
			t.Fatal(err)
		}
		if err := s.MarkMemberWarm(ctx, m.ID, ref, "", nil, now); err != nil {
			t.Fatal(err)
		}
		m, err = s.MemberByID(ctx, m.ID)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

func memberHolds(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) bool {
	t.Helper()
	var holds bool
	err := pool.QueryRow(context.Background(),
		`select holds_slot from pool_members where id = $1`, id).Scan(&holds)
	if err != nil {
		t.Fatal(err)
	}
	return holds
}

func TestStartFillReservesASlotAndTheRow(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	m, err := s.StartFill(ctx, "local", "small", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, m.State, store.MemberFilling)
	eq(t, m.HoldsSlot, true)
	eq(t, m.Origin, store.OriginPool)
	eq(t, m.IdempotencyKey, "pool-"+m.ID.String())
	eq(t, inUse(t, pool, "local"), 1)
	eq(t, recount(t, s, "local"), 1)
}

func TestStartFillStopsAtTheCapacityShare(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	if err := s.SetCapacity(ctx, "local", 8); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := s.StartFill(ctx, "local", "small", 0.25, nil); err != nil {
			t.Fatal(err)
		}
	}
	_, err := s.StartFill(ctx, "local", "small", 0.25, nil)
	eq(t, err, store.ErrPoolFull)
	eq(t, inUse(t, pool, "local"), 2)
}

func TestStartFillKeepsTheProbeMargin(t *testing.T) {
	s, pool := newStoreWithPool(t, store.WithReserveSlots(1))
	ctx := context.Background()
	if err := s.SetCapacity(ctx, "local", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartFill(ctx, "local", "small", 1, nil); err != nil {
		t.Fatal(err)
	}
	_, err := s.StartFill(ctx, "local", "small", 1, nil)
	eq(t, err, store.ErrPoolFull)
	eq(t, inUse(t, pool, "local"), 1)
}

func TestClaimWarmTakesEachMemberOnce(t *testing.T) {
	s, _ := newStoreWithPool(t)
	fillWarm(t, s, "local", "small", 5)
	var mu sync.Mutex
	var got []uuid.UUID
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := newPlacement(t, s, "o-"+strings.Repeat("x", i)+"-claim")
			var m store.Member
			var ok bool
			err := s.WithTx(context.Background(), func(tx pgx.Tx) error {
				var err error
				m, ok, err = s.Tx(tx).ClaimWarm(context.Background(), "local", "small",
					time.Time{}, store.Holder{Placement: p.ID, Attempt: 1})
				return err
			})
			if err != nil {
				t.Error(err)
			}
			if ok {
				mu.Lock()
				got = append(got, m.ID)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	eq(t, len(got), 5)
	seen := map[uuid.UUID]bool{}
	for _, id := range got {
		seen[id] = true
	}
	eq(t, len(seen), 5)
}

func TestClaimWarmTakesTheOldestWarmFirstAndHonoursAge(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	ms := fillWarm(t, s, "local", "small", 2)
	// The second member is the older one by its warm time, but too old for the package
	_, err := pool.Exec(ctx, `
		update pool_members set warm_at = now() - interval '2 hours',
			sandbox_created_at = now() - interval '5 hours' where id = $1`, ms[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	p := newPlacement(t, s, "o-age")
	got, ok, err := s.ClaimWarm(ctx, "local", "small", time.Now().Add(-time.Hour),
		store.Holder{Placement: p.ID, Attempt: 1})
	if err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	eq(t, got.ID, ms[0].ID)
	_, ok, err = s.ClaimWarm(ctx, "local", "small", time.Now().Add(-time.Hour),
		store.Holder{Placement: newPlacement(t, s, "o-age2").ID, Attempt: 1})
	eq(t, ok, false)
	eq(t, err, nil)
	got, ok, err = s.ClaimWarm(ctx, "local", "small", time.Time{},
		store.Holder{Placement: newPlacement(t, s, "o-age3").ID, Attempt: 1})
	eq(t, ok, true)
	eq(t, err, nil)
	eq(t, got.ID, ms[1].ID)
}

func TestPooledClaimMovesTheSlot(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	m := fillWarm(t, s, "local", "small", 1)[0]
	before := inUse(t, pool, "local")
	p := newPlacement(t, s, "o-move")
	got, ok, err := s.ClaimWarm(ctx, "local", "small", time.Time{},
		store.Holder{Placement: p.ID, Attempt: 2})
	if err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	eq(t, got.ID, m.ID)
	eq(t, got.State, store.MemberBound)
	eq(t, inUse(t, pool, "local"), before)
	eq(t, memberHolds(t, pool, m.ID), false)
	eq(t, held(t, s, p.ID), "local")
	var attempt int
	err = pool.QueryRow(ctx, `select capacity_attempt from placements where id = $1`,
		p.ID).Scan(&attempt)
	if err != nil || attempt != 2 {
		t.Fatalf("capacity_attempt = %d, %v, want 2", attempt, err)
	}
	eq(t, recount(t, s, "local"), before)
	// A release by that attempt frees the slot once
	released, err := s.ReleaseAttemptSlot(ctx, p.ID, 2)
	eq(t, released, true)
	eq(t, err, nil)
	eq(t, inUse(t, pool, "local"), 0)
}

func TestClaimMovesTheSlotToTheAttemptWhenThePlacementHoldsOne(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	fillWarm(t, s, "local", "small", 1)
	p := holdPlacement(t, s, "local", "o-hedge")
	holdAttempt(t, s, pool, p, "local", 2)
	_, err := pool.Exec(ctx, `update placement_attempts set slot_provider = null`)
	if err != nil {
		t.Fatal(err)
	}
	// Taking the slot of the attempt row's own provider leaves in_use as two for two holders
	_, ok, err := s.ClaimWarm(ctx, "local", "small", time.Time{},
		store.Holder{Placement: p.ID, Attempt: 2})
	if err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	eq(t, recount(t, s, "local"), inUse(t, pool, "local"))
	eq(t, held(t, s, p.ID), "local")
}

func TestClaimReservesNothingForAReservationHolder(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	m := fillWarm(t, s, "local", "small", 1)[0]
	got, ok, err := s.ClaimWarm(ctx, "local", "small", time.Time{},
		store.Holder{Reservation: uuid.New()})
	if err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	eq(t, got.State, store.MemberHeld)
	eq(t, memberHolds(t, pool, m.ID), true)
}

func TestMemberRefSealedUnderTheMemberThenThePlacement(t *testing.T) {
	_, keys := testenv.DataPlaneKeys(t)
	s, pool := newStoreWithPool(t, store.WithRefSealer(keys))
	ctx := context.Background()
	m := fillWarmWith(t, s, "local", "small", 1, credentialRef().Meta)[0]
	var raw string
	err := pool.QueryRow(ctx, `select provider_ref::text from pool_members where id = $1`,
		m.ID).Scan(&raw)
	if err != nil {
		t.Fatal(err)
	}
	for name, plain := range credentials {
		if strings.Contains(raw, plain) {
			t.Errorf("provider_ref holds %s in plaintext: %s", name, raw)
		}
	}
	eq(t, m.ProviderRef.Meta["envd_token"], credentials["envd_token"])
	// The member's own id opens it, and no other id does
	if _, err := keys.OpenRef(uuid.New(), sealedField(t, pool, m.ID)); err == nil {
		t.Error("a stranger's id opened the member's credential")
	}

	p := newPlacement(t, s, "o-sealed-claim")
	got, ok, err := s.ClaimWarm(ctx, "local", "small", time.Time{},
		store.Holder{Placement: p.ID, Attempt: 1})
	if err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	eq(t, got.ProviderRef.Meta["envd_token"], credentials["envd_token"])
	sealed := sealedField(t, pool, m.ID)
	if _, err := keys.OpenRef(m.ID, sealed); err == nil {
		t.Error("the member's id still opens the credential after the claim")
	}
	if plain, err := keys.OpenRef(p.ID, sealed); err != nil || plain != credentials["envd_token"] {
		t.Errorf("the placement's id opens %q, %v", plain, err)
	}
	again, err := s.MemberByID(ctx, m.ID)
	if err != nil || again.ProviderRef.Meta["envd_token"] != credentials["envd_token"] {
		t.Fatalf("read after the claim: %+v, %v", again.ProviderRef, err)
	}
}

// sealedField is the stored envd_token of a member's ref
func sealedField(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) string {
	t.Helper()
	var sealed string
	err := pool.QueryRow(context.Background(),
		`select provider_ref->'meta'->>'envd_token' from pool_members where id = $1`,
		id).Scan(&sealed)
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

func TestRetireAndGoneFreeTheSlotOnce(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	m := fillWarm(t, s, "local", "small", 1)[0]
	old, err := s.RetireMember(ctx, m.ID, store.EndAged, "")
	eq(t, old, store.MemberWarm)
	eq(t, err, nil)
	old, err = s.RetireMember(ctx, m.ID, store.EndFailed, "")
	eq(t, old, "")
	eq(t, err, nil)
	eq(t, inUse(t, pool, "local"), 1)
	for range 2 {
		if err := s.MemberGone(ctx, m.ID, store.EndGone, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, inUse(t, pool, "local"), 0)
	got, err := s.MemberByID(ctx, m.ID)
	eq(t, err, nil)
	eq(t, got.State, store.MemberGone)
	eq(t, got.EndReason, store.EndAged)
	eq(t, got.EndedAt != nil, true)
}

func TestGoneAfterAClaimFreesNoSlot(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	m := fillWarm(t, s, "local", "small", 1)[0]
	p := newPlacement(t, s, "o-bound-gone")
	if _, ok, err := s.ClaimWarm(ctx, "local", "small", time.Time{},
		store.Holder{Placement: p.ID, Attempt: 1}); err != nil || !ok {
		t.Fatal(ok, err)
	}
	eq(t, s.MemberGone(ctx, m.ID, store.EndFailed, time.Now()), nil)
	// The placement's slot is its own and stays
	eq(t, inUse(t, pool, "local"), 1)
	eq(t, held(t, s, p.ID), "local")
}

func TestMissUpsertsOneCounterPerMinute(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 10, 12, 0, 30, 0, time.UTC)
	for range 3 {
		eq(t, s.RecordMiss(ctx, "local", "small", at), nil)
	}
	eq(t, s.RecordMiss(ctx, "local", "small", at.Add(time.Minute)), nil)
	var rows, total int
	err := pool.QueryRow(ctx, `select count(*), sum(misses) from pool_misses`).Scan(&rows, &total)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, rows, 2)
	eq(t, total, 4)
	d, err := s.DemandSince(ctx, "local", "small", at.Add(-time.Minute))
	eq(t, err, nil)
	eq(t, d, store.Demand{Misses: 4})
}

func TestTargetsAndPauses(t *testing.T) {
	s, _ := newStoreWithPool(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, k := range []string{"a", "b"} {
		eq(t, s.PutTarget(ctx, store.PoolTarget{
			Provider: "local", SizeName: k, Target: 3, ComputedAt: now}), nil)
	}
	eq(t, s.PutTarget(ctx, store.PoolTarget{
		Provider: "local", SizeName: "a", Target: 4, ComputedAt: now}), nil)
	eq(t, s.DeleteTargetsExcept(ctx, [][2]string{{"local", "a"}}), nil)
	got, err := s.Targets(ctx)
	eq(t, err, nil)
	eq(t, got, []store.PoolTarget{{Provider: "local", SizeName: "a", Target: 4, ComputedAt: now}})
	eq(t, s.PutPause(ctx, store.Pause{Provider: "local", SizeName: "a", Until: now,
		Reason: store.PauseDrained, Actor: "ops"}), nil)
	ps, err := s.Pauses(ctx)
	eq(t, err, nil)
	eq(t, ps, []store.Pause{{Provider: "local", SizeName: "a", Until: now,
		Reason: store.PauseDrained, Actor: "ops"}})
}

func TestFillFailuresCountInARow(t *testing.T) {
	s, _ := newStoreWithPool(t)
	ctx := context.Background()
	since := time.Now().Add(-time.Hour)
	for range 3 {
		m, err := s.StartFill(ctx, "local", "small", 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.RetireMember(ctx, m.ID, store.EndFailed, "boom"); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.RecentFillFailures(ctx, "local", "small", since, 3)
	eq(t, err, nil)
	eq(t, n, 3)
	// A pause that began after them wipes the count
	n, err = s.RecentFillFailures(ctx, "local", "small", time.Now().Add(time.Minute), 3)
	eq(t, err, nil)
	eq(t, n, 0)
}

func TestPoolStatsCountsMembersAndDemand(t *testing.T) {
	s, _ := newStoreWithPool(t)
	ctx := context.Background()
	fillWarm(t, s, "local", "small", 3)
	if _, err := s.StartFill(ctx, "local", "small", 1, nil); err != nil {
		t.Fatal(err)
	}
	p := newPlacement(t, s, "o-stats")
	if _, ok, err := s.ClaimWarm(ctx, "local", "small", time.Time{},
		store.Holder{Placement: p.ID, Attempt: 1}); err != nil || !ok {
		t.Fatal(ok, err)
	}
	eq(t, s.RecordMiss(ctx, "local", "small", time.Now()), nil)
	stats, err := s.PoolStats(ctx, time.Now())
	eq(t, err, nil)
	if len(stats) != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	got := stats[0]
	eq(t, [5]int{got.Warm, got.Filling, got.Held, got.ClaimsLastHour, got.MissesLastHour},
		[5]int{2, 1, 0, 1, 1})
}

func TestMigration00025WidensTheSampleChecks(t *testing.T) {
	url := testenv.DatabaseURL(t)
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = db.Close() })
	gp, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	insert := func(kind, outcome string) error {
		_, err := db.ExecContext(ctx, `
			insert into start_samples (kind, provider, started_at, outcome)
			values ($1, 'local', now(), $2)`, kind, outcome)
		return err
	}
	if _, err := gp.UpTo(ctx, 24); err != nil {
		t.Fatal(err)
	}
	if insert("pool", "ok") == nil {
		t.Fatal("kind pool was accepted before migration 25")
	}
	if _, err := gp.UpTo(ctx, 25); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][2]string{{"pool", "ok"}, {"reserve", "ok"}, {"placement", "hedge_lost"}} {
		if err := insert(c[0], c[1]); err != nil {
			t.Fatalf("%v after migration 25: %v", c, err)
		}
	}
	if _, err := gp.DownTo(ctx, 24); err != nil {
		t.Fatalf("down: %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `select count(*) from start_samples`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	eq(t, n, 0)
	if _, err := gp.UpTo(ctx, 25); err != nil {
		t.Fatalf("up again: %v", err)
	}
}

func TestPoolSampleRoundTrips(t *testing.T) {
	s, _ := newStoreWithPool(t)
	ctx := context.Background()
	id := uuid.New()
	at := time.Now().UTC().Truncate(time.Millisecond)
	smp := store.StartSample{
		Kind: store.SamplePool, Provider: "local", SizeName: "small", StartedAt: at,
		Outcome: store.OutcomeOK, StartPath: store.PathPool, MemberID: id, WakeMS: 42,
		CreateMS: 10, ReadyMS: 900, RunnerMS: store.NoStep, GateMS: store.NoStep,
		FirstCmdMS: store.NoStep, WarmCmdMS: store.NoStep, DestroyMS: store.NoStep,
	}
	eq(t, s.InsertStartSample(ctx, smp), nil)
	got, err := s.StartSamples(ctx, "local", at.Add(-time.Minute))
	eq(t, err, nil)
	if len(got) != 1 {
		t.Fatalf("samples = %+v", got)
	}
	eq(t, [4]any{got[0].Kind, got[0].StartPath, got[0].MemberID, got[0].WakeMS},
		[4]any{store.SamplePool, store.PathPool, id, 42})
	p95, n, err := s.RefillP95(ctx, "local", "small", at.Add(-time.Hour))
	eq(t, err, nil)
	eq(t, [2]any{p95, n}, [2]any{900 * time.Millisecond, 1})
	// A fill never counts in the provider's ready p95, which reads cold starts alone
	h, err := s.HealthWindow(ctx, "local", at.Add(-time.Hour))
	eq(t, err, nil)
	eq(t, h.OK, 1)
	eq(t, h.ReadyP95, time.Duration(0))
}

func TestBindProvidedRecordsTheMemberBeforeTheClaim(t *testing.T) {
	_, keys := testenv.DataPlaneKeys(t)
	s, pool := newStoreWithPool(t, store.WithRefSealer(keys))
	ctx := context.Background()
	p := holdPlacement(t, s, "koyeb", "o1")
	rec := store.Attempt{
		PlacementID: p.ID, Attempt: 1, Provider: "koyeb", State: "creating", SizeName: "medium",
		Path: store.PathPooled, IdempotencyKey: "claim-key-1",
	}
	m, err := s.BindProvided(ctx, "koyeb", "medium", "claim-key-1", rec)
	eq(t, err, nil)
	eq(t, m.Origin, store.OriginProvider)
	eq(t, m.State, store.MemberBound)
	eq(t, m.HoldsSlot, false)
	eq(t, m.IdempotencyKey, "claim-key-1")
	eq(t, *m.PlacementID, p.ID)
	eq(t, m.ProviderRef == nil, true)
	atts, err := s.Attempts(ctx, p.ID)
	eq(t, err, nil)
	eq(t, len(atts), 1)
	eq(t, atts[0].MemberID, m.ID)
	// The slot is the attempt's own, so the member adds none
	eq(t, inUse(t, pool, "koyeb"), 1)
	eq(t, recount(t, s, "koyeb"), 1)

	// A retry of the same key finds the row a replica that died left
	again, err := s.BindProvided(ctx, "koyeb", "medium", "claim-key-1", rec)
	eq(t, err, nil)
	eq(t, again.ID, m.ID)

	// The claim's sandbox is stored sealed under the placement, and opens under it
	born := time.Now().Add(-time.Minute)
	ref := provider.Ref{Provider: "koyeb", ID: "app-1", Meta: map[string]string{
		"service_id": "svc-1", "secret": "member-secret",
	}}
	eq(t, s.MarkMemberClaimed(ctx, m.ID, p.ID, ref, born), nil)
	got, err := s.MemberByID(ctx, m.ID)
	eq(t, err, nil)
	eq(t, got.ProviderRef.Meta["secret"], "member-secret")
	eq(t, got.ProviderRef.Meta["service_id"], "svc-1")
	eq(t, got.SandboxCreatedAt.Sub(born) < time.Second, true)
	var raw string
	err = pool.QueryRow(ctx, `select provider_ref->'meta'->>'secret' from pool_members
		where id = $1`, m.ID).Scan(&raw)
	eq(t, err, nil)
	eq(t, strings.HasPrefix(raw, "v1."), true)
	// Another placement cannot record a claim for it
	err = s.MarkMemberClaimed(ctx, m.ID, uuid.New(), ref, born)
	if err == nil {
		t.Fatal("a claim was recorded for a placement that does not hold the member")
	}
}

func TestHedgeLosersLeftNamesPlacementsWhoseLoserStillExists(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	made := func(
		order string, readyAgo time.Duration, loserSandbox string, destroyed bool,
	) uuid.UUID {
		t.Helper()
		p := newPlacement(t, s, order)
		_, err := pool.Exec(ctx, `update placements set state = 'ready', hedged = true,
			ready_at = now() - make_interval(secs => $2) where id = $1`, p.ID, readyAgo.Seconds())
		eq(t, err, nil)
		var gone any
		if destroyed {
			gone = time.Now()
		}
		_, err = pool.Exec(ctx, `insert into placement_attempts (placement_id, attempt, provider,
				state, error_code, sandbox_id, destroyed_at)
			values ($1, 1, 'slow', 'failed', 'hedge_lost', nullif($2, ''), $3)`,
			p.ID, loserSandbox, gone)
		eq(t, err, nil)
		return p.ID
	}
	made("o-destroyed", 10*time.Minute, "a", true)
	made("o-no-sandbox", 10*time.Minute, "", false)
	made("o-young", time.Minute, "b", false)
	left := made("o-left", 10*time.Minute, "c", false)
	got, err := s.HedgeLosersLeft(ctx, time.Now().Add(-5*time.Minute), 10)
	eq(t, err, nil)
	eq(t, got, []uuid.UUID{left})
}
