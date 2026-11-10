package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

func TestMonitorTargetLine(t *testing.T) {
	p := migrated(t)
	st := store.New(p)
	vk, _ := testenv.Valkey(t)
	ctx := t.Context()
	targets := config.Targets{FirstCommandP90: 5 * time.Second, MinPlacements: 20}
	lines := func() []string {
		return reserveLines(discard, st, vk, testPrefix, targets)(ctx)
	}
	// ready adds a placement on a path that became ready now, paid paidAfter before, whose first
	// job showed its first frame after frame
	ready := func(path string, paid, frame time.Duration) {
		t.Helper()
		id := uuid.New()
		_, err := p.Exec(ctx, `insert into placements (id, handle_sha256, order_id, package_id,
				state, ready_at, paid_at, start_path)
			values ($1, $2, $3, 'p', 'ready', now(), now() - make_interval(secs => $4), $5)`,
			id, []byte(id.String()), id.String(), paid.Seconds(), path)
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.Exec(ctx, `insert into jobs (id, placement_id, state, first_frame_ms)
			values ($1, $2, 'exited', $3)`, "j_"+id.String(), id, frame.Milliseconds())
		if err != nil {
			t.Fatal(err)
		}
	}
	// 19 placements are fewer than min_placements, however slow
	for range 19 {
		ready("reserved", 9*time.Second, 500*time.Millisecond)
	}
	eq(t, lines(), []string(nil))
	// The 20th makes the hour's p90 count, 9.5 s against a target of 5 s
	ready("pooled", 9*time.Second, 500*time.Millisecond)
	eq(t, lines(), []string{
		"payment to first command p90 is 9.5 s over the last hour, target 5 s"})
	// Cold placements are reported beside the target and never held to it
	for range 100 {
		ready("cold", 30*time.Second, time.Second)
	}
	eq(t, len(lines()), 1)
	// Twenty fast ones at the 90th percentile bring it under the target
	for range 200 {
		ready("reserved", time.Second, 500*time.Millisecond)
	}
	eq(t, lines(), []string(nil))

	// The line reaches /monitor beside the others
	for range 100 {
		ready("pooled", 20*time.Second, time.Second)
	}
	rec := httptest.NewRecorder()
	monitor(discard, st, vk, testPrefix, healthy, testPools, feedCheck{}, nil, limitsCheck{}, nil,
		canaryCheck{}, lossCheck{}, spendCheck{},
		reserveLines(discard, st, vk, testPrefix, targets))(rec,
		httptest.NewRequest(http.MethodGet, "/monitor", nil))
	eq(t, rec.Code, http.StatusServiceUnavailable)
	eq(t, strings.Contains(rec.Body.String(), "payment to first command p90 is 21.0 s"), true)
}

func TestMonitorBrakeLine(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	ctx := t.Context()
	lines := func() []string {
		return reserveLines(discard, st, vk, testPrefix, config.DefaultTargets())(ctx)
	}
	eq(t, lines(), []string(nil))
	err := vk.Do(ctx, vk.B().Set().Key(testPrefix+saga.BrakeKey).Value("23 4").
		Px(30*time.Minute).Build()).Error()
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(30 * time.Minute).UTC()
	got := lines()
	eq(t, len(got), 1)
	// The minute may have moved on while the test ran
	want := func(at time.Time) string {
		return fmt.Sprintf("reservations are braked until %s, 23 unused and 4 bound in the "+
			"last hour", at.Format("15:04"))
	}
	eq(t, got[0] == want(until) || got[0] == want(until.Add(time.Minute)) ||
		got[0] == want(until.Add(-time.Minute)), true)
	eq(t, vk.Do(ctx, vk.B().Del().Key(testPrefix+saga.BrakeKey).Build()).Error(), nil)
	eq(t, lines(), []string(nil))
}

func TestMonitorSpendLine(t *testing.T) {
	p := migrated(t)
	st := store.New(p)
	ctx := t.Context()
	small := provider.Size{
		Name: "small", Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
	}
	// An hour of this provider costs $3.60, and the cap is $2
	prov := local.New(local.Options{
		Name: "local", Root: t.TempDir(), Sizes: []provider.Size{small},
		HourlyCostMicros: 3_600_000,
	})
	reg, err := provider.NewRegistry([]provider.Provider{prov}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	settings := config.DefaultPools()
	settings.MaxDailyMicros = 2_000_000
	settings.Pool = []config.Pool{{Provider: "local", Size: "small", Min: 1, Max: 2,
		MaxAge: time.Hour}}
	m, err := pool.New(pool.Deps{Store: st, Registry: reg, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, poolLines(discard, st, m)(ctx), []string(nil))
	// A reservation's member held for an hour and then destroyed unused is waste of $3.60
	id := uuid.New()
	_, err = p.Exec(ctx, `insert into pool_members (id, provider, size_name, origin, state,
			idempotency_key, created_at, claimed_at, ended_at, end_reason)
		values ($1, 'local', 'small', 'reserve', 'gone', $2, now() - interval '70 minutes',
			now() - interval '70 minutes', now() - interval '10 minutes', 'lapsed')`,
		id, "reserve-"+id.String())
	if err != nil {
		t.Fatal(err)
	}
	got := poolLines(discard, st, m)(ctx)
	eq(t, got[len(got)-1], "reservations are off: pool waste is $3.60 of the $2.00 daily cap")
	// A day with less waste than the cap prints no spend line
	_, err = p.Exec(ctx, `update pool_members set ended_at = created_at + interval '1 minute'`)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range poolLines(discard, st, m)(ctx) {
		eq(t, strings.Contains(line, "reservations are off"), false)
	}
}

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
