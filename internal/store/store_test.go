package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/testenv"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	s, _ := newStoreWithPool(t)
	return s
}

func newStoreWithPool(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	url := testenv.DatabaseURL(t)
	ctx := context.Background()
	if err := store.Migrate(ctx, url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return store.New(pool), pool
}

func newPlacement(t *testing.T, s *store.Store, order string) store.Placement {
	t.Helper()
	p, created, err := s.CreatePlacement(context.Background(), store.NewPlacement{
		OrderID:      order,
		HandleSHA256: []byte(order + "-handle"),
		PackageID:    "pkg",
	})
	if err != nil || !created {
		t.Fatalf("create placement: created=%v err=%v", created, err)
	}
	return p
}

func TestMigrateTwiceIsNoop(t *testing.T) {
	url := testenv.DatabaseURL(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := store.Migrate(ctx, url); err != nil {
			t.Fatalf("migrate %d: %v", i, err)
		}
	}
}

func TestMigrateConcurrent(t *testing.T) {
	url := testenv.DatabaseURL(t)
	ctx := context.Background()
	errs := make(chan error, 6)
	for i := 0; i < cap(errs); i++ {
		go func() { errs <- store.Migrate(ctx, url) }()
	}
	for i := 0; i < cap(errs); i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent migrate: %v", err)
		}
	}
}

func TestCreatePlacementSameOrderReturnsExisting(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	first := newPlacement(t, s, "order-1")
	if first.State != store.PlacementPlacing {
		t.Fatalf("state = %q", first.State)
	}
	again, created, err := s.CreatePlacement(ctx, store.NewPlacement{
		OrderID: "order-1", HandleSHA256: []byte("other"), PackageID: "pkg",
	})
	if err != nil || created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if again.ID != first.ID {
		t.Fatalf("got %s want %s", again.ID, first.ID)
	}
	byHandle, err := s.PlacementByHandle(ctx, first.HandleSHA256)
	if err != nil || byHandle.ID != first.ID {
		t.Fatalf("by handle: %v %v", byHandle.ID, err)
	}
	if _, err := s.PlacementByHandle(ctx, []byte("missing")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if _, err := s.PlacementByID(ctx, uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestPlacementLifecycle(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o")
	ref := provider.Ref{Provider: "local", ID: "sb-1", Meta: map[string]string{"k": "v"}}
	now := time.Now().UTC().Truncate(time.Microsecond)
	deadline := now.Add(time.Minute)

	if err := s.MarkPlacementReady(ctx, p.ID, ref, "small", now, deadline); err != nil {
		t.Fatal(err)
	}
	err := s.MarkPlacementReady(ctx, p.ID, ref, "small", now, deadline)
	if !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("second ready err = %v", err)
	}
	err = s.MarkPlacementFailed(ctx, p.ID, "x", true)
	if !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("failed after ready err = %v", err)
	}
	got, err := s.PlacementByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != store.PlacementReady || got.Provider != "local" || got.SizeName != "small" ||
		got.ProviderRef == nil || got.ProviderRef.ID != "sb-1" ||
		got.ProviderRef.Meta["k"] != "v" ||
		got.Deadline == nil || !got.Deadline.Equal(deadline) {
		t.Fatalf("placement = %+v", got)
	}
	moved, err := s.MarkPlacementTerminating(ctx, p.ID)
	if err != nil || !moved {
		t.Fatalf("terminating moved=%v err=%v", moved, err)
	}
	moved, err = s.MarkPlacementTerminating(ctx, p.ID)
	if err != nil || moved {
		t.Fatalf("second terminating moved=%v err=%v", moved, err)
	}
	if err := s.MarkPlacementTerminated(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = s.PlacementByID(ctx, p.ID)
	if got.State != store.PlacementTerminated || got.TerminatedAt == nil {
		t.Fatalf("placement = %+v", got)
	}

	q := newPlacement(t, s, "o2")
	if err := s.MarkPlacementFailed(ctx, q.ID, "boom", true); err != nil {
		t.Fatal(err)
	}
	got, _ = s.PlacementByID(ctx, q.ID)
	if got.State != store.PlacementFailed || got.FailureReason != "boom" || !got.Refundable {
		t.Fatalf("placement = %+v", got)
	}
	if err := s.RecordAttempt(ctx, store.Attempt{
		PlacementID: q.ID, Attempt: 1, Provider: "local", IdempotencyKey: "k", State: "failed",
		ErrorCode: "capacity", Error: "full",
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAttempt(ctx, store.Attempt{
		PlacementID: q.ID, Attempt: 1, Provider: "local", IdempotencyKey: "k", State: "ready",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReserveCapacityStopsAtMax(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if err := s.SetCapacity(ctx, "p", 2); err != nil {
		t.Fatal(err)
	}
	for i, want := range []bool{true, true, false} {
		ok, err := s.ReserveCapacity(ctx, "p")
		if err != nil || ok != want {
			t.Fatalf("reserve %d = %v, %v", i, ok, err)
		}
	}
	if err := s.ReleaseCapacity(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.ReserveCapacity(ctx, "p"); err != nil || !ok {
		t.Fatalf("reserve after release = %v, %v", ok, err)
	}

	if err := s.SetCapacity(ctx, "q", 5); err != nil {
		t.Fatal(err)
	}
	var granted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.ReserveCapacity(ctx, "q")
			if err != nil {
				t.Error(err)
			}
			if ok {
				granted.Add(1)
			}
		}()
	}
	wg.Wait()
	if granted.Load() != 5 {
		t.Fatalf("granted %d, want 5", granted.Load())
	}
}

func TestReserveCapacityDefaultsAndReleaseFloor(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	if ok, err := s.ReserveCapacity(ctx, "fresh"); err != nil || !ok {
		t.Fatalf("reserve = %v, %v", ok, err)
	}
	var maxConc, inUse int
	row := pool.QueryRow(ctx,
		"select max_concurrent, in_use from provider_capacity where provider = 'fresh'")
	if err := row.Scan(&maxConc, &inUse); err != nil {
		t.Fatal(err)
	}
	if maxConc != store.DefaultMaxConcurrent || inUse != 1 {
		t.Fatalf("max=%d in_use=%d", maxConc, inUse)
	}
	for i := 0; i < 3; i++ {
		if err := s.ReleaseCapacity(ctx, "fresh"); err != nil {
			t.Fatal(err)
		}
	}
	row = pool.QueryRow(ctx, "select in_use from provider_capacity where provider = 'fresh'")
	if err := row.Scan(&inUse); err != nil || inUse != 0 {
		t.Fatalf("in_use=%d err=%v", inUse, err)
	}
}

func TestExtendDeadlineAppliesOnce(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o")
	_, applied, err := s.ExtendDeadline(ctx, "ext-0", p.ID, time.Minute)
	if !errors.Is(err, store.ErrStateConflict) || applied {
		t.Fatalf("extend while placing applied=%v err=%v", applied, err)
	}

	now := time.Now().UTC()
	deadline := now.Add(time.Minute)
	ref := provider.Ref{Provider: "local", ID: "sb"}
	if err := s.MarkPlacementReady(ctx, p.ID, ref, "s", now, deadline); err != nil {
		t.Fatal(err)
	}
	got, applied, err := s.ExtendDeadline(ctx, "ext-1", p.ID, 10*time.Minute)
	if err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	want := deadline.Add(10 * time.Minute)
	if got.Deadline == nil || got.Deadline.Sub(want).Abs() > time.Millisecond {
		t.Fatalf("deadline = %v want %v", got.Deadline, want)
	}
	again, applied, err := s.ExtendDeadline(ctx, "ext-1", p.ID, 10*time.Minute)
	if err != nil || applied {
		t.Fatalf("repeat applied=%v err=%v", applied, err)
	}
	if !again.Deadline.Equal(*got.Deadline) {
		t.Fatalf("deadline changed: %v vs %v", again.Deadline, got.Deadline)
	}
}

func TestCreateJobConflict(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o")
	first := store.NewJob{ID: "j1", PlacementID: p.ID, IdempotencyKey: "k"}
	j, created, err := s.CreateJob(ctx, first)
	if err != nil || !created || j.State != store.JobRunning {
		t.Fatalf("job=%+v created=%v err=%v", j, created, err)
	}
	again, created, err := s.CreateJob(ctx, store.NewJob{ID: "j1", PlacementID: p.ID})
	if err != nil || created || again.IdempotencyKey != "k" {
		t.Fatalf("job=%+v created=%v err=%v", again, created, err)
	}
	running, err := s.RunningJobs(ctx)
	if err != nil || len(running) != 1 {
		t.Fatalf("running=%v err=%v", running, err)
	}
	if err := s.FinishJob(ctx, "j1", store.JobExited, 3, 10, 20); err != nil {
		t.Fatal(err)
	}
	done, err := s.JobByID(ctx, "j1")
	if err != nil || done.State != store.JobExited || done.ExitCode == nil || *done.ExitCode != 3 ||
		done.StdoutBytes != 10 || done.StderrBytes != 20 || done.FinishedAt == nil {
		t.Fatalf("job=%+v err=%v", done, err)
	}
	if _, err := s.JobByID(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if running, _ = s.RunningJobs(ctx); len(running) != 0 {
		t.Fatalf("running=%v", running)
	}
}

func TestOutboxPendingAndDelivered(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for _, kind := range []string{"a", "b", "c"} {
		err := s.WithTx(ctx, func(tx pgx.Tx) error {
			return s.InsertOutbox(ctx, tx, kind, map[string]string{"kind": kind})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	rollback := errors.New("rollback")
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		if err := s.InsertOutbox(ctx, tx, "gone", 1); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("err = %v", err)
	}

	pending, err := s.PendingOutbox(ctx, 10)
	if err != nil || len(pending) != 3 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	var payload map[string]string
	if err := json.Unmarshal(pending[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if pending[0].EventType != "a" || len(payload) != 1 || payload["kind"] != "a" {
		t.Fatalf("first = %+v payload=%v", pending[0], payload)
	}
	if limited, _ := s.PendingOutbox(ctx, 2); len(limited) != 2 {
		t.Fatalf("limited = %v", limited)
	}
	if err := s.MarkOutboxDelivered(ctx, pending[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkOutboxFailed(ctx, pending[1].ID, "nope"); err != nil {
		t.Fatal(err)
	}
	pending, _ = s.PendingOutbox(ctx, 10)
	if len(pending) != 2 || pending[0].EventType != "b" || pending[0].Attempts != 1 ||
		pending[0].LastError != "nope" {
		t.Fatalf("pending = %+v", pending)
	}
}

func TestTxBoundStoreRollsBackTogether(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o")
	rollback := errors.New("rollback")
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		if err := s.Tx(tx).MarkPlacementFailed(ctx, p.ID, "x", false); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("err = %v", err)
	}
	got, _ := s.PlacementByID(ctx, p.ID)
	if got.State != store.PlacementPlacing {
		t.Fatalf("state = %q", got.State)
	}
}

func TestMarkPlacementTerminatedGuards(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	ready := newPlacement(t, s, "term-ready")
	now := time.Now().UTC()
	ref := provider.Ref{Provider: "local", ID: "sb"}
	if err := s.MarkPlacementReady(ctx, ready.ID, ref, "s", now, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkPlacementTerminated(ctx, ready.ID); !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("terminate ready err = %v", err)
	}
	if moved, err := s.MarkPlacementTerminating(ctx, ready.ID); err != nil || !moved {
		t.Fatalf("terminating moved=%v err=%v", moved, err)
	}
	if err := s.MarkPlacementTerminated(ctx, ready.ID); err != nil {
		t.Fatal(err)
	}
	first, err := s.PlacementByID(ctx, ready.ID)
	if err != nil || first.State != store.PlacementTerminated || first.TerminatedAt == nil {
		t.Fatalf("placement = %+v err=%v", first, err)
	}
	if err := s.MarkPlacementTerminated(ctx, ready.ID); err != nil {
		t.Fatalf("repeat err = %v", err)
	}
	again, _ := s.PlacementByID(ctx, ready.ID)
	if !again.TerminatedAt.Equal(*first.TerminatedAt) {
		t.Fatalf("terminated_at changed: %v vs %v", again.TerminatedAt, first.TerminatedAt)
	}

	placing := newPlacement(t, s, "term-placing")
	if err := s.MarkPlacementTerminated(ctx, placing.ID); !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("terminate placing err = %v", err)
	}
	failed := newPlacement(t, s, "term-failed")
	if err := s.MarkPlacementFailed(ctx, failed.ID, "boom", true); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkPlacementTerminated(ctx, failed.ID); !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("terminate failed err = %v", err)
	}
	if got, _ := s.PlacementByID(ctx, failed.ID); got.State != store.PlacementFailed {
		t.Fatalf("state = %q", got.State)
	}
	if err := s.MarkPlacementTerminated(ctx, uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing err = %v", err)
	}
}

func TestExtendDeadlineRejectsForeignOrderID(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	a := newPlacement(t, s, "start-a")
	b := newPlacement(t, s, "start-b")
	now := time.Now().UTC()
	ref := provider.Ref{Provider: "local", ID: "sb"}
	for _, p := range []store.Placement{a, b} {
		if err := s.MarkPlacementReady(ctx, p.ID, ref, "s", now, now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	_, applied, err := s.ExtendDeadline(ctx, "start-a", a.ID, time.Minute)
	if !errors.Is(err, store.ErrStateConflict) || applied {
		t.Fatalf("start order as extend applied=%v err=%v", applied, err)
	}
	if _, _, err := s.ExtendDeadline(ctx, "ext-a", a.ID, time.Minute); err != nil {
		t.Fatal(err)
	}
	_, applied, err = s.ExtendDeadline(ctx, "ext-a", b.ID, time.Minute)
	if !errors.Is(err, store.ErrStateConflict) || applied {
		t.Fatalf("extend order on other placement applied=%v err=%v", applied, err)
	}
	got, _ := s.PlacementByID(ctx, b.ID)
	if got.Deadline.Sub(now.Add(time.Minute)).Abs() > time.Millisecond {
		t.Fatalf("deadline moved: %v", got.Deadline)
	}
}

func newWidePoolStore(t *testing.T) *store.Store {
	t.Helper()
	url := testenv.DatabaseURL(t)
	ctx := context.Background()
	if err := store.Migrate(ctx, url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 25
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return store.New(pool)
}

func TestCreatePlacementConcurrentSameOrder(t *testing.T) {
	s := newWidePoolStore(t)
	ctx := context.Background()
	const n = 20
	var wg sync.WaitGroup
	var createdCount atomic.Int32
	ids := make([]uuid.UUID, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, created, err := s.CreatePlacement(ctx, store.NewPlacement{
				OrderID: "same", HandleSHA256: []byte("h"), PackageID: "pkg",
			})
			ids[i], errs[i] = p.ID, err
			if created {
				createdCount.Add(1)
			}
		}()
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("goroutine %d id %s want %s", i, ids[i], ids[0])
		}
	}
	if got := createdCount.Load(); got != 1 {
		t.Fatalf("created %d times", got)
	}
}

func TestExtendDeadlineConcurrentSameOrder(t *testing.T) {
	s := newWidePoolStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o")
	now := time.Now().UTC()
	deadline := now.Add(time.Minute)
	ref := provider.Ref{Provider: "local", ID: "sb"}
	if err := s.MarkPlacementReady(ctx, p.ID, ref, "s", now, deadline); err != nil {
		t.Fatal(err)
	}
	const n = 20
	var wg sync.WaitGroup
	var appliedCount atomic.Int32
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, applied, err := s.ExtendDeadline(ctx, "ext-1", p.ID, 10*time.Minute)
			errs[i] = err
			if applied {
				appliedCount.Add(1)
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if got := appliedCount.Load(); got != 1 {
		t.Fatalf("applied %d times", got)
	}
	got, err := s.PlacementByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := deadline.Add(10 * time.Minute)
	if got.Deadline.Sub(want).Abs() > time.Millisecond {
		t.Fatalf("deadline = %v want %v", got.Deadline, want)
	}
}
