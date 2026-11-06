package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/testenv"
	"github.com/oynozan/bugnet-router/migrations"
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
	_, err = s.PlacementByHandle(ctx, []byte("missing"))
	if !errors.Is(err, store.ErrUnknownHandle) || !errors.Is(err, store.ErrNotFound) ||
		err.Error() != store.ErrNotFound.Error() {
		t.Fatalf("by a missing handle err = %v, want ErrUnknownHandle with ErrNotFound's text", err)
	}
	_, err = s.PlacementByID(ctx, uuid.New())
	if !errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUnknownHandle) {
		t.Fatalf("by a missing id err = %v, want ErrNotFound alone", err)
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
	moved, err := s.MarkPlacementTerminating(ctx, p.ID, "stop", time.Now())
	if err != nil || !moved {
		t.Fatalf("terminating moved=%v err=%v", moved, err)
	}
	moved, err = s.MarkPlacementTerminating(ctx, p.ID, "stop", time.Now())
	if err != nil || moved {
		t.Fatalf("second terminating moved=%v err=%v", moved, err)
	}
	endedAt := now.Add(42 * time.Second)
	if err := s.MarkPlacementTerminated(ctx, p.ID, endedAt); err != nil {
		t.Fatal(err)
	}
	got, _ = s.PlacementByID(ctx, p.ID)
	if got.State != store.PlacementTerminated || got.TerminatedAt == nil ||
		!got.TerminatedAt.Equal(endedAt) {
		t.Fatalf("placement = %+v, want terminated at %v", got, endedAt)
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
	exited := store.JobOutcome{
		State: store.JobExited, Code: 3, StdoutBytes: 10, StderrBytes: 20, Truncated: true,
	}
	if err := s.FinishJob(ctx, "j1", exited); err != nil {
		t.Fatal(err)
	}
	done, err := s.JobByID(ctx, "j1")
	if err != nil || done.State != store.JobExited || done.ExitCode == nil || *done.ExitCode != 3 ||
		done.StdoutBytes != 10 || done.StderrBytes != 20 || done.FinishedAt == nil ||
		!done.OutputTruncated {
		t.Fatalf("job=%+v err=%v", done, err)
	}
	if _, err := s.JobByID(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if running, _ = s.RunningJobs(ctx); len(running) != 0 {
		t.Fatalf("running=%v", running)
	}
}

func TestFinishLostJobHasNoExitCode(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o")
	if _, _, err := s.CreateJob(ctx, store.NewJob{ID: "j1", PlacementID: p.ID}); err != nil {
		t.Fatal(err)
	}
	lost := store.JobOutcome{State: store.JobLost, StdoutBytes: 7}
	if err := s.FinishJob(ctx, "j1", lost); err != nil {
		t.Fatal(err)
	}
	j, err := s.JobByID(ctx, "j1")
	if err != nil || j.State != store.JobLost || j.ExitCode != nil || j.StdoutBytes != 7 ||
		j.OutputTruncated {
		t.Fatalf("job=%+v err=%v", j, err)
	}
	err = s.FinishJob(ctx, "j1", store.JobOutcome{State: store.JobExited, StdoutBytes: 7})
	if !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("finish of a lost job: %v, want ErrStateConflict", err)
	}
}

func TestCreateJobKeepsToTheRunningLimit(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p, other := newPlacement(t, s, "o"), newPlacement(t, s, "other")
	const limit = 3
	var wg sync.WaitGroup
	var created, refused atomic.Int64
	for i := range 12 {
		wg.Go(func() {
			j := store.NewJob{ID: fmt.Sprintf("j%d", i), PlacementID: p.ID, Limit: limit}
			_, ok, err := s.CreateJob(ctx, j)
			switch {
			case errors.Is(err, store.ErrJobLimit):
				refused.Add(1)
			case err != nil:
				t.Error(err)
			case ok:
				created.Add(1)
			}
		})
	}
	wg.Wait()
	if created.Load() != limit || refused.Load() != 12-limit {
		t.Fatalf("created %d and refused %d of 12 jobs, want %d created", created.Load(),
			refused.Load(), limit)
	}
	running, err := s.RunningJobs(ctx)
	if err != nil || len(running) != limit {
		t.Fatalf("running jobs %+v, %v, want %d", running, err, limit)
	}
	first := running[0].ID
	again, ok, err := s.CreateJob(ctx, store.NewJob{ID: first, PlacementID: p.ID, Limit: limit})
	if err != nil || ok || again.ID != first {
		t.Fatalf("a repeated id at the limit gave %+v created %v, %v", again, ok, err)
	}
	_, ok, err = s.CreateJob(ctx, store.NewJob{ID: "o1", PlacementID: other.ID, Limit: limit})
	if err != nil || !ok {
		t.Fatalf("a job of another placement gave created %v, %v", ok, err)
	}
	if err := s.FinishJob(ctx, first, store.JobOutcome{State: store.JobExited}); err != nil {
		t.Fatal(err)
	}
	_, ok, err = s.CreateJob(ctx, store.NewJob{ID: "late", PlacementID: p.ID, Limit: limit})
	if err != nil || !ok {
		t.Fatalf("a job after one finished gave created %v, %v", ok, err)
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

	now := time.Now()
	pending, err := s.PendingOutbox(ctx, now, 10)
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
	if limited, _ := s.PendingOutbox(ctx, now, 2); len(limited) != 2 {
		t.Fatalf("limited = %v", limited)
	}
	if err := s.MarkOutboxDelivered(ctx, pending[0].ID); err != nil {
		t.Fatal(err)
	}
	retryAt := now.Add(time.Minute)
	if err := s.MarkOutboxFailed(ctx, pending[1].ID, "nope", retryAt); err != nil {
		t.Fatal(err)
	}
	pending, _ = s.PendingOutbox(ctx, now, 10)
	if len(pending) != 1 || pending[0].EventType != "c" {
		t.Fatalf("pending before the retry is due = %+v, want only c", pending)
	}
	pending, _ = s.PendingOutbox(ctx, retryAt, 10)
	if len(pending) != 2 || pending[0].EventType != "b" || pending[0].Attempts != 1 ||
		pending[0].LastError != "nope" {
		t.Fatalf("pending once due = %+v", pending)
	}
}

func TestOutboxRetriesPastMaxAttempts(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	err := s.WithTx(ctx, func(tx pgx.Tx) error { return s.InsertOutbox(ctx, tx, "a", 1) })
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for n := 1; n <= store.MaxOutboxAttempts+5; n++ {
		pending, err := s.PendingOutbox(ctx, now, 10)
		if err != nil || len(pending) != 1 || pending[0].Attempts != n-1 {
			t.Fatalf("before failure %d pending=%+v err=%v, want the event due again", n,
				pending, err)
		}
		if err := s.MarkOutboxFailed(ctx, pending[0].ID, "nope", now); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := s.StuckOutboxCount(ctx); err != nil || n != 1 {
		t.Fatalf("stuck count %d, %v, want the event that failed past the alert threshold", n,
			err)
	}
}

func TestRedriveOutboxResetsStuckEvents(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for _, kind := range []string{"stuck", "fresh"} {
		err := s.WithTx(ctx, func(tx pgx.Tx) error { return s.InsertOutbox(ctx, tx, kind, 1) })
		if err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	pending, err := s.PendingOutbox(ctx, now, 10)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending %+v, %v", pending, err)
	}
	for range store.MaxOutboxAttempts {
		err := s.MarkOutboxFailed(ctx, pending[0].ID, "nope", now.Add(5*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := s.MarkOutboxFailed(ctx, pending[1].ID, "nope", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if due, err := s.PendingOutbox(ctx, now, 10); err != nil || len(due) != 0 {
		t.Fatalf("due before redrive %+v, %v, want both events backing off", due, err)
	}
	if n, err := s.StuckOutboxCount(ctx); err != nil || n != 1 {
		t.Fatalf("stuck count %d, %v, want 1", n, err)
	}
	if n, err := s.RedriveOutbox(ctx); err != nil || n != 1 {
		t.Fatalf("redrive reset %d, %v, want 1", n, err)
	}
	if n, err := s.StuckOutboxCount(ctx); err != nil || n != 0 {
		t.Fatalf("stuck count after redrive %d, %v, want 0", n, err)
	}
	due, err := s.PendingOutbox(ctx, now, 10)
	if err != nil || len(due) != 1 || due[0].ID != pending[0].ID || due[0].Attempts != 0 {
		t.Fatalf("due after redrive %+v, %v, want the reset event alone, due at once", due, err)
	}
}

func TestPendingOutboxSkipsLockedRows(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	for _, kind := range []string{"a", "b"} {
		err := s.WithTx(ctx, func(tx pgx.Tx) error { return s.InsertOutbox(ctx, tx, kind, 1) })
		if err != nil {
			t.Fatal(err)
		}
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	held, err := s.Tx(tx).PendingOutbox(ctx, time.Now(), 1)
	if err != nil || len(held) != 1 || held[0].EventType != "a" {
		t.Fatalf("held=%+v err=%v", held, err)
	}
	other, err := s.PendingOutbox(ctx, time.Now(), 10)
	if err != nil || len(other) != 1 || other[0].EventType != "b" {
		t.Fatalf("pending while a is held = %+v err=%v, want only b", other, err)
	}
}

func TestReadyOrTerminatingPastDeadline(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	ref := provider.Ref{Provider: "local", ID: "sb"}
	overdue := newPlacement(t, s, "overdue")
	due := newPlacement(t, s, "due")
	newPlacement(t, s, "placing")
	ending := newPlacement(t, s, "ending")
	ended := newPlacement(t, s, "ended")
	for p, deadline := range map[uuid.UUID]time.Time{
		overdue.ID: now.Add(-time.Second), due.ID: now.Add(time.Second),
		ending.ID: now.Add(-time.Hour), ended.ID: now.Add(-time.Hour),
	} {
		err := s.MarkPlacementReady(ctx, p, ref, "s", now.Add(-2*time.Hour), deadline)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []uuid.UUID{ending.ID, ended.ID} {
		if _, err := s.MarkPlacementTerminating(ctx, id, "stop", now); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.MarkPlacementTerminated(ctx, ended.ID, now); err != nil {
		t.Fatal(err)
	}
	ids, err := s.ReadyOrTerminatingPastDeadline(ctx, now)
	if want := []uuid.UUID{ending.ID, overdue.ID}; err != nil || !slices.Equal(ids, want) {
		t.Fatalf("ReadyOrTerminatingPastDeadline = %v, %v, want %v oldest deadline first", ids,
			err, want)
	}
	for cutoff, want := range map[time.Time]int{now: 2, now.Add(-time.Minute): 1} {
		if n, err := s.PastDeadlineCount(ctx, cutoff); err != nil || n != want {
			t.Errorf("PastDeadlineCount %v ago = %d, %v, want %d", now.Sub(cutoff), n, err, want)
		}
	}
}

func TestMarkPlacementTerminatingRecordsReason(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	p := newPlacement(t, s, "reason")
	ref := provider.Ref{Provider: "local", ID: "sb"}
	if err := s.MarkPlacementReady(ctx, p.ID, ref, "s", now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if moved, err := s.MarkPlacementTerminating(ctx, p.ID, "stop", now); err != nil || !moved {
		t.Fatalf("terminating moved=%v err=%v", moved, err)
	}
	later := now.Add(time.Minute)
	if moved, err := s.MarkPlacementTerminating(ctx, p.ID, "deadline", later); err != nil ||
		moved {
		t.Fatalf("second terminating moved=%v err=%v", moved, err)
	}
	got, err := s.PlacementByID(ctx, p.ID)
	if err != nil || got.TerminationReason != "stop" || got.TerminatingAt == nil ||
		!got.TerminatingAt.Equal(now) {
		t.Fatalf("placement = %+v err=%v, want reason stop terminating at %v", got, err, now)
	}
}

func TestMarkPlacementDueHonoursDeadline(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	deadline := now.Add(time.Minute)
	p := newPlacement(t, s, "due")
	ref := provider.Ref{Provider: "local", ID: "sb"}
	if err := s.MarkPlacementReady(ctx, p.ID, ref, "s", now, deadline); err != nil {
		t.Fatal(err)
	}
	early := deadline.Add(-time.Second)
	if moved, err := s.MarkPlacementDue(ctx, p.ID, "deadline", early, now); err != nil || moved {
		t.Fatalf("before the deadline moved=%v err=%v", moved, err)
	}
	if got, _ := s.PlacementByID(ctx, p.ID); got.State != store.PlacementReady {
		t.Fatalf("state %s before the deadline, want ready", got.State)
	}
	if moved, err := s.MarkPlacementDue(ctx, p.ID, "deadline", deadline, now); err != nil ||
		!moved {
		t.Fatalf("at the deadline moved=%v err=%v", moved, err)
	}
	got, err := s.PlacementByID(ctx, p.ID)
	if err != nil || got.State != store.PlacementTerminating ||
		got.TerminationReason != "deadline" {
		t.Fatalf("placement = %+v err=%v, want terminating for deadline", got, err)
	}
	if moved, err := s.MarkPlacementDue(ctx, p.ID, "deadline", deadline, now); err != nil ||
		moved {
		t.Fatalf("terminating moved=%v err=%v, want left alone", moved, err)
	}
}

func TestTerminatingSince(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	now := time.Now().UTC()
	ref := provider.Ref{Provider: "local", ID: "sb"}
	old := newPlacement(t, s, "old")
	unknown := newPlacement(t, s, "unknown")
	recent := newPlacement(t, s, "recent")
	ready := newPlacement(t, s, "ready")
	for _, p := range []store.Placement{old, unknown, recent, ready} {
		if err := s.MarkPlacementReady(ctx, p.ID, ref, "s", now, now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	for p, at := range map[uuid.UUID]time.Time{
		old.ID: now.Add(-5 * time.Minute), unknown.ID: now, recent.ID: now.Add(-time.Second),
	} {
		if _, err := s.MarkPlacementTerminating(ctx, p, "stop", at); err != nil {
			t.Fatal(err)
		}
	}
	// A placement left terminating before the column existed has no time
	if _, err := pool.Exec(ctx, `update placements set terminating_at = null where id = $1`,
		unknown.ID); err != nil {
		t.Fatal(err)
	}
	ids, err := s.TerminatingSince(ctx, now.Add(-2*time.Minute))
	if want := []uuid.UUID{unknown.ID, old.ID}; err != nil || !slices.Equal(ids, want) {
		t.Fatalf("TerminatingSince = %v, %v, want %v", ids, err, want)
	}
}

func TestLongTerminatingCount(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	now := time.Now().UTC()
	ref := provider.Ref{Provider: "local", ID: "sb"}
	for _, c := range []struct {
		name               string
		since, deadline    time.Time
		terminating, unset bool
	}{
		{name: "stopped long ago", since: now.Add(-5 * time.Minute),
			deadline: now.Add(time.Hour), terminating: true},
		{name: "stopped before the column", deadline: now.Add(time.Hour), terminating: true,
			unset: true},
		{name: "stopped just now", since: now.Add(-time.Second), deadline: now.Add(time.Hour),
			terminating: true},
		{name: "ending past its deadline", since: now.Add(-5 * time.Minute),
			deadline: now.Add(-10 * time.Minute), terminating: true},
		{name: "ready", deadline: now.Add(time.Hour)},
	} {
		p := newPlacement(t, s, c.name)
		if err := s.MarkPlacementReady(ctx, p.ID, ref, "s", now, c.deadline); err != nil {
			t.Fatal(err)
		}
		if !c.terminating {
			continue
		}
		if _, err := s.MarkPlacementTerminating(ctx, p.ID, "stop", c.since); err != nil {
			t.Fatal(err)
		}
		if c.unset {
			_, err := pool.Exec(ctx, `update placements set terminating_at = null where id = $1`,
				p.ID)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	// The one past its deadline is PastDeadlineCount's
	if n, err := s.LongTerminatingCount(ctx, now.Add(-2*time.Minute)); err != nil || n != 2 {
		t.Fatalf("LongTerminatingCount = %d, %v, want the 2 stopped long ago", n, err)
	}
}

func TestReleaseEndedCapacity(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	held := map[string]store.Placement{}
	for _, name := range []string{"failed", "terminated", "placing", "ready"} {
		p := newPlacement(t, s, name)
		if ok, err := s.ReservePlacementCapacity(ctx, p.ID, "local"); err != nil || !ok {
			t.Fatalf("reserve %s: %v %v", name, ok, err)
		}
		held[name] = p
	}
	// A release that failed after the state change left these two holding their slots
	_, err := pool.Exec(ctx, `update placements set state = $2 where id = $1`,
		held["failed"].ID, store.PlacementFailed)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `update placements set state = $2 where id = $1`,
		held["terminated"].ID, store.PlacementTerminated)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReleaseEndedCapacity(ctx); err != nil || n != 2 {
		t.Fatalf("ReleaseEndedCapacity = %d, %v, want 2", n, err)
	}
	var inUse int
	err = pool.QueryRow(ctx,
		`select in_use from provider_capacity where provider = 'local'`).Scan(&inUse)
	if err != nil || inUse != 2 {
		t.Fatalf("in use %d, %v, want the placing and ready slots left", inUse, err)
	}
	if n, err := s.ReleaseEndedCapacity(ctx); err != nil || n != 0 {
		t.Fatalf("second ReleaseEndedCapacity = %d, %v, want 0", n, err)
	}
}

func TestStalledPlacing(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	now := time.Now().UTC()
	stale := newPlacement(t, s, "stale")
	fresh := newPlacement(t, s, "fresh")
	silent := newPlacement(t, s, "silent")
	young := newPlacement(t, s, "young")
	failed := newPlacement(t, s, "failed")
	beats := map[uuid.UUID]time.Time{
		stale.ID: now.Add(-61 * time.Second), fresh.ID: now.Add(-59 * time.Second),
		failed.ID: now.Add(-time.Hour),
	}
	for id, at := range beats {
		if err := s.HeartbeatPlacement(ctx, id, "r", at); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.MarkPlacementFailed(ctx, failed.ID, "x", true); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `update placements set created_at = $2 where id = any($1)`,
		[]uuid.UUID{silent.ID, failed.ID}, now.Add(-3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	ids, err := s.StalledPlacing(ctx, now.Add(-time.Minute), now.Add(-2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if want := []uuid.UUID{silent.ID, stale.ID}; !slices.Equal(ids, want) {
		t.Fatalf("StalledPlacing = %v, want %v oldest first, not fresh %s or young %s", ids, want,
			fresh.ID, young.ID)
	}
}

func TestAttempts(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o")
	other := newPlacement(t, s, "other")
	want := []store.Attempt{
		{PlacementID: p.ID, Attempt: 1, Provider: "a", IdempotencyKey: "k1", State: "failed",
			ErrorCode: "capacity", Error: "full"},
		{PlacementID: p.ID, Attempt: 2, Provider: "b", IdempotencyKey: "k2", State: "creating"},
	}
	for _, a := range []store.Attempt{want[1], want[0], {PlacementID: other.ID, Attempt: 1}} {
		if err := s.RecordAttempt(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Attempts(ctx, p.ID)
	if err != nil || !slices.Equal(got, want) {
		t.Fatalf("Attempts = %+v, %v, want %+v", got, err, want)
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
	if err := s.MarkPlacementTerminated(ctx, ready.ID, now); !errors.Is(
		err, store.ErrStateConflict) {
		t.Fatalf("terminate ready err = %v", err)
	}
	moved, err := s.MarkPlacementTerminating(ctx, ready.ID, "stop", now)
	if err != nil || !moved {
		t.Fatalf("terminating moved=%v err=%v", moved, err)
	}
	if err := s.MarkPlacementTerminated(ctx, ready.ID, now); err != nil {
		t.Fatal(err)
	}
	first, err := s.PlacementByID(ctx, ready.ID)
	if err != nil || first.State != store.PlacementTerminated || first.TerminatedAt == nil {
		t.Fatalf("placement = %+v err=%v", first, err)
	}
	if err := s.MarkPlacementTerminated(ctx, ready.ID, now.Add(time.Hour)); err != nil {
		t.Fatalf("repeat err = %v", err)
	}
	again, _ := s.PlacementByID(ctx, ready.ID)
	if !again.TerminatedAt.Equal(*first.TerminatedAt) {
		t.Fatalf("terminated_at changed: %v vs %v", again.TerminatedAt, first.TerminatedAt)
	}

	placing := newPlacement(t, s, "term-placing")
	err = s.MarkPlacementTerminated(ctx, placing.ID, now)
	if !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("terminate placing err = %v", err)
	}
	failed := newPlacement(t, s, "term-failed")
	if err := s.MarkPlacementFailed(ctx, failed.ID, "boom", true); err != nil {
		t.Fatal(err)
	}
	err = s.MarkPlacementTerminated(ctx, failed.ID, now)
	if !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("terminate failed err = %v", err)
	}
	if got, _ := s.PlacementByID(ctx, failed.ID); got.State != store.PlacementFailed {
		t.Fatalf("state = %q", got.State)
	}
	if err := s.MarkPlacementTerminated(ctx, uuid.New(), now); !errors.Is(err, store.ErrNotFound) {
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

func TestPlacementForUpdateLocksRow(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o")
	if _, err := s.PlacementForUpdate(ctx, uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing placement err = %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	locked, err := s.Tx(tx).PlacementForUpdate(ctx, p.ID)
	if err != nil || locked.ID != p.ID {
		t.Fatalf("lock %v: %v", locked.ID, err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.MarkPlacementTerminating(ctx, p.ID, "stop", time.Now())
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("update ran while the row was locked: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCreatePlacementInitiallyFailed(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	np := store.NewPlacement{
		OrderID: "o", HandleSHA256: []byte("o-handle"), PackageID: "pkg",
		FailureReason: "no_candidate",
	}
	p, created, err := s.CreatePlacement(ctx, np)
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if p.State != store.PlacementFailed || p.FailureReason != "no_candidate" || !p.Refundable {
		t.Fatalf("placement = %+v", p)
	}
	if p.TerminatedAt == nil || !p.TerminatedAt.Equal(p.CreatedAt) {
		t.Fatalf("terminated at %v, want its created_at %v", p.TerminatedAt, p.CreatedAt)
	}
	again, created, err := s.CreatePlacement(ctx, store.NewPlacement{
		OrderID: "o", HandleSHA256: []byte("o-handle"), PackageID: "pkg",
	})
	if err != nil || created || again.ID != p.ID || again.State != store.PlacementFailed ||
		again.TerminatedAt == nil || !again.TerminatedAt.Equal(*p.TerminatedAt) {
		t.Fatalf("repeat created=%v err=%v placement=%+v", created, err, again)
	}
	placing := newPlacement(t, s, "o2")
	if placing.FailureReason != "" || placing.Refundable || placing.TerminatedAt != nil {
		t.Fatalf("placing placement = %+v", placing)
	}
}

func TestMarkPlacementFailedRecordsWhenItEnded(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o")
	clock := func() time.Time {
		t.Helper()
		var at time.Time
		if err := pool.QueryRow(ctx, `select clock_timestamp()`).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	before := clock()
	if err := s.MarkPlacementFailed(ctx, p.ID, "boot_failed", true); err != nil {
		t.Fatal(err)
	}
	after := clock()
	got, err := s.PlacementByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TerminatedAt == nil || got.TerminatedAt.Before(before) ||
		got.TerminatedAt.After(after) || got.TerminatedAt.Location() != time.UTC {
		t.Fatalf("terminated at %v, want a UTC time from %v to %v", got.TerminatedAt, before,
			after)
	}
}

func TestHeartbeatPlacement(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o")
	if p.SagaOwner != "" || p.SagaHeartbeat != nil {
		t.Fatalf("new placement = %+v", p)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	for _, beat := range []time.Time{at, at.Add(10 * time.Second)} {
		if err := s.HeartbeatPlacement(ctx, p.ID, "replica-1", beat); err != nil {
			t.Fatal(err)
		}
		got, _ := s.PlacementByID(ctx, p.ID)
		if got.SagaOwner != "replica-1" || got.SagaHeartbeat == nil ||
			!got.SagaHeartbeat.Equal(beat) {
			t.Fatalf("placement = %+v, want heartbeat %v", got, beat)
		}
	}

	ref := provider.Ref{Provider: "local", ID: "sb"}
	if err := s.MarkPlacementReady(ctx, p.ID, ref, "s", at, at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	err := s.HeartbeatPlacement(ctx, p.ID, "replica-2", at.Add(time.Minute))
	if !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("heartbeat on ready err = %v", err)
	}
	if got, _ := s.PlacementByID(ctx, p.ID); got.SagaOwner != "replica-1" ||
		!got.SagaHeartbeat.Equal(at.Add(10*time.Second)) {
		t.Fatalf("ready placement = %+v", got)
	}
}

func TestHasExtendOrder(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	a := newPlacement(t, s, "start-a")
	b := newPlacement(t, s, "start-b")
	now := time.Now().UTC()
	ref := provider.Ref{Provider: "local", ID: "sb"}
	if err := s.MarkPlacementReady(ctx, a.ID, ref, "s", now, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ExtendDeadline(ctx, "ext-a", a.ID, time.Minute); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		order string
		id    uuid.UUID
		want  bool
	}{
		{"ext-a", a.ID, true},
		{"ext-a", b.ID, false},
		{"start-a", a.ID, false},
		{"never", a.ID, false},
	}
	for _, c := range cases {
		got, err := s.HasExtendOrder(ctx, c.order, c.id)
		if err != nil || got != c.want {
			t.Errorf("HasExtendOrder(%s, %s) = %v, %v, want %v", c.order, c.id, got, err, c.want)
		}
	}
}

func TestPing(t *testing.T) {
	s, pool := newStoreWithPool(t)
	if err := s.Ping(t.Context()); err != nil {
		t.Fatalf("ping an open pool: %v", err)
	}
	pool.Close()
	if err := s.Ping(t.Context()); err == nil {
		t.Fatal("ping a closed pool succeeded")
	}
}

func inUse(t *testing.T, pool *pgxpool.Pool, name string) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `select coalesce(
		(select in_use from provider_capacity where provider = $1), 0)`, name).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func held(t *testing.T, s *store.Store, id uuid.UUID) string {
	t.Helper()
	p, err := s.PlacementByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return p.CapacityProvider
}

func TestPlacementCapacityHeldOnce(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	if err := s.SetCapacity(ctx, "a", 1); err != nil {
		t.Fatal(err)
	}
	p := newPlacement(t, s, "p")
	q := newPlacement(t, s, "q")
	if ok, err := s.ReservePlacementCapacity(ctx, p.ID, "a"); err != nil || !ok {
		t.Fatalf("reserve for p = %v, %v", ok, err)
	}
	if got := held(t, s, p.ID); got != "a" {
		t.Fatalf("p holds capacity on %q, want a", got)
	}
	_, err := s.ReservePlacementCapacity(ctx, p.ID, "b")
	if !errors.Is(err, store.ErrStateConflict) || inUse(t, pool, "b") != 0 {
		t.Fatalf("second reserve for p: %v with %d in use on b, want a state conflict and 0",
			err, inUse(t, pool, "b"))
	}
	if ok, err := s.ReservePlacementCapacity(ctx, q.ID, "a"); err != nil || ok {
		t.Fatalf("reserve for q on a full provider = %v, %v, want false", ok, err)
	}
	if got := held(t, s, q.ID); got != "" || inUse(t, pool, "a") != 1 {
		t.Fatalf("q holds %q with %d in use on a, want nothing and 1", got, inUse(t, pool, "a"))
	}

	err = s.WithTx(ctx, func(tx pgx.Tx) error {
		if ok, err := s.Tx(tx).ReleasePlacementCapacity(ctx, p.ID); err != nil || !ok {
			t.Errorf("release in a transaction = %v, %v", ok, err)
		}
		return errors.New("roll back")
	})
	if err == nil || held(t, s, p.ID) != "a" || inUse(t, pool, "a") != 1 {
		t.Fatalf("a rolled back release left p holding %q with %d in use", held(t, s, p.ID),
			inUse(t, pool, "a"))
	}
	for i, want := range []bool{true, false} {
		released, err := s.ReleasePlacementCapacity(ctx, p.ID)
		if err != nil || released != want {
			t.Fatalf("release %d = %v, %v, want %v", i, released, err, want)
		}
	}
	if got := held(t, s, p.ID); got != "" || inUse(t, pool, "a") != 0 {
		t.Fatalf("after release p holds %q with %d in use, want nothing and 0", got,
			inUse(t, pool, "a"))
	}
	if ok, err := s.ReservePlacementCapacity(ctx, q.ID, "a"); err != nil || !ok {
		t.Fatalf("reserve for q after the release = %v, %v", ok, err)
	}

	failed := newPlacement(t, s, "failed")
	if err := s.MarkPlacementFailed(ctx, failed.ID, "x", true); err != nil {
		t.Fatal(err)
	}
	_, err = s.ReservePlacementCapacity(ctx, failed.ID, "b")
	if !errors.Is(err, store.ErrStateConflict) || inUse(t, pool, "b") != 0 {
		t.Fatalf("reserve for a failed placement: %v with %d in use, want a state conflict and 0",
			err, inUse(t, pool, "b"))
	}
	if _, err := s.ReservePlacementCapacity(ctx, uuid.New(), "b"); !errors.Is(err,
		store.ErrStateConflict) {
		t.Fatalf("reserve for an unknown placement: %v, want a state conflict", err)
	}
}

func TestReleasePlacementCapacityRacingReleaseFreesOneSlot(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	x := newPlacement(t, s, "x")
	y := newPlacement(t, s, "y")
	for _, p := range []store.Placement{x, y} {
		if ok, err := s.ReservePlacementCapacity(ctx, p.ID, "b"); err != nil || !ok {
			t.Fatalf("reserve = %v, %v", ok, err)
		}
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if ok, err := s.Tx(tx).ReleasePlacementCapacity(ctx, x.ID); err != nil || !ok {
		t.Fatalf("first release = %v, %v", ok, err)
	}
	// The second release starts while the first is uncommitted and waits on the row lock
	second := make(chan bool, 1)
	go func() {
		ok, err := s.ReleasePlacementCapacity(ctx, x.ID)
		if err != nil {
			t.Error(err)
		}
		second <- ok
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		err := pool.QueryRow(ctx, `select count(*) from pg_stat_activity
			where datname = current_database() and wait_event_type = 'Lock'`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the second release never waited on the first")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if <-second || inUse(t, pool, "b") != 1 {
		t.Fatalf("the racing release freed a slot too, %d in use, want y's slot kept",
			inUse(t, pool, "b"))
	}
}

func TestMigrateKeepsHeldCapacity(t *testing.T) {
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
	if _, err := gp.UpTo(ctx, 2); err != nil {
		t.Fatalf("migrate to 2: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		insert into placements (id, handle_sha256, order_id, package_id, state, provider)
		values ('00000000-0000-0000-0000-000000000001', 'h1', 'o1', 'p', 'ready', 'a'),
			('00000000-0000-0000-0000-000000000002', 'h2', 'o2', 'p', 'terminating', 'b'),
			('00000000-0000-0000-0000-000000000003', 'h3', 'o3', 'p', 'terminated', 'a'),
			('00000000-0000-0000-0000-000000000004', 'h4', 'o4', 'p', 'placing', null),
			('00000000-0000-0000-0000-000000000005', 'h5', 'o5', 'p', 'placing', null);
		insert into placement_attempts (placement_id, attempt, provider, state)
		values ('00000000-0000-0000-0000-000000000004', 1, 'a', 'failed'),
			('00000000-0000-0000-0000-000000000004', 2, 'c', 'creating'),
			('00000000-0000-0000-0000-000000000005', 1, 'a', 'failed')`)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx, url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := store.New(pool)
	want := []string{"a", "b", "", "c", ""}
	for i, w := range want {
		id := uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1))
		if got := held(t, s, id); got != w {
			t.Errorf("placement %d holds capacity on %q, want %q", i+1, got, w)
		}
	}
}

// dataPlaneTables are the tables migration 00006 creates
var dataPlaneTables = []string{
	"previews", "ssh_keys", "ssh_hosts", "ssh_gateway_keys", "secret_links", "secrets",
}

// dataPlaneColumns are the placements columns migration 00006 adds
var dataPlaneColumns = []string{
	"key_id", "ssh_alias", "source_label", "edge_budget_bytes", "egress_budget_bytes",
	"egress_token_sha256", "egress_ca_version", "secrets_version", "gate_ready_at",
}

// schemaCounts counts the data-plane tables and placements columns the database has
func schemaCounts(t *testing.T, db *sql.DB) (tables, columns int) {
	t.Helper()
	ctx := context.Background()
	err := db.QueryRowContext(ctx, `
		select count(*) from information_schema.tables
		where table_schema = current_schema() and table_name = any($1)`,
		dataPlaneTables).Scan(&tables)
	if err != nil {
		t.Fatal(err)
	}
	err = db.QueryRowContext(ctx, `
		select count(*) from information_schema.columns
		where table_schema = current_schema() and table_name = 'placements'
			and column_name = any($1)`, dataPlaneColumns).Scan(&columns)
	if err != nil {
		t.Fatal(err)
	}
	return tables, columns
}

func TestMigration00006UpDownUp(t *testing.T) {
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
	all, cols := len(dataPlaneTables), len(dataPlaneColumns)
	if _, err := gp.UpTo(ctx, 5); err != nil {
		t.Fatalf("up to 5: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		insert into placements (id, handle_sha256, order_id, package_id, state)
		values ('00000000-0000-0000-0000-000000000001', 'h1', 'o1', 'p', 'ready')`)
	if err != nil {
		t.Fatal(err)
	}
	for i, step := range []struct {
		name           string
		run            func() error
		tables, column int
	}{
		{"up", func() error { _, err := gp.UpTo(ctx, 6); return err }, all, cols},
		{"down", func() error { _, err := gp.DownTo(ctx, 5); return err }, 0, 0},
		{"up again", func() error { _, err := gp.UpTo(ctx, 6); return err }, all, cols},
	} {
		if err := step.run(); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		tables, columns := schemaCounts(t, db)
		if tables != step.tables || columns != step.column {
			t.Fatalf("step %d %s: %d tables and %d columns, want %d and %d", i+1, step.name,
				tables, columns, step.tables, step.column)
		}
	}
	var edge, secrets int64
	var keyID *string
	err = db.QueryRowContext(ctx, `
		select edge_budget_bytes, secrets_version, key_id from placements`).
		Scan(&edge, &secrets, &keyID)
	if err != nil || edge != 0 || secrets != 0 || keyID != nil {
		t.Fatalf("an older placement reads %d, %d, %v, %v, want zeros and no key id",
			edge, secrets, keyID, err)
	}
	_, err = db.ExecContext(ctx, `
		insert into previews (id, placement_id, port)
		values ('abcdefghijklmnop', '00000000-0000-0000-0000-000000000001', 0)`)
	if err == nil {
		t.Fatal("a preview of port 0 was stored")
	}
}

func TestPlacementReadsDataPlaneColumns(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-dp")
	eq := func(got, want any, name string) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}
	eq(p.KeyID, "", "new key id")
	eq(p.SSHAlias, "", "new ssh alias")
	eq(p.EdgeBudgetBytes, int64(0), "new edge budget")
	eq(p.EgressCAVersion, 0, "new egress CA version")
	eq(p.GateReadyAt, (*time.Time)(nil), "new gate ready at")

	gate := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	_, err := pool.Exec(ctx, `
		update placements set key_id = 'k2', ssh_alias = 'bn-k3fz2q7max', source_label = 'gh',
			edge_budget_bytes = 2147483648, egress_budget_bytes = 1073741824,
			egress_ca_version = 3, secrets_version = 7, gate_ready_at = $2
		where id = $1`, p.ID, gate)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.PlacementByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(got.KeyID, "k2", "key id")
	eq(got.SSHAlias, "bn-k3fz2q7max", "ssh alias")
	eq(got.SourceLabel, "gh", "source label")
	eq(got.EdgeBudgetBytes, int64(2<<30), "edge budget")
	eq(got.EgressBudgetBytes, int64(1<<30), "egress budget")
	eq(got.EgressCAVersion, 3, "egress CA version")
	eq(got.SecretsVersion, int64(7), "secrets version")
	eq(got.GateReadyAt, &gate, "gate ready at")
}
