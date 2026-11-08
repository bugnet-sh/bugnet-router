package store_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/store"
)

// previewID is a 16 character id numbered n, the store checking no shape
func previewID(n int) string {
	return fmt.Sprintf("p%015d", n)
}

func TestUpsertPreviewIsIdempotentPerPort(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-prev")
	first, err := s.UpsertPreview(ctx, p.ID, 5173, 8, "aaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.UpsertPreview(ctx, p.ID, 5173, 8, "bbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != "aaaaaaaaaaaaaaaa" || again.Port != 5173 || again.Generation != 0 ||
		again.PlacementID != p.ID || again.RevokedAt != nil ||
		!again.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("the second upsert of a port gave %+v, want the first row %+v", again, first)
	}
	got, err := s.PreviewByID(ctx, "aaaaaaaaaaaaaaaa")
	if err != nil || got.Port != 5173 {
		t.Fatalf("by id: %+v, %v", got, err)
	}
	if _, err := s.PreviewByID(ctx, "bbbbbbbbbbbbbbbb"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("an id never stored: %v", err)
	}
}

func TestUpsertPreviewCapCountsLiveRows(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-cap")
	other := newPlacement(t, s, "o-cap-other")
	for port := 1; port <= 3; port++ {
		if _, err := s.UpsertPreview(ctx, p.ID, port, 3, previewID(port)); err != nil {
			t.Fatalf("port %d: %v", port, err)
		}
	}
	if _, err := s.UpsertPreview(ctx, p.ID, 4, 3, previewID(4)); !errors.Is(err,
		store.ErrTooManyPreviews) {
		t.Fatalf("a fourth preview of three: %v", err)
	}
	if _, err := s.UpsertPreview(ctx, p.ID, 2, 3, previewID(5)); err != nil {
		t.Fatalf("an existing port at the cap: %v", err)
	}
	if _, err := s.UpsertPreview(ctx, other.ID, 4, 3, previewID(6)); err != nil {
		t.Fatalf("another placement's first preview: %v", err)
	}
	if _, err := s.RevokePreview(ctx, previewID(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertPreview(ctx, p.ID, 4, 3, previewID(7)); err != nil {
		t.Fatalf("a preview after a revoked one freed its place: %v", err)
	}
}

func TestRacingUpsertsKeepTheCap(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-race")
	const racers = 12
	var wg sync.WaitGroup
	errs := make([]error, racers)
	for i := range racers {
		wg.Go(func() {
			_, errs[i] = s.UpsertPreview(ctx, p.ID, 1000+i, 8, previewID(i))
		})
	}
	wg.Wait()
	ok, full := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, store.ErrTooManyPreviews):
			full++
		default:
			t.Fatal(err)
		}
	}
	if ok != 8 || full != racers-8 {
		t.Fatalf("%d made and %d refused, want 8 and %d", ok, full, racers-8)
	}
	port := 1000
	for errs[port-1000] != nil {
		port++
	}
	same := make([]store.Preview, 6)
	for i := range same {
		wg.Go(func() {
			var err error
			if same[i], err = s.UpsertPreview(ctx, p.ID, port, 8, previewID(100+i)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	for _, pv := range same {
		if pv.ID != same[0].ID {
			t.Fatalf("racing upserts of one port gave %s and %s", pv.ID, same[0].ID)
		}
	}
}

func TestRevokePreviewMovesTheGenerationOnce(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-revoke")
	if _, err := s.UpsertPreview(ctx, p.ID, 3000, 8, "cccccccccccccccc"); err != nil {
		t.Fatal(err)
	}
	r, err := s.RevokePreview(ctx, "cccccccccccccccc")
	if err != nil || r.Generation != 1 || r.RevokedAt == nil || r.PlacementID != p.ID {
		t.Fatalf("revoke: %+v, %v", r, err)
	}
	again, err := s.RevokePreview(ctx, "cccccccccccccccc")
	if err != nil || again.Generation != 1 || !again.RevokedAt.Equal(*r.RevokedAt) {
		t.Fatalf("a second revoke: %+v, %v, want the first one's row", again, err)
	}
	pv, err := s.UpsertPreview(ctx, p.ID, 3000, 8, "dddddddddddddddd")
	if !errors.Is(err, store.ErrPreviewRevoked) || pv.ID != "cccccccccccccccc" {
		t.Fatalf("an upsert of a revoked port: %+v, %v", pv, err)
	}
	if _, err := s.RevokePreview(ctx, "eeeeeeeeeeeeeeee"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("revoking an unknown preview: %v", err)
	}
}

func TestPreviewsByIDs(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-ids")
	for i, port := range []int{80, 81} {
		if _, err := s.UpsertPreview(ctx, p.ID, port, 8, previewID(i)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.PreviewsByIDs(ctx, []string{previewID(1), "zzzzzzzzzzzzzzzz", previewID(0)})
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, pv := range got {
		ids = append(ids, pv.ID)
	}
	slices.Sort(ids)
	want := []string{previewID(0), previewID(1)}
	slices.Sort(want)
	if !slices.Equal(ids, want) {
		t.Fatalf("by ids gave %v, want %v", ids, want)
	}
	if got, err := s.PreviewsByIDs(ctx, nil); err != nil || len(got) != 0 {
		t.Fatalf("no ids: %v, %v", got, err)
	}
}

func TestSetPlacementRouteKeepsAnEarlierGateTime(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-route")
	now := time.Now().UTC().Truncate(time.Microsecond)
	ref := provider.Ref{Provider: "local", ID: "sbx-1", Meta: map[string]string{"a": "1"}}
	moved := provider.Ref{Provider: "local", ID: "sbx-1", Meta: map[string]string{"a": "2"}}
	if err := s.SetPlacementRoute(ctx, p.ID, moved, &now); !errors.Is(err,
		store.ErrStateConflict) {
		t.Fatalf("a placing placement's route: %v", err)
	}
	if err := s.MarkPlacementReady(ctx, p.ID, ref, "s", now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPlacementRoute(ctx, p.ID, moved, &now); err != nil {
		t.Fatal(err)
	}
	later := now.Add(time.Minute)
	moved.Meta["a"] = "3"
	if err := s.SetPlacementRoute(ctx, p.ID, moved, nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.PlacementByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProviderRef.Meta["a"] != "3" || got.GateReadyAt == nil || !got.GateReadyAt.Equal(now) {
		t.Fatalf("route %v at %v, want meta a=3 at %v", got.ProviderRef.Meta, got.GateReadyAt,
			now)
	}
	if err := s.SetPlacementRoute(ctx, p.ID, moved, &later); err != nil {
		t.Fatal(err)
	}
	if got, _ = s.PlacementByID(ctx, p.ID); !got.GateReadyAt.Equal(later) {
		t.Fatalf("gate ready at %v, want %v", got.GateReadyAt, later)
	}
	if err := s.SetPlacementRoute(ctx, uuid.New(), moved, nil); !errors.Is(err,
		store.ErrStateConflict) {
		t.Fatalf("an unknown placement: %v", err)
	}
}

func TestWithLockSerialisesOneName(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	var mu sync.Mutex
	inside, most := 0, 0
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			err := s.WithLock(ctx, "test:one", func(*store.Store) error {
				mu.Lock()
				inside++
				most = max(most, inside)
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				mu.Lock()
				inside--
				mu.Unlock()
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if most != 1 {
		t.Fatalf("%d ran under one lock at once, want 1", most)
	}
	want := errors.New("rolled back")
	if err := s.WithLock(ctx, "test:one", func(*store.Store) error { return want }); !errors.Is(
		err, want) {
		t.Fatalf("WithLock gave %v, want fn's error", err)
	}
}

func TestMarkGateReadyKeepsAnEarlierTime(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-gate")
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := s.MarkGateReady(ctx, p.ID, now); !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("a placing placement's gate: %v", err)
	}
	ref := provider.Ref{Provider: "local", ID: "sbx-1"}
	if err := s.MarkPlacementReady(ctx, p.ID, ref, "s", now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{now, now.Add(time.Minute)} {
		if err := s.MarkGateReady(ctx, p.ID, at); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.PlacementByID(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.GateReadyAt == nil || !got.GateReadyAt.Equal(now) {
		t.Fatalf("gate ready at %v, want %v", got.GateReadyAt, now)
	}
}
