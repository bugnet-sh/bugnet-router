package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// holdCapacity sets provider's cap, its own use and what other deployments use on the account
func holdCapacity(t *testing.T, s *store.Store, name string, limit, inUse, external int) {
	t.Helper()
	ctx := context.Background()
	err := s.WriteLimits(ctx, name, limit, store.LimitFromSettings, external, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for range inUse {
		if ok, err := s.ReserveCapacity(ctx, name, 0); err != nil || !ok {
			t.Fatalf("hold a slot = %v, %v", ok, err)
		}
	}
}

func TestReserveRule(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	// 18 held by this router and 1 by someone else, of 20
	holdCapacity(t, s, "e2b", 20, 18, 1)
	if err := s.WriteLimits(ctx, "e2b", 20, store.LimitFromSettings, 19, time.Now()); err != nil {
		t.Fatal(err)
	}
	// 18 + 1 + 1 < 20 fails for a placement and holds for a probe at margin 0
	if ok, err := s.ReserveCapacity(ctx, "e2b", 1); err != nil || ok {
		t.Fatalf("placement reserve = %v, %v", ok, err)
	}
	if ok, err := s.ReserveCapacity(ctx, "e2b", 0); err != nil || !ok {
		t.Fatalf("probe reserve = %v, %v", ok, err)
	}
	if ok, err := s.ReserveCapacity(ctx, "e2b", 0); err != nil || ok {
		t.Fatalf("a full provider admits no probe either: %v, %v", ok, err)
	}
}

func TestWriteLimitsCountsTheRestAsExternal(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for range 3 {
		if ok, err := s.ReserveCapacity(ctx, "boat", 0); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	at := time.Now().UTC().Truncate(time.Millisecond)
	// The provider counts 5 on the account, 3 of them this router's
	if err := s.WriteLimits(ctx, "boat", 100, store.LimitFromProvider, 5, at); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Capacities(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows = %+v, %v", rows, err)
	}
	want := store.Capacity{
		Provider: "boat", MaxConcurrent: 100, InUse: 3, ExternalInUse: 2,
		LimitSource: store.LimitFromProvider, ProviderActive: 5, PolledAt: at,
	}
	if rows[0] != want {
		t.Fatalf("row = %+v, want %+v", rows[0], want)
	}
	// A provider that counts fewer than this router holds leaves nothing external
	if err := s.WriteLimits(ctx, "boat", 100, store.LimitFromProvider, 1, at); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.Capacities(ctx)
	if rows[0].ExternalInUse != 0 {
		t.Fatalf("external = %d, want 0", rows[0].ExternalInUse)
	}
}

func TestPlacementReserveLeavesTheProbeSlot(t *testing.T) {
	s, _ := newStoreWithPool(t, store.WithReserveSlots(2))
	ctx := context.Background()
	holdCapacity(t, s, "p", 5, 0, 0)
	for i, want := range []bool{true, true, true, false} {
		pl := newPlacement(t, s, "o"+string(rune('a'+i)))
		ok, err := s.ReservePlacementCapacity(ctx, pl.ID, "p", 1)
		if err != nil || ok != want {
			t.Fatalf("placement %d reserve = %v, %v", i, ok, err)
		}
	}
}

func TestRecordAccountSignalsOnlyChanges(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Millisecond)
	steps := []struct {
		block string
		want  store.AccountChange
	}{
		{"", store.AccountSame},
		{"banned", store.AccountBlocked},
		{"banned", store.AccountSame},
		{"unfunded", store.AccountBlocked},
		{"", store.AccountCleared},
	}
	for i, st := range steps {
		when := at.Add(time.Duration(i) * time.Second)
		got, err := s.RecordAccount(ctx, "e2b", st.block, "detail", when)
		if err != nil || got != st.want {
			t.Fatalf("step %d (%q) = %v, %v, want %v", i, st.block, got, err, st.want)
		}
	}
	rows, err := s.ProviderHealths(ctx)
	if err != nil || len(rows) != 1 || rows[0].Blocked != "" || !rows[0].BlockedSince.IsZero() {
		t.Fatalf("rows = %+v, %v", rows, err)
	}
	if rows[0].AccountCheckedAt.IsZero() {
		t.Fatal("the check time was not stored")
	}
}

func TestBlockedSinceHoldsWhileTheBlockStays(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	first := time.Now().UTC().Truncate(time.Millisecond)
	for _, at := range []time.Time{first, first.Add(time.Minute)} {
		if _, err := s.RecordAccount(ctx, "boat", "auth", "401", at); err != nil {
			t.Fatal(err)
		}
	}
	rows, _ := s.ProviderHealths(ctx)
	checked := first.Add(time.Minute)
	if !rows[0].BlockedSince.Equal(first) || !rows[0].AccountCheckedAt.Equal(checked) {
		t.Fatalf("row = %+v", rows[0])
	}
}
