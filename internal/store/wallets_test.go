package store_test

import (
	"context"
	"io/fs"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/migrations"
)

const (
	walletA = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	walletB = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func ready(t *testing.T, s *store.Store, p store.Placement) {
	t.Helper()
	now := time.Now()
	ref := provider.Ref{Provider: "a", ID: "sbx-" + p.OrderID}
	if err := s.MarkPlacementReady(t.Context(), p.ID, ref, "small", now,
		now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationWalletsUpDownUp(t *testing.T) {
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
	walletsVersion := migrationVersion(t, "wallets")
	tables := func() int {
		var n int
		err := db.QueryRowContext(ctx, `
			select count(*) from information_schema.tables
			where table_schema = current_schema()
				and table_name in ('wallets', 'wallet_events', 'provider_signals')`).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if _, err := gp.UpTo(ctx, walletsVersion-1); err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `insert into orders (order_id, kind, placement_id)
		values ('o1', 'start', '00000000-0000-0000-0000-000000000001')`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gp.UpTo(ctx, walletsVersion); err != nil {
		t.Fatal(err)
	}
	eq(t, tables(), 3)
	var amount int64
	if err := db.QueryRowContext(ctx,
		`select amount_micros from orders where order_id = 'o1'`).Scan(&amount); err != nil {
		t.Fatal(err)
	}
	eq(t, amount, int64(0))
	if _, err := gp.DownTo(ctx, walletsVersion-1); err != nil {
		t.Fatal(err)
	}
	eq(t, tables(), 0)
	if _, err := gp.UpTo(ctx, walletsVersion); err != nil {
		t.Fatal(err)
	}
	eq(t, tables(), 3)
}

// migrationVersion is the goose version of the migration file named <version>_<name>.sql
func migrationVersion(t *testing.T, name string) int64 {
	t.Helper()
	files, err := fs.Glob(migrations.FS, "*_"+name+".sql")
	if err != nil || len(files) != 1 {
		t.Fatalf("migration %s: %v, %v", name, files, err)
	}
	v, err := goose.NumericComponent(files[0])
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestPaidMicrosSumsTheRecordedOrders(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	p, _, err := s.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: []byte("h1"), OrderID: "o1", PackageID: "p", Payer: walletA,
		FeePayer: walletB, AmountMicros: 10000,
	})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.PaidMicros, int64(10000))
	eq(t, p.Payer, walletA)
	ready(t, s, p)
	got, applied, err := s.ExtendWithOrder(ctx, store.Order{
		ID: "e1", PlacementID: p.ID, Payer: walletB, AmountMicros: 50000,
	}, time.Minute)
	eq(t, err, nil)
	eq(t, applied, true)
	eq(t, got.PaidMicros, int64(60000))
	again, applied, err := s.ExtendWithOrder(ctx, store.Order{
		ID: "e1", PlacementID: p.ID, Payer: walletB, AmountMicros: 50000,
	}, time.Minute)
	eq(t, err, nil)
	eq(t, applied, false)
	eq(t, again.PaidMicros, int64(60000))
	payers, err := s.ExtendPayers(ctx, p.ID)
	eq(t, err, nil)
	eq(t, payers, []string{walletB})
	live, err := s.LivePlacementsOf(ctx, walletB)
	eq(t, err, nil)
	eq(t, len(live), 1)
	seen, err := s.FeePayerSeen(ctx, walletB)
	eq(t, err, nil)
	eq(t, seen, true)
	n, err := s.LiveCount(ctx, walletA)
	eq(t, err, nil)
	eq(t, n, 1)
	n, err = s.LiveCount(ctx, "")
	eq(t, err, nil)
	eq(t, n, 0)
}

func TestPaidMicrosLeavesOutARefundedFailure(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	for i, refundable := range []bool{true, false} {
		p, _, err := s.CreatePlacement(ctx, store.NewPlacement{
			HandleSHA256: []byte{byte(i)}, OrderID: "o" + strconv.Itoa(i), PackageID: "p",
			Payer: walletA, AmountMicros: 10000,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.MarkPlacementFailed(ctx, p.ID, "no_capacity", refundable); err != nil {
			t.Fatal(err)
		}
		got, err := s.PlacementByID(ctx, p.ID)
		eq(t, err, nil)
		want := int64(10000)
		if refundable {
			want = 0
		}
		eq(t, got.PaidMicros, want)
	}
}

func TestListWalletsPagesAndFilters(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	for _, p := range []string{walletA, walletB} {
		if err := s.SeeWallet(ctx, p, now); err != nil {
			t.Fatal(err)
		}
	}
	past := now.Add(-time.Minute)
	if err := s.SetWalletBlock(ctx, walletB, now.Add(-time.Hour), &past, "refusals",
		"router"); err != nil {
		t.Fatal(err)
	}
	all, err := s.ListWallets(ctx, store.WalletPage{Limit: 1, Now: now})
	eq(t, err, nil)
	eq(t, len(all), 1)
	eq(t, all[0].Payer, walletB)
	rest, err := s.ListWallets(ctx, store.WalletPage{After: walletB, Limit: 10, Now: now})
	eq(t, err, nil)
	eq(t, len(rest), 1)
	eq(t, rest[0].Payer, walletA)
	blocked, err := s.ListWallets(ctx, store.WalletPage{BlockedOnly: true, Limit: 10, Now: now})
	eq(t, err, nil)
	eq(t, len(blocked), 0)
	if err := s.SetWalletBlock(ctx, walletA, now, nil, "operator", "operator:ann"); err != nil {
		t.Fatal(err)
	}
	blocked, err = s.ListWallets(ctx, store.WalletPage{BlockedOnly: true, Limit: 10, Now: now})
	eq(t, err, nil)
	eq(t, len(blocked), 1)
	eq(t, blocked[0].Payer, walletA)

	// last_seen moves at most hourly
	if err := s.SeeWallet(ctx, walletA, now.Add(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	w, err := s.Wallet(ctx, walletA, now)
	eq(t, err, nil)
	eq(t, w.LastSeen, now)
	if err := s.SeeWallet(ctx, walletA, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	w, err = s.Wallet(ctx, walletA, now)
	eq(t, err, nil)
	eq(t, w.LastSeen, now.Add(2*time.Hour))
	eq(t, w.FirstSeen, now)
}
