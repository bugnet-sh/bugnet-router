package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/migrations"
)

func TestMigrationOrderSecondsUpDownUp(t *testing.T) {
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
	v := migrationVersion(t, "order_seconds")
	if _, err := gp.UpTo(ctx, v-1); err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `insert into orders (order_id, kind, placement_id)
		values ('o1', 'start', '00000000-0000-0000-0000-000000000001')`)
	if err != nil {
		t.Fatal(err)
	}
	column := func() int {
		var n int
		err := db.QueryRowContext(ctx, `select count(*) from information_schema.columns
			where table_schema = current_schema() and table_name = 'orders'
				and column_name = 'seconds_bought'`).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	for range 2 {
		if _, err := gp.UpTo(ctx, v); err != nil {
			t.Fatal(err)
		}
		eq(t, column(), 1)
		var seconds int64
		if err := db.QueryRowContext(ctx,
			`select seconds_bought from orders where order_id = 'o1'`).Scan(&seconds); err != nil {
			t.Fatal(err)
		}
		eq(t, seconds, int64(0))
		if _, err := gp.DownTo(ctx, v-1); err != nil {
			t.Fatal(err)
		}
		eq(t, column(), 0)
	}
}

func TestCreatePlacementOrderKeepsThePayerWithoutAClaim(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	p, _, err := s.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: []byte("h1"), OrderID: "o1", PackageID: "p", Payer: walletA,
		SecondsBought: 600,
	})
	eq(t, err, nil)
	o, err := s.OrderByID(ctx, p.OrderID)
	eq(t, err, nil)
	eq(t, []any{o.Payer, o.SecondsBought}, []any{walletA, int64(600)})
	// A claim the router decided against is still the one the order keeps
	p, _, err = s.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: []byte("h2"), OrderID: "o2", PackageID: "p", PayerClaim: walletB,
	})
	eq(t, err, nil)
	o, err = s.OrderByID(ctx, p.OrderID)
	eq(t, err, nil)
	eq(t, []string{o.Payer, p.Payer}, []string{walletB, ""})
}

func TestOrdersNewestFirstEndWithTheStart(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	p, _, err := s.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: []byte("h1"), OrderID: "start", PackageID: "p", AmountMicros: 50000,
		SecondsBought: 600,
	})
	eq(t, err, nil)
	ready(t, s, p)
	for _, id := range []string{"e1", "e2"} {
		if _, _, err := s.ExtendWithOrder(ctx, store.Order{
			ID: id, PlacementID: p.ID, AmountMicros: 25000, SecondsBought: 300,
		}, 5*time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	orders, err := s.OrdersNewestFirst(ctx, p.ID)
	eq(t, err, nil)
	var ids []string
	for _, o := range orders {
		ids = append(ids, o.ID)
	}
	eq(t, ids, []string{"e2", "e1", "start"})
	eq(t, orders[0].SecondsBought, int64(300))
}

func TestFailedRefundsCountsTheDay(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	now := time.Now()
	end := func(order, payer string, at time.Time, evidence string) {
		t.Helper()
		p, _, err := s.CreatePlacement(ctx, store.NewPlacement{
			HandleSHA256: []byte(order), OrderID: order, PackageID: "p", Payer: payer,
		})
		eq(t, err, nil)
		ready(t, s, p)
		eq(t, s.MarkPlacementEnding(ctx, p.ID, store.Ending{
			Reason: "provider_failed", Refundable: true, Evidence: json.RawMessage(evidence),
			Actor: "router", At: at,
		}), nil)
	}
	end("a1", walletA, now, `{}`)
	end("a2", walletA, now.Add(-25*time.Hour), `{}`)
	end("a3", walletA, now, `{"correlated": true}`)
	end("b1", walletB, now, `{}`)
	end("n1", "", now, `{}`)
	since := now.Add(-24 * time.Hour)
	for payer, want := range map[string]int{walletA: 1, walletB: 1, "": 1} {
		n, err := s.FailedRefunds(ctx, payer, since)
		eq(t, err, nil)
		eq(t, n, want)
	}
}

func TestMarginCountsUnderAndLoss(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := t.Context()
	ended := func(id string, cost, paid int64) {
		t.Helper()
		p, _, err := s.CreatePlacement(ctx, store.NewPlacement{
			HandleSHA256: []byte(id), OrderID: id, PackageID: "p", AmountMicros: paid,
		})
		eq(t, err, nil)
		_, err = pool.Exec(ctx, `update placements set state = 'terminated', cost_micros = $2,
			terminated_at = now() where id = $1`, p.ID, cost)
		eq(t, err, nil)
	}
	ended("fits", 3050, 3355)
	ended("under", 3050, 3354)
	ended("loss", 3050, 3000)
	under, loss, err := s.MarginCounts(ctx, time.Now().Add(-time.Hour),
		pricing.Rule{MarkupBP: 1000})
	eq(t, err, nil)
	eq(t, []int{under, loss}, []int{2, 1})
}
