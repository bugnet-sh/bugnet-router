package spend_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/spend"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

var t0 = time.Date(2026, 11, 10, 3, 0, 0, 0, time.UTC)

func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestAttemptCostRoundsUp(t *testing.T) {
	// 61 s rounds up to 2 billed minutes at 60,000 micros an hour, plus a 500 micro start fee
	got := spend.AttemptCost(t0, t0.Add(61*time.Second), time.Minute, 60_000, 500)
	eq(t, got, int64(2_000+500))
}

func TestAttemptCostEdges(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  time.Duration
		incr time.Duration
		want int64
	}{
		{"no time, no cost", 0, time.Second, 0},
		{"an end before the start bills nothing", -time.Minute, time.Second, 0},
		{"a whole increment", time.Minute, time.Minute, 1_000},
		{"no increment bills the time as it ran", 90 * time.Second, 0, 1_500},
		{"the micro rounds up", time.Second, time.Second, 17},
	} {
		got := spend.AttemptCost(t0, t0.Add(tc.end), tc.incr, 60_000, 0)
		eq(t, got, tc.want)
	}
	// A day at a dear rate overflows 64 bits when multiplied in nanoseconds
	eq(t, spend.AttemptCost(t0, t0.Add(24*time.Hour), time.Second, 1_000_000_000_000, 0),
		int64(24_000_000_000_000))
}

func TestJudgeFindsPlacementsUnderTheFloor(t *testing.T) {
	eq(t, spend.Judge(40, 100, 0.5), spend.Verdict{})
	eq(t, spend.Judge(60, 100, 0.5), spend.Verdict{BelowFloor: true})
	eq(t, spend.Judge(150, 100, 0.5), spend.Verdict{BelowFloor: true, Loss: true})
	eq(t, spend.Judge(1, 0, 0.5), spend.Verdict{BelowFloor: true, Loss: true})
}

func TestSpansCostPricesEachAttemptAtItsOwnProvider(t *testing.T) {
	gone := t0.Add(time.Hour)
	spans := []store.AttemptSpan{
		{Provider: "a", HourlyMicros: 60_000, CreatedAt: t0, DestroyedAt: &gone},
		{Provider: "b", HourlyMicros: 60_000, StartFeeMicros: 5, CreatedAt: t0},
	}
	incr := func(name string) time.Duration {
		if name == "b" {
			return time.Hour
		}
		return time.Second
	}
	// The second attempt lives to the end given, 30 minutes, and bills a whole hour
	eq(t, spend.SpansCost(spans, t0.Add(30*time.Minute), incr), int64(60_000+60_000+5))
}
