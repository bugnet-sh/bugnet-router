package saga

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/placement"
)

// race is what the attempts of one placing share, so a winner's commit can cut the losers short
type race struct {
	mu   sync.Mutex
	runs []*run
	// hedged is set once a second attempt starts, which the samples of both then say
	hedged atomic.Bool
	// missed is set once the order was counted as wanting a pool that had no member for it
	missed atomic.Bool
}

// run is one attempt of a placing, or the wait on a reservation that counts as the first one
type run struct {
	race *race
	// n is the attempt's number, 0 for the wait on a reservation
	n     int
	cand  placement.Candidate
	hedge bool
	// started is when the attempt began, the reservation's creation for a wait
	started time.Time
	// unknown is set once a create of the attempt had an unknown outcome, which bars a hedge
	unknown atomic.Bool
	// lost ends when another attempt won, which cuts the attempt's own calls short
	lost       context.Context
	cancelLost context.CancelFunc
}

func (rc *race) newRun(n int, c placement.Candidate, hedge bool, started time.Time) *run {
	lost, cancel := context.WithCancel(context.Background())
	r := &run{race: rc, n: n, cand: c, hedge: hedge, started: started, lost: lost,
		cancelLost: cancel}
	rc.mu.Lock()
	rc.runs = append(rc.runs, r)
	rc.mu.Unlock()
	return r
}

// win cuts every other attempt short, the winner's commit having made its sandbox the placement's
func (rc *race) win(winner *run) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	for _, r := range rc.runs {
		if r != winner {
			r.cancelLost()
		}
	}
}

// stop ends the wait and the attempts alike
func (rc *race) stop() { rc.win(nil) }

// isLost reports whether another attempt won
func (r *run) isLost() bool { return r.lost.Err() != nil }

type runKey struct{}

// withRun carries the attempt through the calls it makes
func withRun(ctx context.Context, r *run) context.Context {
	return context.WithValue(ctx, runKey{}, r)
}

// runOf is the attempt a context belongs to, nil outside a placing
func runOf(ctx context.Context) *run {
	r, _ := ctx.Value(runKey{}).(*run)
	return r
}

// work derives a context for one call of an attempt that also ends when the attempt loses, so a
// winner's commit cuts a loser's create and boot short while its cleanup still runs on ctx
func work(ctx context.Context) (context.Context, context.CancelFunc) {
	r := runOf(ctx)
	if r == nil {
		return ctx, func() {}
	}
	c, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.lost, cancel)
	return c, func() {
		stop()
		cancel()
	}
}

// lostRun reports whether the attempt ctx belongs to lost the race
func lostRun(ctx context.Context) bool {
	r := runOf(ctx)
	return r != nil && r.isLost()
}
