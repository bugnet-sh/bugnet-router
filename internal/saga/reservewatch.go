package saga

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// brakeWindow is how far back the brake counts the reservations that ended unused and bound
const brakeWindow = time.Hour

// WatchReport is what one reserve_watch run did
type WatchReport struct {
	Lapsed, Failed, Retired int
	// Braked says the brake is set after the run
	Braked bool
}

// WatchReservations is reserve_watch (CP 18.14): it lapses the reservations past their expiry and
// grace, fails those whose heartbeat is 30 s stale and the awaited ones whose placement left
// placing, inserts pool_retire for any held member left without a reservation, and sets or clears
// the brake
//
// Each ending retires its member in its own transaction, so no run leaves a sandbox held.
func (s *Saga) WatchReservations(ctx context.Context) (WatchReport, error) {
	var rep WatchReport
	var errs []error
	now := s.now()
	steps := []struct {
		name  string
		state string
		why   string
		end   string
		n     *int
		pick  func(context.Context, *store.Store) ([]store.EndedMember, error)
	}{
		{"lapse", store.ReservationLapsed, "", store.EndLapsed, &rep.Lapsed,
			func(ctx context.Context, ts *store.Store) ([]store.EndedMember, error) {
				return ts.LapseReservations(ctx, now, s.d.Reserve.LapseGrace)
			}},
		{"stale", store.ReservationFailed, "heartbeat stale", store.EndFailed, &rep.Failed,
			func(ctx context.Context, ts *store.Store) ([]store.EndedMember, error) {
				return ts.FailStaleReservations(ctx, now.Add(-staleAfter), now)
			}},
		{"orphan", store.ReservationFailed, "placement left placing", store.EndFailed, &rep.Failed,
			func(ctx context.Context, ts *store.Store) ([]store.EndedMember, error) {
				return ts.FailOrphanedAwaited(ctx, now)
			}},
	}
	for _, st := range steps {
		var ended []store.EndedMember
		err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
			var err error
			if ended, err = st.pick(ctx, s.d.Store.Tx(tx)); err != nil {
				return err
			}
			for _, e := range ended {
				if err := s.retireMemberTx(ctx, tx, e.Member, st.end, st.why); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("%s reservations: %w", st.name, err))
			continue
		}
		*st.n += len(ended)
		for _, e := range ended {
			s.publishReservation(ctx, e.Reservation, st.state)
		}
	}
	n, err := s.retireUnkept(ctx)
	rep.Retired = n
	if err != nil {
		errs = append(errs, err)
	}
	if rep.Braked, err = s.brake(ctx, now); err != nil {
		errs = append(errs, err)
	}
	return rep, errors.Join(errs...)
}

// retireUnkept inserts pool_retire for each held member no open reservation keeps
func (s *Saga) retireUnkept(ctx context.Context) (int, error) {
	ids, err := s.d.Store.UnkeptHeldMembers(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
			return s.retireMemberTx(ctx, tx, id, store.EndUnused, "its reservation ended")
		})
		if err != nil {
			return n, fmt.Errorf("retire held member %s: %w", id, err)
		}
		n++
	}
	return n, nil
}

// brake sets reserve:brake for reserve.brake_for when the last hour's unused reservations reach
// reserve.brake_unused and at least reserve.brake_ratio times the bound ones, and clears it when
// they do not, answering whether it is set
//
// A brake that is set keeps its first expiry, so one brake lasts brake_for. When it ends and the
// hour still holds the unused ones, the next run sets it again at once.
func (s *Saga) brake(ctx context.Context, now time.Time) (bool, error) {
	cfg := s.d.Reserve
	unused, bound, err := s.d.Store.BrakeCounts(ctx, now.Add(-brakeWindow))
	if err != nil {
		return false, err
	}
	key := s.d.Prefix + BrakeKey
	if unused < cfg.BrakeUnused || unused < cfg.BrakeRatio*bound {
		if err := s.d.Valkey.Do(ctx, s.d.Valkey.B().Del().Key(key).Build()).Error(); err != nil {
			return false, fmt.Errorf("clear the reservation brake: %w", err)
		}
		return false, nil
	}
	val := fmt.Sprintf("%d %d", unused, bound)
	cmd := s.d.Valkey.B().Set().Key(key).Value(val).Nx().Px(cfg.BrakeFor).Build()
	res := s.d.Valkey.Do(ctx, cmd)
	if err := res.Error(); err != nil && !valkey.IsValkeyNil(err) {
		return false, fmt.Errorf("set the reservation brake: %w", err)
	} else if err == nil {
		s.d.Log.Error("reservations are braked, too many went unpaid", "unused", unused,
			"bound", bound, "for", cfg.BrakeFor)
	}
	return true, nil
}
