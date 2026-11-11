package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

const (
	// InspectTimeout bounds the provider read of one reconcile
	InspectTimeout = 30 * time.Second
	// lifeEvery is the gap between lifetime fixes of one placement
	lifeEvery = 10 * time.Minute
	// maxReason cuts the provider's words kept as evidence
	maxReason = 500
	// recheckSlack lands a scheduled re-check just after the grace it waits for
	recheckSlack = time.Second
)

// Observation is one reading of a live placement's sandbox, read from the provider when its
// Status has no state
type Observation struct {
	Status  provider.Status
	Source  string // hook, sweep or operator
	EventID string
}

// Deps are what a Reconciler is built from
type Deps struct {
	Saga     *saga.Saga
	Store    *store.Store
	Registry *provider.Registry
	// River inserts the scheduled re-checks, insert only
	River  *river.Client[pgx.Tx]
	Valkey valkey.Client
	Prefix string
	Abuse  config.Abuse
	// Settings holds the drift bound, each zero value at its default
	Settings config.Reconcile
	Now      func() time.Time // time.Now when nil
	Log      *slog.Logger     // slog.Default when nil
}

// Reconciler acts on what providers say about live placements
type Reconciler struct {
	d Deps
}

// New builds a Reconciler, each zero setting at its default
func New(d Deps) *Reconciler {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Log == nil {
		d.Log = slog.Default()
	}
	d.Abuse = d.Abuse.OrDefaults()
	d.Settings = d.Settings.OrDefaults()
	return &Reconciler{d: d}
}

func (r *Reconciler) now() time.Time { return r.d.Now().UTC() }

// Reconcile inspects p's sandbox when o is nil or has no state, classifies the reading and acts
// on it once. An Inspect error stops here, since a sandbox the router cannot read is never ended.
func (r *Reconciler) Reconcile(ctx context.Context, p store.Placement, o *Observation) error {
	prov, ok := r.d.Registry.Get(p.Provider)
	live := p.State == store.PlacementReady || p.State == store.PlacementTerminating
	ended := p.State == store.PlacementTerminated || p.State == store.PlacementFailed
	if !ok || p.ProviderRef == nil || !live && !ended {
		return nil
	}
	if o == nil {
		o = &Observation{Source: "sweep"}
	}
	if o.Status.State == "" {
		ictx, cancel := context.WithTimeout(ctx, InspectTimeout)
		st, err := prov.Inspect(ictx, *p.ProviderRef)
		cancel()
		if err != nil {
			r.d.Log.Warn("reconcile inspect failed", "placement", p.ID, "provider", p.Provider,
				"err", err)
			return nil
		}
		o.Status = st
	}
	st, now := o.Status, r.now()
	switch st.State {
	case provider.StateRunning:
		if p.DegradedSince != nil {
			if err := r.d.Store.ClearDegraded(ctx, p.ID); err != nil {
				return err
			}
		}
		if p.State == store.PlacementReady {
			r.fixLifetime(ctx, prov, p, st)
		}
		return nil
	case provider.StateDegraded:
		if !live {
			return nil
		}
		if p.DegradedSince == nil {
			if err := r.d.Store.MarkDegraded(ctx, p.ID, now); err != nil {
				return err
			}
			p.DegradedSince = &now
		}
	}
	v := Classify(p.Provider, st, LiveOf(p), now, r.d.Abuse)
	if v.Recheck > 0 {
		return r.recheck(ctx, p, v.Recheck)
	}
	// An account-wide action shows within seconds, and trips the guard for the rest (CP 6.4)
	if v.Class == ClassAbuse || v.CheckAccount {
		health.RequestAccountCheck(ctx, r.d.River, p.Provider)
	}
	if v.Class == ClassNone {
		return nil
	}
	// A stop in flight yields only to abuse, and an ended placement takes only an abuse flag
	if v.Class != ClassAbuse && p.State != store.PlacementReady {
		return nil
	}
	if v.Alert != "" {
		r.d.Log.Error("a sandbox was killed and no router asked", "placement", p.ID,
			"provider", p.Provider, "alert", v.Alert)
	}
	return r.act(ctx, p, o, v)
}

// recheck schedules another look when the grace of a degraded sandbox ends
func (r *Reconciler) recheck(ctx context.Context, p store.Placement, after time.Duration) error {
	if r.d.River == nil {
		return nil
	}
	_, err := r.d.River.Insert(ctx, jobargs.Reconcile{PlacementID: p.ID},
		&river.InsertOpts{ScheduledAt: r.now().Add(after + recheckSlack)})
	if err != nil {
		return fmt.Errorf("insert reconcile job: %w", err)
	}
	return nil
}

// act ends the placement for the verdict, the end holding its wallet actions when the guard says
// one provider incident is behind it
func (r *Reconciler) act(
	ctx context.Context, p store.Placement, o *Observation, v Verdict,
) error {
	ended := p.State == store.PlacementTerminated || p.State == store.PlacementFailed
	e := saga.End{
		Reason: string(v.Class), Actor: wallets.ActorRouter,
		Evidence: r.evidence(p, o, v),
	}
	end := func(e saga.End) error {
		var err error
		if ended {
			_, _, err = r.d.Saga.FlagEnded(ctx, p.ID, e)
		} else {
			_, err = r.d.Saga.End(ctx, p.ID, e)
		}
		if errors.Is(err, saga.ErrAlreadyEnded) {
			return nil
		}
		return err
	}
	if guards(v.Class) {
		// The guard runs in the end's own transaction, so its lock never waits on the destroy
		e.Guard = func(ctx context.Context, tx pgx.Tx) (bool, error) {
			return Guard(ctx, tx, p.Provider, v.Class, r.now(), r.d.Abuse)
		}
	}
	return end(e)
}

// guards reports whether the correlation guard looks at a class
func guards(c Class) bool {
	return c == ClassAbuse || c == ClassControlLost || c == ClassSandboxExited
}

// evidence is the signal as JSON, the provider's words cut and never a secret
func (r *Reconciler) evidence(p store.Placement, o *Observation, v Verdict) []byte {
	reason := o.Status.Reason
	if len(reason) > maxReason {
		reason = reason[:maxReason]
	}
	m := map[string]any{
		"provider": p.Provider, "source": o.Source, "event_id": o.EventID,
		"state": string(o.Status.State), "reason": reason, "kill_reason": o.Status.KillReason,
		"class": string(v.Class),
	}
	if !o.Status.Since.IsZero() {
		m["since"] = o.Status.Since.UTC().Format(time.RFC3339)
	}
	if v.Alert != "" {
		m["alert"] = v.Alert
	}
	raw, _ := json.Marshal(m)
	return raw
}

// noLifetimeChange lists the providers whose lifetime the router cannot move without a redeploy
var noLifetimeChange = map[string]bool{"koyeb": true, "hf": true}

// Drifted reports whether a provider-side lifetime strays from the deadline plus the slack by
// more than the bound, false for a provider whose lifetime cannot move or a missing value
func Drifted(prov string, expires time.Time, deadline *time.Time, bound time.Duration) bool {
	if noLifetimeChange[prov] || expires.IsZero() || deadline == nil {
		return false
	}
	return abs(expires.Sub(deadline.Add(saga.LifetimeSlack))) > bound
}

// fixLifetime keeps the provider-side lifetime at the deadline plus the slack (CP 10.4). It never
// acts on the reading alone: an extension holds the placement row until its new deadline commits.
func (r *Reconciler) fixLifetime(
	ctx context.Context, prov provider.Provider, p store.Placement, st provider.Status,
) {
	drift := r.d.Settings.LifetimeDrift
	if p.ProviderRef == nil || !Drifted(p.Provider, st.ExpiresAt, p.Deadline, drift) {
		return
	}
	err := r.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		cur, err := r.d.Store.Tx(tx).PlacementForUpdate(ctx, p.ID)
		if err != nil {
			return err
		}
		if cur.State != store.PlacementReady || cur.Deadline == nil {
			return nil
		}
		want := cur.Deadline.Add(saga.LifetimeSlack)
		if abs(st.ExpiresAt.Sub(want)) <= drift || !r.lifeDue(ctx, p) {
			return nil
		}
		sctx, cancel := context.WithTimeout(ctx, InspectTimeout)
		defer cancel()
		if err := prov.SetLifetime(sctx, *p.ProviderRef, want); err != nil {
			return err
		}
		r.d.Log.Warn("provider lifetime corrected", "placement", p.ID, "provider", p.Provider,
			"was", st.ExpiresAt.UTC().Format(time.RFC3339), "now", want.UTC().Format(time.RFC3339))
		return nil
	})
	if err != nil {
		r.d.Log.Warn("set lifetime from reconcile failed", "placement", p.ID,
			"provider", p.Provider, "err", err)
	}
}

// lifeDue takes the placement's 10 minute slot for a lifetime fix, true when it was free
func (r *Reconciler) lifeDue(ctx context.Context, p store.Placement) bool {
	if r.d.Valkey == nil {
		return true
	}
	key := r.d.Prefix + "reconcile:life:" + p.ID.String()
	err := r.d.Valkey.Do(ctx, r.d.Valkey.B().Set().Key(key).Value("1").Nx().Ex(lifeEvery).
		Build()).Error()
	if valkey.IsValkeyNil(err) {
		return false
	}
	if err != nil {
		r.d.Log.Warn("reconcile lifetime gate unavailable", "err", err)
		return false
	}
	return true
}

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
