package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// fillStuckAfter is how old a filling member may get before its job counts as dead
	fillStuckAfter = 10 * time.Minute
	// ageSlack is how long a sandbox lives at the provider beyond its pool's max age
	ageSlack = time.Hour
	// failedFills is how many fills in a row pause a pool
	failedFills = 3
	// defaultRefill is the refill time assumed with fewer than minRefillSamples fills
	defaultRefill = time.Minute
	// minRefillSamples is the fewest fills that give a refill p95
	minRefillSamples = 5
	// refillWindow is how far back the refill p95 reads
	refillWindow = 24 * time.Hour
	// SpendKey is the Valkey key, under the prefix, a tick keeps set while the spend cap holds
	SpendKey = "reserve:spend"
)

// ErrSnooze means a fill found no create token yet and should run again later
var ErrSnooze = errors.New("pool: the provider's creates are busy")

// ErrUnknownPool means no pool is configured for the provider and size
var ErrUnknownPool = errors.New("pool: no such pool")

// Booted is what a pool boot timed, in milliseconds, store.NoStep when a step did not run
type Booted struct {
	ReadyMS, RunnerMS int
	// Image is the base image record the boot read, nil for none
	Image *store.ReadyImage
	// Reseed is setup's answer for its seed, empty when it gave none
	Reseed string
}

// Saga is what a fill needs of the saga, an interface so the saga can import this package
type Saga interface {
	// TakeCreate spends a create token of the provider, false when none came in time
	TakeCreate(ctx context.Context, name string) bool
	// CreatesPaused reports whether the provider asked for its creates to wait
	CreatesPaused(ctx context.Context, name string) bool
	// ProviderFailed acts on an error from the provider and says whether it counts against it
	ProviderFailed(ctx context.Context, name string, err error, creating bool) bool
	// CreateRetrying creates a sandbox, retrying an outcome that is unknown under the same key
	CreateRetrying(
		ctx context.Context, prov provider.Provider, req provider.CreateRequest,
	) (provider.Ref, error)
	// BootPool waits for the sandbox, puts the runner in place and runs setup's pool phase and
	// harden
	BootPool(
		ctx context.Context, prov provider.Provider, ref provider.Ref,
	) (provider.Ref, Booted, error)
	// DestroyRef destroys a sandbox, one already gone counting as destroyed
	DestroyRef(ctx context.Context, prov provider.Provider, ref provider.Ref) error
	// RecordSample stores a start sample, its outcome taken from err
	RecordSample(
		ctx context.Context, prov provider.Provider, size provider.Size, m store.StartSample,
		err error,
	)
	// OwnedLabels selects every sandbox of this deployment
	OwnedLabels() map[string]string
}

// Deps are what the manager works with
type Deps struct {
	Store    *store.Store
	Registry *provider.Registry
	Saga     Saga
	// River inserts the fill and retire jobs, and may be insert only
	River  *river.Client[pgx.Tx]
	Valkey valkey.Client
	Prefix string
	// Settings are the [pools] settings with the pool tables
	Settings config.Pools
	// Replica names this process in the samples it writes
	Replica string
	Now     func() time.Time
	Log     *slog.Logger
	// ImageAccept is images.accept, a warm member whose image version left it is retired
	ImageAccept []string
}

// Manager sizes, fills, inspects and retires pools, router-worker runs it
type Manager struct {
	d    Deps
	cfgs []Config
	by   map[Key]Config

	mu        sync.Mutex
	inspected map[uuid.UUID]time.Time
}

// Configs turns the pool tables of the settings into pool configs
func Configs(s config.Pools) []Config {
	out := make([]Config, 0, len(s.Pool))
	for _, p := range s.Pool {
		out = append(out, Config{
			Key: Key{Provider: p.Provider, Size: p.Size}, Min: p.Min, Max: p.Max,
			MaxAge: p.MaxAge,
		})
	}
	return out
}

// New checks every pool against the registry and builds the manager
//
// A pool on a provider that is not listed, that neither parks nor claims, that does not sell the
// size, or that is Boat, is refused, so a bad pool fails the start and never a tick.
func New(d Deps) (*Manager, error) {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Log == nil {
		d.Log = slog.Default()
	}
	m := &Manager{d: d, by: map[Key]Config{}, inspected: map[uuid.UUID]time.Time{}}
	if err := Validate(d.Registry, d.Settings); err != nil {
		return nil, err
	}
	for _, c := range Configs(d.Settings) {
		m.cfgs = append(m.cfgs, c)
		m.by[c.Key] = c
	}
	return m, nil
}

// Validate refuses every pool the registry cannot serve, which fails a router's start
func Validate(reg *provider.Registry, s config.Pools) error {
	for _, c := range Configs(s) {
		if err := check(reg, c); err != nil {
			return err
		}
	}
	return nil
}

// check refuses one pool the registry cannot serve
func check(reg *provider.Registry, c Config) error {
	if c.Provider == "boat" {
		return fmt.Errorf("pools: boat cannot keep a pool, every create counts as a start")
	}
	prov, ok := reg.Get(c.Provider)
	if !ok {
		return fmt.Errorf("pools: %s is not in providers", c.Provider)
	}
	_, parks := prov.(provider.Parker)
	_, claims := prov.(provider.PoolClaimer)
	if !parks && !claims {
		return fmt.Errorf("pools: %s keeps no pools, it neither parks sandboxes nor claims them",
			c.Provider)
	}
	if _, ok := sizeOf(prov, c.Size); !ok {
		return fmt.Errorf("pools: %s does not sell the size %q", c.Provider, c.Size)
	}
	return nil
}

// sizeOf finds the size of a provider by name
func sizeOf(prov provider.Provider, name string) (provider.Size, bool) {
	sz, ok := prov.(provider.Sizer)
	if !ok {
		return provider.Size{}, false
	}
	for _, s := range sz.Sizes() {
		if s.Name == name {
			return s, true
		}
	}
	return provider.Size{}, false
}

func (m *Manager) now() time.Time { return m.d.Now().UTC() }

// MaxAge is the longest a warm member of a configured pool lives
func (m *Manager) MaxAge(k Key) (time.Duration, bool) {
	c, ok := m.by[k]
	return c.MaxAge, ok
}

// Pools lists the configured pools
func (m *Manager) Pools() []Config { return slices.Clone(m.cfgs) }

/* Tick */

// Tick sizes every pool and inserts its fills and retirements
//
// A pool whose step fails is logged and the others go on, the error of each coming back joined.
func (m *Manager) Tick(ctx context.Context) error {
	now := m.now()
	capped, cost, err := m.spendCapped(ctx, now)
	if err != nil {
		return fmt.Errorf("pool spend: %w", err)
	}
	if capped {
		m.d.Log.Warn("pool waste and idle cost reached the daily cap, targets drop to min",
			"micros", cost, "cap", m.d.Settings.MaxDailyMicros)
		m.setSpend(ctx)
	}
	pauses, err := m.d.Store.Pauses(ctx)
	if err != nil {
		return err
	}
	live, err := m.d.Store.LiveMembers(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, c := range m.cfgs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var members []store.Member
		for _, mem := range live {
			if mem.Provider == c.Provider && mem.SizeName == c.Size {
				members = append(members, mem)
			}
		}
		pause := pauseOf(pauses, c.Key)
		if err := m.tickPool(ctx, c, members, pause, capped, now); err != nil {
			m.d.Log.Error("pool tick", "provider", c.Provider, "size", c.Size, "err", err)
			errs = append(errs, err)
		}
	}
	keep := make([][2]string, len(m.cfgs))
	for i, c := range m.cfgs {
		keep[i] = [2]string{c.Provider, c.Size}
	}
	if err := m.d.Store.DeleteTargetsExcept(ctx, keep); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// pauseOf is the pause row of a pool, the zero Pause for none
func pauseOf(pauses []store.Pause, k Key) store.Pause {
	for _, p := range pauses {
		if p.Provider == k.Provider && p.SizeName == k.Size {
			return p
		}
	}
	return store.Pause{}
}

func (m *Manager) tickPool(
	ctx context.Context, c Config, members []store.Member, pause store.Pause, capped bool,
	now time.Time,
) error {
	target, err := m.size(ctx, c, capped, now)
	if err != nil {
		return err
	}
	if err := m.d.Store.PutTarget(ctx, store.PoolTarget{
		Provider: c.Provider, SizeName: c.Size, Target: target, ComputedAt: now,
	}); err != nil {
		return err
	}
	retired := map[uuid.UUID]bool{}
	for _, mem := range members {
		if reason := m.staleReason(c, mem, now); reason != "" {
			m.retire(ctx, mem, reason, "")
			retired[mem.ID] = true
		}
	}
	// A retire job that was never inserted, as after a drain cut short, is inserted again
	for _, mem := range members {
		if mem.State == store.MemberRetiring {
			m.queueRetire(ctx, mem.ID, mem.EndReason)
		}
	}
	m.inspect(ctx, members, retired, now)
	have := 0
	for _, mem := range members {
		if !retired[mem.ID] && (mem.State == store.MemberFilling || mem.State == store.MemberWarm) {
			have++
		}
	}
	if pause.Until.After(now) || !m.d.Settings.Enabled {
		return nil
	}
	// A provider images.accept keeps out of placements gets no new members
	if prov, ok := m.d.Registry.Get(c.Provider); ok &&
		!m.d.Registry.ImageAccepted(prov, m.d.ImageAccept) {
		return nil
	}
	return m.fill(ctx, c, target-have)
}

// size is the pool's target, claims a second times the refill p95 plus headroom, its min while the
// spend cap holds
func (m *Manager) size(ctx context.Context, c Config, capped bool, now time.Time) (int, error) {
	if capped {
		return c.Min, nil
	}
	window := m.d.Settings.RateWindow
	d, err := m.d.Store.DemandSince(ctx, c.Provider, c.Size, now.Add(-window))
	if err != nil {
		return 0, err
	}
	p95, n, err := m.d.Store.RefillP95(ctx, c.Provider, c.Size, now.Add(-refillWindow))
	if err != nil {
		return 0, err
	}
	if n < minRefillSamples {
		p95 = defaultRefill
	}
	rate := float64(d.Claims+d.Misses) / window.Seconds()
	return Target(c, rate, p95, m.d.Settings.Headroom), nil
}

// staleReason says why a member must go, empty for a member that stays
func (m *Manager) staleReason(c Config, mem store.Member, now time.Time) string {
	switch mem.State {
	case store.MemberWarm:
		born := mem.CreatedAt
		if mem.SandboxCreatedAt != nil {
			born = *mem.SandboxCreatedAt
		}
		if now.Sub(born) > c.MaxAge {
			return store.EndAged
		}
		// A member of a base image that images.accept dropped is never claimed
		if mem.Image != nil && len(m.d.ImageAccept) > 0 &&
			!slices.Contains(m.d.ImageAccept, mem.Image.Version) {
			return store.EndImage
		}
	case store.MemberFilling:
		if now.Sub(mem.CreatedAt) >= fillStuckAfter {
			return store.EndFailed
		}
	}
	return ""
}

// fill starts up to max_fills_per_tick members of a pool that is short by deficit
func (m *Manager) fill(ctx context.Context, c Config, deficit int) error {
	if deficit <= 0 {
		return nil
	}
	if block, err := store.ProviderBlock(ctx, m.d.Store.Querier(), c.Provider); err != nil ||
		block != "" {
		return err
	}
	if m.d.Saga.CreatesPaused(ctx, c.Provider) {
		return nil
	}
	for range min(deficit, m.d.Settings.MaxFillsPerTick) {
		_, err := m.d.Store.StartFill(ctx, c.Provider, c.Size, m.d.Settings.CapacityShare,
			func(ctx context.Context, tx pgx.Tx, mem store.Member) error {
				_, err := m.d.River.InsertTx(ctx, tx, jobargs.PoolFill{Member: mem.ID}, nil)
				return err
			})
		if errors.Is(err, store.ErrPoolFull) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// inspect reads the warm member of a pool it inspected longest ago, at most every inspect_every
// for one member, and retires one the provider no longer has
//
// An error proves nothing and keeps the member. A parked member is expected to read stopped.
func (m *Manager) inspect(
	ctx context.Context, members []store.Member, retired map[uuid.UUID]bool, now time.Time,
) {
	var pick *store.Member
	m.mu.Lock()
	for i := range members {
		mem := &members[i]
		if mem.State != store.MemberWarm || retired[mem.ID] || mem.ProviderRef == nil {
			continue
		}
		at := m.inspected[mem.ID]
		if now.Sub(at) < m.d.Settings.InspectEvery {
			continue
		}
		if pick == nil || at.Before(m.inspected[pick.ID]) {
			pick = mem
		}
	}
	if pick != nil {
		m.inspected[pick.ID] = now
	}
	live := map[uuid.UUID]bool{}
	for _, mem := range members {
		live[mem.ID] = true
	}
	for id := range m.inspected {
		if !live[id] {
			delete(m.inspected, id)
		}
	}
	m.mu.Unlock()
	if pick == nil {
		return
	}
	prov, ok := m.d.Registry.Get(pick.Provider)
	if !ok {
		return
	}
	ictx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := prov.Inspect(ictx, *pick.ProviderRef)
	if err != nil {
		m.d.Log.Warn("pool member inspection failed", "member", pick.ID, "err", err)
		return
	}
	_, parks := prov.(provider.Parker)
	switch {
	case st.State == provider.StateGone, st.State == provider.StateFailed,
		st.State == provider.StateStopped && !parks && st.Reason != "":
		m.retire(ctx, *pick, store.EndGone, "inspected as "+string(st.State))
	}
}

// retire sets a member retiring and queues the job that destroys its sandbox
func (m *Manager) retire(ctx context.Context, mem store.Member, reason, detail string) {
	old, err := m.d.Store.RetireMember(ctx, mem.ID, reason, detail)
	if err != nil {
		m.d.Log.Error("retire pool member", "member", mem.ID, "err", err)
		return
	}
	if old == "" {
		return
	}
	m.queueRetire(ctx, mem.ID, reason)
}

// queueRetire inserts the retire job of a member, which stays one job while one waits or runs
func (m *Manager) queueRetire(ctx context.Context, id uuid.UUID, reason string) {
	if reason == "" {
		reason = store.EndGone
	}
	args := jobargs.PoolRetire{Member: id, Reason: reason}
	if _, err := m.d.River.Insert(ctx, args, nil); err != nil {
		m.d.Log.Error("insert pool_retire", "member", id, "err", err)
	}
}

// setSpend keeps Reserve off for two ticks, which the next tick renews while the cap holds
func (m *Manager) setSpend(ctx context.Context) {
	if m.d.Valkey == nil {
		return
	}
	ttl := 2 * m.d.Settings.Tick
	cmd := m.d.Valkey.B().Set().Key(m.d.Prefix + SpendKey).Value("1").
		PxMilliseconds(ttl.Milliseconds()).Build()
	if err := m.d.Valkey.Do(ctx, cmd).Error(); err != nil {
		m.d.Log.Warn("cannot set the pool spend flag", "err", err)
	}
}

/* Fill */

// labels are the labels every member carries, never a placement label
func (m *Manager) labels(mem store.Member) map[string]string {
	l := m.d.Saga.OwnedLabels()
	l["bugnet_pool"] = mem.SizeName
	l["pool_member"] = mem.ID.String()
	return l
}

// Fill creates, boots and parks one member, safe to run again for the same member
//
// It answers ErrSnooze when the provider has no create token yet, and nil for a member that is
// no longer filling. A failure retires the member and may pause the pool, and is never an error,
// since the next tick fills again.
func (m *Manager) Fill(ctx context.Context, id uuid.UUID) error {
	mem, err := m.d.Store.MemberByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if mem.State != store.MemberFilling {
		return nil
	}
	c, ok := m.by[Key{mem.Provider, mem.SizeName}]
	prov, pok := m.d.Registry.Get(mem.Provider)
	if !ok || !pok {
		m.failFill(ctx, mem, nil, provider.Size{}, errors.New("the pool is no longer configured"))
		return nil
	}
	size, _ := sizeOf(prov, mem.SizeName)
	if !m.d.Saga.TakeCreate(ctx, mem.Provider) || m.d.Saga.CreatesPaused(ctx, mem.Provider) {
		return ErrSnooze
	}
	started := m.now()
	smp := store.StartSample{
		Kind: store.SamplePool, Provider: mem.Provider, SizeName: mem.SizeName, StartedAt: started,
		Replica: m.d.Replica, StartPath: store.PathPool, MemberID: mem.ID,
		CreateMS: store.NoStep, ReadyMS: store.NoStep, RunnerMS: store.NoStep,
		GateMS: store.NoStep, FirstCmdMS: store.NoStep, WarmCmdMS: store.NoStep,
		DestroyMS: store.NoStep, WakeMS: store.NoStep,
	}
	ref, err := m.d.Saga.CreateRetrying(ctx, prov, provider.CreateRequest{
		IdempotencyKey: mem.IdempotencyKey, Size: size, Labels: m.labels(mem),
		Lifetime: c.MaxAge + ageSlack,
	})
	smp.CreateMS = int(m.now().Sub(started).Milliseconds())
	smp.SandboxID = ref.ID
	if err != nil {
		m.failFill(ctx, mem, &smp, size, err)
		return nil
	}
	if err := m.d.Store.MarkMemberCreated(ctx, mem.ID, ref, m.now()); err != nil {
		m.failFillAfter(ctx, mem, &smp, size, ref, err)
		return nil
	}
	ref, boot, err := m.d.Saga.BootPool(ctx, prov, ref)
	smp.RunnerMS = boot.RunnerMS
	if boot.Image != nil {
		smp.ImageVersion = boot.Image.Version
	}
	smp.Reseed = boot.Reseed
	if err != nil {
		m.failFillAfter(ctx, mem, &smp, size, ref, err)
		return nil
	}
	if pk, ok := prov.(provider.Parker); ok {
		if err := pk.Park(ctx, ref); err != nil {
			m.failFillAfter(ctx, mem, &smp, size, ref, fmt.Errorf("park: %w", err))
			return nil
		}
	}
	if err := m.d.Store.MarkMemberWarm(ctx, mem.ID, ref, "", boot.Image, m.now()); err != nil {
		m.failFillAfter(ctx, mem, &smp, size, ref, err)
		return nil
	}
	smp.ReadyMS = int(m.now().Sub(started).Milliseconds())
	m.d.Saga.RecordSample(ctx, prov, size, smp, nil)
	return nil
}

// failFillAfter fails a fill whose sandbox exists, which pool_retire destroys through the ref the
// member row holds, or the label when the row never got it
func (m *Manager) failFillAfter(
	ctx context.Context, mem store.Member, smp *store.StartSample, size provider.Size,
	ref provider.Ref, err error,
) {
	smp.SandboxID = ref.ID
	m.failFill(ctx, mem, smp, size, err)
}

// failFill records a fill that did not reach warm, retires its member and pauses a failing pool
func (m *Manager) failFill(
	ctx context.Context, mem store.Member, smp *store.StartSample, size provider.Size, err error,
) {
	ctx = context.WithoutCancel(ctx)
	m.d.Log.Warn("pool fill failed", "member", mem.ID, "provider", mem.Provider,
		"size", mem.SizeName, "err", err)
	if prov, ok := m.d.Registry.Get(mem.Provider); ok {
		// The call acts on the error, a rate limit pausing creates and a block queueing a check
		m.d.Saga.ProviderFailed(ctx, mem.Provider, err, true)
		if smp != nil {
			m.d.Saga.RecordSample(ctx, prov, size, *smp, err)
		}
	}
	m.retire(ctx, mem, store.EndFailed, err.Error())
	pauses, perr := m.d.Store.Pauses(ctx)
	if perr != nil {
		return
	}
	since := pauseOf(pauses, Key{mem.Provider, mem.SizeName}).Until
	n, perr := m.d.Store.RecentFillFailures(ctx, mem.Provider, mem.SizeName, since, failedFills)
	if perr != nil || n < failedFills {
		return
	}
	until := m.now().Add(m.d.Settings.FailPause)
	if err := m.d.Store.PutPause(ctx, store.Pause{
		Provider: mem.Provider, SizeName: mem.SizeName, Until: until, Reason: store.PauseFailing,
	}); err != nil {
		m.d.Log.Error("pause a failing pool", "provider", mem.Provider, "err", err)
		return
	}
	m.d.Log.Warn("pool paused after failed fills", "provider", mem.Provider, "size", mem.SizeName,
		"until", until)
}

/* Retire */

// Retire destroys one member and frees its slot, a sandbox already gone counting as destroyed
func (m *Manager) Retire(ctx context.Context, id uuid.UUID, reason string) error {
	mem, err := m.d.Store.MemberByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if mem.State == store.MemberGone {
		return nil
	}
	if _, err := m.d.Store.RetireMember(ctx, id, reason, ""); err != nil {
		return err
	}
	prov, ok := m.d.Registry.Get(mem.Provider)
	if !ok {
		return fmt.Errorf("pool member %s is on %s, which is not configured", id, mem.Provider)
	}
	if mem.ProviderRef != nil {
		if err := m.d.Saga.DestroyRef(ctx, prov, *mem.ProviderRef); err != nil {
			return fmt.Errorf("destroy pool member %s: %w", id, err)
		}
	}
	// A create whose reply was lost, or one that landed late, is found by its label
	owned, err := prov.ListOwned(ctx, map[string]string{"pool_member": id.String()})
	if err != nil {
		return fmt.Errorf("list the sandboxes of pool member %s: %w", id, err)
	}
	for _, o := range owned {
		if err := m.d.Saga.DestroyRef(ctx, prov, o.Ref); err != nil {
			return fmt.Errorf("destroy pool member %s: %w", id, err)
		}
	}
	return m.d.Store.MemberGone(ctx, id, reason, m.now())
}

// Drain retires every warm member of k and keeps refill off for pause
func (m *Manager) Drain(
	ctx context.Context, k Key, pause time.Duration, actor string,
) (int, error) {
	if _, ok := m.by[k]; !ok {
		return 0, ErrUnknownPool
	}
	if pause > 0 {
		err := m.d.Store.PutPause(ctx, store.Pause{
			Provider: k.Provider, SizeName: k.Size, Until: m.now().Add(pause),
			Reason: store.PauseDrained, Actor: actor,
		})
		if err != nil {
			return 0, err
		}
	}
	ids, err := m.d.Store.RetireWarm(ctx, k.Provider, k.Size, store.EndDrained)
	if err != nil {
		return 0, err
	}
	for i, id := range ids {
		_, err := m.d.River.Insert(ctx, jobargs.PoolRetire{Member: id, Reason: store.EndDrained},
			nil)
		if err != nil {
			// The members left stay retiring, and the next tick queues their jobs
			return i, fmt.Errorf("insert pool_retire: %w", err)
		}
	}
	return len(ids), nil
}
