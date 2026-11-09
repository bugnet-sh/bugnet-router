package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/vkinfo"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
)

const (
	// overdueAfter gives the deadline job and several watches the time to end a placement
	overdueAfter = 3 * time.Minute
	// memoryAlertPercent is the share of Valkey's maxmemory in use that /monitor reports
	memoryAlertPercent = 80
	// sessionAlertPercent is the share of sessions.disk_budget in use that /monitor reports
	sessionAlertPercent = 80
	monitorTimeout      = 2 * time.Second
	// snapshotStaleAfter and paymentsStaleAfter are CP 20.9's alert ages of the stats row
	snapshotStaleAfter = 2 * time.Minute
	paymentsStaleAfter = 5 * time.Minute
)

// health is what one /monitor request found, memory holding only what INFO gave
type health struct {
	postgresDown           bool
	overdue, ending, stuck int
	valkey                 valkeyHealth
	pools                  poolHealth
	feed                   feedHealth
	// noSource counts ready placements with secrets and no usable egress address, by provider
	noSource []sourceGap
	// sessions is the session tables' share of their disk budget, -1 when unread
	sessions int64
	limits   limitsHealth
	canaries canaryHealth
}

// sourceGap is the ready placements of one provider with secrets and no stored egress source
type sourceGap struct {
	provider string
	n        int
}

// feedCheck says whether the stats snapshot job runs, and whether it lacks the Payments token
type feedCheck struct {
	on, noToken bool
}

// feedHealth is how old the stats snapshot's two parts were, zero when there is no row to judge
type feedHealth struct {
	check                   feedCheck
	routerAge, paymentsAge  time.Duration
	present, noPaymentsPart bool
}

// diskPercent reads the session tables' share of their disk budget
type diskPercent func(ctx context.Context) (int64, error)

// poolHealth is what the mining pool list held when /monitor read it
type poolHealth int

const (
	poolsListed poolHealth = iota
	poolsEmpty
	poolsUnreadable
)

// monitor answers ok when nothing is wrong, else 503 with one line per problem and counts only
func monitor(
	log *slog.Logger, st *store.Store, vk valkey.Client, prefix string, read vkinfo.Reader,
	poolList string, feed feedCheck, disk diskPercent, limits limitsCheck, sourced []string,
	canaries canaryCheck,
) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), monitorTimeout)
		defer cancel()
		var h health
		// Both run at once so a hung Postgres cannot spend Valkey's share of the timeout
		var wg sync.WaitGroup
		wg.Go(func() { h.overdue, h.ending, h.stuck, h.postgresDown = checkPostgres(ctx, log, st) })
		wg.Go(func() { h.valkey = checkValkey(ctx, log, vk, prefix, read) })
		wg.Go(func() { h.pools = checkPools(log, poolList) })
		wg.Go(func() { h.feed = checkFeed(ctx, log, st, feed) })
		wg.Go(func() { h.sessions = checkSessions(ctx, log, disk) })
		wg.Go(func() { h.limits = checkLimits(ctx, log, st, limits) })
		wg.Go(func() { h.noSource = checkSources(ctx, log, st, sourced) })
		wg.Go(func() { h.canaries = checkCanaries(ctx, log, st, canaries) })
		wg.Wait()
		problems := h.problems()
		if len(problems) == 0 {
			_, _ = io.WriteString(rw, "ok\n")
			return
		}
		http.Error(rw, strings.Join(problems, "\n"), http.StatusServiceUnavailable)
	}
}

// checkPostgres counts placements late to end and stuck events, down when a query fails
func checkPostgres(
	ctx context.Context, log *slog.Logger, st *store.Store,
) (overdue, ending, stuck int, down bool) {
	cutoff := time.Now().Add(-overdueAfter)
	overdue, err := st.PastDeadlineCount(ctx, cutoff)
	if err == nil {
		// A stop keeps the deadline, so only this count sees a stop whose destroy keeps failing
		ending, err = st.LongTerminatingCount(ctx, cutoff)
	}
	if err == nil {
		stuck, err = st.StuckOutboxCount(ctx)
	}
	if err != nil {
		log.Warn("monitor cannot query postgres", "err", err)
		return 0, 0, 0, true
	}
	return overdue, ending, stuck, false
}

// checkSources counts, for each provider with a source check, the placements it cannot check
func checkSources(
	ctx context.Context, log *slog.Logger, st *store.Store, providers []string,
) []sourceGap {
	var out []sourceGap
	for _, name := range providers {
		n, err := st.SecretsWithoutSourceCount(ctx, name)
		if err != nil {
			log.Warn("monitor cannot count placements without an egress source", "err", err)
			continue
		}
		if n > 0 {
			out = append(out, sourceGap{provider: name, n: n})
		}
	}
	return out
}

// checkFeed reads the stats snapshot row's ages while the snapshot job runs
func checkFeed(ctx context.Context, log *slog.Logger, st *store.Store, c feedCheck) feedHealth {
	h := feedHealth{check: c}
	if !c.on {
		return h
	}
	snap, err := st.ReadStatsSnapshot(ctx)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return h
	case err != nil:
		log.Warn("monitor cannot read the stats snapshot", "err", err)
		return h
	}
	now := time.Now()
	h.present = true
	h.routerAge = now.Sub(snap.RouterAt)
	if snap.PaymentsAt.IsZero() {
		h.noPaymentsPart = true
	} else {
		h.paymentsAge = now.Sub(snap.PaymentsAt)
	}
	return h
}

// checkSessions reads the session tables' share of their budget, -1 when it cannot
func checkSessions(ctx context.Context, log *slog.Logger, disk diskPercent) int64 {
	if disk == nil {
		return -1
	}
	n, err := disk(ctx)
	if err != nil {
		log.Warn("monitor cannot read the session tables' size", "err", err)
		return -1
	}
	return n
}

// checkPools reads the mining pool list with the loader router-api starts with
func checkPools(log *slog.Logger, path string) poolHealth {
	pools, err := config.LoadPoolList(path)
	switch {
	case err != nil:
		log.Warn("monitor cannot read the mining pool list", "err", err)
		return poolsUnreadable
	case len(pools) == 0:
		return poolsEmpty
	}
	return poolsListed
}

// valkeyHealth is what /monitor learned from Valkey
type valkeyHealth struct {
	memory                  vkinfo.Memory
	down, unknown, refusing bool
	// unguarded says a payer claim counted as absent within the hour for want of a guard
	unguarded bool
}

// checkValkey pings Valkey and reads its memory, a refused INFO told apart from one that failed
func checkValkey(
	ctx context.Context, log *slog.Logger, vk valkey.Client, prefix string, read vkinfo.Reader,
) valkeyHealth {
	if err := vk.Do(ctx, vk.B().Ping().Build()).Error(); err != nil {
		log.Warn("monitor cannot reach valkey", "err", err)
		return valkeyHealth{down: true}
	}
	n, err := vk.Do(ctx, vk.B().Exists().Key(prefix+wallet.UnguardedKey).Build()).AsInt64()
	if err != nil {
		log.Warn("monitor cannot read the payer guard mark", "err", err)
	}
	m, err := read(ctx, vk)
	if err != nil {
		log.Warn("monitor cannot read valkey memory in full", "err", err)
	}
	var reply *valkey.ValkeyError
	refused := errors.As(err, &reply) && !reply.IsNil()
	return valkeyHealth{
		memory:    m,
		unguarded: n > 0,
		// A managed Valkey may hide a field for good, which must not keep /monitor failing
		unknown:  err != nil && !refused && !errors.Is(err, vkinfo.ErrIncomplete),
		refusing: refused,
	}
}

func (h health) problems() []string {
	var out []string
	if h.postgresDown {
		out = append(out, "postgres is unreachable")
	}
	if h.valkey.down {
		out = append(out, "valkey is unreachable")
	}
	minutes := int(overdueAfter / time.Minute)
	switch {
	case h.overdue == 1:
		out = append(out, fmt.Sprintf("1 placement is over %d minutes past its deadline", minutes))
	case h.overdue > 1:
		out = append(out, fmt.Sprintf("%d placements are over %d minutes past their deadline",
			h.overdue, minutes))
	}
	switch {
	case h.ending == 1:
		out = append(out, fmt.Sprintf("1 placement has been terminating for over %d minutes",
			minutes))
	case h.ending > 1:
		out = append(out, fmt.Sprintf("%d placements have been terminating for over %d minutes",
			h.ending, minutes))
	}
	switch {
	case h.stuck == 1:
		out = append(out, "1 outbox event is stuck")
	case h.stuck > 1:
		out = append(out, fmt.Sprintf("%d outbox events are stuck", h.stuck))
	}
	switch {
	case h.valkey.refusing:
		out = append(out, "valkey refuses INFO, so its memory and policy go unchecked")
	case h.valkey.unknown:
		out = append(out, "valkey memory is unknown")
	}
	if m := h.valkey.memory; m.Max > 0 && m.Used*100 >= memoryAlertPercent*m.Max {
		percent := m.Used * 100 / m.Max
		out = append(out, fmt.Sprintf("valkey memory is at %d%% of maxmemory", percent))
	}
	if p := h.valkey.memory.Policy; p != "" && p != vkinfo.NoEviction {
		out = append(out, fmt.Sprintf(
			"valkey maxmemory-policy is %s, the router needs noeviction", p))
	}
	if h.valkey.unguarded {
		out = append(out, "the payer claim is treated as absent, since no fee_payer came and "+
			"wallets.facilitator_addresses is empty")
	}
	if h.sessions >= sessionAlertPercent {
		out = append(out, fmt.Sprintf("session records at %d%% of their disk budget",
			h.sessions))
	}
	for _, g := range h.noSource {
		out = append(out, fmt.Sprintf(
			"egress sources: %d ready %s placements with secrets and no usable address",
			g.n, g.provider))
	}
	switch h.pools {
	case poolsEmpty:
		out = append(out, "the mining pool list is empty or missing")
	case poolsUnreadable:
		out = append(out, "the mining pool list cannot be read")
	}
	out = append(out, h.limits.problems()...)
	out = append(out, h.canaries.problems()...)
	return append(out, h.feed.problems()...)
}

// problems are CP 20.9's two lines, the Payments one naming an empty token as its cause
func (f feedHealth) problems() []string {
	if !f.present {
		return nil
	}
	var out []string
	if f.routerAge > snapshotStaleAfter {
		out = append(out, fmt.Sprintf("stats snapshot %d s old", int(f.routerAge.Seconds())))
	}
	why := ""
	if f.check.noToken {
		why = ", BUGNET_PAYMENTS_ROUTER_TOKEN is empty in router-worker"
	}
	switch {
	case f.noPaymentsPart:
		out = append(out, "stats: no Payments part yet, so /api/stats answers 503"+why)
	case f.paymentsAge > paymentsStaleAfter:
		out = append(out, fmt.Sprintf("stats: Payments part %d min old%s",
			int(f.paymentsAge.Minutes()), why))
	}
	return out
}

// rateReader reads Boat's RateLimit figures from the last shared /limits reply
type rateReader func(ctx context.Context) (boat.RateLeft, bool, error)

// limitsCheck says what /monitor reads of the limit polls and the account checks
type limitsCheck struct {
	// every is the poll interval of each provider that polls limits
	every map[string]time.Duration
	// staleAfter is the missed polls before old limits are reported
	staleAfter int
	// rate reads Boat's RateLimit header, nil when Boat is not a provider
	rate rateReader
}

// rateAlertPercent is the share of Boat's request window left under which /monitor reports it
const rateAlertPercent = 20

// limitsHealth is what the capacity rows and provider health rows showed
type limitsHealth struct {
	// stale names the polled providers whose limits are old, with their age in seconds
	stale []staleLimits
	// blocked names the providers whose account is blocked, with the block
	blocked []blockedProvider
	// rate is Boat's request window when it is under rateAlertPercent
	rate    boat.RateLeft
	rateLow bool
}

type staleLimits struct {
	provider string
	age      time.Duration
}

type blockedProvider struct{ provider, block string }

// checkLimits finds old limits, blocked accounts and a Boat request window nearly spent
func checkLimits(
	ctx context.Context, log *slog.Logger, st *store.Store, c limitsCheck,
) limitsHealth {
	var h limitsHealth
	now := time.Now()
	rows, err := st.Capacities(ctx)
	if err != nil {
		log.Warn("monitor cannot read the capacity rows", "err", err)
	}
	for _, r := range rows {
		every, ok := c.every[r.Provider]
		if !ok || r.PolledAt.IsZero() {
			continue
		}
		if age := now.Sub(r.PolledAt); age > every*time.Duration(c.staleAfter) {
			h.stale = append(h.stale, staleLimits{r.Provider, age})
		}
	}
	healths, err := st.ProviderHealths(ctx)
	if err != nil {
		log.Warn("monitor cannot read the provider health rows", "err", err)
	}
	for _, r := range healths {
		if r.Blocked != "" {
			h.blocked = append(h.blocked, blockedProvider{r.Provider, r.Blocked})
		}
	}
	if c.rate == nil {
		return h
	}
	left, ok, err := c.rate(ctx)
	if err != nil {
		log.Warn("monitor cannot read Boat's request window", "err", err)
	}
	if ok && left.Remaining*100 < rateAlertPercent*left.Limit {
		h.rate, h.rateLow = left, true
	}
	return h
}

func (l limitsHealth) problems() []string {
	var out []string
	for _, b := range l.blocked {
		out = append(out, fmt.Sprintf("provider %s is blocked: %s", b.provider, b.block))
	}
	for _, s := range l.stale {
		out = append(out, fmt.Sprintf("provider %s limits not polled for %d s", s.provider,
			int(s.age.Seconds())))
	}
	if l.rateLow {
		out = append(out, fmt.Sprintf("provider boat has %d of %d requests left in its window",
			l.rate.Remaining, l.rate.Limit))
	}
	return out
}
