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
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/vkinfo"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
)

const (
	// overdueAfter gives the deadline job and several watches the time to end a placement
	overdueAfter = 3 * time.Minute
	// memoryAlertPercent is the share of Valkey's maxmemory in use that /monitor reports
	memoryAlertPercent = 80
	monitorTimeout     = 2 * time.Second
)

// health is what one /monitor request found, memory holding only what INFO gave
type health struct {
	postgresDown           bool
	overdue, ending, stuck int
	valkey                 valkeyHealth
	pools                  poolHealth
}

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
	poolList string,
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
	switch h.pools {
	case poolsEmpty:
		out = append(out, "the mining pool list is empty or missing")
	case poolsUnreadable:
		out = append(out, "the mining pool list cannot be read")
	}
	return out
}
