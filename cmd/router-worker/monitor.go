package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/vkinfo"
)

const (
	// overdueAfter gives the deadline job and several watches the time to end a placement
	overdueAfter = 3 * time.Minute
	// memoryAlertPercent is the share of Valkey's maxmemory in use that /monitor reports
	memoryAlertPercent = 80
	monitorTimeout     = 2 * time.Second
)

// health is what one /monitor request found, memory nil when INFO could not be read
type health struct {
	postgresDown, valkeyDown bool
	overdue, stuck           int
	memory                   *vkinfo.Memory
}

// monitor answers ok when nothing is wrong, else 503 with one line per problem and counts only
func monitor(
	log *slog.Logger, st *store.Store, vk valkey.Client, read vkinfo.Reader,
) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), monitorTimeout)
		defer cancel()
		var h health
		// Both run at once so a hung Postgres cannot spend Valkey's share of the timeout
		var wg sync.WaitGroup
		wg.Go(func() { h.overdue, h.stuck, h.postgresDown = checkPostgres(ctx, log, st) })
		wg.Go(func() { h.memory, h.valkeyDown = checkValkey(ctx, log, vk, read) })
		wg.Wait()
		problems := h.problems()
		if len(problems) == 0 {
			_, _ = io.WriteString(rw, "ok\n")
			return
		}
		http.Error(rw, strings.Join(problems, "\n"), http.StatusServiceUnavailable)
	}
}

// checkPostgres counts overdue placements and stuck events, down when either query fails
func checkPostgres(ctx context.Context, log *slog.Logger, st *store.Store) (int, int, bool) {
	overdue, err := st.PastDeadlineCount(ctx, time.Now().Add(-overdueAfter))
	if err != nil {
		log.Warn("monitor cannot query postgres", "err", err)
		return 0, 0, true
	}
	stuck, err := st.StuckOutboxCount(ctx)
	if err != nil {
		log.Warn("monitor cannot query postgres", "err", err)
		return 0, 0, true
	}
	return overdue, stuck, false
}

// checkValkey pings Valkey and then reads its memory, which an INFO failure leaves unknown
func checkValkey(
	ctx context.Context, log *slog.Logger, vk valkey.Client, read vkinfo.Reader,
) (*vkinfo.Memory, bool) {
	if err := vk.Do(ctx, vk.B().Ping().Build()).Error(); err != nil {
		log.Warn("monitor cannot reach valkey", "err", err)
		return nil, true
	}
	m, err := read(ctx, vk)
	if err != nil {
		log.Warn("monitor cannot read valkey memory", "err", err)
		return nil, false
	}
	return &m, false
}

func (h health) problems() []string {
	var out []string
	if h.postgresDown {
		out = append(out, "postgres is unreachable")
	}
	if h.valkeyDown {
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
	case h.stuck == 1:
		out = append(out, "1 outbox event is stuck")
	case h.stuck > 1:
		out = append(out, fmt.Sprintf("%d outbox events are stuck", h.stuck))
	}
	if m := h.memory; m != nil {
		if m.Max > 0 && m.Used*100 >= memoryAlertPercent*m.Max {
			percent := m.Used * 100 / m.Max
			out = append(out, fmt.Sprintf("valkey memory is at %d%% of maxmemory", percent))
		}
		if m.Policy != vkinfo.NoEviction {
			out = append(out, fmt.Sprintf(
				"valkey maxmemory-policy is %s, the router needs noeviction", m.Policy))
		}
	}
	return out
}
