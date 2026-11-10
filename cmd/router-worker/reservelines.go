package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// targetWindow is how far back the 5 s target's p90 reads
const targetWindow = time.Hour

// reserveLines reports CP 18.9.3's target line and CP 18.18's brake line
//
// The target line shows when the hour's reserved and pooled placements number at least
// targets.min_placements and their p90 of payment to first command is over targets.first_command_p90.
// The brake line shows while reserve:brake is set, with the counts that set it.
func reserveLines(
	log *slog.Logger, st *store.Store, vk valkey.Client, prefix string, t config.Targets,
) problemSource {
	return func(ctx context.Context) []string {
		var out []string
		n, p90, err := st.FirstCommandP90(ctx, time.Now().Add(-targetWindow))
		switch {
		case err != nil:
			log.Warn("monitor cannot read the first command times", "err", err)
		case n >= t.MinPlacements && p90 > t.FirstCommandP90:
			out = append(out, fmt.Sprintf(
				"payment to first command p90 is %.1f s over the last hour, target %.0f s",
				p90.Seconds(), t.FirstCommandP90.Seconds()))
		}
		if line := brakeLine(ctx, log, vk, prefix); line != "" {
			out = append(out, line)
		}
		return out
	}
}

// brakeLine is the line for a set brake, empty while it is not set or unreadable
func brakeLine(ctx context.Context, log *slog.Logger, vk valkey.Client, prefix string) string {
	key := prefix + saga.BrakeKey
	val, err := vk.Do(ctx, vk.B().Get().Key(key).Build()).ToString()
	if valkey.IsValkeyNil(err) {
		return ""
	}
	if err != nil {
		log.Warn("monitor cannot read the reservation brake", "err", err)
		return ""
	}
	ttl, err := vk.Do(ctx, vk.B().Pttl().Key(key).Build()).AsInt64()
	if err != nil || ttl < 0 {
		return ""
	}
	var unused, bound int
	if _, err := fmt.Sscanf(strings.TrimSpace(val), "%d %d", &unused, &bound); err != nil {
		return ""
	}
	until := time.Now().Add(time.Duration(ttl) * time.Millisecond).UTC()
	return fmt.Sprintf("reservations are braked until %s, %d unused and %d bound in the last hour",
		until.Format("15:04"), unused, bound)
}
