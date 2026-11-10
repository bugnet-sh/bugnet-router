package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// defaultDrainActor is who a drain is logged for when --actor is not given
const defaultDrainActor = "bugnet-dev"

// pools lists the pools, or with drain retires the warm members of one
func (c *cli) pools(args []string) error {
	if len(args) > 0 && args[0] == "drain" {
		return c.drainPool(args[1:])
	}
	if err := parse(newFlags("pools"), args, false); err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.ListPools(c.ctx, connect.NewRequest(&routerv1.ListPoolsRequest{}))
	if err != nil {
		return err
	}
	u := func(n uint32) string { return strconv.FormatUint(uint64(n), 10) }
	var b strings.Builder
	for _, p := range res.Msg.GetPools() {
		fmt.Fprintf(&b, "%s %s kept_by=%s target=%s min=%s max=%s warm=%s filling=%s held=%s "+
			"claims_last_hour=%s misses_last_hour=%s wasted_last_day=%s "+
			"cost_micros_last_day=%d fill_p95_ms=%s wake_p95_ms=%s", p.GetProvider(), p.GetSize(),
			p.GetKeptBy(), u(p.GetTarget()), u(p.GetMin()), u(p.GetMax()), u(p.GetWarm()),
			u(p.GetFilling()), u(p.GetHeld()), u(p.GetClaimsLastHour()),
			u(p.GetMissesLastHour()), u(p.GetWastedLastDay()), p.GetCostMicrosLastDay(),
			u(p.GetFillP95Ms()), u(p.GetWakeP95Ms()))
		if p.GetPausedUntil() != nil {
			fmt.Fprintf(&b, " paused_until=%s paused_reason=%s", stamp(p.GetPausedUntil()),
				p.GetPausedReason())
		}
		b.WriteString("\n")
	}
	_, err = fmt.Fprint(c.stdout, b.String())
	return err
}

func (c *cli) drainPool(args []string) error {
	fs := newFlags("pools drain")
	provider := fs.String("provider", "", "provider of the pool")
	size := fs.String("size", "", "size of the pool")
	pause := fs.Duration("pause", 0, "keep the refill off this long, refilling at once when 0")
	actor := fs.String("actor", defaultDrainActor, "who the drain is logged for")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	for _, r := range [][2]string{{"provider", *provider}, {"size", *size}} {
		if err := required(r[0], r[1]); err != nil {
			return err
		}
	}
	if *pause < 0 {
		return fmt.Errorf("%w: --pause must not be negative", errUsage)
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	req := &routerv1.DrainPoolRequest{Provider: *provider, Size: *size, Actor: *actor}
	if *pause > 0 {
		req.PauseFor = durationpb.New(*pause)
	}
	res, err := op.DrainPool(c.ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	return c.print([][2]string{
		{"retired", strconv.FormatUint(uint64(res.Msg.GetRetired()), 10)},
		{"pause", (*pause).Round(time.Second).String()},
	})
}
