package api

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/pool"
)

// maxDrainPause is the longest an operator may keep a pool's refill off in one call
const maxDrainPause = 7 * 24 * time.Hour

// ListPools answers every configured pool's members, demand, target and pause, read from Postgres
// alone so router-api needs nothing of router-worker's memory
func (o *OperatorServer) ListPools(
	ctx context.Context, _ *connect.Request[routerv1.ListPoolsRequest],
) (*connect.Response[routerv1.ListPoolsResponse], error) {
	s := o.s
	var out []*routerv1.PoolStatus
	if s.d.Pools != nil {
		pools, err := s.d.Pools.Status(ctx)
		if err != nil {
			return nil, s.status(err)
		}
		for _, p := range pools {
			out = append(out, poolStatusOf(p))
		}
	}
	return connect.NewResponse(&routerv1.ListPoolsResponse{Pools: out}), nil
}

func poolStatusOf(p pool.Status) *routerv1.PoolStatus {
	out := &routerv1.PoolStatus{
		Provider: p.Provider, Size: p.Size, KeptBy: p.KeptBy, Target: uint32(p.Target),
		Min: uint32(p.Min), Max: uint32(p.Max), Warm: uint32(p.Warm),
		Filling: uint32(p.Filling), Held: uint32(p.Held),
		ClaimsLastHour: uint32(p.ClaimsLastHour), MissesLastHour: uint32(p.MissesLastHour),
		WastedLastDay: uint32(p.WastedLastDay), CostMicrosLastDay: p.CostMicrosLastDay,
		FillP95Ms: uint32(p.FillP95.Milliseconds()), WakeP95Ms: uint32(p.WakeP95.Milliseconds()),
		PausedReason: p.PausedReason,
	}
	if !p.PausedUntil.IsZero() {
		out.PausedUntil = timestamppb.New(p.PausedUntil)
	}
	return out
}

// DrainPool retires every warm member of one pool and, with pause_for, keeps its refill off
func (o *OperatorServer) DrainPool(
	ctx context.Context, req *connect.Request[routerv1.DrainPoolRequest],
) (*connect.Response[routerv1.DrainPoolResponse], error) {
	m, s := req.Msg, o.s
	var pause time.Duration
	if m.GetPauseFor() != nil {
		pause = m.GetPauseFor().AsDuration()
	}
	switch {
	case m.GetProvider() == "" || m.GetSize() == "":
		return nil, invalid("provider and size are required")
	case !validActor(m.GetActor()):
		return nil, invalid("actor must be 1 to %d bytes", maxActor)
	case pause < 0 || pause > maxDrainPause:
		return nil, invalid("pause_for must be 0 to %s", maxDrainPause)
	case s.d.Pools == nil:
		return nil, poolUnknown()
	}
	n, err := s.d.Pools.Drain(ctx, pool.Key{Provider: m.GetProvider(), Size: m.GetSize()}, pause,
		m.GetActor())
	if errors.Is(err, pool.ErrUnknownPool) {
		return nil, poolUnknown()
	}
	if err != nil {
		return nil, s.status(err)
	}
	s.d.Log.Info("operator drained a pool", "actor", m.GetActor(), "provider", m.GetProvider(),
		"size", m.GetSize(), "retired", n, "pause", pause)
	return connect.NewResponse(&routerv1.DrainPoolResponse{Retired: uint32(n)}), nil
}

func poolUnknown() error {
	return withInfo(connect.NewError(connect.CodeNotFound, errors.New("no such pool")),
		reasonPoolUnknown)
}
