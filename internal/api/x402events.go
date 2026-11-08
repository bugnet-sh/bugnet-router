package api

import (
	"cmp"
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/x402"
)

const (
	// maxX402Events is the most events one AppendX402Events call carries
	maxX402Events = 50
	// x402PathChars is how much of an event's path the record keeps
	x402PathChars = 80
	// x402HostChars is how much of an event's host the record keeps
	x402HostChars = 253
)

// AppendX402Events stores egress's x402 events once per ref and moves the placement's counters
func (g *EgressServer) AppendX402Events(
	ctx context.Context, req *connect.Request[routerv1.AppendX402EventsRequest],
) (*connect.Response[routerv1.AppendX402EventsResponse], error) {
	s, m := g.s, req.Msg
	if len(m.GetEvents()) > maxX402Events {
		return nil, invalid("at most %d events a call", maxX402Events)
	}
	id, err := uuid.Parse(m.GetPlacementId())
	if err != nil {
		return nil, invalid("placement_id is not a placement")
	}
	p, err := s.d.Store.PlacementByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, invalid("placement_id is not a placement")
	}
	if err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.AppendX402EventsResponse{}
	rec, err := s.d.Store.SessionRecordByPlacement(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return connect.NewResponse(out), nil
	}
	if err != nil {
		return nil, s.status(err)
	}
	now := time.Now()
	if end := cmp.Or(rec.EndedAt, p.TerminatedAt); end != nil && now.After(end.Add(appendWindow)) {
		return connect.NewResponse(out), nil
	}
	for i, ev := range m.GetEvents() {
		if err := x402.CheckEvent(ev); err != nil {
			s.d.Log.Warn("api: x402 event left out", "placement", id, "index", i,
				"rule", err.Error())
			out.Rejected = append(out.Rejected, uint32(i))
			continue
		}
		data := x402Data(ev, rec, now)
		land := data.AmountMicros > 0 || !x402.Foldable(data.Status, 0)
		_, _, err := s.d.Store.InsertX402Event(ctx, id, data, land)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, s.status(err)
		}
	}
	return connect.NewResponse(out), nil
}

// x402Data is the row an event stores, its strings cleaned and its time kept inside the record
func x402Data(ev *routerv1.X402Event, rec store.SessionRecord, now time.Time) store.X402EventData {
	at := now
	if t := ev.GetAt(); t.IsValid() && t.AsTime().After(rec.CreatedAt.Add(-eventSkew)) &&
		t.AsTime().Before(now.Add(eventSkew)) {
		at = t.AsTime()
	}
	return store.X402EventData{
		At: at.UTC(), Ref: ev.GetRef(), PaymentRef: ev.GetPaymentRef(),
		Host:   sessions.Clean(ev.GetHost(), x402HostChars),
		Method: ev.GetMethod(), Path: sessions.Clean(ev.GetPath(), x402PathChars),
		Status: ev.GetStatus(), Reason: ev.GetReason(), AskedMicros: ev.GetAskedMicros(),
		AmountMicros: ev.GetAmountMicros(), Asset: ev.GetAsset(), Network: ev.GetNetwork(),
		Scheme: ev.GetScheme(), X402Version: ev.GetX402Version(), PayTo: ev.GetPayTo(),
		ServiceID: ev.GetServiceId(), HTTPStatus: ev.GetHttpStatus(),
		DurationMS: ev.GetDurationMs(), BytesOut: ev.GetBytesOut(), BytesIn: ev.GetBytesIn(),
		StreamCut: ev.GetStreamCut(), AllowanceLeftMicros: ev.GetAllowanceLeftMicros(),
		Count: ev.GetCount(),
	}
}
