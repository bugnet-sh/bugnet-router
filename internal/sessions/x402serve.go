package sessions

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// x402Row is one stored x402 event with what a resolve and the money rule add at serve time
type x402Row struct {
	at   time.Time
	d    storedX402
	paid bool
	// resolved is what Payments settled for the row's payment, nil when nothing has
	resolved *storedResolved
	resent   bool
	keeper   bool
}

type storedX402 struct {
	Ref                 string `json:"ref"`
	PaymentRef          string `json:"payment_ref"`
	Host                string `json:"host"`
	Method              string `json:"method"`
	Path                string `json:"path"`
	Status              string `json:"status"`
	Reason              string `json:"reason"`
	AskedMicros         int64  `json:"asked_micros"`
	AmountMicros        int64  `json:"amount_micros"`
	Asset               string `json:"asset"`
	Network             string `json:"network"`
	HTTPStatus          uint32 `json:"http_status"`
	DurationMS          uint32 `json:"duration_ms"`
	BytesOut            uint64 `json:"bytes_out"`
	BytesIn             uint64 `json:"bytes_in"`
	StreamCut           bool   `json:"stream_cut"`
	AllowanceLeftMicros int64  `json:"allowance_left_micros"`
	Count               uint32 `json:"count"`
}

type storedResolved struct {
	Ref          string `json:"ref"`
	Status       string `json:"status"`
	AmountMicros int64  `json:"amount_micros"`
	Tx           string `json:"tx"`
	Host         string `json:"host"`
	Method       string `json:"method"`
	Path         string `json:"path"`
}

// x402Served holds the served object of each x402 row and each lone resolve, by event id
type x402Served map[int64]object

// joinX402 joins resolves into rows and counts each payment's money once (XS 8.3)
func joinX402(events []store.SessionEvent) x402Served {
	var rows []*x402Row
	byEvent := map[int64]*x402Row{}
	resolves := map[int64]storedResolved{}
	for _, e := range events {
		switch e.Kind {
		case "x402":
			r := &x402Row{at: e.At}
			if json.Unmarshal(e.Data, &r.d) == nil {
				rows = append(rows, r)
				byEvent[e.ID] = r
			}
		case KindX402Resolved:
			var rs storedResolved
			if json.Unmarshal(e.Data, &rs) == nil {
				resolves[e.ID] = rs
			}
		}
	}
	// Money first: the row whose own ref is the payment's, else the earliest, keeps it
	groups := map[string][]*x402Row{}
	for _, r := range rows {
		if r.d.PaymentRef != "" {
			groups[r.d.PaymentRef] = append(groups[r.d.PaymentRef], r)
		}
	}
	for ref, g := range groups {
		keeper := slices.IndexFunc(g, func(r *x402Row) bool { return r.d.Ref == ref })
		if keeper < 0 {
			keeper = slices.IndexFunc(g, func(r *x402Row) bool {
				return !slices.ContainsFunc(g, func(o *x402Row) bool { return o.at.Before(r.at) })
			})
		}
		for i, r := range g {
			r.paid = true
			r.keeper = i == keeper
			r.resent = i != keeper
		}
	}
	out := x402Served{}
	lone := map[int64]storedResolved{}
	for id, rs := range resolves {
		var hit []*x402Row
		for _, r := range rows {
			if r.d.PaymentRef == rs.Ref {
				hit = append(hit, r)
			}
		}
		if len(hit) == 0 {
			hit = slices.DeleteFunc(slices.Clone(rows), func(r *x402Row) bool {
				return r.d.Ref != rs.Ref
			})
		}
		if len(hit) == 0 {
			lone[id] = rs
		}
		for _, r := range hit {
			r.resolved = &storedResolved{Status: rs.Status, Tx: rs.Tx}
		}
	}
	for id, r := range byEvent {
		out[id] = r.serve()
	}
	for id, rs := range lone {
		out[id] = rs.serveAlone()
	}
	return out
}

func (r *x402Row) serve() object {
	d := r.d
	amount := d.AmountMicros
	if r.resent {
		amount = 0
	}
	o := object{{"host", d.Host}, {"method", d.Method}, {"path", d.Path},
		{"status", d.Status}, {"reason", d.Reason}, {"count", d.Count},
		{"askedMicros", d.AskedMicros}, {"amountMicros", amount}}
	if d.Asset == usdcMint {
		o = o.with("asset", "USDC")
	}
	o = o.with("network", d.Network).with("httpStatus", d.HTTPStatus).
		with("durationMs", d.DurationMS).with("bytesOut", d.BytesOut).
		with("bytesIn", d.BytesIn).with("streamCut", d.StreamCut).
		with("allowanceLeftMicros", d.AllowanceLeftMicros)
	var resolved any
	if r.resolved != nil {
		resolved = r.resolved.Status
	}
	o = o.with("resolved", resolved)
	if r.resolved != nil && r.resolved.Status == "paid" && !r.resent {
		o = o.with("tx", r.resolved.Tx)
	}
	if r.paid {
		o = o.with("resent", r.resent)
	}
	return o
}

// serveAlone serves a resolve no row has reached yet as an uncertain row of its own
func (rs storedResolved) serveAlone() object {
	o := object{{"host", rs.Host}, {"method", rs.Method}, {"path", rs.Path},
		{"status", "uncertain"}, {"reason", ""}, {"count", 1}, {"askedMicros", 0},
		{"amountMicros", rs.AmountMicros}, {"network", ""}, {"httpStatus", 0},
		{"durationMs", 0}, {"bytesOut", 0}, {"bytesIn", 0}, {"streamCut", false},
		{"allowanceLeftMicros", 0}, {"resolved", rs.Status}}
	if rs.Status == "paid" {
		o = o.with("tx", rs.Tx)
	}
	return o
}
