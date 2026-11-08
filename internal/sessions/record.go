package sessions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Version is the v both payloads carry, the page refusing any other
const Version = 1

// usdcMint is the one mint the page names, as USDC
const usdcMint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

// EndedBecause names a placement's end as the record's endedBecause does
func EndedBecause(state, terminationReason string) string {
	if state == store.PlacementFailed {
		return "failed"
	}
	switch terminationReason {
	case "stop":
		return "stopped"
	case "deadline", "":
		return "deadline"
	case "sandbox_exited":
		return "exited"
	case "provider_failed", "control_lost":
		return "fault"
	}
	return "ended"
}

// ClosingEnd names who closes the record of a placement ended for reason, false when no one does
func ClosingEnd(terminationReason string) (string, bool) {
	switch terminationReason {
	case "abuse", "wallet_blocked":
		return store.ClosedByAbuse, true
	case "operator":
		return store.ClosedByOperator, true
	}
	return "", false
}

/* Ordered JSON */

// object is a JSON object that keeps its keys in the order they were added
type object []field

type field struct {
	k string
	v any
}

func (o object) with(k string, v any) object { return append(o, field{k, v}) }

// MarshalJSON writes the keys in order
func (o object) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, f := range o {
		if i > 0 {
			b.WriteByte(',')
		}
		k, err := json.Marshal(f.k)
		if err != nil {
			return nil, err
		}
		v, err := json.Marshal(f.v)
		if err != nil {
			return nil, err
		}
		b.Write(k)
		b.WriteByte(':')
		b.Write(v)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func stampPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return stamp(*t)
}

/* Serving */

// serveFunc builds a served event's keys after at and kind, false leaving the row out
type serveFunc func(data map[string]json.RawMessage) (object, bool)

// fields serves each stored key that is present under its camel case name, in order
func fields(data map[string]json.RawMessage, pairs ...string) object {
	var o object
	for i := 0; i+1 < len(pairs); i += 2 {
		if v, ok := data[pairs[i]]; ok {
			o = o.with(pairs[i+1], v)
		}
	}
	return o
}

func pick(pairs ...string) serveFunc {
	return func(data map[string]json.RawMessage) (object, bool) {
		return fields(data, pairs...), true
	}
}

// serve holds one function per served kind, so a kind with none, such as x402, stays hidden
var serve = map[string]serveFunc{
	KindPlacement: pick("phase", "phase", "package_id", "packageId", "vcpu", "vcpu",
		"memory_mib", "memoryMib", "disk_gib", "diskGib", "minutes", "minutes", "deadline",
		"deadline", "ended_because", "endedBecause"),
	KindPayment: servePayment,
	KindJob: pick("seq", "seq", "phase", "phase", "exit_code", "exitCode", "oom", "oom",
		"peak_mib", "peakMib", "duration_ms", "durationMs", "out_bytes", "outBytes",
		"err_bytes", "errBytes"),
	KindFile: pick("op", "op", "path", "path", "bytes", "bytes"),
	KindPreview: pick("port", "port", "phase", "phase", "requests", "requests", "bytes",
		"bytes"),
	KindTransfer:   pick("direction", "direction", "name", "name", "bytes", "bytes"),
	KindSSH:        pick("phase", "phase", "bytes_in", "bytesIn", "bytes_out", "bytesOut"),
	KindSecretHost: pick("host", "host", "requests", "requests", "bytes", "bytes"),
	KindMCP:        pick("tool", "tool", "outcome", "outcome", "duration_ms", "durationMs"),
	KindAllowance:  serveAllowance,
}

func servePayment(data map[string]json.RawMessage) (object, bool) {
	o := fields(data, "pay_kind", "payKind", "amount_micros", "amountMicros")
	if tx := str(data["tx"]); tx != "" {
		o = o.with("tx", tx)
	}
	return o, true
}

func serveAllowance(data map[string]json.RawMessage) (object, bool) {
	o := fields(data, "phase", "phase", "amount_micros", "amountMicros")
	if str(data["asset"]) == usdcMint {
		o = o.with("asset", "USDC")
	}
	o = append(o, fields(data, "network", "network")...)
	if tx := str(data["tx"]); tx != "" {
		o = o.with("tx", tx)
	}
	return o, true
}

func str(raw json.RawMessage) string {
	var s string
	_ = json.Unmarshal(raw, &s)
	return s
}

// served turns stored rows into the page's flat events, leaving out every kind it cannot serve
func served(events []store.SessionEvent) []object {
	out := []object{}
	for _, e := range events {
		fn, ok := serve[e.Kind]
		if !ok {
			continue
		}
		var data map[string]json.RawMessage
		if err := json.Unmarshal(e.Data, &data); err != nil {
			continue
		}
		rest, ok := fn(data)
		if !ok {
			continue
		}
		out = append(out, append(object{{"at", stamp(e.At)}, {"kind", e.Kind}}, rest...))
	}
	return out
}

// jobEnds reads each job's oom and peak memory from its end event, by seq
func jobEnds(events []store.SessionEvent) map[int]struct {
	OOM     bool   `json:"oom"`
	PeakMiB uint32 `json:"peak_mib"`
} {
	type end = struct {
		OOM     bool   `json:"oom"`
		PeakMiB uint32 `json:"peak_mib"`
	}
	out := map[int]end{}
	for _, e := range events {
		if e.Kind != KindJob {
			continue
		}
		var d struct {
			Seq   int    `json:"seq"`
			Phase string `json:"phase"`
			end
		}
		if json.Unmarshal(e.Data, &d) == nil && d.Phase == "end" {
			out[d.Seq] = d.end
		}
	}
	return out
}

// jobState serves a job's state at the end, a job still running then counting as lost
func jobState(state string) string {
	switch state {
	case store.JobExited, store.JobCancelled:
		return state
	}
	return store.JobLost
}

// BuildRecord builds record_json, with one serve function per kind
func BuildRecord(ctx context.Context, st *store.Store, placement uuid.UUID) ([]byte, error) {
	rec, err := st.SessionRecordByPlacement(ctx, placement)
	if err != nil {
		return nil, err
	}
	p, err := st.PlacementByID(ctx, placement)
	if err != nil {
		return nil, err
	}
	events, err := st.SessionEvents(ctx, rec.ID)
	if err != nil {
		return nil, err
	}
	jobs, err := st.SessionJobs(ctx, rec.ID)
	if err != nil {
		return nil, err
	}
	ended := rec.EndedAt
	if ended == nil {
		ended = p.TerminatedAt
	}
	ends := jobEnds(events)
	served := served(events)
	list := make([]object, 0, len(jobs))
	for _, j := range jobs {
		state := jobState(j.State)
		o := object{{"seq", j.Seq}, {"command", j.Command}, {"state", state},
			{"startedAt", stamp(j.StartedAt)}}
		if j.FinishedAt != nil {
			o = o.with("finishedAt", stamp(*j.FinishedAt))
		}
		if j.ExitCode != nil && state != store.JobLost {
			o = o.with("exitCode", *j.ExitCode)
		}
		end := ends[j.Seq]
		o = o.with("oom", end.OOM).with("peakMib", end.PeakMiB).with("cutBytes", j.CutBytes)
		if j.Frames != nil {
			o = o.with("frames", j.Frames)
		}
		list = append(list, o)
	}
	out := object{
		{"v", Version}, {"startedAt", stamp(rec.CreatedAt)}, {"endedAt", stampPtr(ended)},
		{"endedBecause", EndedBecause(p.State, p.TerminationReason)},
		{"deletesAt", stampPtr(rec.ExpiresAt)}, {"output", rec.CopyState},
		{"events", served}, {"eventsDropped", rec.EventsDropped}, {"jobs", list},
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("encode the session record: %w", err)
	}
	return raw, nil
}
