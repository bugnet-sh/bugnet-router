package worker_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel/trace"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/telemetrytest"
)

// tracedEnv is an environment whose saga and worker export spans to a receiver
func tracedEnv(t *testing.T, o options) (*env, *telemetrytest.Telemetry) {
	t.Helper()
	tel := telemetrytest.New(t, "router-worker", 1, nil)
	o.tel = tel.Telemetry
	return newEnv(t, o), tel
}

// argsOf reads the encoded args of the newest job of kind
func (e *env) argsOf(t *testing.T, kind string) map[string]any {
	t.Helper()
	var raw []byte
	err := e.pool.QueryRow(t.Context(),
		`select args from river_job where kind = $1 order by id desc limit 1`, kind).Scan(&raw)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

const execName = "bugnet.router.v1.SandboxRouterService/Exec"

func TestInsertCarriesTrace(t *testing.T) {
	e, tel := tracedEnv(t, options{})
	ctx, span := tel.Tracer("test").Start(t.Context(), execName)
	id := uuid.New()
	if _, err := e.river.Insert(ctx, jobargs.PrepareSSH{PlacementID: id}, nil); err != nil {
		t.Fatal(err)
	}
	span.End()
	trace, _ := e.argsOf(t, "prepare_ssh")["trace"].(map[string]any)
	want := "00-" + span.SpanContext().TraceID().String() + "-" +
		span.SpanContext().SpanID().String() + "-01"
	eq(t, trace["traceparent"], any(want))
	// A job inserted with no span carries none, and its args are as the call site made them
	_, err := e.river.Insert(t.Context(), jobargs.ApplySecrets{PlacementID: id}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, has := e.argsOf(t, "apply_secrets")["trace"]
	eq(t, has, false)
}

func TestUniqueKeyIgnoresTrace(t *testing.T) {
	e, tel := tracedEnv(t, options{})
	id := uuid.New()
	var ids []int64
	for range 2 {
		ctx, span := tel.Tracer("test").Start(t.Context(), execName)
		res, err := e.river.Insert(ctx, jobargs.PrepareSSH{PlacementID: id}, nil)
		span.End()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.Job.ID)
		if len(ids) == 2 {
			eq(t, res.UniqueSkippedAsDuplicate, true)
		}
	}
	eq(t, ids[0], ids[1])
}

func TestWorkerSpanFromArgs(t *testing.T) {
	e, tel := tracedEnv(t, options{})
	p, _ := e.placeReady(t, "ord-trace-1")
	e.start(t)
	ctx, span := tel.Tracer("test").Start(t.Context(), "saga.commit_ready")
	res, err := e.river.Insert(ctx, jobargs.Deadline{PlacementID: p.ID}, nil)
	span.End()
	if err != nil {
		t.Fatal(err)
	}
	e.finished(t, "deadline", res.Job.ID)
	var got []telemetrytest.Span
	for _, s := range tel.Flushed(t) {
		if s.Name == "river.deadline" {
			got = append(got, s)
		}
	}
	eq(t, len(got), 1)
	s := got[0]
	eq(t, s.ParentID, span.SpanContext().SpanID().String())
	eq(t, s.TraceID, span.SpanContext().TraceID().String())
	eq(t, at(s.Attrs, "bugnet.job.kind"), "deadline")
	eq(t, at(s.Attrs, "bugnet.job.queue"), "default")
	eq(t, at(s.Attrs, "bugnet.job.outcome"), "ok")
	eq(t, at(s.Attrs, "bugnet.job.attempt"), "1")
}

func TestPeriodicJobsMakeNoRoots(t *testing.T) {
	e, tel := tracedEnv(t, options{})
	e.start(t)
	for _, args := range []river.JobArgs{jobargs.Sweep{}, jobargs.Watch{}, jobargs.Outbox{},
		jobargs.PoolTick{}, jobargs.ReserveWatch{}, jobargs.HealthRollup{}} {
		e.run(t, args)
	}
	eq(t, len(tel.Flushed(t)), 0)
}

func TestOutboxCarriesTraceparent(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	recv := newReceiver(t, pub)
	e, tel := tracedEnv(t, options{periodic: true, eventsURL: recv.srv.URL, eventKey: priv})
	ctx, span := tel.Tracer("test").Start(t.Context(), "saga.commit_ready")
	err = e.store.WithTx(ctx, func(tx pgx.Tx) error {
		return e.store.InsertOutbox(ctx, tx, saga.EventReady, map[string]string{"n": "traced"})
	})
	span.End()
	if err != nil {
		t.Fatal(err)
	}
	traced := e.eventID(t, "traced")
	plain := e.insertEvent(t, "plain")
	e.start(t)
	eventually(t, "both events", func() bool {
		return e.outboxRow(t, traced).deliveredAt != nil && e.outboxRow(t, plain).deliveredAt != nil
	})
	recv.checkClean(t)
	var withHeader, without int
	for _, d := range recv.deliveries() {
		if d.traceparent == "" {
			without++
			continue
		}
		withHeader++
		// The delivery's own span is a child of the row's context, in the same trace
		if !strings.HasPrefix(d.traceparent, "00-"+span.SpanContext().TraceID().String()+"-") {
			t.Fatalf("traceparent %s is not in trace %s", d.traceparent,
				span.SpanContext().TraceID())
		}
		if d.traceparent == "00-"+span.SpanContext().TraceID().String()+"-"+
			span.SpanContext().SpanID().String()+"-01" {
			t.Fatal("the POST carries the row's span, not its own")
		}
	}
	eq(t, withHeader, 1)
	eq(t, without, 1)
	var deliver []telemetrytest.Span
	for _, s := range tel.Flushed(t) {
		if s.Name == "outbox.deliver" {
			deliver = append(deliver, s)
		}
	}
	eq(t, len(deliver), 1)
	eq(t, deliver[0].ParentID, span.SpanContext().SpanID().String())
	eq(t, at(deliver[0].Attrs, "bugnet.event_type"), saga.EventReady)
	eq(t, at(deliver[0].Attrs, "http.response.status_code"), "200")
}

func TestOutboxBadContextStoredAsNull(t *testing.T) {
	e, _ := tracedEnv(t, options{})
	// A tracestate over 512 bytes is dropped and the traceparent kept
	var members []string
	for _, k := range []string{"ka", "kb", "kc"} {
		members = append(members, k+"="+strings.Repeat("x", 200))
	}
	ts, err := trace.ParseTraceState(strings.Join(members, ","))
	if err != nil {
		t.Fatal(err)
	}
	ctx := trace.ContextWithRemoteSpanContext(t.Context(), trace.NewSpanContext(
		trace.SpanContextConfig{
			TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
			TraceState: ts,
		}))
	err = e.store.WithTx(ctx, func(tx pgx.Tx) error {
		return e.store.InsertOutbox(ctx, tx, saga.EventReady, map[string]string{"n": "long"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var parent, state *string
	err = e.pool.QueryRow(t.Context(),
		`select traceparent, tracestate from outbox where payload->>'n' = 'long'`).Scan(&parent,
		&state)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, parent != nil && state == nil, true)
	// With no span the event is written all the same, with NULL columns
	id := e.insertEvent(t, "bare")
	err = e.pool.QueryRow(t.Context(), `select traceparent, tracestate from outbox where id = $1`,
		id).Scan(&parent, &state)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, parent == nil && state == nil, true)
}

// eventID reads the id of the event whose payload carries n
func (e *env) eventID(t *testing.T, n string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := e.pool.QueryRow(t.Context(),
		`select id from outbox where payload->>'n' = $1`, n).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// at reads one exported attribute as text
func at(attrs map[string]any, key string) string {
	v, ok := attrs[key]
	if !ok {
		return ""
	}
	return strings.TrimSpace(strings.Trim(toString(v), "\""))
}

func toString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
