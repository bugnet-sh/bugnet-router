package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/attr"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/carrier"
)

// rootKinds are the periodic jobs that start a trace of their own, kept by the ratio
var rootKinds = map[string]bool{"probe": true, "bench": true}

// InsertMiddleware writes the caller's span context into every inserted job's args
func (t *Telemetry) InsertMiddleware() rivertype.JobInsertMiddleware { return &insertMW{t: t} }

type insertMW struct {
	river.MiddlewareDefaults
	t *Telemetry
}

func (m *insertMW) InsertMany(ctx context.Context, params []*rivertype.JobInsertParams,
	doInner func(context.Context) ([]*rivertype.JobInsertResult, error),
) ([]*rivertype.JobInsertResult, error) {
	if parent, state := carrier.Carrier(ctx); parent != "" && m.t.Enabled() {
		for _, p := range params {
			p.EncodedArgs = withTrace(p.EncodedArgs, jobargs.Trace{Parent: parent, State: state})
		}
	}
	return doInner(ctx)
}

// withTrace adds the trace field to a job's encoded args, leaving them as they were on any doubt
func withTrace(encoded []byte, tr jobargs.Trace) []byte {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil || fields == nil {
		return encoded
	}
	raw, err := json.Marshal(tr)
	if err != nil {
		return encoded
	}
	fields["trace"] = raw
	out, err := json.Marshal(fields)
	if err != nil {
		return encoded
	}
	return out
}

// WorkerMiddleware starts each job's span from the trace its args carry
func (t *Telemetry) WorkerMiddleware() rivertype.WorkerMiddleware { return &workerMW{t: t} }

type workerMW struct {
	river.MiddlewareDefaults
	t *Telemetry
}

func (m *workerMW) Work(ctx context.Context, job *rivertype.JobRow,
	doInner func(context.Context) error) error {
	if !m.t.Enabled() {
		return doInner(ctx)
	}
	var env struct {
		Trace jobargs.Trace `json:"trace"`
	}
	_ = json.Unmarshal(job.EncodedArgs, &env)
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer)}
	var linked bool
	ctx, linked = m.t.Continue(ctx, env.Trace.Parent, env.Trace.State)
	switch {
	case linked:
	case rootKinds[job.Kind]:
		opts = append(opts, trace.WithNewRoot())
	default:
		return doInner(ctx)
	}
	start := time.Now()
	wait := max(start.Sub(job.ScheduledAt), 0)
	opts = append(opts, trace.WithAttributes(
		attr.JobKind(job.Kind), attr.JobQueue(job.Queue), attr.JobAttempt(job.Attempt),
		attr.JobWaitMS(wait.Milliseconds())), trace.WithTimestamp(start))
	ctx, span := m.t.provider.Tracer(tracerScope).Start(ctx, "river."+job.Kind, opts...)
	err := doInner(ctx)
	outcome := "ok"
	var snooze *rivertype.JobSnoozeError
	var cancel *rivertype.JobCancelError
	switch {
	case errors.As(err, &snooze):
		outcome = "snoozed"
	case errors.As(err, &cancel):
		outcome = "cancelled"
	case err != nil:
		outcome = "error"
		span.SetStatus(codes.Error, "JOB_ERROR")
	}
	span.SetAttributes(attr.JobOutcome(outcome))
	span.End()
	return err
}
