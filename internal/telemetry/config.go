package telemetry

import (
	"context"
	"log/slog"
	"time"

	"github.com/riverqueue/river/rivertype"

	"github.com/bugnet-sh/bugnet-router/internal/config"
)

// ShutdownTimeout bounds the flush every binary runs after its other shutdown steps
const ShutdownTimeout = 5 * time.Second

// FromConfig builds the telemetry of one binary from the settings and the authorization
func FromConfig(cfg config.Config, service string, log *slog.Logger) (*Telemetry, error) {
	var keys [][]byte
	for _, r := range cfg.DataPlaneKeys.Roots() {
		keys = append(keys, r.Key)
	}
	t := cfg.Telemetry
	return New(Settings{
		Traces: t.Traces, OTLPEndpoint: t.OTLPEndpoint, SampleRatio: t.SampleRatio,
		Environment: t.Environment, ExportTimeout: t.ExportTimeout, BatchTimeout: t.BatchTimeout,
		BatchSize: t.BatchSize, QueueSize: t.QueueSize,
	}, Process{Service: service, InstanceID: cfg.ReplicaID}, cfg.OTLPAuthorization, keys, log)
}

// Close flushes the queue within ShutdownTimeout, for a binary's last shutdown step
func (t *Telemetry) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()
	_ = t.Shutdown(ctx)
}

// Middleware lists the River middleware a client takes, none while traces are off
func (t *Telemetry) Middleware(worker bool) []rivertype.Middleware {
	if !t.Enabled() {
		return nil
	}
	mw := []rivertype.Middleware{t.InsertMiddleware()}
	if worker {
		mw = append(mw, t.WorkerMiddleware())
	}
	return mw
}
