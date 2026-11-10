package config

import (
	"cmp"
	"fmt"
	"net/url"
	"regexp"
	"time"
)

// Telemetry holds the trace export settings of DP 20.7.2
type Telemetry struct {
	// Traces turns span export on, nothing is built or sent while it is false
	Traces bool
	// OTLPEndpoint is the full traces URL
	OTLPEndpoint string
	// SampleRatio is the share of new traces kept, 0 to 1
	SampleRatio float64
	// Environment is deployment.environment.name on every span
	Environment string
	// ExportTimeout bounds one export request
	ExportTimeout time.Duration
	// BatchTimeout is the longest a finished span waits before export
	BatchTimeout time.Duration
	// BatchSize is the spans per export request
	BatchSize int
	// QueueSize is the finished spans waiting for export, more are dropped
	QueueSize int
}

type telemetryFile struct {
	Traces        bool    `toml:"traces"`
	OTLPEndpoint  string  `toml:"otlp_endpoint"`
	SampleRatio   float64 `toml:"sample_ratio"`
	Environment   string  `toml:"environment"`
	ExportTimeout string  `toml:"export_timeout"`
	BatchTimeout  string  `toml:"batch_timeout"`
	BatchSize     int64   `toml:"batch_size"`
	QueueSize     int64   `toml:"queue_size"`
}

func defaultTelemetry() telemetryFile {
	return telemetryFile{
		SampleRatio: 1, Environment: "main", ExportTimeout: "10s", BatchTimeout: "5s",
		BatchSize: 512, QueueSize: 2048,
	}
}

const maxTraceQueue = 1_000_000

var environmentName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)

// read checks the telemetry table against its bounds, d holding the defaults
func (f telemetryFile) read(d telemetryFile) (Telemetry, error) {
	t := Telemetry{Traces: f.Traces, OTLPEndpoint: f.OTLPEndpoint, SampleRatio: f.SampleRatio,
		Environment: cmp.Or(f.Environment, d.Environment)}
	if err := traceEndpoint(f.OTLPEndpoint, f.Traces); err != nil {
		return Telemetry{}, err
	}
	if !(f.SampleRatio >= 0 && f.SampleRatio <= 1) {
		return Telemetry{}, fmt.Errorf("telemetry.sample_ratio must be 0 to 1")
	}
	if !environmentName.MatchString(t.Environment) {
		return Telemetry{}, fmt.Errorf("telemetry.environment must be 1 to 32 characters of " +
			"a to z, 0 to 9, _ and -")
	}
	for _, v := range []struct {
		key, raw, def string
		into          *time.Duration
	}{
		{"telemetry.export_timeout", f.ExportTimeout, d.ExportTimeout, &t.ExportTimeout},
		{"telemetry.batch_timeout", f.BatchTimeout, d.BatchTimeout, &t.BatchTimeout},
	} {
		n, err := time.ParseDuration(cmp.Or(v.raw, v.def))
		if err != nil || n < 100*time.Millisecond || n > time.Minute {
			return Telemetry{}, fmt.Errorf("%s must be a Go duration of 100ms to 1m", v.key)
		}
		*v.into = n
	}
	if f.QueueSize < 1 || f.QueueSize > maxTraceQueue {
		return Telemetry{}, fmt.Errorf("telemetry.queue_size must be 1 to %d", maxTraceQueue)
	}
	if f.BatchSize < 1 || f.BatchSize > f.QueueSize {
		return Telemetry{}, fmt.Errorf("telemetry.batch_size must be 1 to telemetry.queue_size")
	}
	t.BatchSize, t.QueueSize = int(f.BatchSize), int(f.QueueSize)
	return t, nil
}

// traceEndpoint checks the OTLP URL, https or http to a loopback host, needed while traces are on
func traceEndpoint(raw string, on bool) error {
	if raw == "" {
		if on {
			return fmt.Errorf("telemetry.otlp_endpoint is required while telemetry.traces is true")
		}
		return nil
	}
	const want = "telemetry.otlp_endpoint must be an https URL with no user, or http to a " +
		"loopback host"
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Opaque != "" || u.Fragment != "" {
		return fmt.Errorf("%s", want)
	}
	switch {
	case u.Scheme == "https":
	case u.Scheme == "http" && loopback(u.Hostname()):
	default:
		return fmt.Errorf("%s", want)
	}
	return nil
}
