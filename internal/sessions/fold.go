package sessions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Event kinds of SP 6.2
const (
	KindPlacement  = "placement"
	KindPayment    = "payment"
	KindJob        = "job"
	KindFile       = "file"
	KindPreview    = "preview"
	KindTransfer   = "transfer"
	KindSSH        = "ssh"
	KindSecretHost = "secret_host"
	KindMCP        = "mcp"
	KindAllowance  = "allowance"
)

// Placement phases
const (
	PhasePlaced   = "placed"
	PhaseReady    = "ready"
	PhaseExtended = "extended"
	PhaseEnded    = "ended"
)

// alwaysLand are the kinds that pass sessions.max_events, being few
var alwaysLand = map[string]bool{KindPlacement: true, KindPayment: true, KindAllowance: true}

// maxChars cuts each stored string to SP 6.2's length, 200 characters for any other
var maxChars = map[string]int{
	"path": 200, "name": 100, "host": 253, "tool": 48, "outcome": 32, "tx": 100, "asset": 64,
	"network": 64,
}

const (
	defaultChars = 200
	// commandBytes is the part of a command a record keeps (D-f)
	commandBytes = 1 << 10
)

// Writer is the one way an event reaches a session record
type Writer struct {
	st   *store.Store
	keys dpkeys.Keys
	log  *slog.Logger
	now  func() time.Time
}

// NewWriter writes events through st, redacting commands with values keys derive
func NewWriter(st *store.Store, keys dpkeys.Keys, log *slog.Logger) *Writer {
	if log == nil {
		log = slog.Default()
	}
	return &Writer{st: st, keys: keys, log: log, now: time.Now}
}

// SetClock replaces the writer's clock, for tests
func (w *Writer) SetClock(now func() time.Time) { w.now = now }

// Fields are one event's stored fields, snake case as SP 6.2 names them
type Fields map[string]any

// clean cleans and cuts every string of f, which it changes in place
func (f Fields) clean() {
	for k, v := range f {
		if s, ok := v.(string); ok {
			n, ok := maxChars[k]
			if !ok {
				n = defaultChars
			}
			f[k] = Clean(s, n)
		}
	}
}

// Store writes one event, answering whether it was stored, an unknown record storing nothing
func (w *Writer) Store(
	ctx context.Context, placement uuid.UUID, kind string, f Fields,
) (bool, error) {
	if w == nil {
		return false, nil
	}
	f = maps.Clone(f)
	f.clean()
	raw, err := json.Marshal(f)
	if err != nil {
		return false, fmt.Errorf("encode %s event: %w", kind, err)
	}
	inserted, _, err := w.st.InsertSessionEvent(ctx, placement, kind, raw, alwaysLand[kind])
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	return inserted, err
}

// StoreAll writes events of one kind in one transaction, so a failure stores none of them
func (w *Writer) StoreAll(
	ctx context.Context, placement uuid.UUID, kind string, events []Fields,
) error {
	if w == nil || len(events) == 0 {
		return nil
	}
	data := make([][]byte, 0, len(events))
	for _, f := range events {
		f = maps.Clone(f)
		f.clean()
		raw, err := json.Marshal(f)
		if err != nil {
			return fmt.Errorf("encode %s event: %w", kind, err)
		}
		data = append(data, raw)
	}
	err := w.st.InsertSessionEvents(ctx, placement, kind, data, alwaysLand[kind])
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	return err
}

// Write stores one event and logs a failure, which never fails the call that caused it
func (w *Writer) Write(ctx context.Context, placement uuid.UUID, kind string, f Fields) {
	if _, err := w.Store(ctx, placement, kind, f); err != nil {
		w.failed(kind, placement, err)
	}
}

func (w *Writer) failed(kind string, placement uuid.UUID, err error) {
	w.log.Warn("sessions: write an event", "kind", kind, "placement", placement, "err", err)
}

// Fold adds counts to the minute's row of kind whose fields hold match, logging a failure
func (w *Writer) Fold(ctx context.Context, placement uuid.UUID, kind string, at time.Time,
	match Fields, add map[string]int64) {
	if w == nil {
		return
	}
	match = maps.Clone(match)
	match.clean()
	raw, err := json.Marshal(match)
	if err == nil {
		err = w.st.FoldSessionEvent(ctx, placement, kind, at.UTC().Truncate(time.Minute), raw,
			add)
	}
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		w.failed(kind, placement, err)
	}
}

// Allowance stores one allowance event, once per phase of the record (SP 7.3)
func (w *Writer) Allowance(ctx context.Context, placement uuid.UUID, f Fields) error {
	if w == nil {
		return nil
	}
	f = maps.Clone(f)
	f.clean()
	raw, err := json.Marshal(f)
	if err != nil {
		return err
	}
	match, err := json.Marshal(Fields{"phase": f["phase"]})
	if err != nil {
		return err
	}
	_, err = w.st.InsertSessionEventOnce(ctx, placement, KindAllowance, match, raw, true)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	return err
}

/* Kinds */

// Placement writes a placement event, minutes being the package's or an extension's
func (w *Writer) Placement(ctx context.Context, placement uuid.UUID, phase string,
	pkg catalog.Package, deadline *time.Time, endedBecause string) {
	f := Fields{
		"phase": phase, "package_id": pkg.ID, "vcpu": pkg.Resources.VCPU,
		"memory_mib": pkg.Resources.MemoryMiB, "disk_gib": pkg.Resources.DiskGiB,
		"minutes": pkg.Minutes,
	}
	if deadline != nil {
		f["deadline"] = deadline.UTC().Format(time.RFC3339)
	}
	if phase == PhaseEnded {
		f["ended_because"] = endedBecause
	}
	w.Write(ctx, placement, KindPlacement, f)
}

// Payment writes a start or extend payment, amount being the package's share of the order
func (w *Writer) Payment(
	ctx context.Context, placement uuid.UUID, payKind string, amountMicros int64, tx string,
) {
	w.Write(ctx, placement, KindPayment, Fields{
		"pay_kind": payKind, "amount_micros": amountMicros, "tx": tx,
	})
}

// File writes a file operation, its path cut to 200 characters
func (w *Writer) File(ctx context.Context, placement uuid.UUID, op, p string, bytes int64) {
	w.Write(ctx, placement, KindFile, Fields{"op": op, "path": p, "bytes": bytes})
}

// Preview writes a preview issued or revoked
func (w *Writer) Preview(ctx context.Context, placement uuid.UUID, port int, phase string) {
	w.Write(ctx, placement, KindPreview, Fields{"port": port, "phase": phase})
}

// PreviewTraffic folds one usage report's requests and bytes into the port's minute
func (w *Writer) PreviewTraffic(ctx context.Context, placement uuid.UUID, port int,
	at time.Time, requests, bytes int64) {
	w.Fold(ctx, placement, KindPreview, at, Fields{"port": port, "phase": "traffic"},
		map[string]int64{"requests": requests, "bytes": bytes})
}

// Transfer writes a finished transfer by its file's name, never its whole path
func (w *Writer) Transfer(
	ctx context.Context, placement uuid.UUID, direction, filePath string, bytes int64,
) {
	w.Write(ctx, placement, KindTransfer, Fields{
		"direction": direction, "name": path.Base(filePath), "bytes": bytes,
	})
}

// SSH writes an SSH session's start or end
func (w *Writer) SSH(
	ctx context.Context, placement uuid.UUID, phase string, bytesIn, bytesOut int64,
) {
	w.Write(ctx, placement, KindSSH, Fields{
		"phase": phase, "bytes_in": bytesIn, "bytes_out": bytesOut,
	})
}

// SecretHost folds one usage report's calls to a secret host into the host's minute
func (w *Writer) SecretHost(ctx context.Context, placement uuid.UUID, host string, at time.Time,
	requests, bytes int64) {
	w.Fold(ctx, placement, KindSecretHost, at, Fields{"host": host},
		map[string]int64{"requests": requests, "bytes": bytes})
}

/* Jobs */

// StartJob gives a new job its seq, with the first KiB of its command, and its start event
func (w *Writer) StartJob(ctx context.Context, p store.Placement, jobID string, argv []string) {
	if w == nil {
		return
	}
	cmd := CutBytes(KnownRedactor(p, w.keys).Redact(cleanText(Command(argv))), commandBytes)
	seq, created, err := w.st.StartSessionJob(ctx, p.ID, jobID, cmd, w.now())
	switch {
	case errors.Is(err, store.ErrNotFound):
		return
	case err != nil:
		w.failed(KindJob, p.ID, err)
		return
	case !created:
		return
	}
	w.Write(ctx, p.ID, KindJob, Fields{"seq": seq, "phase": "start"})
}

// JobEnd is how a job ended, as its exit frame and the jobs table say
type JobEnd struct {
	ExitCode           *int
	OOM                bool
	PeakMiB            uint32
	OutBytes, ErrBytes int64
}

// EndJob records a job's end on its record and writes its end event
func (w *Writer) EndJob(ctx context.Context, placement uuid.UUID, jobID string, e JobEnd) {
	if w == nil {
		return
	}
	now := w.now()
	seq, started, err := w.st.FinishSessionJob(ctx, placement, jobID, now, e.ExitCode)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return
	case err != nil:
		w.failed(KindJob, placement, err)
		return
	}
	f := Fields{
		"seq": seq, "phase": "end", "exit_code": nil, "oom": e.OOM, "peak_mib": e.PeakMiB,
		"duration_ms": max(now.Sub(started).Milliseconds(), 0), "out_bytes": e.OutBytes,
		"err_bytes": e.ErrBytes,
	}
	if e.ExitCode != nil {
		f["exit_code"] = *e.ExitCode
	}
	w.Write(ctx, placement, KindJob, f)
}

// Command writes argv as one line, quoting an argument a reader could not tell apart
func Command(argv []string) string {
	parts := make([]string, len(argv))
	for i, a := range argv {
		if a == "" || strings.ContainsAny(a, " \t\n'\"\\$`") {
			a = strconv.Quote(a)
		}
		parts[i] = a
	}
	return strings.Join(parts, " ")
}

// KnownRedactor redacts the shapes and the placement's gate key and provider credentials
func KnownRedactor(p store.Placement, keys dpkeys.Keys) *Redactor {
	var known []string
	if p.KeyID != "" && len(keys.Roots()) > 0 {
		if gate, err := keys.GateKey(p.KeyID, p.ID); err == nil {
			known = append(known, gate)
		}
	}
	if p.ProviderRef != nil {
		for _, field := range store.CredentialFields() {
			if v := p.ProviderRef.Meta[field]; v != "" {
				known = append(known, v)
			}
		}
	}
	return NewRedactor(known...)
}
