package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/bugnet-sh/bugnet-run/frame"
	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/streams"
)

const (
	copyTimeout = 60 * time.Second
	// copyWindow is the longest the page waits for its output after the end
	copyWindow = 10 * time.Minute
	copyPage   = 256
	// tailBytes is what each output of an earlier job keeps once the record's budget is spent
	tailBytes = 8 << 10
	// diskEvery caches the session tables' size, since pg_total_relation_size reads the disk
	diskEvery = 60 * time.Second
	// copyBackoffCap keeps retries frequent enough to land inside the window
	copyBackoffCap = time.Minute
)

type sessionCopyWorker struct {
	river.WorkerDefaults[jobargs.SessionCopy]
	w *Worker
}

// Timeout keeps one copy under the rescue, its retries bounded by the window
func (*sessionCopyWorker) Timeout(*river.Job[jobargs.SessionCopy]) time.Duration {
	return copyTimeout
}

// NextRetry backs off by doubling up to a minute, so a few tries fit the 10-minute window
func (*sessionCopyWorker) NextRetry(job *river.Job[jobargs.SessionCopy]) time.Time {
	wait := time.Second << min(job.Attempt, 6)
	return time.Now().Add(min(wait, copyBackoffCap))
}

func (cw *sessionCopyWorker) Work(ctx context.Context, job *river.Job[jobargs.SessionCopy]) error {
	return cw.w.copySession(ctx, job.Args.PlacementID)
}

// diskGauge caches the session tables' share of their disk budget
type diskGauge struct {
	mu      sync.Mutex
	percent int64
	read    time.Time
}

// SessionDiskPercent is the session tables' size as a share of sessions.disk_budget, cached 60 s
func (w *Worker) SessionDiskPercent(ctx context.Context) (int64, error) {
	g := &w.disk
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.read.IsZero() && time.Since(g.read) < diskEvery {
		return g.percent, nil
	}
	n, err := w.store.SessionDiskBytes(ctx)
	if err != nil {
		return 0, err
	}
	g.percent, g.read = n*100/max(w.d.Sessions.DiskBudget, 1), time.Now()
	return g.percent, nil
}

// copySession copies an ended placement's job output into its record, then removes the streams
func (w *Worker) copySession(ctx context.Context, id uuid.UUID) error {
	// The jobs table lists every stream, a job started after Delete now or a record already
	// expired included, since the record's own rows may be gone (F8)
	streamIDs, err := w.store.PlacementJobIDs(ctx, id)
	if err != nil {
		return err
	}
	rec, err := w.store.SessionRecordByPlacement(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		w.unlink(ctx, id, streamIDs)
		return nil
	}
	if err != nil {
		return err
	}
	if rec.CopyState != store.CopyPending {
		w.unlink(ctx, id, streamIDs)
		return nil
	}
	p, err := w.store.PlacementByID(ctx, id)
	if err != nil {
		return err
	}
	jobs, err := w.store.SessionJobs(ctx, rec.ID)
	if err != nil {
		return err
	}
	now := w.now()
	ended := rec.EndedAt
	if ended == nil {
		ended = p.TerminatedAt
	}
	switch {
	case rec.ClosedAt != nil && !rec.Held(now):
		// The page was deleted, so nothing is kept and the output only goes
		return w.copied(ctx, rec, streamIDs, store.CopyExpired)
	case ended != nil && now.After(ended.Add(copyWindow)):
		return w.copied(ctx, rec, streamIDs, store.CopyExpired)
	}
	percent, err := w.SessionDiskPercent(ctx)
	if err != nil {
		return err
	}
	full := percent >= 100
	red := sessions.KnownRedactor(p, w.keys)
	left := w.d.Sessions.PlacementOutput
	missing, read := 0, 0
	// The newest jobs keep the most, so they spend the record's budget first
	for i := len(jobs) - 1; i >= 0; i-- {
		j := jobs[i]
		ok, err := w.bus.Exists(ctx, j.JobID)
		if err != nil {
			return err
		}
		if !ok {
			if j.StdoutBytes+j.StderrBytes > 0 || j.State == store.JobRunning {
				missing++
			}
			continue
		}
		// Once the record's budget is spent, an earlier job keeps its last 8 KiB per output
		limit, total := w.d.Sessions.JobOutput, left
		if full || left < 2*tailBytes {
			limit, total = tailBytes, 0
		}
		frames, cut, kept, err := copyJob(ctx, w.bus, red, j.JobID, limit, total)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(frames)
		if err != nil {
			return err
		}
		if err := w.store.SetSessionJobFrames(ctx, rec.ID, j.Seq, raw, cut); err != nil {
			return err
		}
		left -= kept
		read++
	}
	state := store.CopyComplete
	switch {
	case missing > 0 && read == 0:
		state = store.CopyExpired
	case missing > 0 || full:
		state = store.CopyPartial
	}
	return w.copied(ctx, rec, streamIDs, state)
}

// copied records how the copy ended, then removes the job streams (F8)
func (w *Worker) copied(
	ctx context.Context, rec store.SessionRecord, streamIDs []string, state string,
) error {
	if err := w.store.SetSessionCopyState(ctx, rec.ID, state); err != nil {
		return err
	}
	w.unlink(ctx, rec.PlacementID, streamIDs)
	return nil
}

// unlink removes the placement's job streams, a failure leaving them to their expiry
func (w *Worker) unlink(ctx context.Context, placement uuid.UUID, streamIDs []string) {
	if err := w.bus.Unlink(ctx, streamIDs...); err != nil {
		// The streams still expire 10 minutes past the deadline
		w.log.Warn("remove a session's job streams", "placement", placement, "err", err)
	}
}

// copyJob reads a job's stream through the page's cleaner, keeping limit bytes of each output and
// total across both, 0 for no total
func copyJob(ctx context.Context, bus *streams.Bus, red *sessions.Redactor, jobID string,
	limit, total int64) (frames []sessions.Frame, cut, kept int64, err error) {
	k := sessions.NewKeeper(limit, total)
	outs := map[frame.Kind]*sessions.Stream{
		frame.KindStdout: red.Stream(sessions.Stdout),
		frame.KindStderr: red.Stream(sessions.Stderr),
	}
	next := map[frame.Kind]int64{}
	flush := func() {
		k.Add(outs[frame.KindStdout].Flush()...)
		k.Add(outs[frame.KindStderr].Flush()...)
	}
	// gap counts output the stream left out before offset to
	gap := func(kind frame.Kind, to int64) {
		if to > next[kind] {
			k.Skip(to - next[kind])
			next[kind] = to
		}
	}
	cursor := ""
	for {
		page, err := bus.After(ctx, jobID, cursor, copyPage)
		if err != nil {
			return nil, 0, 0, err
		}
		for _, e := range page {
			f := e.Frame
			switch f.Kind {
			case frame.KindStdout, frame.KindStderr:
				data := f.Data
				// A frame a takeover sent again is trimmed to the part not yet seen
				if have := next[f.Kind] - f.Offset; have > 0 {
					data = data[min(have, int64(len(data))):]
				} else {
					gap(f.Kind, f.Offset)
				}
				next[f.Kind] = max(next[f.Kind], f.Offset+int64(len(f.Data)))
				k.Add(outs[f.Kind].Write(data)...)
			case frame.KindSkip, frame.KindExit:
				flush()
				gap(frame.KindStdout, f.Out)
				gap(frame.KindStderr, f.Err)
			}
		}
		if len(page) < copyPage {
			break
		}
		cursor = page[len(page)-1].ID
	}
	flush()
	return k.Frames(), k.Cut(), k.Kept(), nil
}
