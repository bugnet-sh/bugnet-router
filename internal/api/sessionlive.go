package api

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-run/frame"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/streams"
)

// Why a live stream ended, as SessionEnd names it
const (
	liveExited = "exited"
	liveEnded  = "ended"
	liveClosed = "closed"
	liveLimit  = "limit"
)

const (
	liveHeartbeat = 15 * time.Second
	// liveMaxTime ends a stream the page then resumes, so no stream holds a reader for long
	liveMaxTime = 30 * time.Minute
	// liveGrace keeps the placement's live byte count past its deadline, as job streams are
	liveGrace = 10 * time.Minute
)

// liveRecheck is how often a stream re-reads its record and placement, since pub/sub can drop
var liveRecheck = 5 * time.Second

// liveView serves the session page's live output on its own Valkey client and byte budget
type liveView struct {
	vk      valkey.Client
	bus     *streams.Bus
	prefix  string
	cfg     config.SessionsLive
	readers chan struct{}
}

func newLiveView(vk valkey.Client, prefix string, cfg config.SessionsLive) *liveView {
	return &liveView{
		vk: vk, bus: streams.NewBus(vk, prefix), prefix: prefix, cfg: cfg,
		readers: make(chan struct{}, max(cfg.MaxReaders, 1)),
	}
}

// liveOpen says whether the record's page may stream the placement's output at now
func liveOpen(rec store.SessionRecord, p store.Placement, now time.Time) bool {
	return rec.Open(now) && p.State == store.PlacementReady && p.Deadline != nil &&
		now.Before(*p.Deadline)
}

// WatchSessionJob streams one job's cleaned output while the placement runs
func (ss *SessionPageServer) WatchSessionJob(
	ctx context.Context, req *connect.Request[routerv1.WatchSessionJobRequest],
	stream *connect.ServerStream[routerv1.WatchSessionJobResponse],
) error {
	s, lv, m := ss.s, ss.live, req.Msg
	if !s.d.Sessions.Live.Enabled {
		return withInfo(connect.NewError(connect.CodeFailedPrecondition, errNoLive),
			reasonFeatureUnavailable)
	}
	rec, p, err := s.sessionFor(ctx, m.GetId(), m.GetKey())
	if err == nil && !liveOpen(rec, p, time.Now()) {
		err = errSessionUnknown
	}
	var job store.SessionJob
	if err == nil {
		job, err = s.d.Store.SessionJobBySeq(ctx, rec.ID, int(m.GetSeq()))
		if errors.Is(err, store.ErrNotFound) {
			err = errSessionUnknown
		}
	}
	if err != nil {
		return s.status(err)
	}
	ls := &liveStream{ss: ss, rec: rec, p: p, job: job, send: stream.Send, started: time.Now()}
	select {
	case lv.readers <- struct{}{}:
		defer func() { <-lv.readers }()
	default:
		return ls.end(liveLimit)
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(s.ctx, cancel)()
	err = ls.run(wctx, m.GetCursor())
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return s.status(ctx.Err())
	case s.ctx.Err() != nil:
		return s.status(errShuttingDown)
	}
	return s.status(err)
}

// liveStream is one page's live view of one job
type liveStream struct {
	ss  *SessionPageServer
	rec store.SessionRecord
	p   store.Placement
	job store.SessionJob
	// pos is the stream entry read last, empty to read from the stream's start
	pos      string
	send     func(*routerv1.WatchSessionJobResponse) error
	out, err *sessions.Stream
	// outAt and errAt are how far each output was read, -1 until known
	outAt, errAt int64
	started      time.Time
	sent         int64
	beat         time.Time
	checked      time.Time
}

func (ls *liveStream) end(reason string) error {
	return ls.send(&routerv1.WatchSessionJobResponse{Frame: &routerv1.WatchSessionJobResponse_End{
		End: &routerv1.SessionEnd{Reason: reason},
	}})
}

// held reports whether text waits for a later frame, so no cursor may be given yet
func (ls *liveStream) held() bool { return ls.out.Holding() || ls.err.Holding() }

// at is the cursor a frame carries, empty while text is held back
func (ls *liveStream) at() string {
	if ls.held() || ls.pos == "" {
		return ""
	}
	s := ls.ss.s
	pos := ls.pos + "." + strconv.FormatInt(ls.outAt, 10) + "." + strconv.FormatInt(ls.errAt, 10)
	c, err := s.d.Keys.SessionCursor(s.sessionKeyID(ls.p), ls.rec.ID, ls.rec.Generation,
		uint32(ls.job.Seq), pos)
	if err != nil {
		return ""
	}
	return c
}

// resume takes the position a cursor of this record's job seals, false for any other cursor
func (ls *liveStream) resume(cursor string) bool {
	s := ls.ss.s
	pos, ok := s.d.Keys.OpenSessionCursor(s.sessionKeyID(ls.p), ls.rec.ID, ls.rec.Generation,
		uint32(ls.job.Seq), cursor)
	parts := strings.Split(pos, ".")
	if !ok || len(parts) != 3 {
		return false
	}
	out, err1 := strconv.ParseInt(parts[1], 10, 64)
	errAt, err2 := strconv.ParseInt(parts[2], 10, 64)
	if err1 != nil || err2 != nil {
		return false
	}
	ls.pos, ls.outAt, ls.errAt = parts[0], out, errAt
	return true
}

func (ls *liveStream) run(ctx context.Context, cursor string) error {
	s, lv := ls.ss.s, ls.ss.live
	red := sessions.KnownRedactor(ls.p, s.d.Keys)
	ls.out, ls.err = red.Stream(sessions.Stdout), red.Stream(sessions.Stderr)
	ls.outAt, ls.errAt = -1, -1
	// A cursor the page made up could resume inside a token, so it replays as a first connection
	resumed := cursor != "" && ls.resume(cursor)
	// The first frame tells the sessions role the key was taken
	if err := ls.heartbeat(); err != nil {
		return err
	}
	if !resumed {
		r, err := sessions.ReplayStart(ctx, lv.bus, ls.job.JobID, lv.cfg.Replay)
		if err != nil {
			return err
		}
		ls.outAt, ls.errAt = r.Out, r.Err
		// A replay may start inside a token or a key block, so what it may have cut is dropped
		late(ls.out, r.Out, r.MidOut)
		late(ls.err, r.Err, r.MidErr)
		if r.Skip {
			ls.pos = r.Cursor
			if err := ls.skip(r.Out, r.Err); err != nil {
				return err
			}
		}
	}
	ls.checked = time.Now()
	for {
		now := time.Now()
		if now.Sub(ls.started) >= liveMaxTime {
			return ls.end(liveLimit)
		}
		if now.Sub(ls.checked) >= liveRecheck || !now.Before(*ls.p.Deadline) {
			if reason, done, err := ls.recheck(ctx); err != nil || done {
				if err != nil {
					return err
				}
				return ls.end(reason)
			}
		}
		block := min(liveRecheck-time.Since(ls.checked), time.Until(*ls.p.Deadline))
		entries, err := lv.bus.Read(ctx, ls.job.JobID, ls.pos, max(block, time.Millisecond))
		if err != nil {
			return err
		}
		for _, e := range entries {
			reason, done, err := ls.entry(ctx, e)
			if err != nil || done {
				if err != nil {
					return err
				}
				return ls.end(reason)
			}
		}
		if time.Since(ls.beat) >= liveHeartbeat {
			if err := ls.heartbeat(); err != nil {
				return err
			}
		}
	}
}

// recheck ends the stream once the record closed, its link moved on or the placement ended
func (ls *liveStream) recheck(ctx context.Context) (string, bool, error) {
	st := ls.ss.s.d.Store
	ls.checked = time.Now()
	rec, err := st.SessionRecordByID(ctx, ls.rec.ID)
	if errors.Is(err, store.ErrNotFound) {
		return liveClosed, true, nil
	}
	if err != nil {
		return "", false, err
	}
	if !rec.Open(time.Now()) || rec.Generation != ls.rec.Generation {
		return liveClosed, true, nil
	}
	p, err := st.PlacementByID(ctx, ls.p.ID)
	if err != nil {
		return "", false, err
	}
	ls.p = p
	if !liveOpen(rec, p, time.Now()) {
		return liveEnded, true, nil
	}
	return "", false, nil
}

// entry sends what one stream entry holds, done once the job's stream ended
func (ls *liveStream) entry(ctx context.Context, e streams.Entry) (string, bool, error) {
	f := e.Frame
	var texts []sessions.Frame
	switch f.Kind {
	case frame.KindStdout:
		texts = write(ls.out, &ls.outAt, f)
	case frame.KindStderr:
		texts = write(ls.err, &ls.errAt, f)
	case frame.KindSkip:
		texts = ls.moved(f)
	case frame.KindExit:
		texts = append(ls.moved(f), append(ls.out.Flush(), ls.err.Flush()...)...)
	case frame.KindError:
		texts = append(ls.out.Flush(), ls.err.Flush()...)
	}
	ls.pos = e.ID
	if reason, done, err := ls.texts(ctx, texts); err != nil || done {
		return reason, done, err
	}
	if f.Kind == frame.KindSkip {
		return "", false, ls.skip(f.Out, f.Err)
	}
	if !streams.Terminal(f) {
		return "", false, nil
	}
	if f.Kind == frame.KindExit {
		err := ls.send(&routerv1.WatchSessionJobResponse{Cursor: ls.at(),
			Frame: &routerv1.WatchSessionJobResponse_Exit{Exit: &routerv1.JobExit{
				ExitCode: int32(f.Code), StdoutBytes: uint64(f.Out), StderrBytes: uint64(f.Err),
				OomKilled: f.OOM, PeakMemoryMib: f.PeakMiB,
			}}})
		if err != nil {
			return "", false, err
		}
	}
	// A job cut short by its placement's end or a closed record says so, not exited
	if reason, done, err := ls.recheck(ctx); err != nil || done {
		return reason, true, err
	}
	return liveExited, true, nil
}

// moved starts each output a skip or exit frame moves past what was read anew, as after a gap
func (ls *liveStream) moved(f frame.Frame) []sessions.Frame {
	var texts []sessions.Frame
	if ls.outAt < 0 || f.Out > ls.outAt {
		texts = append(texts, ls.out.Resume()...)
	}
	if ls.errAt < 0 || f.Err > ls.errAt {
		texts = append(texts, ls.err.Resume()...)
	}
	ls.outAt, ls.errAt = max(ls.outAt, f.Out), max(ls.errAt, f.Err)
	return texts
}

// late starts an output its replay reads from offset at, mid saying inside a line
func late(st *sessions.Stream, at int64, mid bool) {
	switch {
	case mid:
		st.Resume()
	case at > 0:
		st.Late()
	}
}

// write passes an output's frame through its stream, which starts anew past a gap in the offsets
func write(st *sessions.Stream, at *int64, f frame.Frame) []sessions.Frame {
	var texts []sessions.Frame
	if *at >= 0 && f.Offset > *at {
		texts = st.Resume()
	}
	*at = max(*at, f.Offset+int64(len(f.Data)))
	return append(texts, st.Write(f.Data)...)
}

// texts spends the placement's live bytes and the stream's pace on frames and sends them
func (ls *liveStream) texts(ctx context.Context, texts []sessions.Frame) (string, bool, error) {
	lv := ls.ss.live
	for i, t := range texts {
		n := int64(len(t.T))
		used, err := lv.spend(ctx, ls.p, n)
		if err != nil {
			return "", false, err
		}
		if used > lv.cfg.PlacementBytes {
			return liveLimit, true, nil
		}
		if err := ls.pace(ctx, n); err != nil {
			return "", false, err
		}
		out := routerv1.OutputStream_OUTPUT_STREAM_STDOUT
		if t.S == sessions.Stderr {
			out = routerv1.OutputStream_OUTPUT_STREAM_STDERR
		}
		msg := &routerv1.WatchSessionJobResponse{Frame: &routerv1.WatchSessionJobResponse_Text{
			Text: &routerv1.SessionText{Stream: out, Text: t.T, Overwrite: t.CR},
		}}
		if i == len(texts)-1 {
			msg.Cursor = ls.at()
		}
		if err := ls.send(msg); err != nil {
			return "", false, err
		}
		ls.beat = time.Now()
	}
	return "", false, nil
}

// pace holds the stream to its bytes a second
func (ls *liveStream) pace(ctx context.Context, n int64) error {
	rate := ls.ss.live.cfg.BytesPerSecond
	ls.sent += n
	if rate <= 0 {
		return nil
	}
	due := ls.started.Add(time.Duration(float64(ls.sent) / float64(rate) * float64(time.Second)))
	wait := time.Until(due)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (ls *liveStream) skip(out, errBytes int64) error {
	ls.beat = time.Now()
	return ls.send(&routerv1.WatchSessionJobResponse{Cursor: ls.at(),
		Frame: &routerv1.WatchSessionJobResponse_Skip{Skip: &routerv1.JobSkip{
			StdoutOffset: uint64(max(out, 0)), StderrOffset: uint64(max(errBytes, 0)),
		}}})
}

func (ls *liveStream) heartbeat() error {
	ls.beat = time.Now()
	return ls.send(&routerv1.WatchSessionJobResponse{Cursor: ls.at(),
		Frame: &routerv1.WatchSessionJobResponse_Heartbeat{Heartbeat: true}})
}

// spend counts live bytes in the placement's own counter, never in the edge budget
func (lv *liveView) spend(ctx context.Context, p store.Placement, n int64) (int64, error) {
	key := lv.prefix + "live:used:" + p.ID.String()
	expire := p.Deadline.Add(liveGrace)
	res := lv.vk.DoMulti(ctx,
		lv.vk.B().Incrby().Key(key).Increment(n).Build(),
		lv.vk.B().Pexpireat().Key(key).MillisecondsTimestamp(expire.UnixMilli()).Build())
	used, err := res[0].AsInt64()
	if err == nil {
		err = res[1].Error()
	}
	return used, err
}
