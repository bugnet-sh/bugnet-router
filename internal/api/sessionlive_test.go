package api_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/api"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/streams"
)

// liveWatch is what one WatchSessionJob call sent, in order
type liveWatch struct {
	frames []*routerv1.WatchSessionJobResponse
	err    error
}

func (w liveWatch) text() string {
	var b strings.Builder
	for _, f := range w.frames {
		b.WriteString(f.GetText().GetText())
	}
	return b.String()
}

func (w liveWatch) end() string {
	if n := len(w.frames); n > 0 {
		return w.frames[n-1].GetEnd().GetReason()
	}
	return ""
}

// watchSession reads one job's live stream until it ends or ctx does
func (e *env) watchSession(
	ctx context.Context, t *testing.T, url string, seq uint32, cursor string,
	each func(*routerv1.WatchSessionJobResponse),
) liveWatch {
	t.Helper()
	id, key := link(t, url)
	stream, err := e.sessionPage(t).WatchSessionJob(ctx, connect.NewRequest(
		&routerv1.WatchSessionJobRequest{Id: id, Key: key, Seq: seq, Cursor: cursor}))
	if err != nil {
		return liveWatch{err: err}
	}
	defer func() { _ = stream.Close() }()
	var w liveWatch
	for stream.Receive() {
		w.frames = append(w.frames, stream.Msg())
		if each != nil {
			each(stream.Msg())
		}
	}
	w.err = stream.Err()
	return w
}

// startJob starts a script and returns before it ends
func (e *env) startJob(t *testing.T, h, script string) {
	t.Helper()
	req := sh(h, script)
	req.WaitMs = 1
	e.exec(t, req)
}

func liveSettings(adjust func(*config.SessionsLive)) *config.Sessions {
	s := sessionSettings
	adjust(&s.Live)
	return &s
}

func TestWatchSessionJob(t *testing.T) {
	api.SetLiveRecheck(t, 100*time.Millisecond)
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	tok := "bnsec_" + strings.Repeat("a", 26)
	e.exec(t, sh(h, "echo one; echo "+tok+"; sleep 1; printf 'a\\rb\\n' >&2; sleep 1; echo two"))
	w := e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, "", nil)
	eq(t, w.err, nil)
	eq(t, w.frames[0].GetHeartbeat(), true)
	eq(t, w.text(), "one\n[redacted token]\nab\ntwo\n")
	eq(t, strings.Contains(w.text(), "bnsec_"), false)
	var cr *routerv1.SessionText
	var cursor, rest string
	for _, f := range w.frames {
		if f.GetText().GetOverwrite() {
			cr = f.GetText()
		}
		switch {
		case cursor != "":
			rest += f.GetText().GetText()
		case strings.HasPrefix(f.GetText().GetText(), "one\n"):
			cursor = f.GetCursor()
		}
	}
	eq(t, cr.GetText(), "b\n")
	eq(t, cr.GetStream(), routerv1.OutputStream_OUTPUT_STREAM_STDERR)
	n := len(w.frames)
	eq(t, w.frames[n-2].GetExit().GetExitCode(), int32(0))
	eq(t, w.end(), "exited")
	// A resume after a cursor never repeats the text before it
	eq(t, cursor != "", true)
	again := e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, cursor, nil)
	eq(t, again.text(), rest)
	eq(t, strings.HasSuffix(rest, "ab\ntwo\n"), true)
	// An unknown seq answers as an unknown id does
	_, err := e.watchFirstSession(t, p.GetSessionUrl(), 9)
	mustCode(t, err, connect.CodeNotFound, "SESSION_UNKNOWN")
}

// watchFirstSession opens a stream and reads its first frame
func (e *env) watchFirstSession(
	t *testing.T, url string, seq uint32,
) (*routerv1.WatchSessionJobResponse, error) {
	t.Helper()
	id, key := link(t, url)
	stream, err := e.sessionPage(t).WatchSessionJob(timeout(t), connect.NewRequest(
		&routerv1.WatchSessionJobRequest{Id: id, Key: key, Seq: seq}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = stream.Close() }()
	if !stream.Receive() {
		return nil, stream.Err()
	}
	return stream.Msg(), nil
}

func TestWatchSessionJobSkipsToTheReplay(t *testing.T) {
	e := newEnv(t, options{sessions: liveSettings(func(l *config.SessionsLive) {
		l.Replay = 2000
	})})
	h, p := e.readyPlacement(t)
	// Each line is its own frame, so the replay can start partway
	e.exec(t, &routerv1.ExecRequest{Handle: h, Argv: []string{"sh", "-c",
		"i=1; while [ $i -le 60 ]; do printf 'line-%04d %040d\\n' $i 0; i=$((i+1)); " +
			"sleep 0.02; done"}, Until: &routerv1.Until{Exit: true}, WaitMs: 20000})
	w := e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, "", nil)
	eq(t, w.err, nil)
	var skip *routerv1.JobSkip
	for _, f := range w.frames {
		if f.GetSkip() != nil {
			skip = f.GetSkip()
		}
	}
	eq(t, skip != nil, true)
	txt := w.text()
	eq(t, skip.GetStdoutOffset(), uint64(3060-len(txt)))
	eq(t, strings.Contains(txt, "line-0001"), false)
	eq(t, strings.HasPrefix(txt, "line-"), true)
	eq(t, strings.HasSuffix(txt, "line-0060 "+strings.Repeat("0", 40)+"\n"), true)
	eq(t, len(txt) >= 2000 && len(txt) < 3060, true)
	eq(t, w.end(), "exited")
}

func TestWatchSessionJobEnds(t *testing.T) {
	api.SetLiveRecheck(t, 100*time.Millisecond)
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.startJob(t, h, "echo started; sleep 20")
	got := make(chan liveWatch, 1)
	go func() { got <- e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, "", nil) }()
	time.Sleep(500 * time.Millisecond)
	id, key := link(t, p.GetSessionUrl())
	if _, err := e.sessionPage(t).DeleteSessionRecord(t.Context(), connect.NewRequest(
		&routerv1.DeleteSessionRecordRequest{Id: id, Key: key})); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-got:
		eq(t, w.end(), "closed")
		eq(t, strings.Contains(w.text(), "started"), true)
	case <-time.After(5 * time.Second):
		t.Fatal("a deleted record's stream did not end within 5 s")
	}
	e.stop(t, h)

	h2, p2 := e.readyPlacement(t)
	e.startJob(t, h2, "sleep 20")
	go func() { got <- e.watchSession(timeout(t), t, p2.GetSessionUrl(), 1, "", nil) }()
	time.Sleep(500 * time.Millisecond)
	e.stop(t, h2)
	select {
	case w := <-got:
		eq(t, w.end(), "ended")
	case <-time.After(5 * time.Second):
		t.Fatal("an ended placement's stream did not end within 5 s")
	}
	// An ended placement has no live view
	_, err := e.watchFirstSession(t, p2.GetSessionUrl(), 1)
	mustCode(t, err, connect.CodeNotFound, "SESSION_UNKNOWN")
}

func TestWatchSessionJobEndsOnRotate(t *testing.T) {
	api.SetLiveRecheck(t, 100*time.Millisecond)
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.startJob(t, h, "echo started; sleep 20")
	got := make(chan liveWatch, 1)
	go func() { got <- e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, "", nil) }()
	time.Sleep(500 * time.Millisecond)
	if _, err := e.client.RotateSessionLink(t.Context(), connect.NewRequest(
		&routerv1.RotateSessionLinkRequest{Handle: h, SessionUrl: p.GetSessionUrl()},
	)); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-got:
		eq(t, w.end(), "closed")
		eq(t, strings.Contains(w.text(), "started"), true)
	case <-time.After(5 * time.Second):
		t.Fatal("a rotated link's stream did not end within 5 s")
	}
}

func TestWatchSessionJobReplayDropsACutToken(t *testing.T) {
	e := newEnv(t, options{sessions: liveSettings(func(l *config.SessionsLive) {
		l.Replay = 30
	})})
	h, p := e.readyPlacement(t)
	// The token's prefix and body are separate frames, and the replay starts at the body
	e.exec(t, &routerv1.ExecRequest{Handle: h, Argv: []string{"sh", "-c",
		"printf 'export T=bnt_'; sleep 0.3; printf '%030d\\n' 7; sleep 0.3; echo done"},
		Until: &routerv1.Until{Exit: true}, WaitMs: 20000})
	w := e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, "", nil)
	eq(t, w.err, nil)
	eq(t, w.text(), "done\n")
	eq(t, w.end(), "exited")
}

func TestWatchSessionJobRefusesAMadeUpCursor(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.exec(t, &routerv1.ExecRequest{Handle: h, Argv: []string{"sh", "-c",
		"printf 'export T=bnt_'; sleep 0.3; printf '%030d\\n' 7; sleep 0.3; echo done"},
		Until: &routerv1.Until{Exit: true}, WaitMs: 20000})
	const view = "export T=[redacted token]\ndone\n"
	first := e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, "", nil)
	eq(t, first.err, nil)
	eq(t, first.text(), view)
	job, err := e.store.SessionJobBySeq(t.Context(), e.sessionRecord(t, p).ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := streams.NewBus(e.vk, e.prefix).After(t.Context(), job.JobID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	prefix := ""
	for _, en := range entries {
		if string(en.Frame.Data) == "export T=bnt_" {
			prefix = en.ID
		}
	}
	eq(t, prefix != "", true)
	// The entry that ends inside the token, which the router never gives as a cursor
	forged := []string{prefix, prefix + ".13.0", prefix + ".13.0." + strings.Repeat("A", 22)}
	for _, cursor := range forged {
		w := e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, cursor, nil)
		eq(t, w.err, nil)
		eq(t, w.text(), view)
	}
	// A cursor the router gave resumes after its text
	var given string
	for _, f := range first.frames {
		if f.GetCursor() != "" && f.GetText() != nil {
			given = f.GetCursor()
			break
		}
	}
	eq(t, given != "", true)
	again := e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, given, nil)
	eq(t, again.err, nil)
	eq(t, strings.HasSuffix(view, again.text()), true)
	eq(t, len(again.text()) < len(view), true)
}

func TestWatchSessionJobLimits(t *testing.T) {
	e := newEnv(t, options{sessions: liveSettings(func(l *config.SessionsLive) {
		l.MaxReaders = 1
		l.PlacementBytes = 64
		l.BytesPerSecond = 1000
	})})
	h, p := e.readyPlacement(t)
	e.startJob(t, h, "echo first; sleep 20")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	opened := make(chan struct{})
	done := make(chan liveWatch, 1)
	go func() {
		done <- e.watchSession(ctx, t, p.GetSessionUrl(), 1, "", func(
			*routerv1.WatchSessionJobResponse) {
			select {
			case <-opened:
			default:
				close(opened)
			}
		})
	}()
	<-opened
	full, err := e.watchFirstSession(t, p.GetSessionUrl(), 1)
	eq(t, err, nil)
	eq(t, full.GetEnd().GetReason(), "limit")
	cancel()
	<-done

	// Past the placement's live bytes the stream ends with limit
	e.exec(t, sh(h, "for i in $(seq 1 20); do echo 0123456789; done"))
	w := e.watchSession(timeout(t), t, p.GetSessionUrl(), 2, "", nil)
	eq(t, w.end(), "limit")
	if len(w.text()) > 64 {
		t.Fatalf("sent %d bytes past a cap of 64: %q", len(w.text()), w.text())
	}
}

func TestWatchSessionJobPace(t *testing.T) {
	e := newEnv(t, options{sessions: liveSettings(func(l *config.SessionsLive) {
		l.BytesPerSecond = 1000
	})})
	h, p := e.readyPlacement(t)
	e.exec(t, &routerv1.ExecRequest{Handle: h, Argv: []string{"sh", "-c",
		"for i in $(seq 1 200); do echo 0123456789; done"}, Until: &routerv1.Until{Exit: true},
		WaitMs: 20000})
	start := time.Now()
	w := e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, "", nil)
	eq(t, len(w.text()), 2200)
	if took := time.Since(start); took < 1500*time.Millisecond {
		t.Fatalf("2200 bytes at 1000 a second took %v", took)
	}
}

// Settings are read at start, so turning the live view off takes a restart that ends the streams
func TestLiveKillSwitch(t *testing.T) {
	e := newEnv(t, options{sessions: liveSettings(func(l *config.SessionsLive) {
		l.Enabled = false
	})})
	h, p := e.readyPlacement(t)
	e.exec(t, sh(h, "echo hi"))
	_, err := e.watchFirstSession(t, p.GetSessionUrl(), 1)
	mustCode(t, err, connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")
	// The snapshot still answers, it holds no output
	res, err := e.record(t, p.GetSessionUrl())
	eq(t, err, nil)
	eq(t, res.GetState(), "running")
}

func TestLiveBytesNeverSpendTheEdgeBudget(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.exec(t, &routerv1.ExecRequest{Handle: h, Argv: []string{"sh", "-c", "echo 0123456789"},
		Until: &routerv1.Until{Exit: true}, WaitMs: 20000})
	w := e.watchSession(timeout(t), t, p.GetSessionUrl(), 1, "", nil)
	eq(t, w.text(), "0123456789\n")
	used, err := e.vk.Do(t.Context(), e.vk.B().Get().
		Key(e.prefix+"live:used:"+p.GetPlacementId()).Build()).AsInt64()
	eq(t, err, nil)
	eq(t, used, int64(11))
	edge, err := e.vk.Do(t.Context(), e.vk.B().Keys().Pattern(e.prefix+"edge:*").Build()).
		AsStrSlice()
	eq(t, err, nil)
	eq(t, len(edge), 0)
}
