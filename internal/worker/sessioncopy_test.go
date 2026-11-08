package worker_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-run/frame"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// ended makes an ended placement with a record, its end moved back by ago
func (e *env) ended(t *testing.T, order string, ago time.Duration) (store.Placement,
	store.SessionRecord) {
	t.Helper()
	p, _, err := e.store.CreatePlacement(t.Context(), store.NewPlacement{
		OrderID: order, HandleSHA256: []byte(order), PackageID: smallPkg,
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := e.store.CreateSessionRecord(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-ago)
	if _, err := e.pool.Exec(t.Context(), `update placements set state = 'terminated',
		termination_reason = 'stop', terminated_at = $2 where id = $1`, p.ID, at); err != nil {
		t.Fatal(err)
	}
	if err := e.store.EndSessionRecord(t.Context(), p.ID, store.SessionEnd{EndedAt: at,
		Keep: 48 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	return p, rec
}

// job records a finished job of out bytes, its stream holding them unless gone
func (e *env) job(t *testing.T, p store.Placement, id string, out []byte, gone bool) {
	t.Helper()
	if _, _, err := e.store.CreateJob(t.Context(), store.NewJob{ID: id,
		PlacementID: p.ID}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.FinishJob(t.Context(), id, store.JobOutcome{State: store.JobExited,
		StdoutBytes: int64(len(out))}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.store.StartSessionJob(t.Context(), p.ID, id, "make "+id,
		time.Now()); err != nil {
		t.Fatal(err)
	}
	if gone {
		return
	}
	var frames []frame.Frame
	for off := 0; off < len(out); off += 4096 {
		end := min(off+4096, len(out))
		frames = append(frames, frame.Frame{Kind: frame.KindStdout, Offset: int64(off),
			Data: out[off:end]})
	}
	frames = append(frames, frame.Frame{Kind: frame.KindExit, Out: int64(len(out))})
	for _, f := range frames {
		raw, _ := json.Marshal(f)
		cmd := e.vk.B().Xadd().Key(e.stream(id)).Id("*").FieldValue().
			FieldValue("f", string(raw)).Build()
		if err := e.vk.Do(t.Context(), cmd).Error(); err != nil {
			t.Fatal(err)
		}
	}
}

func (e *env) stream(id string) string { return e.prefix + "job:" + id + ":s" }

// lines is n bytes of numbered lines
func lines(n int) []byte {
	var b strings.Builder
	for i := 0; b.Len() < n; i++ {
		fmt.Fprintf(&b, "line %07d\n", i)
	}
	return []byte(b.String()[:n])
}

// copied runs the copy and returns the record and its jobs' kept stdout by seq
func (e *env) copied(t *testing.T, p store.Placement, rec store.SessionRecord) (
	store.SessionRecord, map[int]string, map[int]int64) {
	t.Helper()
	e.run(t, jobargs.SessionCopy{PlacementID: p.ID})
	got, err := e.store.SessionRecordByID(t.Context(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := e.store.SessionJobs(t.Context(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	text, cut := map[int]string{}, map[int]int64{}
	for _, j := range jobs {
		var frames []sessions.Frame
		if j.Frames != nil {
			if err := json.Unmarshal(j.Frames, &frames); err != nil {
				t.Fatal(err)
			}
		}
		var b strings.Builder
		for _, f := range frames {
			b.WriteString(f.T)
		}
		text[j.Seq], cut[j.Seq] = b.String(), j.CutBytes
	}
	return got, text, cut
}

func TestCopyKeepsTheNewest64KiB(t *testing.T) {
	e := newEnv(t, options{})
	e.start(t)
	p, rec := e.ended(t, "copy-1", time.Second)
	out := lines(100 << 10)
	e.job(t, p, "j_1", out, false)
	got, text, cut := e.copied(t, p, rec)
	eq(t, got.CopyState, store.CopyComplete)
	eq(t, len(text[1]), 64<<10)
	eq(t, text[1], string(out[len(out)-64<<10:]))
	eq(t, cut[1], int64(36<<10))
}

func TestCopyPlacementBudget(t *testing.T) {
	e := newEnv(t, options{sessions: config.Sessions{PlacementOutput: 96 << 10}})
	e.start(t)
	p, rec := e.ended(t, "copy-2", time.Second)
	for i := range 3 {
		e.job(t, p, fmt.Sprintf("j_%d", i+1), lines(100<<10), false)
	}
	_, text, _ := e.copied(t, p, rec)
	// The newest job keeps the most, earlier ones what is left, then their last 8 KiB
	eq(t, len(text[3]), 64<<10)
	eq(t, len(text[2]), 32<<10)
	eq(t, len(text[1]), 8<<10)
}

func TestCopyAfterStreamsExpired(t *testing.T) {
	e := newEnv(t, options{})
	e.start(t)
	p, rec := e.ended(t, "copy-3", time.Second)
	e.job(t, p, "j_1", lines(1000), true)
	if _, _, err := e.store.InsertSessionEvent(t.Context(), p.ID, "file", []byte(`{}`),
		false); err != nil {
		t.Fatal(err)
	}
	got, text, _ := e.copied(t, p, rec)
	eq(t, got.CopyState, store.CopyExpired)
	eq(t, text[1], "")
	events, _ := e.store.SessionEvents(t.Context(), rec.ID)
	eq(t, len(events), 1)
	// A copy whose window passed sets expired without reading anything
	late, lrec := e.ended(t, "copy-4", 11*time.Minute)
	e.job(t, late, "j_2", lines(1000), false)
	got, text, _ = e.copied(t, late, lrec)
	eq(t, got.CopyState, store.CopyExpired)
	eq(t, text[1], "")
	// One stream of two gone is partial
	half, hrec := e.ended(t, "copy-5", time.Second)
	e.job(t, half, "j_3", lines(100), false)
	e.job(t, half, "j_4", lines(100), true)
	got, _, _ = e.copied(t, half, hrec)
	eq(t, got.CopyState, store.CopyPartial)
}

func TestCopyUnlinksTheStreams(t *testing.T) {
	e := newEnv(t, options{})
	e.start(t)
	p, rec := e.ended(t, "copy-6", time.Second)
	e.job(t, p, "j_1", []byte("hello bnsec_"+strings.Repeat("z", 26)+"\n"), false)
	_, text, _ := e.copied(t, p, rec)
	eq(t, text[1], "hello [redacted token]\n")
	n, err := e.vk.Do(t.Context(), e.vk.B().Exists().Key(e.stream("j_1")).Build()).AsInt64()
	eq(t, err, nil)
	eq(t, n, int64(0))
	// A second copy of the same placement changes nothing
	e.run(t, jobargs.SessionCopy{PlacementID: p.ID})
	_, again, _ := e.copied(t, p, rec)
	eq(t, again[1], text[1])
}

func TestCopyDiskBudget(t *testing.T) {
	e := newEnv(t, options{sessions: config.Sessions{DiskBudget: 1}})
	e.start(t)
	percent, err := e.worker.SessionDiskPercent(t.Context())
	eq(t, err, nil)
	eq(t, percent >= 100, true)
	p, rec := e.ended(t, "copy-7", time.Second)
	e.job(t, p, "j_1", lines(100<<10), false)
	got, text, _ := e.copied(t, p, rec)
	eq(t, got.CopyState, store.CopyPartial)
	eq(t, len(text[1]), 8<<10)
	// Events still land at 100%
	inserted, _, err := e.store.InsertSessionEvent(t.Context(), p.ID, "file", []byte(`{}`),
		false)
	eq(t, err, nil)
	eq(t, inserted, true)
}

func TestExpiryKeepsAHeldRecord(t *testing.T) {
	e := newEnv(t, options{})
	e.start(t)
	gone, grec := e.ended(t, "exp-1", time.Second)
	held, hrec := e.ended(t, "exp-2", time.Second)
	_, live := e.ended(t, "exp-3", time.Second)
	if err := e.store.CloseSessionRecord(t.Context(), held.ID, store.ClosedByAbuse,
		time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(t.Context(), `update session_records set expires_at = now()
		where placement_id = any($1)`, []uuid.UUID{gone.ID, held.ID}); err != nil {
		t.Fatal(err)
	}
	e.run(t, jobargs.SessionExpiry{})
	for id, want := range map[string]bool{grec.ID: false, hrec.ID: true, live.ID: true} {
		_, err := e.store.SessionRecordByID(t.Context(), id)
		eq(t, err == nil, want)
	}
}

func TestSessionJobsRunOnTheirQueues(t *testing.T) {
	e := newEnv(t, options{})
	p, h := e.placeReady(t, "q-1")
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	var queue string
	err := e.pool.QueryRow(t.Context(), `select queue from river_job where kind = 'session_copy'
		and args->>'placement_id' = $1`, p.ID.String()).Scan(&queue)
	eq(t, err, nil)
	eq(t, queue, jobargs.QueueDataplane)
}

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
