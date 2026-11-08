package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/migrations"
)

var sessionTables = []string{"session_records", "session_events", "session_jobs"}

// newRecord creates a placement and its session record
func newRecord(t *testing.T, s *store.Store, order string) (store.Placement, store.SessionRecord) {
	t.Helper()
	p := newPlacement(t, s, order)
	rec, err := s.CreateSessionRecord(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	return p, rec
}

func insert(t *testing.T, s *store.Store, p uuid.UUID, kind string, always bool) (bool, bool) {
	t.Helper()
	inserted, dropped, err := s.InsertSessionEvent(t.Context(), p, kind, []byte(`{"n":1}`),
		always)
	if err != nil {
		t.Fatal(err)
	}
	return inserted, dropped
}

func eventsDropped(t *testing.T, s *store.Store, id string) int {
	t.Helper()
	rec, err := s.SessionRecordByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return rec.EventsDropped
}

func TestCreateSessionRecordOncePerPlacement(t *testing.T) {
	s := newStore(t)
	p, rec := newRecord(t, s, "rec-1")
	eq(t, regexp.MustCompile(`^[a-z2-7]{16}$`).MatchString(rec.ID), true)
	again, err := s.CreateSessionRecord(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, again.ID, rec.ID)
	eq(t, rec.CopyState, store.CopyPending)
	eq(t, rec.Generation, uint32(0))
	eq(t, rec.Open(time.Now()), true)
	_, other := newRecord(t, s, "rec-2")
	eq(t, other.ID == rec.ID, false)
}

func TestInsertSessionEventCap(t *testing.T) {
	s, pool := newStoreWithPool(t)
	p, rec := newRecord(t, s, "cap-1")
	_, err := pool.Exec(t.Context(), `
		insert into session_events (record_id, at, kind, data)
		select $1, now(), 'mcp', '{}' from generate_series(1, 5000)`, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	inserted, dropped := insert(t, s, p.ID, "mcp", false)
	eq(t, inserted, false)
	eq(t, dropped, true)
	inserted, _ = insert(t, s, p.ID, "allowance", true)
	eq(t, inserted, true)
	eq(t, eventsDropped(t, s, rec.ID), 1)
	events, err := s.SessionEvents(t.Context(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(events), 5001)
}

func TestInsertSessionEventCapSetting(t *testing.T) {
	s, _ := newStoreWithPool(t, store.WithSessionMaxEvents(2))
	p, rec := newRecord(t, s, "cap-2")
	for range 2 {
		inserted, _ := insert(t, s, p.ID, "file", false)
		eq(t, inserted, true)
	}
	_, dropped := insert(t, s, p.ID, "file", false)
	eq(t, dropped, true)
	eq(t, eventsDropped(t, s, rec.ID), 1)
}

func TestRepeatAtTheCapIsNotDropped(t *testing.T) {
	s, pool := newStoreWithPool(t, store.WithSessionMaxEvents(2))
	p, rec := newRecord(t, s, "cap-4")
	// x402 plan X16 adds this index, so egress can send an event again under its ref
	_, err := pool.Exec(t.Context(), `create unique index session_events_ref_test
		on session_events (record_id, (data ->> 'ref')) where data ? 'ref'`)
	if err != nil {
		t.Fatal(err)
	}
	put := func(data string) (bool, bool) {
		inserted, dropped, err := s.InsertSessionEvent(t.Context(), p.ID, "x402", []byte(data),
			false)
		if err != nil {
			t.Fatal(err)
		}
		return inserted, dropped
	}
	inserted, _ := put(`{"ref":"a"}`)
	eq(t, inserted, true)
	inserted, _ = put(`{"n":1}`)
	eq(t, inserted, true)
	inserted, dropped := put(`{"ref":"a"}`)
	eq(t, [2]bool{inserted, dropped}, [2]bool{false, false})
	eq(t, eventsDropped(t, s, rec.ID), 0)
	inserted, dropped = put(`{"ref":"b"}`)
	eq(t, [2]bool{inserted, dropped}, [2]bool{false, true})
	eq(t, eventsDropped(t, s, rec.ID), 1)
	events, err := s.SessionEvents(t.Context(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(events), 2)
}

func TestInsertSessionEventsStoresAllOrNone(t *testing.T) {
	s := newStore(t)
	p, rec := newRecord(t, s, "batch-1")
	err := s.InsertSessionEvents(t.Context(), p.ID, "mcp",
		[][]byte{[]byte(`{"n":1}`), []byte(`not json`)}, false)
	eq(t, err != nil, true)
	err = s.InsertSessionEvents(t.Context(), p.ID, "mcp",
		[][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`)}, false)
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.SessionEvents(t.Context(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(events), 2)
}

func TestInsertsAtOnceNeverPassTheCap(t *testing.T) {
	s, _ := newStoreWithPool(t, store.WithSessionMaxEvents(20))
	p, rec := newRecord(t, s, "cap-3")
	var wg sync.WaitGroup
	for range 40 {
		wg.Go(func() {
			if _, _, err := s.InsertSessionEvent(context.Background(), p.ID, "mcp",
				[]byte(`{}`), false); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	events, err := s.SessionEvents(t.Context(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(events), 20)
	eq(t, eventsDropped(t, s, rec.ID), 20)
}

func TestInsertSessionEventTakesItsTimeFromData(t *testing.T) {
	s := newStore(t)
	p, rec := newRecord(t, s, "at-1")
	at := time.Date(2026, 11, 6, 10, 0, 1, 0, time.UTC)
	if _, _, err := s.InsertSessionEvent(t.Context(), p.ID, "mcp",
		[]byte(`{"at":"2026-11-06T10:00:01Z","tool":"bugnet_exec"}`), false); err != nil {
		t.Fatal(err)
	}
	events, err := s.SessionEvents(t.Context(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, events[0].At, at)
	_, _, err = s.InsertSessionEvent(t.Context(), uuid.New(), "mcp", []byte(`{}`), false)
	eq(t, errors.Is(err, store.ErrNotFound), true)
}

func TestInsertSessionEventOnce(t *testing.T) {
	s := newStore(t)
	p, rec := newRecord(t, s, "once-1")
	for _, want := range []bool{true, false} {
		inserted, err := s.InsertSessionEventOnce(t.Context(), p.ID, "allowance",
			[]byte(`{"phase":"paid"}`), []byte(`{"phase":"paid","amount_micros":5}`), true)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, inserted, want)
	}
	inserted, err := s.InsertSessionEventOnce(t.Context(), p.ID, "allowance",
		[]byte(`{"phase":"refunded"}`), []byte(`{"phase":"refunded"}`), true)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, inserted, true)
	events, _ := s.SessionEvents(t.Context(), rec.ID)
	eq(t, len(events), 2)
}

func TestFoldSessionEventPerMinute(t *testing.T) {
	s := newStore(t)
	p, rec := newRecord(t, s, "fold-1")
	minute := time.Date(2026, 11, 6, 10, 1, 0, 0, time.UTC)
	fold := func(at time.Time, port int, req, bytes int64) {
		t.Helper()
		match, _ := json.Marshal(map[string]any{"port": port, "phase": "traffic"})
		if err := s.FoldSessionEvent(t.Context(), p.ID, "preview", at, match,
			map[string]int64{"requests": req, "bytes": bytes}); err != nil {
			t.Fatal(err)
		}
	}
	fold(minute, 3000, 2, 100)
	fold(minute, 3000, 3, 50)
	fold(minute, 8080, 1, 1)
	fold(minute.Add(time.Minute), 3000, 1, 1)
	events, err := s.SessionEvents(t.Context(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(events), 3)
	eq(t, events[0].At, minute)
	type folded struct {
		Port     int    `json:"port"`
		Phase    string `json:"phase"`
		Requests int64  `json:"requests"`
		Bytes    int64  `json:"bytes"`
	}
	var got []folded
	for _, e := range events {
		var d folded
		if err := json.Unmarshal(e.Data, &d); err != nil {
			t.Fatal(err)
		}
		got = append(got, d)
	}
	eq(t, got[0].Requests+got[1].Requests, int64(6))
	for i, e := range events {
		if got[i].Port == 3000 && e.At.Equal(minute) {
			eq(t, got[i], folded{Port: 3000, Phase: "traffic", Requests: 5, Bytes: 150})
		}
	}
}

func TestSessionJobsBySeq(t *testing.T) {
	s := newStore(t)
	p, rec := newRecord(t, s, "jobs-1")
	now := time.Now()
	for i, id := range []string{"j_a", "j_b", "j_a"} {
		seq, created, err := s.StartSessionJob(t.Context(), p.ID, id, "echo "+id, now)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, seq, map[int]int{0: 1, 1: 2, 2: 1}[i])
		eq(t, created, i < 2)
	}
	code := 3
	seq, started, err := s.FinishSessionJob(t.Context(), p.ID, "j_b", now.Add(time.Second),
		&code)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, seq, 2)
	eq(t, started.Equal(now.UTC().Truncate(time.Microsecond)), true)
	jobs, err := s.SessionJobs(t.Context(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(jobs), 2)
	eq(t, jobs[1].Command, "echo j_b")
	eq(t, *jobs[1].ExitCode, 3)
	eq(t, jobs[0].State, store.JobLost)
	_, _, err = s.FinishSessionJob(t.Context(), p.ID, "j_none", now, nil)
	eq(t, errors.Is(err, store.ErrNotFound), true)
}

func TestEndAndCloseSessionRecord(t *testing.T) {
	s := newStore(t)
	p, rec := newRecord(t, s, "end-1")
	ended := time.Now().UTC().Truncate(time.Second)
	err := s.EndSessionRecord(t.Context(), p.ID, store.SessionEnd{EndedAt: ended,
		Keep: 48 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := s.SessionRecordByID(t.Context(), rec.ID)
	eq(t, *got.EndedAt, ended)
	eq(t, *got.ExpiresAt, ended.Add(48*time.Hour))
	eq(t, got.ClosedAt == nil, true)
	hold := ended.Add(720 * time.Hour)
	p2, rec2 := newRecord(t, s, "end-2")
	err = s.EndSessionRecord(t.Context(), p2.ID, store.SessionEnd{EndedAt: ended,
		Keep: 48 * time.Hour, ClosedBy: store.ClosedByAbuse, HoldUntil: hold})
	if err != nil {
		t.Fatal(err)
	}
	got, _ = s.SessionRecordByID(t.Context(), rec2.ID)
	eq(t, got.ClosedBy, store.ClosedByAbuse)
	eq(t, *got.HoldUntil, hold)
	eq(t, got.Open(time.Now()), false)
	eq(t, got.Held(time.Now()), true)
}

func TestDeleteSessionRecordNow(t *testing.T) {
	s := newStore(t)
	p, rec := newRecord(t, s, "del-1")
	insert(t, s, p.ID, "file", false)
	if _, _, err := s.StartSessionJob(t.Context(), p.ID, "j_1", "ls", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSessionRecordNow(t.Context(), rec.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, _ := s.SessionRecordByID(t.Context(), rec.ID)
	eq(t, got.ClosedBy, store.ClosedByUser)
	events, _ := s.SessionEvents(t.Context(), rec.ID)
	jobs, _ := s.SessionJobs(t.Context(), rec.ID)
	eq(t, len(events)+len(jobs), 0)

	held, hrec := newRecord(t, s, "del-2")
	insert(t, s, held.ID, "file", false)
	if err := s.CloseSessionRecord(t.Context(), held.ID, store.ClosedByOperator,
		time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSessionRecordNow(t.Context(), hrec.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, _ = s.SessionRecordByID(t.Context(), hrec.ID)
	eq(t, got.ClosedBy, store.ClosedByOperator)
	events, _ = s.SessionEvents(t.Context(), hrec.ID)
	eq(t, len(events), 1)
}

func TestRotateSessionGeneration(t *testing.T) {
	s := newStore(t)
	_, rec := newRecord(t, s, "rot-1")
	gen, err := s.RotateSessionGeneration(t.Context(), rec.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, gen, uint32(1))
	// A resent call naming the generation it already ended changes nothing
	gen, err = s.RotateSessionGeneration(t.Context(), rec.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, gen, uint32(1))
}

func TestExpireSessionRecordsKeepsHeldOnes(t *testing.T) {
	s, pool := newStoreWithPool(t)
	_, live := newRecord(t, s, "exp-1")
	gone, past := newRecord(t, s, "exp-2")
	held, hrec := newRecord(t, s, "exp-3")
	_, closed := newRecord(t, s, "exp-4")
	insert(t, s, gone.ID, "file", false)
	ago := time.Now().Add(-time.Hour)
	if _, err := pool.Exec(t.Context(), `update session_records set expires_at = $2
		where id = $1`, past.ID, ago); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `update session_records set closed_at = now()
		where id = $1`, closed.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseSessionRecord(t.Context(), held.ID, store.ClosedByAbuse,
		time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	n, err := s.ExpireSessionRecords(t.Context(), time.Now(), 10_000)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, n, 2)
	for id, want := range map[string]bool{
		live.ID: true, past.ID: false, hrec.ID: true, closed.ID: false,
	} {
		_, err := s.SessionRecordByID(t.Context(), id)
		eq(t, err == nil, want)
	}
	size, err := s.SessionDiskBytes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, size > 0, true)
}

// sessionsVersion is the goose version of the migration that adds the session tables
func sessionsVersion(t *testing.T) int64 {
	t.Helper()
	names, err := fs.Glob(migrations.FS, "*_sessions.sql")
	if err != nil || len(names) != 1 {
		t.Fatalf("the sessions migration: %v %v", names, err)
	}
	v, err := strconv.ParseInt(strings.SplitN(names[0], "_", 2)[0], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestMigrationSessionsUpDownUp(t *testing.T) {
	version := sessionsVersion(t)
	url := testenv.DatabaseURL(t)
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = db.Close() })
	gp, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	count := func() int {
		var n int
		err := db.QueryRowContext(ctx, `
			select count(*) from information_schema.tables
			where table_schema = current_schema() and table_name = any($1)`, sessionTables).
			Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	up := func() {
		if _, err := gp.UpTo(ctx, version); err != nil {
			t.Fatalf("up to %d: %v", version, err)
		}
	}
	up()
	eq(t, count(), 3)
	var compression sql.NullString
	err = db.QueryRowContext(ctx, `
		select attcompression::text from pg_attribute
		where attrelid = 'session_jobs'::regclass and attname = 'frames'`).Scan(&compression)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, compression.String, "l")
	if _, err := gp.DownTo(ctx, version-1); err != nil {
		t.Fatal(err)
	}
	eq(t, count(), 0)
	up()
	eq(t, count(), 3)
}

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
