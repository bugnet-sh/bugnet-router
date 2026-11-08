package stats

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// fakeSource answers like the store, counting reads, and blocks page queries while block is open
type fakeSource struct {
	mu      sync.Mutex
	snap    store.StatsSnapshot
	snapErr error
	reads   int
	rows    []store.StatsSessionRow
	listErr error
	lists   int
	since   time.Time
	before  time.Time
	id      uuid.UUID
	limit   int
	block   chan struct{}
	started chan struct{}
}

func (f *fakeSource) ReadStatsSnapshot(context.Context) (store.StatsSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	return f.snap, f.snapErr
}

func (f *fakeSource) ListStatsSessions(
	_ context.Context, since, beforeAt time.Time, beforeID uuid.UUID, limit int,
) ([]store.StatsSessionRow, error) {
	f.mu.Lock()
	f.lists++
	f.since, f.before, f.id, f.limit = since, beforeAt, beforeID, limit
	block, started := f.block, f.started
	rows, err := f.rows, f.listErr
	f.mu.Unlock()
	if started != nil {
		started <- struct{}{}
	}
	if block != nil {
		<-block
	}
	return rows, err
}

func (f *fakeSource) set(fn func(*fakeSource)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

// clock is a settable now
type clock struct {
	mu sync.Mutex
	at time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

const servedBody = `{"version":1}`

func newHandler(t *testing.T, lines ...string) (*Handler, *fakeSource, *clock) {
	t.Helper()
	src := &fakeSource{snap: store.StatsSnapshot{Body: []byte(servedBody), RouterAt: t0}}
	c := &clock{at: t0}
	return NewHandler(src, testCatalog(t), cfg(t, lines...), c.now, nil), src, c
}

// answer is what one request got back
type answer struct {
	status int
	header http.Header
	body   string
}

func call(h http.Handler, method, target string, header ...string) answer {
	r := httptest.NewRequest(method, target, nil)
	r.RemoteAddr = "127.0.0.1:40000"
	for i := 0; i+1 < len(header); i += 2 {
		r.Header.Add(header[i], header[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	body, _ := io.ReadAll(w.Result().Body)
	return answer{status: w.Code, header: w.Result().Header, body: string(body)}
}

func TestHandlerAnswers(t *testing.T) {
	h, _, _ := newHandler(t)
	cases := []struct {
		name, method, target string
		status               int
		body, cache, retry   string
	}{
		{"summary", "GET", "/api/stats", 200, servedBody, "public, max-age=30", ""},
		{"summary ignores the query", "GET", "/api/stats?before=x&a=1", 200, servedBody,
			"public, max-age=30", ""},
		{"first page", "GET", "/api/stats/sessions", 200, `{"items":[],"next":null}`,
			"public, max-age=30", ""},
		{"empty before", "GET", "/api/stats/sessions?before=", 200,
			`{"items":[],"next":null}`, "public, max-age=30", ""},
		{"bad cursor", "GET", "/api/stats/sessions?before=s1.x", 400,
			`{"error":"bad_cursor"}`, "no-store", ""},
		{"post", "POST", "/api/stats", 405, `{"error":"method_not_allowed"}`, "no-store", ""},
		{"payments is not the router's", "GET", "/api/stats/payments", 404,
			`{"error":"not_found"}`, "no-store", ""},
		{"other path", "GET", "/", 404, `{"error":"not_found"}`, "no-store", ""},
	}
	for _, c := range cases {
		a := call(h, c.method, c.target)
		if a.status != c.status || a.body != c.body ||
			a.header.Get("Cache-Control") != c.cache {
			t.Errorf("%s: %d %s %q", c.name, a.status, a.body, a.header.Get("Cache-Control"))
		}
		if a.header.Get("Content-Type") != "application/json" ||
			a.header.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s: headers %v", c.name, a.header)
		}
	}
	eq(t, call(h, "PUT", "/api/stats/sessions").header.Get("Allow"), "GET, HEAD")

	// No body stored yet, before the first run or before Payments first answered
	h, src, _ := newHandler(t)
	src.set(func(f *fakeSource) { f.snap.Body = nil })
	a := call(h, "GET", "/api/stats")
	eq(t, []string{strconv.Itoa(a.status), a.body, a.header.Get("Retry-After"),
		a.header.Get("Cache-Control")},
		[]string{"503", `{"error":"unavailable"}`, "30", "no-store"})
	h, src, _ = newHandler(t)
	src.set(func(f *fakeSource) { f.snapErr = store.ErrNotFound })
	eq(t, call(h, "GET", "/api/stats").status, http.StatusServiceUnavailable)
	eq(t, h.Counts()[http.StatusServiceUnavailable], int64(1))
}

func TestHeadHasHeadersAndNoBody(t *testing.T) {
	h, _, _ := newHandler(t)
	for _, target := range []string{"/api/stats", "/api/stats/sessions"} {
		get, head := call(h, "GET", target), call(h, "HEAD", target)
		eq(t, head.status, http.StatusOK)
		eq(t, head.body, "")
		eq(t, head.header, get.header)
		eq(t, head.header.Get("Content-Length"), strconv.Itoa(len(get.body)))
	}
}

func TestRowReadOncePerRowCache(t *testing.T) {
	h, src, c := newHandler(t, `stats.row_cache = "5s"`)
	for range 10 {
		call(h, "GET", "/api/stats")
	}
	eq(t, src.reads, 1)
	src.set(func(f *fakeSource) { f.snap.Body = []byte(`{"version":1,"n":2}`) })
	c.add(4 * time.Second)
	eq(t, call(h, "GET", "/api/stats").body, servedBody)
	c.add(time.Second)
	eq(t, call(h, "GET", "/api/stats").body, `{"version":1,"n":2}`)
	eq(t, src.reads, 2)
}

func TestPerSourceBucket(t *testing.T) {
	h, _, c := newHandler(t)
	for i := range 20 {
		if a := call(h, "GET", "/api/stats"); a.status != http.StatusOK {
			t.Fatalf("request %d: %d", i+1, a.status)
		}
	}
	a := call(h, "GET", "/api/stats")
	eq(t, a.status, http.StatusTooManyRequests)
	eq(t, a.body, `{"error":"rate_limited"}`)
	eq(t, a.header.Get("Retry-After"), "1")
	eq(t, a.header.Get("Cache-Control"), "no-store")
	// Caddy's entry is the last one, whatever the client wrote before it
	eq(t, call(h, "GET", "/api/stats", "X-Forwarded-For", "127.0.0.1, 198.51.100.7").status,
		http.StatusOK)
	for range 19 {
		call(h, "GET", "/api/stats", "X-Forwarded-For", "198.51.100.7")
	}
	eq(t, call(h, "GET", "/api/stats", "X-Forwarded-For", "198.51.100.7").status,
		http.StatusTooManyRequests)
	// One /64 shares a bucket
	for i := range 20 {
		call(h, "GET", "/api/stats", "X-Forwarded-For", "2001:db8::"+strconv.Itoa(i+1))
	}
	eq(t, call(h, "GET", "/api/stats", "X-Forwarded-For", "2001:db8::ffff:1").status,
		http.StatusTooManyRequests)
	eq(t, call(h, "GET", "/api/stats", "X-Forwarded-For", "2001:db8:0:1::1").status,
		http.StatusOK)
	// 60 a minute refills one a second
	c.add(time.Second)
	eq(t, call(h, "GET", "/api/stats").status, http.StatusOK)
	eq(t, call(h, "GET", "/api/stats").status, http.StatusTooManyRequests)
}

func TestSourcesAreBounded(t *testing.T) {
	h, _, _ := newHandler(t, "stats.max_sources = 3")
	for i := range 10 {
		call(h, "GET", "/api/stats", "X-Forwarded-For", "198.51.100."+strconv.Itoa(i))
	}
	eq(t, h.sources.size(), 3)
}

func TestSessionQueriesCapped(t *testing.T) {
	h, src, _ := newHandler(t, "stats.max_queries = 4")
	eq(t, h.queueWait, 2*time.Second)
	h.queueWait = 100 * time.Millisecond
	block, started := make(chan struct{}), make(chan struct{}, 8)
	src.set(func(f *fakeSource) { f.block, f.started = block, started })
	var wg sync.WaitGroup
	for i := range 4 {
		cursor := EncodeCursor(t0, uuid.New())
		wg.Go(func() {
			if a := call(h, "GET", "/api/stats/sessions?before="+cursor); a.status != 200 {
				t.Errorf("query %d: %d", i+1, a.status)
			}
		})
	}
	for range 4 {
		<-started
	}
	start := time.Now()
	a := call(h, "GET", "/api/stats/sessions")
	eq(t, a.status, http.StatusServiceUnavailable)
	eq(t, a.body, `{"error":"busy"}`)
	eq(t, a.header.Get("Retry-After"), "5")
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Errorf("answered busy after %v", waited)
	}
	close(block)
	wg.Wait()
}

func TestSessionsReadTheWindowAndTheCursor(t *testing.T) {
	h, src, c := newHandler(t)
	id := uuid.New()
	at := t0.Add(-time.Hour)
	call(h, "GET", "/api/stats/sessions?before="+EncodeCursor(at, id))
	eq(t, src.since, t0.Add(-30*24*time.Hour))
	eq(t, src.before, at)
	eq(t, src.id, id)
	eq(t, src.limit, PageSize+1)
	// The page is cached by its cursor for 30 s
	call(h, "GET", "/api/stats/sessions?before="+EncodeCursor(at, id))
	eq(t, src.lists, 1)
	call(h, "GET", "/api/stats/sessions")
	eq(t, src.before, time.Time{})
	eq(t, src.lists, 2)
	c.add(30 * time.Second)
	call(h, "GET", "/api/stats/sessions")
	eq(t, src.lists, 3)
}

func TestCursorsPostgresNeverSees(t *testing.T) {
	h, src, _ := newHandler(t)
	a := call(h, "GET", "/api/stats/sessions?before="+allOnes)
	eq(t, []string{strconv.Itoa(a.status), a.body, a.header.Get("Cache-Control")},
		[]string{"400", `{"error":"bad_cursor"}`, "no-store"})
	far := cursorOf([]byte{0x80, 0, 0, 0, 0, 0, 0, 0})
	eq(t, call(h, "GET", "/api/stats/sessions?before="+far).status, http.StatusBadRequest)

	// No row is both inside the 30 days and older than such a cursor
	for _, at := range []time.Time{t0.Add(-31 * 24 * time.Hour), t0.Add(-Window)} {
		a := call(h, "GET", "/api/stats/sessions?before="+EncodeCursor(at, uuid.New()))
		eq(t, []string{strconv.Itoa(a.status), a.body, a.header.Get("Cache-Control")},
			[]string{"200", `{"items":[],"next":null}`, "public, max-age=30"})
	}
	eq(t, src.lists, 0)
	call(h, "GET", "/api/stats/sessions?before="+
		EncodeCursor(t0.Add(-Window).Add(time.Microsecond), uuid.New()))
	eq(t, src.lists, 1)
}

func TestServesLastBodyWhilePostgresFails(t *testing.T) {
	h, src, c := newHandler(t)
	eq(t, call(h, "GET", "/api/stats").body, servedBody)
	call(h, "GET", "/api/stats/sessions")
	down := errors.New("connection refused")
	src.set(func(f *fakeSource) { f.snapErr, f.listErr = down, down })
	c.add(time.Minute)
	eq(t, call(h, "GET", "/api/stats").body, servedBody)
	a := call(h, "GET", "/api/stats/sessions")
	eq(t, a.status, http.StatusOK)
	eq(t, a.body, `{"items":[],"next":null}`)
	a = call(h, "GET", "/api/stats/sessions?before="+EncodeCursor(t0, uuid.New()))
	eq(t, a.status, http.StatusServiceUnavailable)
	eq(t, a.body, `{"error":"unavailable"}`)

	// A fresh process with Postgres down has nothing to serve
	h, src, _ = newHandler(t)
	src.set(func(f *fakeSource) { f.snapErr = down })
	eq(t, call(h, "GET", "/api/stats").status, http.StatusServiceUnavailable)
}
