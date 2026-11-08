package stats

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// The two routes router-api serves on the stats listener
const (
	SummaryPath  = "/api/stats"
	SessionsPath = "/api/stats/sessions"
)

const (
	// pageTTL is how long a session page is served from the cache
	pageTTL = 30 * time.Second
	// queueWait is how long a page query waits for a free slot before answering busy
	queueWait = 2 * time.Second
	// readTimeout bounds one snapshot row read
	readTimeout = 2 * time.Second
	// unavailableRetry and busyRetry are the Retry-After of the two 503 answers
	unavailableRetry = "30"
	busyRetry        = "5"
	publicCache      = "public, max-age=30"
)

// Source is what the routes read, a *store.Store in router-api
type Source interface {
	ReadStatsSnapshot(ctx context.Context) (store.StatsSnapshot, error)
	ListStatsSessions(ctx context.Context, since, beforeAt time.Time, beforeID uuid.UUID,
		limit int) ([]store.StatsSessionRow, error)
}

// Handler serves CP 20.2's two routes with CP 20.8's caches and buckets
type Handler struct {
	src       Source
	sessions  Sessions
	settings  config.Stats
	now       func() time.Time
	log       *slog.Logger
	queueWait time.Duration

	rowMu   sync.Mutex
	row     []byte
	rowRead time.Time

	pagesMu sync.Mutex
	pages   *lru[string, cachedPage]
	slots   chan struct{}

	sourcesMu sync.Mutex
	sources   *lru[string, *rate.Limiter]

	countsMu sync.Mutex
	counts   map[int]int64
}

type cachedPage struct {
	body []byte
	at   time.Time
}

// NewHandler serves CP 20.2's two routes with CP 20.8's caches and buckets
func NewHandler(
	st Source, cat *catalog.Catalog, c *config.Config, now func() time.Time, log *slog.Logger,
) *Handler {
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	return &Handler{
		src: st, settings: c.Stats, now: now, log: log, queueWait: queueWait,
		sessions: Sessions{
			Catalog: cat, Facilitators: c.Wallets.FacilitatorAddresses, Ends: NewEnds(c),
		},
		pages:   newLRU[string, cachedPage](c.Stats.SessionsCache),
		slots:   make(chan struct{}, c.Stats.MaxQueries),
		sources: newLRU[string, *rate.Limiter](c.Stats.MaxSources),
		counts:  map[int]int64{},
	}
}

// Counts returns how many answers went out with each status
func (h *Handler) Counts() map[int]int64 {
	h.countsMu.Lock()
	defer h.countsMu.Unlock()
	return maps.Clone(h.counts)
}

// ServeHTTP answers CP 20.2, checking the path, then the method, then the bucket
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != SummaryPath && r.URL.Path != SessionsPath {
		h.fail(w, r, http.StatusNotFound, "not_found", nil)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		h.fail(w, r, http.StatusMethodNotAllowed, "method_not_allowed",
			http.Header{"Allow": {"GET, HEAD"}})
		return
	}
	if wait, ok := h.take(r); !ok {
		h.fail(w, r, http.StatusTooManyRequests, "rate_limited",
			http.Header{"Retry-After": {strconv.Itoa(wait)}})
		return
	}
	if r.URL.Path == SummaryPath {
		h.serveSummary(w, r)
		return
	}
	h.serveSessions(w, r)
}

func (h *Handler) serveSummary(w http.ResponseWriter, r *http.Request) {
	body := h.summary(r.Context())
	if body == nil {
		h.fail(w, r, http.StatusServiceUnavailable, "unavailable",
			http.Header{"Retry-After": {unavailableRetry}})
		return
	}
	h.write(w, r, http.StatusOK, body, http.Header{"Cache-Control": {publicCache}})
}

// summary reads the row at most once per row_cache, and keeps the last body while Postgres fails
func (h *Handler) summary(ctx context.Context) []byte {
	h.rowMu.Lock()
	defer h.rowMu.Unlock()
	now := h.now()
	if !h.rowRead.IsZero() && now.Sub(h.rowRead) < h.settings.RowCache {
		return h.row
	}
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	snap, err := h.src.ReadStatsSnapshot(ctx)
	switch {
	case err == nil:
		h.row = snap.Body
	case errors.Is(err, store.ErrNotFound):
		h.row = nil
	default:
		h.log.Warn("stats: snapshot row unread, serving the last body", "err", err)
	}
	h.rowRead = now
	return h.row
}

func (h *Handler) serveSessions(w http.ResponseWriter, r *http.Request) {
	cursor := r.URL.Query().Get("before")
	var beforeAt time.Time
	var beforeID uuid.UUID
	if cursor != "" {
		var err error
		if beforeAt, beforeID, err = DecodeCursor(cursor); err != nil {
			h.fail(w, r, http.StatusBadRequest, "bad_cursor", nil)
			return
		}
	}
	if body, ok := h.cachedPage(cursor, true); ok {
		h.write(w, r, http.StatusOK, body, http.Header{"Cache-Control": {publicCache}})
		return
	}
	select {
	case h.slots <- struct{}{}:
	case <-time.After(h.queueWait):
		h.fail(w, r, http.StatusServiceUnavailable, "busy",
			http.Header{"Retry-After": {busyRetry}})
		return
	case <-r.Context().Done():
		return
	}
	body, err := h.page(r.Context(), cursor, beforeAt, beforeID)
	<-h.slots
	if err != nil {
		h.log.Warn("stats: session page unread", "err", err)
		if stale, ok := h.cachedPage(cursor, false); ok {
			body = stale
		} else {
			h.fail(w, r, http.StatusServiceUnavailable, "unavailable",
				http.Header{"Retry-After": {unavailableRetry}})
			return
		}
	}
	h.write(w, r, http.StatusOK, body, http.Header{"Cache-Control": {publicCache}})
}

// page reads one page, or takes the one a request ahead of it read meanwhile
func (h *Handler) page(
	ctx context.Context, cursor string, beforeAt time.Time, beforeID uuid.UUID,
) ([]byte, error) {
	if body, ok := h.cachedPage(cursor, true); ok {
		return body, nil
	}
	now := h.now()
	rows, err := h.src.ListStatsSessions(ctx, now.Add(-Window), beforeAt, beforeID,
		PageSize+1)
	if err != nil {
		return nil, err
	}
	body, err := Encode(h.sessions.Page(rows, now))
	if err != nil {
		return nil, err
	}
	h.pagesMu.Lock()
	h.pages.put(cursor, cachedPage{body: body, at: now})
	h.pagesMu.Unlock()
	return body, nil
}

// cachedPage returns the page of a cursor, only one under 30 s old when fresh is set
func (h *Handler) cachedPage(cursor string, fresh bool) ([]byte, bool) {
	h.pagesMu.Lock()
	defer h.pagesMu.Unlock()
	p, ok := h.pages.get(cursor)
	if !ok || fresh && h.now().Sub(p.at) >= pageTTL {
		return nil, false
	}
	return p.body, true
}

// take spends a token of the request's source, or says how many seconds until one is free
func (h *Handler) take(r *http.Request) (int, bool) {
	now := h.now()
	key := sourceKey(r)
	h.sourcesMu.Lock()
	lim, ok := h.sources.get(key)
	if !ok {
		lim = rate.NewLimiter(rate.Limit(float64(h.settings.PerSourcePerMinute)/60),
			h.settings.PerSourceBurst)
		h.sources.put(key, lim)
	}
	h.sourcesMu.Unlock()
	if lim.AllowN(now, 1) {
		return 0, true
	}
	res := lim.ReserveN(now, 1)
	wait := res.DelayFrom(now)
	res.CancelAt(now)
	return max(int(math.Ceil(wait.Seconds())), 1), false
}

// sourceKey is the last X-Forwarded-For entry, which Caddy appends, else the peer, IPv6 by /64
func sourceKey(r *http.Request) string {
	var addr netip.Addr
	if xff := r.Header.Values("X-Forwarded-For"); len(xff) > 0 {
		last := xff[len(xff)-1]
		last = strings.TrimSpace(last[strings.LastIndex(last, ",")+1:])
		addr, _ = netip.ParseAddr(last)
	}
	if !addr.IsValid() {
		if ap, err := netip.ParseAddrPort(r.RemoteAddr); err == nil {
			addr = ap.Addr()
		}
	}
	addr = addr.Unmap()
	if addr.Is6() {
		p, _ := addr.Prefix(64)
		return p.String()
	}
	return addr.String()
}

func (h *Handler) fail(w http.ResponseWriter, r *http.Request, status int, code string,
	extra http.Header) {
	if extra == nil {
		extra = http.Header{}
	}
	extra.Set("Cache-Control", "no-store")
	// code is always one of this file's constants, so no request value reaches a body
	h.write(w, r, status, []byte(`{"error":"`+code+`"}`), extra)
}

// write sends the common headers, and the body except on HEAD
func (h *Handler) write(w http.ResponseWriter, r *http.Request, status int, body []byte,
	extra http.Header) {
	hd := w.Header()
	for k, v := range extra {
		hd[k] = slices.Clone(v)
	}
	hd.Set("Content-Type", "application/json")
	hd.Set("X-Content-Type-Options", "nosniff")
	hd.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
	h.countsMu.Lock()
	h.counts[status]++
	h.countsMu.Unlock()
}
