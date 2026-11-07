package api

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/oynozan/bugnet-router/internal/budget"
	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/dpkeys"
	"github.com/oynozan/bugnet-router/internal/edgeevents"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/transfer"
)

const (
	// xferChunk is the largest data message of a transfer stream (DP 8.3.3)
	xferChunk = 256 << 10
	// xferStepTimeout bounds the record and staging work that ends a stream
	xferStepTimeout = time.Minute
	// cutGrace is how long a cut download may still send its error before its writes fail
	cutGrace = 500 * time.Millisecond
	upload   = routerv1.TransferDirection_TRANSFER_DIRECTION_UPLOAD
	download = routerv1.TransferDirection_TRANSFER_DIRECTION_DOWNLOAD
)

var (
	// xferCheck is how often a stream checks its idle time and hard stop
	xferCheck = time.Second
	// renewEvery is how often a stream renews its lease and reads its placement again
	renewEvery = 10 * time.Second
	// xferLease is how long a claim holds a transfer without a renewal
	xferLease = transfer.LeaseLength
)

var (
	errWrongWay  = errors.New("api: the transfer goes the other way")
	errIdle      = errors.New("api: the transfer moved no byte for its idle timeout")
	errHardStop  = errors.New("api: the transfer reached its hard stop")
	errFinished  = errors.New("api: the transfer stream finished")
	errFirstPart = errors.New("api: an upload's first message names its transfer and lease")
)

// transferSettings fills each zero transfer setting with its default
func transferSettings(x config.Transfers) config.Transfers {
	x.URLTTL = cmp.Or(x.URLTTL, 10*time.Minute)
	x.MaxBytes = cmp.Or(x.MaxBytes, 1<<30)
	x.MaxDuration = cmp.Or(x.MaxDuration, 30*time.Minute)
	x.IdleTimeout = cmp.Or(x.IdleTimeout, time.Minute)
	x.MaxActivePerPlacement = cmp.Or(x.MaxActivePerPlacement, 2)
	x.MaxTokensPerPlacement = cmp.Or(x.MaxTokensPerPlacement, 16)
	x.MaxClaims = cmp.Or(x.MaxClaims, 5)
	return x
}

/* Minting */

// ask is a CreateTransfer request that passed the checks needing no placement
type ask struct {
	dir     routerv1.TransferDirection
	path    string
	size    int64
	mode    fs.FileMode
	sha     []byte
	wantSHA bool
}

// transferAsk checks a request's own fields before any placement is read or operation spent
func transferAsk(m *routerv1.CreateTransferRequest) (ask, error) {
	a := ask{dir: m.GetDirection(), path: m.GetPath()}
	if a.dir != upload && a.dir != download {
		return ask{}, invalid("direction must be upload or download")
	}
	if _, err := transfer.Expand(a.path, "/"); err != nil {
		return ask{}, err
	}
	if a.dir == download {
		a.wantSHA = m.GetWantSha256()
		return a, nil
	}
	if m.SizeBytes == nil {
		return ask{}, errSizeRequired
	}
	if m.GetSizeBytes() > math.MaxInt64 {
		return ask{}, errOverCap
	}
	a.size, a.mode = int64(m.GetSizeBytes()), fs.FileMode(m.GetMode())
	switch {
	case a.mode == 0:
		a.mode = defaultFileMode
	case a.mode&^fs.ModePerm != 0:
		return ask{}, invalid("mode %#o has bits beyond the permission bits", m.GetMode())
	}
	if n := len(m.GetSha256()); n != 0 && n != sha256.Size {
		return ask{}, invalid("sha256 must be %d bytes, not %d", sha256.Size, n)
	}
	a.sha = m.GetSha256()
	if len(a.sha) == 0 {
		a.sha = nil
	}
	return a, nil
}

// CreateTransfer mints a one-use URL for one file going into or out of a ready placement
func (s *Server) CreateTransfer(
	ctx context.Context, req *connect.Request[routerv1.CreateTransferRequest],
) (*connect.Response[routerv1.CreateTransferResponse], error) {
	a, err := transferAsk(req.Msg)
	if err != nil {
		return nil, s.status(err)
	}
	p, prov, err := s.sandbox(ctx, req.Msg.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	if s.d.TransferHost == "" || len(s.d.Keys.Roots()) == 0 {
		return nil, s.status(errNoHost)
	}
	key := req.Msg.GetIdempotencyKey()
	tok := dpkeys.NewTransferToken()
	if key != "" {
		if tok, err = s.d.Keys.TransferToken(cmp.Or(p.KeyID, s.d.Keys.Current().ID), p.ID,
			key); err != nil {
			return nil, s.status(err)
		}
	}
	sum := sha256.Sum256([]byte(tok))
	id, limit, now := transfer.ID(sum[:]), s.transferCap(p), time.Now()
	if key != "" {
		rec, err := s.xfers.Get(ctx, id)
		switch {
		case err == nil:
			return s.minted(ctx, rec, tok, limit, now, true)
		case !errors.Is(err, transfer.ErrUnknown):
			return nil, s.status(err)
		}
		// A record gone from Valkey that the index still knows ran past its window
		indexed, err := s.d.Store.TransferIndexed(ctx, sum[:])
		if err != nil {
			return nil, s.status(err)
		}
		if indexed {
			return nil, s.status(transfer.ErrExpired)
		}
	}
	rec, err := s.newTransfer(ctx, p, prov, a, id, limit, now)
	if err != nil {
		return nil, s.fileStatus(err)
	}
	got, created, err := s.xfers.Put(ctx, rec, now)
	if err != nil {
		return nil, s.status(err)
	}
	return s.minted(ctx, got, tok, limit, now, !created)
}

// transferCap is the per-file cap, the package's own or the setting
func (s *Server) transferCap(p store.Placement) int64 {
	if pkg, ok := s.d.Catalog.Get(p.PackageID); ok && pkg.Edge.TransferMaxBytes > 0 {
		return pkg.Edge.TransferMaxBytes
	}
	return s.x.MaxBytes
}

// newTransfer resolves the path, decides staging, stats the file and builds the record
func (s *Server) newTransfer(
	ctx context.Context, p store.Placement, prov provider.Provider, a ask, id string,
	limit int64, now time.Time,
) (transfer.Record, error) {
	if a.dir == upload && a.size > limit {
		return transfer.Record{}, errOverCap
	}
	ref, caps := *p.ProviderRef, s.d.Registry.Capabilities(prov)
	t, err := transfer.Resolve(ctx, prov, ref, caps, a.dir, a.path)
	if err != nil {
		return transfer.Record{}, err
	}
	if err := transfer.Sweep(ctx, prov, ref, t); err != nil {
		s.d.Log.Warn("api: sweep transfer leftovers", "placement", p.ID, "err", err)
	}
	rec := transfer.Record{
		ID: id, Placement: p.ID, Direction: a.dir, Path: t.Path, Size: a.size, Mode: a.mode,
		SHA256: a.sha, ExpiresAt: now.Add(s.x.URLTTL).Truncate(time.Millisecond),
	}
	if t.Staged {
		rec.StagedPath = t.StagedPath(id)
	}
	entry, err := transfer.Stat(ctx, prov, ref, t)
	if a.dir == upload {
		switch {
		case err == nil && entry.IsDir:
			return transfer.Record{}, transfer.ErrIsDirectory
		case err != nil && !isCode(err, provider.CodeNotFound):
			return transfer.Record{}, err
		}
		return rec, nil
	}
	if err == nil {
		err = checkDownload(entry, limit)
	}
	if err != nil {
		return transfer.Record{}, err
	}
	rec.Size, rec.ETag, rec.ModifiedAt = entry.Size, etag(entry), entry.Modified
	if a.wantSHA {
		if rec.SHA256, err = transfer.Digest(ctx, prov, ref, caps, t); err != nil {
			return transfer.Record{}, err
		}
	}
	return rec, nil
}

// minted answers a record's URL, refusing one already used or past its window when it existed
func (s *Server) minted(
	ctx context.Context, rec transfer.Record, tok string, limit int64, now time.Time,
	existed bool,
) (*connect.Response[routerv1.CreateTransferResponse], error) {
	switch {
	case existed && rec.State == transfer.Done:
		return nil, s.status(transfer.ErrUsed)
	case existed && !now.Before(rec.ExpiresAt):
		return nil, s.status(transfer.ErrExpired)
	}
	sum := sha256.Sum256([]byte(tok))
	if err := s.d.Store.IndexTransfer(ctx, sum[:], rec.Placement); err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.CreateTransferResponse{
		Url: "https://" + s.d.TransferHost + "/" + tok, ExpiresAt: timestamppb.New(rec.ExpiresAt),
		Size: uint64(rec.Size), MaxBytes: uint64(limit),
	}
	if rec.Direction == download {
		out.ModifiedAt, out.Sha256 = timestamppb.New(rec.ModifiedAt), rec.SHA256
	}
	return connect.NewResponse(out), nil
}

// checkDownload refuses a directory, anything but a regular file and a file over the cap
func checkDownload(e provider.FileEntry, limit int64) error {
	switch {
	case e.IsDir:
		return transfer.ErrIsDirectory
	case !e.Regular:
		return transfer.ErrNotRegular
	case e.Size > limit:
		return errOverCap
	}
	return nil
}

// etag is a strong HTTP validator from a file's size and modification time
func etag(e provider.FileEntry) string {
	return fmt.Sprintf(`"%x-%x"`, e.Size, e.Modified.UnixNano())
}

func isCode(err error, code provider.Code) bool {
	e, ok := provider.AsError(err)
	return ok && e.Code == code
}

/* Opening */

// OpenTransfer claims a transfer for one stream of the edge, or with probe set only describes it
func (e *EdgeServer) OpenTransfer(
	ctx context.Context, req *connect.Request[routerv1.OpenTransferRequest],
) (*connect.Response[routerv1.OpenTransferResponse], error) {
	s, m := e.s, req.Msg
	sum := sha256.Sum256([]byte(m.GetToken()))
	rec, err := s.xfers.Get(ctx, transfer.ID(sum[:]))
	if err != nil {
		return nil, s.status(err)
	}
	if m.GetDirection() != rec.Direction {
		return nil, s.status(errWrongWay)
	}
	p, err := s.d.Store.PlacementByID(ctx, rec.Placement)
	if err != nil {
		return nil, s.status(err)
	}
	prov, err := s.home(p)
	if err == nil {
		err = s.spend(ctx, p)
	}
	if err != nil {
		return nil, s.status(err)
	}
	left, err := s.edgeLeft(ctx, p)
	if err != nil {
		return nil, s.status(err)
	}
	if rec.Direction == download {
		if err := s.restat(ctx, p, prov, rec); err != nil {
			return nil, s.fileStatus(err)
		}
	}
	lease := ""
	now := time.Now()
	if m.GetProbe() {
		err = probe(rec, now)
	} else {
		rec, lease, err = s.xfers.Claim(ctx, sum[:], m.GetEdgeId(), now)
	}
	if err != nil {
		return nil, s.status(err)
	}
	s.d.Log.Info("api: transfer opened", "transfer", transfer.Short(rec.ID), "placement", p.ID,
		"probe", m.GetProbe(), "claims", rec.Claims)
	out := &routerv1.OpenTransferResponse{
		TransferId: rec.ID, Lease: lease, PlacementId: p.ID.String(),
		FileName: path.Base(rec.Path), Size: uint64(rec.Size), Etag: rec.ETag,
		HardStop:  timestamppb.New(minTime(*p.Deadline, now.Add(s.x.MaxDuration))),
		ExpiresAt: timestamppb.New(*p.Deadline), Budget: s.edgeBudget(p, left),
	}
	if rec.Direction == download {
		out.ModifiedAt = timestamppb.New(rec.ModifiedAt)
	}
	return connect.NewResponse(out), nil
}

// probe refuses a probe of a used URL or one past its window, as a claim would
func probe(rec transfer.Record, now time.Time) error {
	switch {
	case rec.State == transfer.Done:
		return transfer.ErrUsed
	case !now.Before(rec.ExpiresAt) && (rec.Claims == 0 || rec.Direction == upload):
		return transfer.ErrExpired
	}
	return nil
}

// edgeLeft is the placement's edge bytes left, refusing a placement that spent its budget
func (s *Server) edgeLeft(ctx context.Context, p store.Placement) (uint64, error) {
	spent, err := s.meter.Spent(ctx, budget.Edge, p.ID)
	if err != nil {
		return 0, err
	}
	left, err := s.meter.Remaining(ctx, budget.Edge, s.limit(p, budget.Edge))
	if err != nil {
		return 0, err
	}
	if spent || left == 0 {
		return 0, errBudgetSpent
	}
	return left, nil
}

// edgeBudget is the placement's edge limits as its package and the settings set them
func (s *Server) edgeBudget(p store.Placement, left uint64) *routerv1.EdgeBudget {
	b := s.d.Budget
	pkg, _ := s.d.Catalog.Get(p.PackageID)
	return &routerv1.EdgeBudget{
		BytesRemaining:    left,
		BytesPerSecond:    uint64(budget.PaceFor(pkg, b, budget.Edge)),
		QueuedRequests:    uint32(cmp.Or(pkg.Edge.QueuedRequests, b.QueuedRequests)),
		Websockets:        uint32(cmp.Or(pkg.Edge.WebSockets, b.WebSockets)),
		RequestsPerSecond: uint32(cmp.Or(pkg.Edge.RequestsPerSecond, b.RequestsPerSecond)),
		RequestsBurst:     uint32(cmp.Or(pkg.Edge.RequestsBurst, b.RequestsBurst)),
	}
}

// restat checks that a download's file still has the size and time its URL pinned
func (s *Server) restat(
	ctx context.Context, p store.Placement, prov provider.Provider, rec transfer.Record,
) error {
	entry, err := transfer.Stat(ctx, prov, *p.ProviderRef, target(rec))
	if err != nil {
		return err
	}
	if etag(entry) != rec.ETag {
		return transfer.ErrFileChanged
	}
	return checkDownload(entry, s.transferCap(p))
}

// target is where a record's transfer reads or writes
func target(rec transfer.Record) transfer.Target {
	return transfer.Target{Path: rec.Path, Staged: rec.StagedPath != ""}
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

/* Streams */

// stream is one transfer stream, cut when idle, at its hard stop, or when its placement ends
type stream struct {
	s        *Server
	rec      transfer.Record
	lease    string
	p        store.Placement
	prov     provider.Provider
	parent   context.Context
	ctx      context.Context
	cancel   context.CancelCauseFunc
	w        http.ResponseWriter // nil when the handler was mounted without withWriter
	start    time.Time
	deadline atomic.Int64 // the placement's deadline in Unix nanoseconds
	last     atomic.Int64 // when a byte last moved, in Unix nanoseconds
	moved    atomic.Int64 // bytes moved and not yet counted in the record
	busy     atomic.Bool  // staging runs, which moves no byte of the stream
	cause    atomic.Pointer[error]
	watched  chan struct{}
}

// openStream checks the lease and starts the watcher that cuts the stream
func (s *Server) openStream(
	ctx context.Context, id, lease string, dir routerv1.TransferDirection,
) (*stream, error) {
	rec, err := s.xfers.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if rec.Direction != dir {
		return nil, errWrongWay
	}
	if rec, err = s.xfers.Renew(ctx, rec, lease, 0, time.Now()); err != nil {
		return nil, err
	}
	p, err := s.d.Store.PlacementByID(ctx, rec.Placement)
	if err != nil {
		return nil, err
	}
	prov, err := s.home(p)
	if err != nil {
		return nil, err
	}
	st := &stream{s: s, rec: rec, lease: lease, p: p, prov: prov, parent: ctx,
		start: time.Now(), watched: make(chan struct{})}
	st.ctx, st.cancel = context.WithCancelCause(ctx)
	st.w, _ = ctx.Value(writerKey{}).(http.ResponseWriter)
	st.deadline.Store(p.Deadline.UnixNano())
	st.last.Store(time.Now().UnixNano())
	events, unhub := s.hub.watch(p.ID.String())
	wake, unwake := s.notes.watch(saga.Channel(s.d.Prefix, p.ID))
	go func() {
		defer close(st.watched)
		defer unhub()
		defer unwake()
		st.watch(events, wake)
	}()
	return st, nil
}

// watch cuts the stream on its placement's end, a spent budget, idleness or the hard stop
func (st *stream) watch(events <-chan *routerv1.EdgeEvent, wake <-chan struct{}) {
	check := time.NewTicker(xferCheck)
	defer check.Stop()
	renew := time.NewTicker(renewEvery)
	defer renew.Stop()
	closing := st.s.life.Done()
	for {
		select {
		case <-st.ctx.Done():
			return
		case <-closing:
			st.cut(errShuttingDown)
			closing = nil
		case ev := <-events:
			st.event(ev)
		case <-wake:
			st.reread()
		case <-renew.C:
			st.renew()
			st.reread()
		case now := <-check.C:
			switch idle := now.Sub(time.Unix(0, st.last.Load())); {
			case idle >= st.s.x.IdleTimeout && !st.busy.Load():
				st.cut(errIdle)
			case !now.Before(st.hardStop()):
				st.cut(errHardStop)
			}
		}
	}
}

// hardStop is the earlier of the placement's deadline and the longest a transfer may run
func (st *stream) hardStop() time.Time {
	return minTime(time.Unix(0, st.deadline.Load()), st.start.Add(st.s.x.MaxDuration))
}

func (st *stream) event(ev *routerv1.EdgeEvent) {
	switch {
	case ev.GetPlacementEnded() != nil:
		st.cut(errNotReady)
	case ev.GetBudgetSpent().GetKind() == routerv1.BudgetKind_BUDGET_KIND_EDGE:
		st.cut(errBudgetSpent)
	case ev.GetDeadlineMoved() != nil:
		st.deadline.Store(ev.GetDeadlineMoved().GetDeadline().AsTime().UnixNano())
	case ev.GetResync() != nil:
		st.reread()
	}
}

// reread cuts a stream whose placement left ready and follows a moved deadline
func (st *stream) reread() {
	p, err := st.s.d.Store.PlacementByID(st.ctx, st.p.ID)
	switch {
	case err != nil:
		return
	case p.State != store.PlacementReady || p.Deadline == nil:
		st.cut(errNotReady)
	default:
		st.deadline.Store(p.Deadline.UnixNano())
	}
}

// renew extends the lease and counts the bytes moved since the last renewal
func (st *stream) renew() {
	moved := st.moved.Swap(0)
	if _, err := st.s.xfers.Renew(st.ctx, st.rec, st.lease, moved, time.Now()); err != nil {
		st.moved.Add(moved)
		if errors.Is(err, transfer.ErrInUse) {
			st.cut(transfer.ErrInUse)
			return
		}
		st.s.d.Log.Warn("api: renew a transfer lease", "transfer", transfer.Short(st.rec.ID),
			"err", err)
	}
}

// cut ends the stream for cause, unblocking a body read or a send waiting on the edge
func (st *stream) cut(cause error) {
	if !st.cause.CompareAndSwap(nil, &cause) {
		return
	}
	st.cancel(cause)
	if st.w == nil {
		return
	}
	// A passed write deadline resets an HTTP/2 stream, so a download first gets cutGrace to answer
	rc := http.NewResponseController(st.w)
	if st.rec.Direction == upload {
		_ = rc.SetReadDeadline(time.Now())
	} else {
		_ = rc.SetWriteDeadline(time.Now().Add(cutGrace))
	}
}

// progress records n bytes moved now
func (st *stream) progress(n int) {
	st.moved.Add(int64(n))
	st.last.Store(time.Now().UnixNano())
}

// staging marks a staging step, during which the idle timeout waits
func (st *stream) staging(run func() error) error {
	st.busy.Store(true)
	defer func() {
		st.last.Store(time.Now().UnixNano())
		st.busy.Store(false)
	}()
	return run()
}

// finish stops the watcher, ends the claim as done or released and removes the staged copy
func (st *stream) finish(ok bool) {
	st.cancel(errFinished)
	<-st.watched
	ctx, cancel := context.WithTimeout(context.WithoutCancel(st.parent), xferStepTimeout)
	defer cancel()
	end := st.s.xfers.Release
	if ok {
		end = st.s.xfers.Done
	}
	log := st.s.d.Log.With("transfer", transfer.Short(st.rec.ID), "placement", st.p.ID)
	err := end(ctx, st.rec, st.lease, st.moved.Swap(0))
	if err != nil {
		log.Warn("api: end a transfer claim", "done", ok, "err", err)
	}
	// Staging is per URL, so a claim taken over leaves it to the new holder and the hourly sweep
	if st.rec.StagedPath != "" && !errors.Is(err, transfer.ErrInUse) {
		if err := transfer.Unstage(ctx, st.prov, *st.p.ProviderRef, st.rec.StagedPath); err != nil {
			log.Warn("api: remove a staged transfer", "err", err)
		}
	}
	log.Info("api: transfer stream ended", "done", ok)
}

// status answers err, or the reason the stream was cut when it was
func (st *stream) status(err error) error {
	if cause := st.cause.Load(); cause != nil {
		return st.s.status(*cause)
	}
	return st.s.fileStatus(err)
}

// ReadTransfer streams a claimed download from offset, holding the last bytes until it is verified
func (e *EdgeServer) ReadTransfer(
	ctx context.Context, req *connect.Request[routerv1.ReadTransferRequest],
	out *connect.ServerStream[routerv1.ReadTransferResponse],
) error {
	m := req.Msg
	st, err := e.s.openStream(ctx, m.GetTransferId(), m.GetLease(), download)
	if err != nil {
		return e.s.status(err)
	}
	err = st.read(out, m.GetOffset(), m.GetLength())
	if err != nil {
		err = st.status(err)
	}
	st.finish(err == nil)
	return err
}

func (st *stream) read(
	out *connect.ServerStream[routerv1.ReadTransferResponse], offset, length uint64,
) error {
	size := uint64(st.rec.Size)
	if offset > size || length > size-offset {
		return errRange
	}
	if length == 0 {
		length = size - offset
	}
	ref, from := *st.p.ProviderRef, st.rec.Path
	if st.rec.StagedPath != "" {
		caps := st.s.d.Registry.Capabilities(st.prov)
		err := st.staging(func() error {
			copied, err := transfer.StageOut(st.ctx, st.prov, ref, caps, target(st.rec),
				st.rec.StagedPath)
			if err == nil && copied.Size != st.rec.Size {
				err = transfer.ErrFileChanged
			}
			return err
		})
		if err != nil {
			return err
		}
		from = st.rec.StagedPath
	}
	r, err := st.prov.OpenReader(st.ctx, ref, from, int64(offset))
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()
	// A reader's own stat may differ in precision, so the time is checked by Stat at either end
	if r.Info().Size != st.rec.Size {
		return transfer.ErrFileChanged
	}
	bufs := [2][]byte{make([]byte, xferChunk), make([]byte, xferChunk)}
	var held []byte
	for i, left := 0, length; left > 0; i++ {
		buf := bufs[i%2][:min(xferChunk, left)]
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		if held != nil {
			if err := out.Send(&routerv1.ReadTransferResponse{Data: held}); err != nil {
				return err
			}
			st.progress(len(held))
		}
		held, left = buf, left-uint64(len(buf))
	}
	// The last bytes wait for the end check, so a changed file never ends a clean download
	if err := st.restatOriginal(); err != nil {
		return err
	}
	if err := out.Send(&routerv1.ReadTransferResponse{Data: held, Verified: true}); err != nil {
		return err
	}
	st.progress(len(held))
	return nil
}

// restatOriginal compares the original path with the ETag the URL pinned
func (st *stream) restatOriginal() error {
	return st.staging(func() error {
		entry, err := transfer.Stat(st.ctx, st.prov, *st.p.ProviderRef, target(st.rec))
		if err == nil && etag(entry) != st.rec.ETag {
			err = transfer.ErrFileChanged
		}
		return err
	})
}

// WriteTransfer writes a claimed upload and commits it only on its exact size and digest
func (e *EdgeServer) WriteTransfer(
	ctx context.Context, in *connect.ClientStream[routerv1.WriteTransferRequest],
) (*connect.Response[routerv1.WriteTransferResponse], error) {
	if !in.Receive() {
		return nil, e.s.status(cmp.Or(in.Err(), errFirstPart))
	}
	first := in.Msg()
	st, err := e.s.openStream(ctx, first.GetTransferId(), first.GetLease(), upload)
	if err != nil {
		return nil, e.s.status(err)
	}
	resp, err := st.write(in, first.GetData())
	if err != nil {
		err = st.status(err)
	}
	st.finish(err == nil)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (st *stream) write(
	in *connect.ClientStream[routerv1.WriteTransferRequest], data []byte,
) (*routerv1.WriteTransferResponse, error) {
	rec, ref := st.rec, *st.p.ProviderRef
	to, spec := rec.Path, provider.WriteSpec{Size: rec.Size, Mode: rec.Mode, SHA256: rec.SHA256}
	if rec.StagedPath != "" {
		// The staged copy is the exec user's alone until place gives it its owner and mode
		to, spec.Mode = rec.StagedPath, 0o600
	}
	w, err := st.prov.OpenWriter(st.ctx, ref, to, spec)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = w.Abort()
		}
	}()
	up := upstream{w: w, h: sha256.New(), size: rec.Size, st: st}
	if err := up.put(data); err != nil {
		return nil, err
	}
	for in.Receive() {
		if err := up.put(in.Msg().GetData()); err != nil {
			return nil, err
		}
	}
	// Only a clean close commits, a call the edge cancelled ends here with its error
	if err := in.Err(); err != nil {
		return nil, err
	}
	if up.n != rec.Size {
		return nil, errSizeMismatch
	}
	sum := up.h.Sum(nil)
	if rec.SHA256 != nil && !bytes.Equal(sum, rec.SHA256) {
		return nil, errChecksum
	}
	if err := st.ctx.Err(); err != nil {
		return nil, err
	}
	committed = true
	if _, err := w.Commit(st.ctx, sum); err != nil {
		return nil, err
	}
	if rec.StagedPath != "" {
		err := st.staging(func() error {
			_, err := transfer.Place(st.ctx, st.prov, ref, rec.StagedPath, target(rec), rec.Mode)
			return err
		})
		if err != nil {
			return nil, err
		}
	}
	return &routerv1.WriteTransferResponse{Path: rec.Path, Size: uint64(up.n), Sha256: sum}, nil
}

// upstream counts and hashes an upload's bytes on their way to the writer
type upstream struct {
	w    provider.FileWriter
	h    hash.Hash
	n    int64
	size int64
	st   *stream
}

func (u *upstream) put(data []byte) error {
	if int64(len(data)) > u.size-u.n {
		return errSizeMismatch
	}
	if len(data) == 0 {
		return nil
	}
	u.h.Write(data)
	if _, err := u.w.Write(data); err != nil {
		return err
	}
	u.n += int64(len(data))
	u.st.progress(len(data))
	return nil
}

/* Events */

// hub fans the edge events out to the transfer streams of each placement
type hub struct {
	bus  *edgeevents.Bus
	log  *slog.Logger
	mu   sync.Mutex
	subs map[string]map[chan *routerv1.EdgeEvent]struct{}
}

func newHub(bus *edgeevents.Bus, log *slog.Logger) *hub {
	return &hub{bus: bus, log: log, subs: map[string]map[chan *routerv1.EdgeEvent]struct{}{}}
}

// watch returns the events of placement until stop is called, a Resync going to every watcher
func (h *hub) watch(placement string) (<-chan *routerv1.EdgeEvent, func()) {
	c := make(chan *routerv1.EdgeEvent, 8)
	h.mu.Lock()
	defer h.mu.Unlock()
	set := h.subs[placement]
	if set == nil {
		set = map[chan *routerv1.EdgeEvent]struct{}{}
		h.subs[placement] = set
	}
	set[c] = struct{}{}
	return c, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(set, c)
		if len(set) == 0 {
			delete(h.subs, placement)
		}
	}
}

// deliver hands ev to its placement's watchers, never waiting on a full one
func (h *hub) deliver(ev *routerv1.EdgeEvent) {
	id := cmp.Or(ev.GetPlacementEnded().GetPlacementId(), ev.GetDeadlineMoved().GetPlacementId(),
		ev.GetBudgetSpent().GetPlacementId())
	h.mu.Lock()
	defer h.mu.Unlock()
	for placement, set := range h.subs {
		if ev.GetResync() == nil && placement != id {
			continue
		}
		for c := range set {
			select {
			case c <- ev:
			default:
			}
		}
	}
}

// run follows the edge events from now until ctx ends, going on after the last one on an error
func (h *hub) run(ctx context.Context) {
	cursor := ""
	for {
		err := h.bus.Watch(ctx, cursor, nil, func(c string, ev *routerv1.EdgeEvent) error {
			cursor = c
			h.deliver(ev)
			return nil
		})
		if ctx.Err() != nil {
			return
		}
		h.log.Warn("api: edge events for transfers ended", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(resubscribeDelay):
		}
	}
}
