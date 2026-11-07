// Package edgeevents carries the events the edges act on through one Valkey stream.
//
// router-api and router-worker publish to edge:events right after the commit that causes an
// event, and the edges read it through WatchEdgeEvents, WatchSshEvents and WatchEgressEvents. A
// reader resumes after the stream id it last saw. A cursor the stream no longer holds, because it
// was trimmed or Valkey lost the stream, gets one Resync first, after which the reader re-checks
// every lease it holds. The edges' lease checks repair any event that is lost on the way.
package edgeevents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/protobuf/proto"
)

const (
	streamKey = "edge:events"
	field     = "e"
	// readCount bounds the entries one read returns
	readCount = 100
	// block is how long one read waits for an entry, so a cancelled reader stops within it
	block = 2 * time.Second
	// publishTimeout bounds a publish with its retries, whatever the caller's context
	publishTimeout = 5 * time.Second
	// noEntry is the cursor before every entry
	noEntry = "0-0"
)

// maxLen is the length the stream is trimmed to, approximately
var maxLen = 100_000

// retryDelays are the waits before each of a failed publish's three retries
var retryDelays = []time.Duration{50 * time.Millisecond, 100 * time.Millisecond,
	200 * time.Millisecond}

// ErrBadCursor means a cursor is not a stream id
var ErrBadCursor = errors.New("edgeevents: cursor is not a stream id")

var cursorPattern = regexp.MustCompile(`^[0-9]{1,20}-[0-9]{1,20}$`)

// Bus publishes and reads the edge event stream under one key prefix
type Bus struct {
	c   valkey.Client
	key string
	log *slog.Logger
}

// New returns a Bus on the stream under prefix, logging to log or slog.Default when nil
func New(c valkey.Client, prefix string, log *slog.Logger) *Bus {
	if log == nil {
		log = slog.Default()
	}
	return &Bus{c: c, key: prefix + streamKey, log: log}
}

// Publish appends to edge:events with MAXLEN ~ 100000, retrying 3 times, then logs
func (b *Bus) Publish(ctx context.Context, ev *routerv1.EdgeEvent) {
	raw, err := proto.Marshal(ev)
	if err != nil {
		b.log.Error("encode edge event", "err", err)
		return
	}
	// The event follows a commit, so a caller that went away must not lose it
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	// Pinned, so each retry can send the same command again
	cmd := b.c.B().Xadd().Key(b.key).Maxlen().Almost().Threshold(strconv.Itoa(maxLen)).
		Id("*").FieldValue().FieldValue(field, string(raw)).Build().Pin()
	for try := 0; ; try++ {
		err = b.c.Do(ctx, cmd).Error()
		if err == nil || try == len(retryDelays) {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(retryDelays[try]):
			continue
		}
		break
	}
	if err != nil {
		b.log.Error("edge event lost, the edges' lease checks repair it", "event", kind(ev),
			"err", err)
	}
}

// kind names an event's type for logs, which never carry its fields
func kind(ev *routerv1.EdgeEvent) string {
	if m := ev.ProtoReflect().WhichOneof(ev.ProtoReflect().Descriptor().Oneofs().Get(0)); m != nil {
		return string(m.Name())
	}
	return "none"
}

// Watch calls opened at a fixed start, then sends each entry after cursor, a Resync first if lost
func (b *Bus) Watch(
	ctx context.Context, cursor string, opened func(),
	send func(cursor string, ev *routerv1.EdgeEvent) error,
) error {
	from, resync, err := b.start(ctx, cursor)
	if err != nil {
		return err
	}
	if opened != nil {
		opened()
	}
	if resync {
		if err := send(from, resyncEvent()); err != nil {
			return err
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		cmd := b.c.B().Xread().Count(readCount).Block(block.Milliseconds()).Streams().
			Key(b.key).Id(from).Build()
		streams, err := b.c.Do(ctx, cmd).AsXRead()
		if valkey.IsValkeyNil(err) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("edgeevents: read: %w", err)
		}
		for _, e := range streams[b.key] {
			from = e.ID
			ev := &routerv1.EdgeEvent{}
			if err := proto.Unmarshal([]byte(e.FieldValues[field]), ev); err != nil {
				// A lost event is what Resync is for, so the reader re-checks its leases
				b.log.Error("undecodable edge event, sending resync", "id", e.ID, "err", err)
				ev = resyncEvent()
			}
			if err := send(e.ID, ev); err != nil {
				return err
			}
		}
	}
}

func resyncEvent() *routerv1.EdgeEvent {
	return &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_Resync{Resync: &routerv1.Resync{}}}
}

// start finds where a reader goes on from and whether it missed entries the stream lost
func (b *Bus) start(ctx context.Context, cursor string) (string, bool, error) {
	var at id
	if cursor != "" {
		var ok bool
		if at, ok = parseID(cursor); !ok {
			return "", false, ErrBadCursor
		}
	}
	res := b.c.DoMulti(ctx,
		b.c.B().Xrange().Key(b.key).Start("-").End("+").Count(1).Build(),
		b.c.B().Xrevrange().Key(b.key).End("+").Start("-").Count(1).Build())
	first, err := res[0].AsXRange()
	if err == nil {
		var last []valkey.XRangeEntry
		if last, err = res[1].AsXRange(); err == nil && len(first) > 0 && len(last) > 0 {
			return b.from(cursor, at, first[0].ID, last[0].ID)
		}
	}
	if err != nil {
		return "", false, fmt.Errorf("edgeevents: read the stream's ends: %w", err)
	}
	// An empty stream holds no cursor, so a reader that had one missed what the stream lost
	return noEntry, cursor != "", nil
}

// from places cursor against the stream's first and last ids
func (b *Bus) from(cursor string, at id, first, last string) (string, bool, error) {
	f, okF := parseID(first)
	l, okL := parseID(last)
	switch {
	case !okF || !okL:
		return "", false, fmt.Errorf("edgeevents: the stream holds an id %q or %q", first, last)
	case cursor == "":
		return last, false, nil
	case at.less(f) || l.less(at):
		return last, true, nil
	}
	return cursor, false, nil
}

// id is a stream entry id, milliseconds and a sequence number
type id struct{ ms, seq uint64 }

func (a id) less(b id) bool {
	return a.ms < b.ms || a.ms == b.ms && a.seq < b.seq
}

func parseID(s string) (id, bool) {
	if !cursorPattern.MatchString(s) {
		return id{}, false
	}
	msText, seqText, _ := strings.Cut(s, "-")
	ms, err1 := strconv.ParseUint(msText, 10, 64)
	seq, err2 := strconv.ParseUint(seqText, 10, 64)
	return id{ms, seq}, err1 == nil && err2 == nil
}
