package edgeevents_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/oynozan/bugnet-router/internal/edgeevents"
	"github.com/oynozan/bugnet-router/internal/testenv"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

var discard = slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

func ended(id string) *routerv1.EdgeEvent {
	return &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_PlacementEnded{
		PlacementEnded: &routerv1.PlacementEnded{
			PlacementId: id, Reason: routerv1.EndReason_END_REASON_STOPPED,
		},
	}}
}

type got struct {
	cursor string
	ev     *routerv1.EdgeEvent
}

// watch runs Watch from cursor until n events arrived or 5 s passed
func watch(t *testing.T, b *edgeevents.Bus, cursor string, n int, during func()) []got {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var out []got
	var mu sync.Mutex
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		done <- b.Watch(ctx, cursor, func(c string, ev *routerv1.EdgeEvent) error {
			mu.Lock()
			defer mu.Unlock()
			out = append(out, got{c, ev})
			if len(out) == n {
				cancel()
			}
			return nil
		})
	}()
	<-started
	if during != nil {
		during()
	}
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("watch ended with %v, want it cancelled after %d events", err, n)
	}
	mu.Lock()
	defer mu.Unlock()
	return out
}

func streamLen(t *testing.T, vk valkey.Client, key string) int64 {
	t.Helper()
	n, err := vk.Do(t.Context(), vk.B().Xlen().Key(key).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func ids(t *testing.T, vk valkey.Client, key string) []string {
	t.Helper()
	entries, err := vk.Do(t.Context(), vk.B().Xrange().Key(key).Start("-").End("+").Build()).
		AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.ID)
	}
	return out
}

func TestWatchFromNowSendsOnlyLaterEvents(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	b := edgeevents.New(vk, prefix, discard)
	b.Publish(t.Context(), ended("before"))
	evs := watch(t, b, "", 2, func() {
		time.Sleep(500 * time.Millisecond)
		b.Publish(t.Context(), ended("a"))
		b.Publish(t.Context(), &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_DeadlineMoved{
			DeadlineMoved: &routerv1.DeadlineMoved{
				PlacementId: "b", Deadline: timestamppb.New(time.Unix(1800000000, 0)),
				EdgeBytesRemaining: 7,
			},
		}})
	})
	eq(t, evs[0].ev.GetPlacementEnded().GetPlacementId(), "a")
	eq(t, evs[1].ev.GetDeadlineMoved().GetEdgeBytesRemaining(), uint64(7))
	all := ids(t, vk, prefix+"edge:events")
	eq(t, []string{evs[0].cursor, evs[1].cursor}, all[1:])
}

func TestWatchResumesAfterTheCursor(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	b := edgeevents.New(vk, prefix, discard)
	for _, id := range []string{"a", "b", "c"} {
		b.Publish(t.Context(), ended(id))
	}
	all := ids(t, vk, prefix+"edge:events")
	evs := watch(t, b, all[0], 2, nil)
	eq(t, evs[0].ev.GetPlacementEnded().GetPlacementId(), "b")
	eq(t, evs[1].ev.GetPlacementEnded().GetPlacementId(), "c")
	eq(t, evs[1].cursor, all[2])
}

func TestEventsResyncAfterTrim(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	b := edgeevents.New(vk, prefix, discard)
	for _, id := range []string{"a", "b", "c"} {
		b.Publish(t.Context(), ended(id))
	}
	key := prefix + "edge:events"
	trimmed := ids(t, vk, key)[0]
	err := vk.Do(t.Context(), vk.B().Xtrim().Key(key).Maxlen().Threshold("1").Build()).Error()
	if err != nil {
		t.Fatal(err)
	}
	last := ids(t, vk, key)[0]
	evs := watch(t, b, trimmed, 2, func() {
		time.Sleep(500 * time.Millisecond)
		b.Publish(t.Context(), ended("d"))
	})
	first := evs[0]
	eq(t, first.ev.GetResync() != nil, true)
	eq(t, first.cursor, last)
	eq(t, evs[1].ev.GetPlacementEnded().GetPlacementId(), "d")
}

func TestCursorAStreamCannotHoldResyncs(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	b := edgeevents.New(vk, prefix, discard)
	// A stream lost with Valkey's data leaves the reader's cursor naming nothing
	evs := watch(t, b, "1700000000000-0", 1, nil)
	eq(t, evs[0].ev.GetResync() != nil, true)

	b.Publish(t.Context(), ended("a"))
	last := ids(t, vk, prefix+"edge:events")[0]
	evs = watch(t, b, "9999999999999-0", 1, nil)
	eq(t, evs[0].ev.GetResync() != nil, true)
	eq(t, evs[0].cursor, last)
}

func TestWatchRefusesAMalformedCursor(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	b := edgeevents.New(vk, prefix, discard)
	for _, c := range []string{"x", "1-", "-1", "1-2-3", "0x1-0", " 1-0", "1"} {
		err := b.Watch(t.Context(), c, func(string, *routerv1.EdgeEvent) error { return nil })
		if !errors.Is(err, edgeevents.ErrBadCursor) {
			t.Errorf("cursor %q gave %v, want ErrBadCursor", c, err)
		}
	}
}

func TestWatchStopsWhenSendFails(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	b := edgeevents.New(vk, prefix, discard)
	b.Publish(t.Context(), ended("a"))
	gone := errors.New("client went away")
	err := b.Watch(t.Context(), "0-0", func(string, *routerv1.EdgeEvent) error { return gone })
	if !errors.Is(err, gone) {
		t.Fatalf("watch gave %v, want the send error", err)
	}
}

func TestUndecodableEntryResyncs(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	b := edgeevents.New(vk, prefix, discard)
	b.Publish(t.Context(), ended("a"))
	key := prefix + "edge:events"
	cursor := ids(t, vk, key)[0]
	err := vk.Do(t.Context(), vk.B().Xadd().Key(key).Id("*").FieldValue().
		FieldValue("e", "\xff\xff not a proto").Build()).Error()
	if err != nil {
		t.Fatal(err)
	}
	bad := ids(t, vk, key)[1]
	evs := watch(t, b, cursor, 1, nil)
	eq(t, evs[0].ev.GetResync() != nil, true)
	eq(t, evs[0].cursor, bad)
}

// flaky fails the first fails calls to Do, then passes them to the real client
type flaky struct {
	valkey.Client
	fails int32
	calls atomic.Int32
}

func (f *flaky) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	if f.calls.Add(1) <= f.fails {
		return f.Client.Do(ctx, f.Client.B().Hset().Key("").FieldValue().Build())
	}
	return f.Client.Do(ctx, cmd)
}

func TestPublishRetriesThreeTimesThenLogs(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	f := &flaky{Client: vk, fails: 3}
	edgeevents.New(f, prefix, log).Publish(t.Context(), ended("a"))
	eq(t, f.calls.Load(), int32(4))
	eq(t, streamLen(t, vk, prefix+"edge:events"), int64(1))
	eq(t, logs.String(), "")

	f = &flaky{Client: vk, fails: 4}
	edgeevents.New(f, prefix, log).Publish(t.Context(), ended("b"))
	eq(t, f.calls.Load(), int32(4))
	eq(t, streamLen(t, vk, prefix+"edge:events"), int64(1))
	eq(t, strings.Contains(logs.String(), "level=ERROR"), true)
	eq(t, strings.Contains(logs.String(), "edge event lost"), true)
}

func TestPublishOutlivesTheCallersContext(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	edgeevents.New(vk, prefix, discard).Publish(ctx, ended("a"))
	eq(t, streamLen(t, vk, prefix+"edge:events"), int64(1))
}

func TestPublishTrimsTheStream(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	edgeevents.SetMaxLen(t, 10)
	b := edgeevents.New(vk, prefix, discard)
	for range 300 {
		b.Publish(t.Context(), ended("a"))
	}
	if n := streamLen(t, vk, prefix+"edge:events"); n >= 300 {
		t.Fatalf("the stream holds %d entries after 300 with MAXLEN ~ 10", n)
	}
}

func TestEventsRoundTrip(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	b := edgeevents.New(vk, prefix, discard)
	sent := &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_BudgetSpent{
		BudgetSpent: &routerv1.BudgetSpent{
			PlacementId: "p", Kind: routerv1.BudgetKind_BUDGET_KIND_EGRESS,
		},
	}}
	evs := watch(t, b, "", 1, func() {
		time.Sleep(500 * time.Millisecond)
		b.Publish(t.Context(), sent)
	})
	eq(t, proto.Equal(evs[0].ev, sent), true)
}
