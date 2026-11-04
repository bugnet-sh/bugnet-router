package streams_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/oynozan/bugnet-run/frame"
	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/streams"
	"github.com/oynozan/bugnet-router/internal/testenv"
)

func newBus(t *testing.T) (*streams.Bus, valkey.Client, string) {
	t.Helper()
	vk, prefix := testenv.Valkey(t)
	return streams.NewBus(vk, prefix), vk, prefix
}

func acquire(t *testing.T, b *streams.Bus, job, replica string) streams.Lease {
	t.Helper()
	l, ok, err := b.AcquireLease(t.Context(), job, replica)
	if err != nil || !ok {
		t.Fatalf("acquire %s for %s: ok %v, %v", job, replica, ok, err)
	}
	return l
}

func out(off int64, data string) frame.Frame {
	return frame.Frame{Kind: frame.KindStdout, Offset: off, Data: []byte(data)}
}

func TestLeaseExclusive(t *testing.T) {
	b, _, _ := newBus(t)
	ctx := t.Context()

	const contenders = 20
	var wg sync.WaitGroup
	won := make(chan streams.Lease, contenders)
	for i := range contenders {
		wg.Go(func() {
			l, ok, err := b.AcquireLease(ctx, "j1", "r"+string(rune('a'+i)))
			if err != nil {
				t.Errorf("acquire: %v", err)
			}
			if ok {
				won <- l
			}
		})
	}
	wg.Wait()
	close(won)
	var winners []streams.Lease
	for l := range won {
		winners = append(winners, l)
	}
	if len(winners) != 1 {
		t.Fatalf("%d contenders won the lease, want exactly 1", len(winners))
	}
	first := winners[0]
	if err := first.Renew(ctx); err != nil {
		t.Fatalf("renew by the owner: %v", err)
	}
	if _, ok, err := b.AcquireLease(ctx, "j1", "late"); err != nil || ok {
		t.Fatalf("acquire of a held lease: ok %v, %v", ok, err)
	}

	if err := first.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := first.Renew(ctx); !errors.Is(err, streams.ErrLeaseLost) {
		t.Fatalf("renew after release: %v, want ErrLeaseLost", err)
	}
	next := acquire(t, b, "j1", "late")
	if next.Owner() == first.Owner() {
		t.Fatalf("the second owner reuses the first owner value %q", first.Owner())
	}
	if _, err := next.Append(ctx, out(0, "x")); err != nil {
		t.Fatalf("append by the new owner: %v", err)
	}
}

func TestAppendRejectsStaleOwner(t *testing.T) {
	b, vk, prefix := newBus(t)
	ctx := t.Context()

	stale := acquire(t, b, "j1", "r1")
	if _, err := stale.Append(ctx, out(0, "one")); err != nil {
		t.Fatalf("append while owning: %v", err)
	}
	// Losing the owner key is what an expired lease looks like to the next owner
	if err := vk.Do(ctx, vk.B().Del().Key(prefix+"job:j1:owner").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	fresh := acquire(t, b, "j1", "r1")
	if fresh.Owner() == stale.Owner() {
		t.Fatalf("both owners are %q, the epoch did not advance", fresh.Owner())
	}

	if _, err := stale.Append(ctx, out(3, "two")); !errors.Is(err, streams.ErrLeaseLost) {
		t.Fatalf("append by the stale owner: %v, want ErrLeaseLost", err)
	}
	if err := stale.Renew(ctx); !errors.Is(err, streams.ErrLeaseLost) {
		t.Fatalf("renew by the stale owner: %v, want ErrLeaseLost", err)
	}
	if err := stale.Release(ctx); !errors.Is(err, streams.ErrLeaseLost) {
		t.Fatalf("release by the stale owner: %v, want ErrLeaseLost", err)
	}
	if _, err := fresh.Append(ctx, out(3, "three")); err != nil {
		t.Fatalf("append by the new owner after the stale release: %v", err)
	}

	entries, err := b.Read(ctx, "j1", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, string(e.Frame.Data))
	}
	if want := []string{"one", "three"}; !slices.Equal(got, want) {
		t.Fatalf("stream holds %q, want %q", got, want)
	}
}

func TestReadFromCursorBlocks(t *testing.T) {
	b, _, _ := newBus(t)
	ctx := t.Context()
	l := acquire(t, b, "j1", "r1")
	cursor, err := l.Append(ctx, out(0, "first"))
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		entries []streams.Entry
		err     error
		at      time.Time
	}
	done := make(chan result, 1)
	go func() {
		entries, err := b.Read(ctx, "j1", cursor, 5*time.Second)
		done <- result{entries, err, time.Now()}
	}()
	time.Sleep(200 * time.Millisecond)
	appended := time.Now()
	id, err := l.Append(ctx, out(5, "second"))
	if err != nil {
		t.Fatal(err)
	}

	var r result
	select {
	case r = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocked reader never woke")
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	if woke := r.at.Sub(appended); woke > 50*time.Millisecond {
		t.Fatalf("the blocked reader woke %v after the append, want within 50ms", woke)
	}
	if len(r.entries) != 1 || r.entries[0].ID != id || string(r.entries[0].Frame.Data) != "second" {
		t.Fatalf("the blocked reader got %+v, want only entry %s", r.entries, id)
	}

	all, err := b.Read(ctx, "j1", "", 0)
	if err != nil || len(all) != 2 || all[0].ID != cursor || all[1].ID != id {
		t.Fatalf("read from the start: %+v, %v", all, err)
	}
	start := time.Now()
	none, err := b.Read(ctx, "j1", id, 100*time.Millisecond)
	if err != nil || len(none) != 0 {
		t.Fatalf("read past the end: %+v, %v", none, err)
	}
	if waited := time.Since(start); waited < 90*time.Millisecond {
		t.Fatalf("an empty blocking read returned after %v, want about 100ms", waited)
	}
	if _, err := b.Read(ctx, "j1", "not-a-cursor", 0); !errors.Is(err, streams.ErrBadCursor) {
		t.Fatalf("read from a malformed cursor: %v, want ErrBadCursor", err)
	}
}

func TestReadStopsWithContext(t *testing.T) {
	b, _, _ := newBus(t)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := b.Read(ctx, "j1", "", 10*time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read past its context: %v, want DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("read returned %v after its context ended", waited)
	}
}

func TestExpiredLeasesListed(t *testing.T) {
	b, _, _ := newBus(t)
	ctx := t.Context()
	now := time.Now()
	held := acquire(t, b, "held", "r1")
	released := acquire(t, b, "released", "r1")
	ended := acquire(t, b, "ended", "r1")

	expired, err := b.ExpiredLeases(ctx, now)
	if err != nil || len(expired) != 0 {
		t.Fatalf("expired leases right after acquiring: %q, %v", expired, err)
	}
	if err := released.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ended.End(ctx); err != nil {
		t.Fatal(err)
	}
	expired, err = b.ExpiredLeases(ctx, now)
	if err != nil || !slices.Equal(expired, []string{"released"}) {
		t.Fatalf("expired leases after a release: %q, %v, want [released]", expired, err)
	}
	expired, err = b.ExpiredLeases(ctx, now.Add(6*time.Second))
	slices.Sort(expired)
	if err != nil || !slices.Equal(expired, []string{"held", "released"}) {
		t.Fatalf("expired leases 6s later: %q, %v, want [held released]", expired, err)
	}

	time.Sleep(time.Second)
	if err := held.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	expired, err = b.ExpiredLeases(ctx, now.Add(5500*time.Millisecond))
	if err != nil || !slices.Equal(expired, []string{"released"}) {
		t.Fatalf("expired leases after a renew: %q, %v, want [released]", expired, err)
	}
}

func TestLastOffsets(t *testing.T) {
	b, _, _ := newBus(t)
	ctx := t.Context()
	check := func(step string, wantOut, wantErr int64, wantDone bool) {
		t.Helper()
		o, e, done, err := b.LastOffsets(ctx, "j1")
		if err != nil || o != wantOut || e != wantErr || done != wantDone {
			t.Fatalf("%s: offsets %d/%d done %v (%v), want %d/%d done %v",
				step, o, e, done, err, wantOut, wantErr, wantDone)
		}
	}
	check("empty stream", 0, 0, false)

	l := acquire(t, b, "j1", "r1")
	appendAll := func(fs ...frame.Frame) {
		t.Helper()
		for _, f := range fs {
			if _, err := l.Append(ctx, f); err != nil {
				t.Fatal(err)
			}
		}
	}
	appendAll(out(0, "hello"))
	check("one stdout frame", 5, 0, false)
	appendAll(frame.Frame{Kind: frame.KindStderr, Offset: 0, Data: []byte("abc")}, out(5, "!!"))
	check("stdout after stderr", 7, 3, false)
	for i := range 150 {
		appendAll(out(int64(7+i), "x"))
	}
	check("stderr far behind many stdout frames", 157, 3, false)
	appendAll(frame.Frame{Kind: frame.KindHeartbeat, Out: 157, Err: 3}, out(157, "y"))
	check("stdout after a heartbeat", 158, 3, false)
	appendAll(frame.Frame{Kind: frame.KindExit, Code: 2, Out: 158, Err: 3})
	check("exit", 158, 3, true)

	last, ok, err := b.Last(ctx, "j1")
	if err != nil || !ok || last.Frame.Kind != frame.KindExit || last.Frame.Code != 2 {
		t.Fatalf("last entry: %+v ok %v, %v", last, ok, err)
	}
	if _, ok, err := b.Last(ctx, "missing"); err != nil || ok {
		t.Fatalf("last entry of a missing stream: ok %v, %v", ok, err)
	}
}

func TestLastOffsetsNeverRewindToStaleEntries(t *testing.T) {
	b, _, _ := newBus(t)
	ctx := t.Context()
	l := acquire(t, b, "j1", "r1")
	check := func(step string, wantOut, wantErr int64, wantDone bool) {
		t.Helper()
		o, e, done, err := b.LastOffsets(ctx, "j1")
		if err != nil || o != wantOut || e != wantErr || done != wantDone {
			t.Fatalf("%s: offsets %d/%d done %v (%v), want %d/%d done %v",
				step, o, e, done, err, wantOut, wantErr, wantDone)
		}
	}
	stale := []frame.Frame{
		out(0, "hello world"),
		{Kind: frame.KindStderr, Offset: 0, Data: []byte("abcd")},
		{Kind: frame.KindHeartbeat, Out: 11, Err: 4},
		out(3, "lo"),
		{Kind: frame.KindStderr, Offset: 0, Data: []byte("a")},
		{Kind: frame.KindHeartbeat, Out: 2, Err: 1},
	}
	// Enough stale heartbeats push the furthest output onto an earlier page of the scan
	for range 300 {
		stale = append(stale, frame.Frame{Kind: frame.KindHeartbeat, Out: 1})
	}
	for _, f := range stale {
		if _, err := l.Append(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	check("stale output and heartbeats after the furthest", 11, 4, false)
	beat := frame.Frame{Kind: frame.KindHeartbeat, Out: 13, Err: 4}
	if _, err := l.Append(ctx, beat); err != nil {
		t.Fatal(err)
	}
	check("a heartbeat counting past the stored output", 13, 4, false)
	if _, err := l.Append(ctx, frame.Frame{Kind: frame.KindExit, Out: 5, Err: 0}); err != nil {
		t.Fatal(err)
	}
	check("a stale exit", 13, 4, true)
}

func TestLastOffsetsTerminalError(t *testing.T) {
	b, _, _ := newBus(t)
	ctx := t.Context()
	l := acquire(t, b, "j1", "r1")
	if _, err := l.Append(ctx, out(0, "abc")); err != nil {
		t.Fatal(err)
	}
	lost := frame.Frame{Kind: frame.KindError, Reason: frame.ReasonJobLost}
	if _, err := l.Append(ctx, lost); err != nil {
		t.Fatal(err)
	}
	o, e, done, err := b.LastOffsets(ctx, "j1")
	if err != nil || o != 3 || e != 0 || !done {
		t.Fatalf("offsets %d/%d done %v (%v), want 3/0 done", o, e, done, err)
	}
	if streams.Terminal(frame.Frame{Kind: frame.KindError, Reason: frame.ReasonJobExists}) {
		t.Fatal("job_exists is not the end of a stream")
	}
}

func TestAppendSetsStreamExpiry(t *testing.T) {
	b, vk, prefix := newBus(t)
	ctx := t.Context()
	l := acquire(t, b, "j1", "r1")
	l.StreamExpiry = time.Now().Add(time.Hour)
	if _, err := l.Append(ctx, out(0, "a")); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"job:j1:s", "job:j1:epoch"} {
		ttl, err := vk.Do(ctx, vk.B().Pttl().Key(prefix+key).Build()).AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		if ttl < (59*time.Minute).Milliseconds() || ttl > time.Hour.Milliseconds() {
			t.Fatalf("%s expires in %dms, want about an hour", key, ttl)
		}
	}
}

func TestCancelRequest(t *testing.T) {
	b, vk, prefix := newBus(t)
	ctx := t.Context()
	if asked, err := b.CancelRequested(ctx, "j1"); err != nil || asked {
		t.Fatalf("cancel requested before any request: %v, %v", asked, err)
	}
	if err := b.RequestCancel(ctx, "j1", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if asked, err := b.CancelRequested(ctx, "j1"); err != nil || !asked {
		t.Fatalf("cancel requested after a request: %v, %v", asked, err)
	}
	ttl, err := vk.Do(ctx, vk.B().Pttl().Key(prefix+"job:j1:cancel").Build()).AsInt64()
	if err != nil || ttl <= 0 {
		t.Fatalf("cancel key ttl %d, %v, want it to expire", ttl, err)
	}
}

func TestClearCancel(t *testing.T) {
	b, _, _ := newBus(t)
	ctx := t.Context()
	if err := b.RequestCancel(ctx, "j1", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := b.ClearCancel(ctx, "j1"); err != nil {
		t.Fatal(err)
	}
	if asked, err := b.CancelRequested(ctx, "j1"); err != nil || asked {
		t.Fatalf("cancel requested after clearing it: %v, %v", asked, err)
	}
	if err := b.ClearCancel(ctx, "j2"); err != nil {
		t.Fatalf("clear a cancel nobody asked for: %v", err)
	}
}

func TestPostponedLeaseWaitsForItsTime(t *testing.T) {
	b, _, _ := newBus(t)
	ctx := t.Context()
	now := time.Now()
	l := acquire(t, b, "j1", "r1")
	if err := l.Postpone(ctx, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if expired, err := b.ExpiredLeases(ctx, now.Add(4*time.Second)); err != nil ||
		len(expired) != 0 {
		t.Fatalf("expired leases before the retry time: %q, %v, want none", expired, err)
	}
	expired, err := b.ExpiredLeases(ctx, now.Add(5*time.Second))
	if err != nil || !slices.Equal(expired, []string{"j1"}) {
		t.Fatalf("expired leases at the retry time: %q, %v, want [j1]", expired, err)
	}
	if err := l.Postpone(ctx, now); !errors.Is(err, streams.ErrLeaseLost) {
		t.Fatalf("postpone a lease already given up: %v, want ErrLeaseLost", err)
	}
	next := acquire(t, b, "j1", "r2")
	if next.Owner() == l.Owner() {
		t.Fatalf("the next owner reuses %s", l.Owner())
	}
}
