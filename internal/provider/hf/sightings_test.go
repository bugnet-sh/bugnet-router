package hf

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// sharedProvider is an adapter on f keeping its account sightings in vk under prefix
func sharedProvider(t *testing.T, f *fakeHF, vk valkey.Client, prefix string) *Provider {
	t.Helper()
	opts := testOptions()
	opts.Valkey, opts.Prefix = vk, prefix
	return f.provider(t, opts)
}

// sharedFake is router-api's and router-worker's adapters on one fake namespace and one prefix
func sharedFake(t *testing.T) (f *fakeHF, api, worker *Provider, vk valkey.Client, prefix string) {
	t.Helper()
	f = newFake(t)
	vk, prefix = testenv.Valkey(t)
	return f, sharedProvider(t, f, vk, prefix), sharedProvider(t, f, vk, prefix), vk, prefix
}

// sameClocks gives api and worker one clock the test moves
func sameClocks(api, worker *Provider) *clock {
	c := stopClock(api)
	worker.jobs.now = c.read
	return c
}

// zcard counts the members of the sorted set at key
func zcard(t *testing.T, vk valkey.Client, key string) int64 {
	t.Helper()
	n, err := vk.Do(t.Context(), vk.B().Zcard().Key(key).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestHFAccountSightingsReachEveryProcess(t *testing.T) {
	ctx := context.Background()
	f, api, worker, vk, prefix := sharedFake(t)
	wantAccount(t, worker, provider.BlockNone)
	// router-api's wait sees a Job HF ended for credit, and its create a refusal for credit
	j := endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
	if err := api.WaitReady(ctx, refTo(j)); !isCode(err, provider.CodeUnfunded) {
		t.Fatalf("WaitReady = %v", err)
	}
	f.inject(createRoute, fault{status: 402, message: "No credits left", times: 1})
	if _, err := api.Create(ctx, keyed()); !isCode(err, provider.CodeUnfunded) {
		t.Fatalf("Create = %v", err)
	}
	// router-worker read neither, and its own auth check passes
	wantAccount(t, worker, provider.BlockUnfunded, `hf ended Job "`+j.id+`" for no_credits`,
		"POST /api/jobs/"+testNamespace+" hf answered 402", "No credits left")
	eq(t, len(f.requestsTo("GET", "/api/jobs/"+testNamespace+"/"+j.id)), 1)
	// The sightings expire a window after the last one
	for _, key := range []string{"hf:sightings", "hf:sightings:words"} {
		ttl, err := vk.Do(ctx, vk.B().Pttl().Key(prefix+key).Build()).AsInt64()
		if err != nil || ttl <= 0 || ttl > accountWindow.Milliseconds() {
			t.Fatalf("%s expires in %dms, %v, want within the window", key, ttl, err)
		}
	}
	// A process under another prefix shares none of them
	wantAccount(t, sharedProvider(t, f, vk, prefix+"other:"), provider.BlockNone)
}

func TestHFSharedSightingsKeepTheWindow(t *testing.T) {
	ctx := context.Background()
	t.Run("a Job is held from when it was first seen for 10 minutes", func(t *testing.T) {
		f, api, worker, vk, prefix := sharedFake(t)
		c := sameClocks(api, worker)
		start := c.now
		j := endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
		if _, err := api.Inspect(ctx, refTo(j)); err != nil {
			t.Fatal(err)
		}
		// Reading the Job again, in the process that saw it or another, does not make it new
		c.now = start.Add(9 * time.Minute)
		for _, p := range []*Provider{api, worker} {
			if _, err := p.Inspect(ctx, refTo(j)); err != nil {
				t.Fatal(err)
			}
		}
		c.now = start.Add(10 * time.Minute)
		wantAccount(t, worker, provider.BlockUnfunded, j.id)
		c.now = c.now.Add(time.Second)
		wantAccount(t, worker, provider.BlockNone)
		// A newer sighting trims the ones that left the window
		late := endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
		if _, err := api.Inspect(ctx, refTo(late)); err != nil {
			t.Fatal(err)
		}
		eq(t, zcard(t, vk, prefix+"hf:sightings"), int64(1))
		wantAccount(t, worker, provider.BlockUnfunded, late.id)
	})
	t.Run("a Job HF ended counts from its end", func(t *testing.T) {
		f, api, worker, vk, prefix := sharedFake(t)
		c := sameClocks(api, worker)
		old := endedJob(f, "CANCELED", "NO_CREDITS", c.now.Add(-9*time.Minute))
		gone := endedJob(f, "CANCELED", "NO_CREDITS", c.now.Add(-11*time.Minute))
		for _, j := range []*fakeJob{old, gone} {
			if _, err := api.Inspect(ctx, refTo(j)); err != nil {
				t.Fatal(err)
			}
		}
		eq(t, zcard(t, vk, prefix+"hf:sightings"), int64(1))
		wantAccount(t, worker, provider.BlockUnfunded, old.id)
		c.now = c.now.Add(time.Minute + time.Second)
		wantAccount(t, worker, provider.BlockNone)
	})
	t.Run("a refusal counts from the newest", func(t *testing.T) {
		f, api, worker, _, _ := sharedFake(t)
		c := sameClocks(api, worker)
		f.inject(listRoute, fault{status: 402, message: "Payment required", times: 2})
		for range 2 {
			if _, err := api.ReadLimits(ctx); err != nil {
				t.Fatal(err)
			}
			c.now = c.now.Add(9 * time.Minute)
		}
		wantAccount(t, worker, provider.BlockUnfunded, "Payment required")
		c.now = c.now.Add(time.Minute + time.Second)
		wantAccount(t, worker, provider.BlockNone)
	})
}

func TestHFSharedSightingsKeepTheNewest(t *testing.T) {
	ctx := context.Background()
	f, api, worker, vk, prefix := sharedFake(t)
	c := sameClocks(api, worker)
	var jobs []*fakeJob
	for i := range maxSightings + 44 {
		end := c.now.Add(-time.Duration(maxSightings+44-i) * time.Second)
		jobs = append(jobs, endedJob(f, "CANCELED", "NO_CREDITS", end))
	}
	f.unfiltered = true
	if _, err := api.ListOwned(ctx, nil); err != nil {
		t.Fatal(err)
	}
	acc, err := worker.CheckAccount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var words []string
	for _, j := range jobs[len(jobs)-3:] {
		words = append([]string{`hf ended Job "` + j.id + `" for no_credits`}, words...)
	}
	eq(t, acc, provider.Account{Block: provider.BlockUnfunded, Detail: strings.Join(
		append(words, fmt.Sprintf("and %d more", maxSightings-3)), ", ")})
	eq(t, zcard(t, vk, prefix+"hf:sightings"), int64(maxSightings))
	n, err := vk.Do(ctx, vk.B().Hlen().Key(prefix+"hf:sightings:words").Build()).AsInt64()
	if err != nil || n != maxSightings {
		t.Fatalf("the words hold %d sightings, %v, want %d", n, err, maxSightings)
	}
}

func TestHFSightingsFallBackWithoutValkey(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	gone, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{testenv.ValkeyAddr()}})
	if err != nil {
		t.Fatal(err)
	}
	gone.Close()
	logs := &syncBuffer{}
	opts := testOptions()
	opts.Valkey, opts.Prefix = gone, "bn:test:gone:"
	opts.Log = slog.New(slog.NewTextHandler(logs, nil))
	p := f.provider(t, opts)
	// A create whose answer shows a Job HF ended for credit still answers its ref
	f.mu.Lock()
	f.show = func(job map[string]any) {
		job["status"] = map[string]any{"stage": "CANCELED", "cancelReason": "NO_CREDITS"}
	}
	f.mu.Unlock()
	ref, err := p.Create(ctx, keyed())
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.show = nil
	f.mu.Unlock()
	j := endedJob(f, "CANCELED", "NO_SUBSCRIPTION", time.Time{})
	if err := p.WaitReady(ctx, refTo(j)); !isCode(err, provider.CodeUnfunded) {
		t.Fatalf("WaitReady = %v", err)
	}
	f.inject(createRoute, fault{status: 402, message: "No credits left", times: 1})
	other := keyed()
	other.IdempotencyKey = strings.Repeat("b", 32)
	if _, err := p.Create(ctx, other); !isCode(err, provider.CodeUnfunded) {
		t.Fatalf("Create = %v", err)
	}
	for range 2 {
		wantAccount(t, p, provider.BlockUnfunded, ref.ID, j.id, "No credits left")
	}
	if n := strings.Count(logs.String(), "level=WARN"); n != 1 ||
		!strings.Contains(logs.String(), "valkey") {
		t.Fatalf("an outage logged %d warnings, want one naming valkey: %s", n, logs)
	}
}

func TestHFSkipsFailingSharedSightingsForAWhile(t *testing.T) {
	logs := &syncBuffer{}
	c := newJobsClient(fakeToken, testNamespace, 0, slog.New(slog.NewTextHandler(logs, nil)))
	c.account.shared = &sharedSightings{}
	now := time.Now()
	if !c.account.useShared(now) {
		t.Fatal("healthy shared sightings are skipped")
	}
	for range 2 {
		c.noteShared(errors.New("valkey hung"))
	}
	if c.account.useShared(now.Add(time.Second)) {
		t.Fatal("failing shared sightings are asked again at once")
	}
	if !c.account.useShared(now.Add(sharedRetryAfter + time.Second)) {
		t.Fatalf("the shared sightings are still skipped %v after they failed", sharedRetryAfter)
	}
	c.noteShared(nil)
	if !c.account.useShared(now) {
		t.Fatal("shared sightings that answer again are skipped")
	}
	eq(t, strings.Count(logs.String(), "level=WARN"), 1)
	eq(t, strings.Count(logs.String(), "level=INFO"), 1)
}

// hungValkey is a client of the test Valkey through a relay that swallows every byte once hung
func hungValkey(t *testing.T) (valkey.Client, *atomic.Bool) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	hung := &atomic.Bool{}
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			out, err := net.Dial("tcp", testenv.ValkeyAddr())
			if err != nil {
				_ = in.Close()
				continue
			}
			go func() {
				buf := make([]byte, 4096)
				for {
					n, err := in.Read(buf)
					if err != nil {
						break
					}
					if hung.Load() {
						continue
					}
					if _, err := out.Write(buf[:n]); err != nil {
						break
					}
				}
				_ = out.Close()
			}()
			go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		}
	}()
	vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{ln.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(vk.Close)
	return vk, hung
}

func TestHFHungSharedSightingsBoundEveryCall(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	vk, hung := hungValkey(t)
	_, prefix := testenv.Valkey(t)
	logs := &syncBuffer{}
	opts := testOptions()
	opts.Valkey, opts.Prefix = vk, prefix
	opts.Log = slog.New(slog.NewTextHandler(logs, nil))
	p := f.provider(t, opts)
	hung.Store(true)
	// The write of a create's refusal gives up at sharedTimeout, far before Valkey's own timeouts
	f.inject(createRoute, fault{status: 402, message: "No credits left", times: 1})
	start := time.Now()
	if _, err := p.Create(ctx, keyed()); !isCode(err, provider.CodeUnfunded) {
		t.Fatalf("Create = %v", err)
	}
	if took := time.Since(start); took > sharedTimeout+2*time.Second {
		t.Fatalf("a create answered 402 took %v against a hung valkey, want about %v", took,
			sharedTimeout)
	}
	// The next wait and check skip Valkey, which failed moments ago
	j := endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
	start = time.Now()
	if err := p.WaitReady(ctx, refTo(j)); !isCode(err, provider.CodeUnfunded) {
		t.Fatalf("WaitReady = %v", err)
	}
	wantAccount(t, p, provider.BlockUnfunded, j.id, "No credits left")
	if took := time.Since(start); took > sharedTimeout/2 {
		t.Fatalf("a wait and a check took %v, want valkey skipped", took)
	}
	eq(t, strings.Count(logs.String(), "level=WARN"), 1)
}

func TestHFReadOnlyOutageWarnsOnce(t *testing.T) {
	f := newFake(t)
	gone, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{testenv.ValkeyAddr()}})
	if err != nil {
		t.Fatal(err)
	}
	gone.Close()
	logs := &syncBuffer{}
	opts := testOptions()
	opts.Valkey, opts.Prefix = gone, "bn:test:gone:"
	opts.Log = slog.New(slog.NewTextHandler(logs, nil))
	worker := f.provider(t, opts)
	// router-worker reads and writes nothing, its own sightings came in before Valkey went
	now := time.Now()
	worker.jobs.account.add("job 6911", sighting{
		what: `hf ended Job "6911" for no_credits`, unfunded: true, at: now, seen: now,
	}, false)
	for range 3 {
		wantAccount(t, worker, provider.BlockUnfunded, `"6911"`)
		if worker.jobs.account.useShared(time.Now()) {
			t.Fatal("a check that found valkey down asks it again at once")
		}
		worker.jobs.account.mu.Lock()
		worker.jobs.account.sharedRetry = time.Now()
		worker.jobs.account.mu.Unlock()
	}
	if n := strings.Count(logs.String(), "level=WARN"); n != 1 {
		t.Fatalf("an outage only checks saw logged %d warnings, want one: %s", n, logs)
	}
}

func TestHFSightingsOutliveTheirCall(t *testing.T) {
	f, api, _, vk, prefix := sharedFake(t)
	logs := &syncBuffer{}
	opts := testOptions()
	opts.Valkey, opts.Prefix = vk, prefix
	opts.Log = slog.New(slog.NewTextHandler(logs, nil))
	worker := f.provider(t, opts)
	j := endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
	// An adapter with no Valkey reads the Job, so only the note below reaches Valkey
	read, err := f.provider(t, testOptions()).jobs.getJob(context.Background(), j.id)
	if err != nil {
		t.Fatal(err)
	}
	// A wait or create that ended with this very answer still leaves its sighting
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	api.jobs.noteJob(ended, read)
	wantAccount(t, worker, provider.BlockUnfunded, j.id)
	// A check given up on still reads the window, and says nothing of Valkey
	if got := worker.jobs.recent(ended); len(got) != 1 || !strings.Contains(got[0].what, j.id) {
		t.Fatalf("a canceled read gave %+v, want the Job %s", got, j.id)
	}
	if strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("a canceled call marked valkey down: %s", logs)
	}
}
