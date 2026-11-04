package placement

import (
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClock() *clock { return &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }

func TestTrackerUnhealthyAfterThreeQuickFailures(t *testing.T) {
	c := newClock()
	tr := NewTracker(c.now)
	tr.RecordFailure("a")
	c.advance(20 * time.Second)
	tr.RecordFailure("a")
	if !tr.Healthy("a") {
		t.Fatal("unhealthy after two failures")
	}
	c.advance(20 * time.Second)
	tr.RecordFailure("a")
	if tr.Healthy("a") {
		t.Fatal("healthy after three failures")
	}
	if !tr.Healthy("b") {
		t.Fatal("other provider affected")
	}
	c.advance(119 * time.Second)
	if tr.Healthy("a") {
		t.Fatal("recovered early")
	}
	c.advance(time.Second)
	if !tr.Healthy("a") {
		t.Fatal("still unhealthy after 120s")
	}
}

func TestTrackerSlowFailuresStayHealthy(t *testing.T) {
	c := newClock()
	tr := NewTracker(c.now)
	for range 5 {
		tr.RecordFailure("a")
		c.advance(31 * time.Second)
	}
	if !tr.Healthy("a") {
		t.Fatal("unhealthy though failures spread over more than 60s")
	}
}

func TestTrackerReadyResetsFailures(t *testing.T) {
	c := newClock()
	tr := NewTracker(c.now)
	tr.RecordFailure("a")
	tr.RecordFailure("a")
	tr.RecordReady("a", time.Second)
	tr.RecordFailure("a")
	if !tr.Healthy("a") {
		t.Fatal("ready did not reset the count")
	}
}

func TestTrackerReadyP95(t *testing.T) {
	tr := NewTracker(newClock().now)
	if tr.ReadyP95("a") != 0 {
		t.Fatal("want 0 without samples")
	}
	for i := 1; i <= 100; i++ {
		tr.RecordReady("a", time.Duration(i)*time.Second)
	}
	// last 50 samples are 51..100 s, nearest rank 95% is the 48th, 98 s
	if got := tr.ReadyP95("a"); got != 98*time.Second {
		t.Fatalf("p95 = %v", got)
	}
	tr.RecordReady("b", 3*time.Second)
	if got := tr.ReadyP95("b"); got != 3*time.Second {
		t.Fatalf("p95 = %v", got)
	}
}

func TestTrackerConcurrent(*testing.T) {
	tr := NewTracker(time.Now)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				tr.RecordFailure("a")
				tr.RecordReady("a", time.Second)
				tr.Healthy("a")
				tr.ReadyP95("a")
			}
		}()
	}
	wg.Wait()
}
