package api

import (
	"testing"
	"time"
)

func TestLivePacerCapsAnIdleBurst(t *testing.T) {
	const rate, frame = 1 << 20, 16 << 10
	now := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	p := newPacer(rate, now)
	// send passes total bytes in frames, the clock moving only by each wait, and answers the time
	send := func(total int64) time.Duration {
		start := now
		for sent := int64(0); sent < total; sent += frame {
			now = now.Add(p.take(frame, now))
		}
		return now.Sub(start)
	}
	near := func(got, want time.Duration) {
		t.Helper()
		if got < want-time.Millisecond || got > want+time.Millisecond {
			t.Fatalf("took %v, want %v", got, want)
		}
	}
	near(send(rate), 0)
	near(send(rate), time.Second)
	// An idle minute saves one second of bytes, not sixty
	now = now.Add(time.Minute)
	near(send(rate), 0)
	near(send(rate), time.Second)
	// The steady state still reaches the rate
	near(send(64*rate), 64*time.Second)
}
