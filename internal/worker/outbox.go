package worker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/oynozan/bugnet-router/internal/jobargs"
	"github.com/oynozan/bugnet-router/internal/store"
)

const (
	maxBackoff   = 5 * time.Minute
	drainedBytes = 64 << 10
)

type outboxWorker struct {
	river.WorkerDefaults[jobargs.Outbox]
	w *Worker
}

func (ow *outboxWorker) Work(ctx context.Context, _ *river.Job[jobargs.Outbox]) error {
	return ow.w.deliverOutbox(ctx)
}

// deliverOutbox sends due events one at a time and stops claiming new ones after outboxRunTime
func (w *Worker) deliverOutbox(ctx context.Context) error {
	if w.d.EventsURL == "" {
		w.noURL.Do(func() {
			w.log.Warn("BUGNET_EVENTS_URL is not set, outbox events stay pending")
		})
		return nil
	}
	start := time.Now()
	for time.Since(start) < outboxRunTime {
		handled, err := w.deliverNext(ctx)
		if err != nil || !handled {
			return err
		}
	}
	return nil
}

// deliverNext sends the oldest due event while its row is locked, so no other run sends it too
func (w *Worker) deliverNext(ctx context.Context) (bool, error) {
	handled := false
	err := w.store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := w.store.Tx(tx)
		due, err := ts.PendingOutbox(ctx, w.now(), 1)
		if err != nil || len(due) == 0 {
			return err
		}
		handled = true
		ev := due[0]
		perr := w.post(ctx, ev)
		if perr == nil {
			return ts.MarkOutboxDelivered(ctx, ev.ID)
		}
		attempts := ev.Attempts + 1
		if attempts >= store.MaxOutboxAttempts {
			w.log.Error("outbox event is dead", "event", ev.ID, "type", ev.EventType,
				"attempts", attempts, "err", perr)
		} else {
			w.log.Warn("outbox delivery failed", "event", ev.ID, "attempts", attempts,
				"err", perr)
		}
		return ts.MarkOutboxFailed(ctx, ev.ID, perr.Error(), w.now().Add(backoff(ev.Attempts)))
	})
	return handled, err
}

// backoff waits 2^n seconds after the nth earlier failure, at most maxBackoff
func backoff(n int) time.Duration {
	if n >= 30 {
		return maxBackoff
	}
	return min(time.Second<<n, maxBackoff)
}

// body is the JSON an events endpoint receives for one outbox event
type body struct {
	ID        uuid.UUID       `json:"id"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

// post signs the exact bytes it sends and treats anything but a 2xx answer as a failure
func (w *Worker) post(ctx context.Context, ev store.OutboxEvent) error {
	raw, err := json.Marshal(body{
		ID: ev.ID, Type: ev.EventType, CreatedAt: ev.CreatedAt, Payload: ev.Payload,
	})
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.d.EventsURL,
		bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bugnet-Event-Id", ev.ID.String())
	req.Header.Set("X-Bugnet-Signature",
		base64.StdEncoding.EncodeToString(ed25519.Sign(w.d.SigningKey, raw)))
	resp, err := w.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	// Reading a bounded rest of the answer lets the connection be reused
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainedBytes))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
