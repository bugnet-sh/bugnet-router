package worker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

// delivery is what one run did with the oldest due event
type delivery struct {
	ev  *store.OutboxEvent // nil when no event was due
	err error              // why the event was not delivered
}

// deliverNext sends the oldest due event while its row is locked, so no other run sends it too
func (w *Worker) deliverNext(ctx context.Context) (bool, error) {
	var d delivery
	err := w.store.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		d, err = w.deliver(ctx, w.store.Tx(tx))
		return err
	})
	if err != nil || d.ev == nil {
		return false, err
	}
	if d.err == nil {
		return true, nil
	}
	// Logged after the commit, so a failure the store did not record is never reported
	if attempts := d.ev.Attempts + 1; attempts >= store.MaxOutboxAttempts {
		w.log.Error("outbox event is dead", "event", d.ev.ID, "type", d.ev.EventType,
			"attempts", attempts, "err", d.err)
	} else {
		w.log.Warn("outbox delivery failed", "event", d.ev.ID, "attempts", attempts,
			"err", d.err)
	}
	return true, nil
}

// deliver posts the oldest due event and records on ts whether it arrived
func (w *Worker) deliver(ctx context.Context, ts *store.Store) (delivery, error) {
	due, err := ts.PendingOutbox(ctx, w.now(), 1)
	if err != nil || len(due) == 0 {
		return delivery{}, err
	}
	ev := due[0]
	d := delivery{ev: &ev, err: w.post(ctx, ev)}
	if d.err == nil {
		return d, ts.MarkOutboxDelivered(ctx, ev.ID)
	}
	return d, ts.MarkOutboxFailed(ctx, ev.ID, d.err.Error(), w.now().Add(backoff(ev.Attempts)))
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
		return fmt.Errorf("build request: %w", redact(err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bugnet-Event-Id", ev.ID.String())
	req.Header.Set("X-Bugnet-Signature",
		base64.StdEncoding.EncodeToString(ed25519.Sign(w.d.SigningKey, raw)))
	resp, err := w.http.Do(req)
	if err != nil {
		return redact(err)
	}
	defer func() { _ = resp.Body.Close() }()
	// Reading a bounded rest of the answer lets the connection be reused
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainedBytes))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// redact keeps a request error's method and cause but drops the URL, whose query may hold secrets
func redact(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: %w", ue.Op, ue.Err)
	}
	return err
}
