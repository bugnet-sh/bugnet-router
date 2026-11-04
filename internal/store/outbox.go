package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// MaxOutboxAttempts is how many failed deliveries an event gets before it is left undelivered
const MaxOutboxAttempts = 10

// OutboxEvent is an event waiting for delivery
type OutboxEvent struct {
	ID        uuid.UUID
	EventType string
	Payload   json.RawMessage
	CreatedAt time.Time
	Attempts  int
	LastError string
}

// InsertOutbox adds an event on tx so it commits with the state change that caused it
func (s *Store) InsertOutbox(ctx context.Context, tx pgx.Tx, eventType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	_, err = tx.Exec(ctx,
		`insert into outbox (id, event_type, payload) values ($1, $2, $3)`,
		uuid.New(), eventType, raw)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

// PendingOutbox locks up to limit events due at now, oldest first, skipping ones locked elsewhere
func (s *Store) PendingOutbox(
	ctx context.Context, now time.Time, limit int,
) ([]OutboxEvent, error) {
	rows, err := s.q.Query(ctx, `
		select id, event_type, payload, created_at, attempts, coalesce(last_error, '')
		from outbox
		where delivered_at is null and attempts < $1
			and (next_attempt_at is null or next_attempt_at <= $2)
		order by created_at, id limit $3
		for update skip locked`, MaxOutboxAttempts, utc(now), limit)
	if err != nil {
		return nil, fmt.Errorf("pending outbox: %w", err)
	}
	defer rows.Close()
	var out []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(
			&e.ID, &e.EventType, &e.Payload, &e.CreatedAt, &e.Attempts, &e.LastError,
		); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		e.CreatedAt = utc(e.CreatedAt)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pending outbox: %w", err)
	}
	return out, nil
}

// MarkOutboxDelivered records that the event was delivered
func (s *Store) MarkOutboxDelivered(ctx context.Context, id uuid.UUID) error {
	_, err := s.q.Exec(ctx, `update outbox set delivered_at = now() where id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark outbox delivered: %w", err)
	}
	return nil
}

// MarkOutboxFailed counts a failed delivery and holds the event back until retryAt
func (s *Store) MarkOutboxFailed(
	ctx context.Context, id uuid.UUID, msg string, retryAt time.Time,
) error {
	_, err := s.q.Exec(ctx, `
		update outbox set attempts = attempts + 1, last_error = $2, next_attempt_at = $3
		where id = $1`, id, msg, utc(retryAt))
	if err != nil {
		return fmt.Errorf("mark outbox failed: %w", err)
	}
	return nil
}
