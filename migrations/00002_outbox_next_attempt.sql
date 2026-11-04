-- +goose Up
alter table outbox add column next_attempt_at timestamptz;

-- +goose Down
alter table outbox drop column next_attempt_at;
