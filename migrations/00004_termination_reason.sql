-- +goose Up
alter table placements add column termination_reason text, add column terminating_at timestamptz;

-- +goose Down
alter table placements drop column terminating_at, drop column termination_reason;
