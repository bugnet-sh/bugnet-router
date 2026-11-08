-- +goose Up
-- the secrets_version the last apply_secrets run left, so a delete since then is still pending
alter table placements add column secrets_applied_version bigint not null default 0;

-- +goose Down
alter table placements drop column secrets_applied_version;
