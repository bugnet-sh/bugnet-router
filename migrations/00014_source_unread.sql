-- +goose Up
set local lock_timeout = '5s';

alter table placements
    add column source_unread text,
    add column bootstrap_revision text;

-- +goose Down
alter table placements drop column bootstrap_revision, drop column source_unread;
