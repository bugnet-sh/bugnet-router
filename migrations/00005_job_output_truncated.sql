-- +goose Up
alter table jobs add column output_truncated boolean not null default false;

-- +goose Down
alter table jobs drop column output_truncated;
