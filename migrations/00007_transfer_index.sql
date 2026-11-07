-- +goose Up
create table transfer_index (
    token_sha256 bytea primary key,
    placement_id uuid not null references placements (id),
    created_at timestamptz not null default now()
);

create index transfer_index_created_idx on transfer_index (created_at);

-- +goose Down
drop table transfer_index;
