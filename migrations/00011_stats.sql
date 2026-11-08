-- +goose Up
-- a long query holding orders or placements makes the deploy fail instead of queueing every
-- query behind the alter
set local lock_timeout = '5s';

create table stats_snapshot (
    id smallint primary key check (id = 1),
    -- the served JSON, null until a Payments part exists
    body bytea,
    -- when the router part was read
    router_at timestamptz not null,
    -- the last good Payments answer, as checked
    payments jsonb,
    -- when that answer came
    payments_at timestamptz,
    updated_at timestamptz not null default now()
);

-- the order token's Tx claim, as Verify checked it, null for orders taken before this column
alter table orders add column tx text;

-- the sessions route's keyset order, orders_placement_id_idx (00010) serves its price sum
create index placements_created_idx on placements (created_at desc, id desc);

-- +goose Down
drop index placements_created_idx;
alter table orders drop column tx;
drop table stats_snapshot;
