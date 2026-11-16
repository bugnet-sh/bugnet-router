-- +goose Up
create table price_quotes (
    estimate_id uuid not null,                     -- no foreign key, an estimate may be pruned first
    package_id text not null,
    provider text not null,
    size_name text not null,
    cost_micros bigint not null,
    price_micros bigint not null,
    quoted_at timestamptz not null default now(),
    order_id text,                                 -- the order that claimed it, null for none
    primary key (estimate_id, package_id)
);

create index price_quotes_quoted_at_idx on price_quotes (quoted_at);

-- +goose Down
drop table price_quotes;
