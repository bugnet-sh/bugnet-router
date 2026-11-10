-- +goose Up
-- a long query holding placements makes the deploy fail instead of queueing every query behind it
set local lock_timeout = '5s';

alter table placements
    -- our meter's provider cost, summed over the attempts and written at termination
    add column cost_micros bigint not null default 0,
    -- what the edges and the egress proxy counted, written at termination
    add column edge_bytes bigint not null default 0,
    add column egress_bytes bigint not null default 0;

alter table placement_attempts
    -- the size the attempt asked for, which prices its provider time
    add column size_name text,
    -- the start fee the provider quoted when the slot was reserved
    add column start_fee_micros bigint not null default 0,
    -- when the attempt's sandbox was destroyed, null while it lives or never existed
    add column destroyed_at timestamptz;

-- one row per provider and UTC day, theirs read page by page from the provider's usage
create table provider_cost_daily (
    provider text not null,
    day date not null,
    ours_micros bigint not null default 0,
    -- -1 when the provider gives none
    theirs_micros bigint not null default 0,
    theirs_complete boolean not null default false,
    unknown_ids int not null default 0,
    -- the next page to read, null once the day is read
    cursor text,
    checked_at timestamptz not null default now(),
    -- the day was first read at this time, so an incomplete day can alert after two days
    first_checked_at timestamptz not null default now(),
    -- the day's sandbox ids that were not ours, kept between pages for the count
    seen_unknown text[] not null default '{}',
    primary key (provider, day)
);

-- +goose Down
drop table provider_cost_daily;
alter table placement_attempts drop column destroyed_at;
alter table placement_attempts drop column start_fee_micros;
alter table placement_attempts drop column size_name;
alter table placements drop column egress_bytes;
alter table placements drop column edge_bytes;
alter table placements drop column cost_micros;
