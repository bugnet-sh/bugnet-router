-- +goose Up
-- a long query holding placements makes the deploy fail instead of queueing every query behind it
set local lock_timeout = '5s';

create table pool_members (
    id uuid primary key,
    provider text not null,
    size_name text not null,
    origin text not null check (origin in ('pool', 'provider', 'reserve')),
    state text not null
        check (state in ('filling', 'warm', 'held', 'bound', 'retiring', 'gone')),
    idempotency_key text not null unique,          -- pool-<id>, reserve-<id> or a claim key
    provider_ref jsonb,                            -- sealed as data-plane 4.4, the id as AAD
    image text,                                    -- template or image it was built from
    sandbox_created_at timestamptz,
    holds_slot bool not null default false,
    reservation_id uuid,
    placement_id uuid,
    created_at timestamptz not null default now(),
    warm_at timestamptz,
    parked_at timestamptz,
    claimed_at timestamptz,
    bound_at timestamptz,
    ended_at timestamptz,
    end_reason text,                               -- aged, unused, lapsed, failed, drained, gone
    error text
);

create index pool_members_warm_idx on pool_members (provider, size_name, warm_at)
    where state = 'warm';
create index pool_members_live_idx on pool_members (state)
    where state in ('filling', 'warm', 'held', 'retiring');
create index pool_members_sandbox_idx on pool_members (provider, (provider_ref->>'id'));
create index pool_members_placement_idx on pool_members (placement_id)
    where placement_id is not null;

create table reservations (
    id uuid primary key,
    quote_sha256 bytea not null,
    package_id text not null,
    requirements jsonb not null,
    placement_id uuid not null unique,             -- minted at Reserve, the bound placement's id
    key_id text not null,                          -- data-plane root that derived its gate key
    member_id uuid not null references pool_members (id),
    path text not null check (path in ('pooled', 'speculative')),
    state text not null check (state in
        ('preparing', 'ready', 'awaited', 'bound', 'lapsed', 'released', 'failed')),
    prepared jsonb,                                -- Prepare's answer without the ref (18.11.4)
    client_ip inet,                                -- outside shared_ranges only, cleared at end
    owner text,                                    -- the replica preparing it
    heartbeat timestamptz,
    created_at timestamptz not null default now(),
    expires_at timestamptz not null,
    hold_until timestamptz not null,
    ready_at timestamptz,
    ended_at timestamptz,
    failure text
);

create unique index reservations_open_quote_idx on reservations (quote_sha256)
    where state in ('preparing', 'ready', 'awaited');
create index reservations_quote_idx on reservations (quote_sha256);
create index reservations_open_idx on reservations (expires_at)
    where state in ('preparing', 'ready');
create index reservations_ended_idx on reservations (ended_at)
    where state in ('bound', 'lapsed', 'released', 'failed');

create table pool_pauses (
    provider text not null,
    size_name text not null,
    paused_until timestamptz not null,
    reason text not null,                          -- drained, failing or spend
    actor text,
    primary key (provider, size_name)
);

create table pool_targets (
    provider text not null,
    size_name text not null,
    target int not null,                           -- pool_tick's last answer (18.6.5)
    computed_at timestamptz not null,
    primary key (provider, size_name)
);

create table pool_misses (
    provider text not null,
    size_name text not null,
    minute timestamptz not null,                   -- date_trunc('minute', now())
    misses int not null,
    primary key (provider, size_name, minute)
);

alter table placements
    add column start_path text,                    -- cold, pooled or reserved
    add column hedged bool not null default false,
    add column paid_at timestamptz,
    add column sandbox_created_at timestamptz,
    add column capacity_attempt int;               -- the attempt whose slot the row holds

alter table placement_attempts
    add column path text,
    add column member_id uuid,
    add column hedge bool not null default false,
    add column slot_provider text;                 -- a parallel attempt's own slot

create index placement_attempts_slot_idx on placement_attempts (slot_provider)
    where slot_provider is not null;

alter table jobs add column first_frame_ms int;

alter table start_samples
    add column start_path text,
    add column member_id uuid,
    add column reservation_id uuid,
    add column hedge bool not null default false,
    add column wake_ms int,
    add column bind_ms int,
    add column paid_to_ready_ms int;

alter table provider_health
    add column cold_total_p95_ms int,
    add column pooled_total_p95_ms int;

-- +goose Down
alter table provider_health drop column pooled_total_p95_ms, drop column cold_total_p95_ms;
alter table start_samples
    drop column paid_to_ready_ms, drop column bind_ms, drop column wake_ms, drop column hedge,
    drop column reservation_id, drop column member_id, drop column start_path;
alter table jobs drop column first_frame_ms;
drop index placement_attempts_slot_idx;
alter table placement_attempts
    drop column slot_provider, drop column hedge, drop column member_id, drop column path;
alter table placements
    drop column capacity_attempt, drop column sandbox_created_at, drop column paid_at,
    drop column hedged, drop column start_path;
drop table pool_misses;
drop table pool_targets;
drop table pool_pauses;
drop table reservations;
drop table pool_members;
