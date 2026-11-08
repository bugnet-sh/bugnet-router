-- +goose Up
-- a long query holding orders or placements makes the deploy fail instead of queueing every
-- query behind the alter
set local lock_timeout = '5s';

alter table orders
    -- the payer as the router decided it, null when the order counts as having none
    add column payer text,
    -- the token's fee_payer claim, the facilitator's address
    add column fee_payer text,
    -- the package share, a start token's x402 allowance left out
    add column amount_micros bigint not null default 0;

create index orders_fee_payer_idx on orders (fee_payer) where fee_payer is not null;
-- the orders a payer paid, read for each blocked payer's live placements
create index orders_payer_idx on orders (payer) where payer is not null;
create index orders_placement_id_idx on orders (placement_id);

alter table placements
    -- the start order's payer as the router decided it
    add column payer text,
    add column end_evidence jsonb,
    add column end_actor text;

create index placements_live_payer_idx on placements (payer)
    where state in ('placing', 'ready', 'terminating');

create table wallets (
    payer text primary key,
    first_seen timestamptz not null,
    -- moves on a new order at most hourly
    last_seen timestamptz not null,
    -- null when not blocked
    blocked_at timestamptz,
    -- null with blocked_at set means until an operator lifts it
    blocked_until timestamptz,
    blocked_reason text,
    blocked_by text
);

create index wallets_blocked_idx on wallets (payer) where blocked_at is not null;

create table wallet_events (
    id uuid primary key,
    payer text not null,
    kind text not null check (kind in ('block', 'unblock', 'strike', 'note')),
    reason text not null default '',
    placement_id uuid,
    provider text,
    -- never secrets
    evidence jsonb not null default '{}',
    actor text not null,
    created_at timestamptz not null default now()
);

create index wallet_events_payer_idx on wallet_events (payer, created_at desc);

create table provider_signals (
    id uuid primary key,
    class text not null,
    provider text not null,
    -- null for account signals
    placement_id uuid,
    source text not null,
    observed jsonb not null default '{}',
    action text not null,
    -- the class the correlation guard held back, null otherwise
    held_class text,
    created_at timestamptz not null default now()
);

create index provider_signals_provider_idx on provider_signals (provider, created_at desc);
create index provider_signals_created_idx on provider_signals (created_at);

-- +goose Down
drop table provider_signals;
drop table wallet_events;
drop table wallets;
drop index placements_live_payer_idx;
alter table placements drop column end_actor, drop column end_evidence, drop column payer;
drop index orders_placement_id_idx;
drop index orders_payer_idx;
drop index orders_fee_payer_idx;
alter table orders drop column amount_micros, drop column fee_payer, drop column payer;
