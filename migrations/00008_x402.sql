-- +goose Up
create table x402_services (
    placement_id uuid not null references placements (id) on delete cascade,
    -- xs_ and 16 base32 characters, the same id the gateway and Payments compute
    service_id text not null check (service_id ~ '^xs_[a-z2-7]{16}$'),
    host text not null,
    -- starts and ends with a slash, never only a slash
    prefix text not null check (prefix ~ '^/.+/$'),
    method text not null check (method in ('GET', 'POST')),
    max_price_micros bigint not null check (max_price_micros > 0),
    -- egress's pre-check only, Payments holds the pin that decides
    pay_to text not null,
    indexed boolean not null,
    -- the one paid path of a service that is not indexed, null when indexed
    paid_path text,
    state text not null check (state in ('applying', 'active', 'failed', 'disabled')),
    failure_reason text,
    -- counts from x402 events, for bugnet_status
    requests int not null default 0,
    paid int not null default 0,
    uncertain int not null default 0,
    updated_at timestamptz not null default now(),
    primary key (placement_id, service_id),
    check ((paid_path is null) = indexed)
);

-- refusals per placement and reason, with the last time, for bugnet_status and the exec hint
create table x402_refusals (
    placement_id uuid not null references placements (id) on delete cascade,
    reason text not null,
    count int not null default 0,
    last_at timestamptz not null,
    primary key (placement_id, reason)
);

-- the hosts the last apply_secrets run wrote, so a later run sees a removed host
alter table placements add column applied_hosts text[] not null default '{}';
-- the order asked for secrets, so x402 may be enabled while placing (6.1)
alter table placements add column needs_secrets boolean not null default false;

-- +goose Down
alter table placements drop column needs_secrets, drop column applied_hosts;
drop table x402_refusals;
drop table x402_services;
