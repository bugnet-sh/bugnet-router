-- +goose Up
create table start_samples (
    id uuid primary key default gen_random_uuid(),
    -- placement, canary or bench
    kind text not null check (kind in ('placement', 'canary', 'bench')),
    bench_id text,
    placement_id uuid,
    provider text not null,
    size_name text not null default '',
    -- the provider's id once Create returned one
    sandbox_id text not null default '',
    started_at timestamptz not null,
    create_ms int,
    ready_ms int,
    -- null where the runner is baked in
    runner_ms int,
    -- null when the gate steps were skipped
    gate_ms int,
    first_cmd_ms int,
    warm_cmd_ms int,
    destroy_ms int,
    outcome text not null check (outcome in ('ok', 'failed', 'capacity', 'rate_limited', 'quota')),
    error_code text not null default '',
    error text not null default '',
    cost_micros bigint not null default 0,
    replica text not null default ''
);

create index start_samples_provider_idx on start_samples (provider, started_at desc);
create index start_samples_started_idx on start_samples (started_at);

-- one row per probe holding a capacity slot, so the recount can tell it from a placement's
create table probe_slots (
    id uuid primary key,
    provider text not null,
    reserved_at timestamptz not null default now()
);

-- +goose Down
drop table probe_slots;
drop table start_samples;
