-- +goose Up
create table provider_capacity (
    provider text primary key,
    max_concurrent int not null,
    in_use int not null default 0 check (in_use >= 0)
);

create table orders (
    order_id text primary key,
    kind text not null,
    placement_id uuid not null,
    created_at timestamptz not null default now()
);

create table placements (
    id uuid primary key,
    handle_sha256 bytea unique not null,
    order_id text not null,
    package_id text not null,
    state text not null,
    provider text,
    provider_ref jsonb,
    size_name text,
    deadline timestamptz,
    created_at timestamptz not null default now(),
    ready_at timestamptz,
    terminated_at timestamptz,
    failure_reason text,
    refundable bool not null default false,
    saga_owner text,
    saga_heartbeat timestamptz
);

create index placements_state_deadline_idx on placements (state, deadline);

create table placement_attempts (
    placement_id uuid not null references placements (id),
    attempt int not null,
    provider text,
    idempotency_key text,
    state text,
    error_code text,
    error text,
    created_at timestamptz not null default now(),
    primary key (placement_id, attempt)
);

create table jobs (
    id text primary key,
    placement_id uuid not null references placements (id),
    idempotency_key text,
    state text not null,
    exit_code int,
    stdout_bytes bigint not null default 0,
    stderr_bytes bigint not null default 0,
    created_at timestamptz not null default now(),
    finished_at timestamptz
);

create index jobs_placement_id_idx on jobs (placement_id);

create table outbox (
    id uuid primary key,
    event_type text not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    delivered_at timestamptz,
    attempts int not null default 0,
    last_error text
);

create index outbox_pending_idx on outbox (created_at) where delivered_at is null;

-- +goose Down
drop table outbox;
drop table jobs;
drop table placement_attempts;
drop table placements;
drop table orders;
drop table provider_capacity;
