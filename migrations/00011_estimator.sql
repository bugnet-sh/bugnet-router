-- +goose Up
-- a long query holding placements or jobs makes the deploy fail instead of queueing every query
-- behind the alter
set local lock_timeout = '5s';

-- distilled facts, the long-term cache and the replay store
create table source_facts (
    -- github.com/o/r or huggingface.co/org/name lowercased, a subdirectory after the repository
    source_key text not null,
    revision text not null,
    extractor_version text not null,
    -- RFC 8785 bytes, at most 8 KiB
    facts bytea not null,
    facts_sha256 bytea not null,
    fetched_at timestamptz not null default now(),
    -- moves on each use, at most hourly
    used_at timestamptz not null default now(),
    primary key (source_key, revision, extractor_version)
);

create index source_facts_used_at_idx on source_facts (used_at);

-- every catalog an estimate saw
create table catalogs (
    sha256 bytea primary key,
    -- RFC 8785 bytes of the packages and the image manifest
    bytes bytea not null,
    first_seen timestamptz not null default now()
);

-- one row per distinct answer
create table estimates (
    id uuid primary key,
    created_at timestamptz not null default now(),
    -- moves when a quote reuses the row
    served_at timestamptz not null default now(),
    estimator_version text not null,
    inputs_sha256 bytea not null,
    -- RFC 8785 bytes, facts named by reference, no command text
    inputs bytea not null,
    -- RFC 8785 bytes of the floor, reasons, warnings, fits and recommendation
    output bytea not null,
    -- RFC 8785 bytes of the packages the recommendation saw available
    availability bytea not null,
    catalog_sha256 bytea not null references catalogs (sha256),
    source_kind text,
    source_key text,
    revision text,
    lock_key text,
    workload text,
    source_read boolean not null default false
);

create index estimates_inputs_idx on estimates (inputs_sha256, served_at desc);
create index estimates_served_at_idx on estimates (served_at);

-- one row per placement with stats and an estimate
create table usage_observations (
    placement_id uuid primary key references placements (id),
    estimate_id uuid references estimates (id),
    source_key text,
    revision text,
    lock_key text,
    workload text,
    entry_sha256 bytea,
    package_id text not null,
    size_name text,
    peak_memory_mib int,
    peak_swap_mib int,
    peak_disk_mib int,
    gpu_peak_memory_mib int,
    cpu_seconds bigint,
    busy_p95_millicores int,
    active_seconds int,
    install_seconds int,
    oom_kills int not null default 0,
    extended boolean not null default false,
    -- the termination reason
    ended_by text,
    -- true when ended by abuse or control_lost, or flagged after the end
    flagged boolean not null default false,
    recorded_at timestamptz not null default now()
);

create index usage_observations_source_idx
    on usage_observations (source_key, workload, recorded_at desc);

alter table placements
    add column estimate_id uuid references estimates (id),
    add column source_kind text,
    add column source_canonical text,
    add column source_ref text,
    add column source_revision text,
    add column source_read boolean,
    add column bootstrap_state text,
    add column bootstrap_job_id text,
    add column bootstrap_path text,
    add column bootstrap_reason text,
    add column bootstrap_heartbeat timestamptz,
    add column bootstrap_attempt int not null default 0,
    add column bootstrap_private_attempts int not null default 0;

create index placements_estimate_id_idx on placements (estimate_id) where estimate_id is not null;

alter table jobs
    add column oom_killed boolean not null default false,
    add column peak_memory_mib int,
    add column install boolean not null default false;

-- +goose Down
alter table jobs drop column install, drop column peak_memory_mib, drop column oom_killed;
drop index placements_estimate_id_idx;
alter table placements
    drop column bootstrap_private_attempts, drop column bootstrap_attempt,
    drop column bootstrap_heartbeat, drop column bootstrap_reason, drop column bootstrap_path,
    drop column bootstrap_job_id, drop column bootstrap_state, drop column source_read,
    drop column source_revision, drop column source_ref, drop column source_canonical,
    drop column source_kind, drop column estimate_id;
drop table usage_observations;
drop table estimates;
drop table catalogs;
drop table source_facts;
