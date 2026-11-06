-- +goose Up
alter table placements
    add column key_id text,                        -- data-plane root key of this placement
    add column ssh_alias text unique,
    add column source_label text,
    add column edge_budget_bytes bigint not null default 0,
    add column egress_budget_bytes bigint not null default 0,
    add column egress_token_sha256 bytea unique,
    add column egress_ca_version int,
    add column secrets_version bigint not null default 0,  -- moves on every secret change
    add column gate_ready_at timestamptz;

create table previews (
    id text primary key,                           -- 16 base32 characters, the host label
    placement_id uuid not null references placements (id),
    port int not null check (port between 1 and 65535),
    generation int not null default 0,
    created_at timestamptz not null default now(),
    revoked_at timestamptz,                        -- set by RevokePreview, CreatePreview refuses
    unique (placement_id, port)
);

create table ssh_keys (
    placement_id uuid not null references placements (id),
    fingerprint text not null,                     -- SHA256:...
    public_key bytea not null,                     -- SSH wire format
    created_at timestamptz not null default now(),
    removed_at timestamptz,
    primary key (placement_id, fingerprint)
);

create table ssh_hosts (
    placement_id uuid primary key references placements (id),
    transport text not null,                       -- gate | boat_native
    sshd_port int not null,
    ssh_user text not null,                        -- Capabilities.ExecUser, never ssh-up output
    host_key text not null,                        -- authorized_keys format
    prepared_at timestamptz not null default now()
);

create table ssh_gateway_keys (
    placement_id uuid not null references placements (id),
    gateway_key_fp text not null,
    authorized_at timestamptz not null default now(),
    primary key (placement_id, gateway_key_fp)
);

create table secret_links (
    id uuid primary key,
    placement_id uuid not null references placements (id),
    token_sha256 bytea unique not null,
    pairing_code text not null,
    suggested_name text,
    suggested_hosts text[] not null default '{}',
    suggested_header_names text[] not null default '{}',
    created_at timestamptz not null default now(),
    expires_at timestamptz not null,
    used_at timestamptz,
    submission_id text,
    failed_attempts int not null default 0
);

create table secrets (
    id uuid primary key,
    placement_id uuid not null references placements (id),
    name text not null,
    hosts text[] not null,
    header_names text[] not null default '{}',
    fetch_only bool not null default false,
    placeholder_sha256 bytea unique not null,
    sealed bytea not null,
    key_id text not null,
    source text not null,                          -- link | bridge | gateway
    state text not null,                           -- applying | active | failed
    failure_reason text,
    idempotency_key text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (placement_id, name)
);

create index secrets_placement_idx on secrets (placement_id);
create index previews_placement_idx on previews (placement_id);

-- +goose Down
drop table secrets;
drop table secret_links;
drop table ssh_gateway_keys;
drop table ssh_hosts;
drop table ssh_keys;
drop table previews;
alter table placements
    drop column gate_ready_at, drop column secrets_version, drop column egress_ca_version,
    drop column egress_token_sha256,
    drop column egress_budget_bytes, drop column edge_budget_bytes, drop column source_label,
    drop column ssh_alias, drop column key_id;
