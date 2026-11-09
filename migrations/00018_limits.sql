-- +goose Up
alter table provider_capacity
    add column external_in_use int not null default 0 check (external_in_use >= 0),
    add column limit_source text not null default 'default'
        check (limit_source in ('provider', 'settings', 'default')),
    add column provider_active int not null default 0,
    add column polled_at timestamptz;

create table provider_health (
    provider text primary key,
    -- ok, slow or down, from start samples
    state text not null default 'ok',
    state_since timestamptz not null default now(),
    samples int not null default 0,
    success_lb real not null default 1,
    ready_p95_ms int not null default 0,
    -- banned, unfunded or auth, null while the account is usable
    blocked text check (blocked in ('banned', 'unfunded', 'auth')),
    blocked_detail text not null default '',
    blocked_since timestamptz,
    account_checked_at timestamptz,
    foreign_sandboxes int not null default 0,
    updated_at timestamptz not null default now()
);

-- +goose Down
drop table provider_health;
alter table provider_capacity
    drop column polled_at, drop column provider_active, drop column limit_source,
    drop column external_in_use;
