-- +goose Up
-- one row per benchmark, whose samples sit in start_samples under its id
create table benches (
    id text primary key,
    provider text not null,
    size_name text not null,
    runs int not null check (runs > 0),
    concurrency int not null check (concurrency > 0),
    estimated_cost_micros bigint not null default 0,
    actor text not null,
    created_at timestamptz not null default now(),
    finished_at timestamptz
);

create index benches_created_idx on benches (created_at);
create index start_samples_bench_idx on start_samples (bench_id) where bench_id is not null;

-- +goose Down
drop index start_samples_bench_idx;
drop table benches;
