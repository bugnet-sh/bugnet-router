-- +goose Up
create table session_records (
    id text primary key,                      -- 16 random lower-case base32 characters, the URL id
    placement_id uuid not null unique references placements (id),
    created_at timestamptz not null default now(),
    ended_at timestamptz,                     -- the placement's end, set at the copy
    expires_at timestamptz,                   -- ended_at + sessions.keep (48 h, D-a)
    closed_at timestamptz,                    -- the public page ended early
    closed_by text,                           -- user, operator or abuse
    hold_until timestamptz,                   -- operator-only hold (D-b), never served
    generation int not null default 0,        -- D-g (b): a new link adds 1
    copy_state text not null default 'pending', -- pending, complete, partial, expired
    events_dropped int not null default 0
);

create table session_events (
    id bigint generated always as identity primary key,
    record_id text not null references session_records (id) on delete cascade,
    at timestamptz not null,
    kind text not null,
    data jsonb not null
);
create index session_events_record_idx on session_events (record_id, at);

create table session_jobs (
    record_id text not null references session_records (id) on delete cascade,
    seq int not null,                         -- 1, 2, 3 in start order; job ids never shown
    job_id text not null,                     -- internal, maps seq to the stream (F13)
    command text,                             -- D-f, sanitised and redacted
    started_at timestamptz not null,
    finished_at timestamptz,
    exit_code int,
    frames jsonb compression lz4,             -- [{"s":"o"|"e","t":"text","cr":false}], in order
    cut_bytes bigint not null default 0,      -- output not kept
    primary key (record_id, seq)
);

-- +goose Down
drop table session_jobs;
drop table session_events;
drop table session_records;
