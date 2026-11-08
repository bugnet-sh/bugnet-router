-- +goose Up
-- one x402 or x402_resolved row per (record, ref), so a resent batch stores nothing twice
create unique index session_events_ref on session_events (record_id, kind, (data->>'ref'))
    where kind in ('x402', 'x402_resolved');

-- +goose Down
drop index session_events_ref;
