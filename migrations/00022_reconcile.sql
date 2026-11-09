-- +goose Up
-- a long query holding placements makes the deploy fail instead of queueing every query behind it
set local lock_timeout = '5s';

alter table placements
    -- when the provider first reported the sandbox degraded, null while it is not
    add column degraded_since timestamptz;

alter table placement_attempts
    -- the provider's id, written when Create returned
    add column sandbox_id text;

-- finds a placement by its sandbox for webhooks and abuse reports
create index placements_provider_ref_idx on placements (provider, (provider_ref ->> 'id'));

-- +goose Down
drop index placements_provider_ref_idx;
alter table placement_attempts drop column sandbox_id;
alter table placements drop column degraded_since;
