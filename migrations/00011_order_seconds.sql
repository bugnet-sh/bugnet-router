-- +goose Up
-- a long query holding orders or placements makes the deploy fail instead of queueing every
-- query behind the alter
set local lock_timeout = '5s';

alter table orders
    -- the paid seconds the order added to the deadline, 0 for orders from before this column,
    -- which refund nothing
    add column seconds_bought integer not null default 0;

-- the refundable provider_failed ends of the last day, counted per start payer (CP 6.4)
create index placements_failed_refunds_idx on placements (terminating_at)
    where termination_reason = 'provider_failed' and refundable;

-- +goose Down
drop index placements_failed_refunds_idx;
alter table orders drop column seconds_bought;
