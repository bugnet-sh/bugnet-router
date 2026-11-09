-- +goose Up
-- a long query holding placements makes the deploy fail instead of queueing every query behind
-- the index
set local lock_timeout = '5s';

-- the history query counts the placements a payer paid for
create index placements_payer_idx on placements (payer) where payer is not null;

-- +goose Down
drop index placements_payer_idx;
