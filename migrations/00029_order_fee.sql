-- +goose Up
alter table orders add column fee_micros bigint not null default 0; -- the x402 session fee paid

-- +goose Down
alter table orders drop column fee_micros;
