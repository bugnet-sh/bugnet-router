-- +goose Up
-- the hourly figure a quote priced its server at, a per-node server's rent cap, 0 for none
alter table price_quotes add column hourly_micros bigint not null default 0;

-- what the node a placement's sandbox runs on costs an hour as rented, 0 when unknown
alter table placements add column rented_hourly_micros bigint not null default 0;

-- +goose Down
alter table placements drop column rented_hourly_micros;
alter table price_quotes drop column hourly_micros;
