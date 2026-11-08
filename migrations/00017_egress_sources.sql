-- +goose Up
alter table placements
    add column egress_source cidr,                -- provider-reported prefix tunnels come from
    add column egress_source_bind inet,           -- the address the forwarder dials from
    add column egress_source_read_at timestamptz; -- the last read that stored a usable source

-- +goose Down
alter table placements
    drop column egress_source_read_at, drop column egress_source_bind,
    drop column egress_source;
