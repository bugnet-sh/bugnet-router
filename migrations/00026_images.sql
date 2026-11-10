-- +goose Up
alter table placements
    add column image_version text,     -- the version the sandbox's manifest named
    add column image_manifest jsonb;   -- os and tools from the manifest, at most 2 KiB
alter table pool_members
    add column image_version text,     -- the same, read by the fill's Boot
    add column image_manifest jsonb;
alter table start_samples
    add column image_version text,
    add column reseed text;            -- ioctl | write | failed, null before setup
create index placements_image_live_idx on placements (provider, image_version)
    where state in ('ready', 'terminating');

-- +goose Down
drop index placements_image_live_idx;
alter table start_samples drop column reseed, drop column image_version;
alter table pool_members drop column image_manifest, drop column image_version;
alter table placements drop column image_manifest, drop column image_version;
