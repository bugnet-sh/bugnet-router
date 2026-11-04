-- +goose Up
alter table placements add column capacity_provider text;

-- Placements that already hold a slot keep it under the new column
update placements set capacity_provider = provider where state in ('ready', 'terminating');
update placements p set capacity_provider = a.provider
from placement_attempts a
where a.placement_id = p.id and a.state = 'creating' and p.state = 'placing';

-- +goose Down
alter table placements drop column capacity_provider;
