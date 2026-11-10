-- +goose Up
alter table start_samples drop constraint start_samples_kind_check;
alter table start_samples add constraint start_samples_kind_check
    check (kind in ('placement', 'canary', 'bench', 'pool', 'reserve'));
alter table start_samples drop constraint start_samples_outcome_check;
alter table start_samples add constraint start_samples_outcome_check
    check (outcome in ('ok', 'failed', 'capacity', 'rate_limited', 'quota', 'hedge_lost'));

-- +goose Down
delete from start_samples where kind in ('pool', 'reserve') or outcome = 'hedge_lost';
alter table start_samples drop constraint start_samples_outcome_check;
alter table start_samples add constraint start_samples_outcome_check
    check (outcome in ('ok', 'failed', 'capacity', 'rate_limited', 'quota'));
alter table start_samples drop constraint start_samples_kind_check;
alter table start_samples add constraint start_samples_kind_check
    check (kind in ('placement', 'canary', 'bench'));
