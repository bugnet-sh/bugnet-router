-- +goose Up
alter table outbox
    add column traceparent text,                   -- W3C context of the span that wrote the row
    add column tracestate text;

-- +goose Down
alter table outbox drop column tracestate, drop column traceparent;
