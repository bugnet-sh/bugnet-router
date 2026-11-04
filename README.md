# bugnet-router

The Bugnet sandbox router: `router-api` and `router-worker` share one Postgres schema.
Postgres is the system of record, Valkey carries output streams and leases, River runs
durable jobs.

Run `make test`, `make lint` and `make build`. Tests use the local Postgres and Valkey
(`BUGNET_TEST_PG_URL`, default `postgres://localhost:5432/postgres?sslmode=disable`, and
`BUGNET_TEST_VALKEY_ADDR`, default `localhost:6379`). Each test creates its own database
and key prefix.
