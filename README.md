# bugnet-router

The Bugnet sandbox router. It takes a paid order token, places a sandbox on one of several
providers, runs commands in it and ends it when the paid time is up.

Two binaries share one Postgres database. `router-api` serves the Connect API, runs the
placement saga and streams command output. `router-worker` ends placements at their deadline,
sweeps stray sandboxes and posts signed events to `BUGNET_EVENTS_URL`. Postgres is the system of
record, Valkey carries output streams and job leases, and River runs durable jobs.

## Layout

| Path | What it holds |
| --- | --- |
| `cmd/router-api` | API server, `router-api migrate` applies migrations and exits |
| `cmd/router-worker` | Deadline, watch, sweep and outbox jobs, `router-worker outbox-redrive` sends stuck events at once |
| `cmd/bugnet-dev` | Developer CLI for keys, tokens, placements and commands |
| `internal/api` | Connect handlers, health and readiness |
| `internal/saga` | Start, extend and stop of placements |
| `internal/placement` | Provider choice by price, margin and health |
| `internal/jobs`, `internal/streams` | Commands through `bugnet-run`, output streams, leases |
| `internal/worker` | River workers for deadlines, the sweep and the outbox |
| `internal/store`, `migrations` | Postgres access and schema |
| `internal/provider`, `internal/providers` | Provider port, adapters and the registry |
| `internal/config`, `internal/catalog` | Environment settings and the package catalog |
| `internal/ordertoken`, `internal/handle` | Order tokens and sandbox handles |
| `test/e2e` | End-to-end tests over the built binaries |
| `scripts` | `dev-up.sh` and the development catalog |
| `deploy/e2b` | E2B template definitions, one template per size |

## Prerequisites

- Go 1.27
- Postgres 18 on `localhost:5432`, the current user a superuser
- Valkey 9 on `localhost:6379`
- `bugnet-run` and `bugnet-proto` checked out next to this repository, `go.mod` points at them
  with `replace` until they are published

## Tests

```sh
go test -race ./...
```

Tests use the local Postgres and Valkey (`BUGNET_TEST_PG_URL`, default
`postgres://localhost:5432/postgres?sslmode=disable`, and `BUGNET_TEST_VALKEY_ADDR`, default
`localhost:6379`). Each test creates its own database and key prefix and removes them after.

The end-to-end tests build `bugnet-run`, `router-api`, `router-worker` and `bugnet-dev`, then
start two API replicas and one worker per test on the local provider. They take a little over
a minute because one waits for a one-minute package to expire. To run only them:

```sh
go test ./test/e2e/... -v
```

`make test`, `make lint` and `make build` wrap the usual commands.

## Running locally

```sh
createdb bugnet_router_dev
cp .env.example .env
scripts/dev-up.sh
```

`dev-up.sh` builds the binaries into `bin/`, writes fresh secrets to `.env.keys` with
`bin/bugnet-dev keys > .env.keys` when that file is missing, migrates the database and starts
one `router-api` and one `router-worker` on the local provider. Logs go to `bin/logs/`. Ctrl-C
stops both. Delete `.env.keys` to get new secrets. Both `.env` and `.env.keys` are ignored by
git.

The local provider runs commands as host processes under `bin/local/`. Use it only with code
you trust.

Drive the router from another terminal in the repository root:

```sh
set -a; . ./.env; . ./.env.keys; set +a
bin/bugnet-dev create --package dev-small-10m
bin/bugnet-dev exec --handle sbx_... -- sh -c 'echo hello; sleep 1; echo done'
bin/bugnet-dev extend --handle sbx_... --package dev-small-10m-more
bin/bugnet-dev stop --handle sbx_...
```

`create` signs an order token with `BUGNET_ORDER_PRIVATE_KEY`, priced from the catalog, and
waits until the sandbox is ready. `exec` streams the output and exits with the command's exit
code. If the stream breaks, it prints the `bugnet-dev watch` command that resumes from the last
cursor. `bugnet-dev token` prints a token for other clients, and `bugnet-dev` with no arguments
lists every command.

## Environment

Both `router-api` and `router-worker` read these. `router-api migrate` reads only the database
URL.

| Variable | Default | Meaning |
| --- | --- | --- |
| `BUGNET_DATABASE_URL` | required | Postgres URL of the router's database |
| `BUGNET_INTERNAL_TOKEN` | required | Bearer token for API calls, 32 characters or more |
| `BUGNET_ORDER_TOKEN_PUBLIC_KEY` | required | Base64 Ed25519 key that verifies order tokens |
| `BUGNET_HANDLE_KEY` | required | Base64 key of 32 bytes or more that derives handles |
| `BUGNET_PACKAGES_FILE` | required | Package catalog JSON |
| `BUGNET_VALKEY_ADDR` | `127.0.0.1:6379` | Valkey address |
| `BUGNET_VALKEY_PREFIX` | `bn:` | Prefix of every Valkey key |
| `BUGNET_PROVIDERS` | `koyeb,boat,e2b,boxd` | Providers in preference order |
| `BUGNET_MARGIN_FLOOR` | `0.5` | Lowest margin a placement may have, 0 to 10 |
| `BUGNET_REPLICA_ID` | host and pid | Name of this process in leases and logs |
| `BUGNET_DEPLOYMENT_ID` | `main` | Label that keeps the sweep to this deployment's sandboxes |
| `BUGNET_ENABLE_LOCAL_PROVIDER` | empty | Must be `1` for the local provider to start |
| `BUGNET_LOCAL_ROOT` | `$TMPDIR/bugnet-local` | Absolute directory of local sandboxes |
| `BUGNET_LOCAL_RUNNER` | required for local | Host build of `bugnet-run` the local provider runs |
| `BUGNET_LOCAL_HOURLY_MICROS` | `0` | Hourly cost the local provider reports |
| `BOXD_API_KEY` | empty | boxd credentials |
| `BUGNET_BOXD_REGION` | `fra` | boxd region, served at `<region>.boxd.sh:9443` outside `fra` |
| `BUGNET_BOXD_ENDPOINT` | `boxd.sh:9443` in `fra` | boxd gRPC address, overrides the region |
| `E2B_API_KEY` | empty | E2B credentials |
| `BUGNET_E2B_TEMPLATES` | empty | Comma-separated E2B sizes, `deploy/e2b` gives the format |
| `BUGNET_E2B_DOMAIN` | `e2b.app` | E2B region domain, `e2b-juliett.dev` for EU-West-1 |
| `BUGNET_E2B_MAX_LIFETIME` | `1h` | Longest lifetime the E2B plan allows, `24h` on Pro |
| `KOYEB_API_TOKEN` | empty | Koyeb credentials |
| `BUGNET_KOYEB_REGION` | `fra` | Koyeb region every sandbox runs in |
| `BUGNET_KOYEB_INSTANCE_TYPES` | every fra type | Comma-separated `size=instanceType` sizes to sell |
| `BOAT_API_KEY` | empty | Boat credentials |
| `BUGNET_BOAT_TYPES` | every type | Comma-separated `size=type` sizes, types `small` to `xlarge` |

Only `router-api` reads these.

| Variable | Default | Meaning |
| --- | --- | --- |
| `BUGNET_LISTEN_ADDR` | `:8080` | API listen address |
| `BUGNET_RUNNER_LINUX_AMD64` | required for boxd, E2B, Koyeb and Boat | Linux amd64 `bugnet-run` uploaded into their sandboxes, `make build` in `bugnet-run` writes `dist/bugnet-run-linux-amd64` |
| `BUGNET_RUNNER_LINUX_ARM64` | empty | Linux arm64 `bugnet-run` for providers with arm64 sandboxes, none today |

`router-api` reads each build once at start and refuses to start when a configured provider
needs one that is unset or unreadable.

Only `router-worker` reads these.

| Variable | Default | Meaning |
| --- | --- | --- |
| `BUGNET_EVENT_SIGNING_KEY` | required | Base64 32-byte Ed25519 seed that signs events |
| `BUGNET_EVENTS_URL` | empty | Where signed events are posted, they stay pending when empty |
| `BUGNET_WORKER_LISTEN_ADDR` | `:8081` | Address of `/healthz` and `/readyz` |

`bugnet-dev` reads `BUGNET_INTERNAL_TOKEN` and `BUGNET_PACKAGES_FILE` as well as these.

| Variable | Default | Meaning |
| --- | --- | --- |
| `BUGNET_API_URL` | `http://127.0.0.1:8080` | API the CLI calls |
| `BUGNET_ORDER_PRIVATE_KEY` | empty | Base64 Ed25519 seed the CLI signs order tokens with |

Money is integer micro-USD and times are UTC.

## Job output

Every frame `bugnet-run` emits goes into the job's Valkey stream until the job's output has cost
8 MiB of stream memory, or the placement's jobs 32 MiB together. Past that the router follows the
job with `bugnet-run attach --tail`, stores its newest 64 KiB per output every 10 s until another
quarter of the job's budget is spent, and once more when the job ends. Once the placement's
32 MiB are spent, a job's last 8 KiB per output, the tail `Exec` returns, is still stored when it
ends, from a separate 8 MiB reserve per placement that only these final tails use and that
expires with the streams, so a small command run later still shows its output. A `skip` frame
names the offsets the stored output goes on from. `WatchJob` sends it as `skip`, `Exec` sets `truncated`
and the job row records `output_truncated`. The whole output stays in the sandbox in
`/tmp/bugnet/jobs/<job>/out` and `err`, which `ReadFile` reads. A placement runs at most 16 jobs
at once, and `Exec` answers `ResourceExhausted` past that.

## Events

`router-worker` posts each outbox event to `BUGNET_EVENTS_URL` as JSON `{id, type, created_at,
payload}`, signed with `BUGNET_EVENT_SIGNING_KEY` in `X-Bugnet-Signature` (base64 Ed25519 over the
exact body) and deduplicated by `X-Bugnet-Event-Id`. The payload carries:

| Field | Meaning |
| --- | --- |
| `placement_id` | The placement |
| `order_id` | The start order of the placement, on every event |
| `extend_order_id` | On `placement.extended` only, the extension order it settles |
| `state` | The placement's state after the change |
| `provider` | The provider that ran the sandbox, empty before one was ready |
| `deadline` | The paid deadline, null before ready |
| `billed_seconds` | Seconds from ready to terminated |
| `refundable` | Whether the order should be refunded |
| `reason` | A stable code such as `stop`, `deadline`, `no_candidate`, `provider_capacity`, `boot_failed` or `placing_stalled` |

An event is retried until it is delivered, 2^n seconds after its nth failure and at most every
5 minutes. An event that failed 10 times or more is stuck. The worker logs `outbox event keeps
failing` at error level at its 10th failure, and the watch logs `outbox holds stuck events` at
error level with their count whenever it changes and every 10 minutes while any is undelivered,
which is the signal to alert on. `router-worker outbox-redrive` (which reads only
`BUGNET_DATABASE_URL`) gives every stuck event its attempts back so it is sent at once, for
example after the events endpoint is fixed.

## Providers

Every provider has an adapter: boxd, E2B, Koyeb and Boat, beside `local` for development. Each
reads its credentials and settings from the environment table above.

- `local` runs sandboxes as host directories and processes, for tests and development only.
- `boxd` runs isolated boxd machines, with `BOXD_API_KEY`, `BUGNET_BOXD_REGION` and
  `BUGNET_BOXD_ENDPOINT`.
- `e2b` runs E2B sandboxes from templates built with `deploy/e2b`, with `E2B_API_KEY`,
  `BUGNET_E2B_TEMPLATES`, `BUGNET_E2B_DOMAIN` and `BUGNET_E2B_MAX_LIFETIME`. Without
  `BUGNET_E2B_TEMPLATES` it has no sizes and is never chosen.
- `koyeb` runs Koyeb sandbox services, one app each, with the RTX 4000 SFF Ada for GPU packages,
  with `KOYEB_API_TOKEN`, `BUGNET_KOYEB_REGION` and `BUGNET_KOYEB_INSTANCE_TYPES`.
- `boat` runs Boat sandboxes in the EU without the account's secrets, snapshots or monitoring,
  with `BOAT_API_KEY` and `BUGNET_BOAT_TYPES`. `large` needs a first payment and `xlarge` the
  $100 plan, so `BUGNET_BOAT_TYPES` can leave them out.
