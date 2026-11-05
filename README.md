# bugnet-router

The Bugnet sandbox router. It takes a paid order token, places a sandbox on one of several
providers, runs commands in it and ends it when the paid time is up.

Two binaries share one Postgres database. `router-api` serves the Connect API, runs the
placement saga and streams command output. `router-worker` ends placements at their deadline,
sweeps stray sandboxes and posts signed events to `events_url`. Postgres is the system of record,
Valkey carries output streams and job leases, and River runs durable jobs. Both read their
settings from a TOML file and their secrets from the environment.

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
| `internal/config`, `internal/catalog` | Settings file, secrets and the package catalog |
| `internal/ordertoken`, `internal/handle` | Order tokens and sandbox handles |
| `test/e2e` | End-to-end tests over the built binaries |
| `config` | `example.toml` with every setting, `dev.toml` and the catalog `dev-up.sh` runs with |
| `scripts` | `dev-up.sh` for a local stack and `live-test.sh` for the live provider tests |
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

## Live provider tests

Each adapter has live tests that run the provider contract against the real service. They skip
without the provider's key. Paste the key into `.env` (see `.env.example`), then run:

```sh
scripts/live-test.sh e2b
scripts/live-test.sh boxd koyeb boat
```

The script fails with the variable's name when a key is empty, and otherwise runs
`go test -run Live` for each provider named, with every other setting from `config/dev.toml`
(E2B uses its `[[e2b.templates]]`). The tests create real sandboxes and remove them again, which
costs a little on the provider's account.

## Running locally

```sh
createdb bugnet_router_dev
cp .env.example .env
scripts/dev-up.sh
```

`.env` holds only secrets, the database URL and the provider keys. Every other setting comes
from `config/dev.toml`, or from the file given as `dev-up.sh`'s argument. `dev-up.sh` builds the
binaries into `bin/`, writes fresh router secrets to `.env.keys` with
`bin/bugnet-dev keys > .env.keys` when that file is missing, writes `bin/dev.toml` as the
settings file with `order_token_public_key` from `bin/bugnet-dev pubkey` in front, migrates the
database and starts one `router-api` and one `router-worker` with `-config bin/dev.toml` on the
local provider. Logs go to `bin/logs/`. Ctrl-C stops both. Delete `.env.keys` to get new
secrets. Both `.env` and `.env.keys` are ignored by git.

The local provider runs commands as host processes under `bin/local/`. Use it only with code
you trust. To try a real provider, add it to `providers` in `config/dev.toml` and its key to
`.env`.

Drive the router from another terminal in the repository root:

```sh
set -a; . ./.env.keys; set +a
bin/bugnet-dev create --package dev-small-10m
bin/bugnet-dev exec --handle sbx_... -- sh -c 'echo hello; sleep 1; echo done'
bin/bugnet-dev extend --handle sbx_... --package dev-small-10m-more
bin/bugnet-dev stop --handle sbx_...
```

`bugnet-dev` reads `config/dev.toml`, or the file given with `--config FILE` before the command,
for the API address and the catalog, and needs only `BUGNET_INTERNAL_TOKEN` and
`BUGNET_ORDER_PRIVATE_KEY` from the environment. `create` signs an order token with
`BUGNET_ORDER_PRIVATE_KEY`, priced from the catalog, and waits until the sandbox is ready.
`exec` streams the output and exits with the command's exit code. If the stream breaks, it
prints the `bugnet-dev watch` command that resumes from the last cursor, with the same
`--config` when one was given. `bugnet-dev token`
prints a token for other clients, `bugnet-dev pubkey` the public key that verifies its tokens,
and `bugnet-dev` with no arguments lists every command.

A recorded session against `dev-up.sh` on the local provider (2026-11-05):

```text
$ bin/bugnet-dev create --package dev-small-10m
handle=sbx_incf5d4j2g46mypgm6wajtyic5
placement_id=5bef8fc5-a11f-4e46-a88e-17d69739f859
state=ready
package_id=dev-small-10m
created_at=2026-11-05T01:25:34.268894Z
ready_at=2026-11-05T01:25:34.277819Z
deadline=2026-11-05T01:35:34.277819Z

$ bin/bugnet-dev exec --handle sbx_incf5d4j2g46mypgm6wajtyic5 -- \
    sh -c 'echo hello; sleep 1; echo done; exit 3'
hello
done
$ echo $?
3

$ bin/bugnet-dev exec --handle sbx_incf5d4j2g46mypgm6wajtyic5 -- \
    sh -c 'for i in 1 2 3; do date +%T; sleep 0.5; done'
02:25:43
02:25:44
02:25:44

$ bin/bugnet-dev extend --handle sbx_incf5d4j2g46mypgm6wajtyic5 --package dev-small-10m-more
placement_id=5bef8fc5-a11f-4e46-a88e-17d69739f859
state=ready
package_id=dev-small-10m
created_at=2026-11-05T01:25:34.268894Z
ready_at=2026-11-05T01:25:34.277819Z
deadline=2026-11-05T01:45:34.277819Z

$ bin/bugnet-dev stop --handle sbx_incf5d4j2g46mypgm6wajtyic5
placement_id=5bef8fc5-a11f-4e46-a88e-17d69739f859
state=terminated
package_id=dev-small-10m
created_at=2026-11-05T01:25:34.268894Z
ready_at=2026-11-05T01:25:34.277819Z
deadline=2026-11-05T01:45:34.277819Z
```

The outbox then held `placement.ready`, `placement.extended` and `placement.terminated` with
`billed_seconds` 11 and reason `stop`, pending because `events_url` was empty.

## Secrets

Secrets come only from the environment and never from a settings file, which refuses any key it
does not know.

| Variable | Who reads it |
| --- | --- |
| `BUGNET_DATABASE_URL` | `router-api`, `router-worker`, `router-api migrate` and `router-worker outbox-redrive`, may hold a password |
| `BUGNET_INTERNAL_TOKEN` | `router-api`, `router-worker` and `bugnet-dev`, the API's bearer token, 32 characters or more |
| `BUGNET_HANDLE_KEY` | `router-api` and `router-worker`, base64 of 32 bytes or more that derives handles |
| `BUGNET_EVENT_SIGNING_KEY` | `router-worker`, which requires it, and `router-api` when set, base64 Ed25519 seed that signs events |
| `BOXD_API_KEY`, `E2B_API_KEY`, `KOYEB_API_TOKEN`, `BOAT_API_KEY` | the binaries for each provider in `providers`, and the live tests |
| `BUGNET_ORDER_PRIVATE_KEY` | `bugnet-dev` only, base64 Ed25519 seed it signs order tokens with |

`router-api migrate` and `router-worker outbox-redrive` read only `BUGNET_DATABASE_URL` and need
no settings file.

## Settings

`router-api -config FILE` and `router-worker -config FILE` read every other setting from a TOML
file. [`config/example.toml`](config/example.toml) lists each key at its default with what it
means, and [`config/dev.toml`](config/dev.toml) is the file `dev-up.sh` runs with. The file is
decoded strictly, so an unknown or misspelt key is an error naming the key and its line.
Relative paths resolve against the working directory the binary starts in. Serving needs
`order_token_public_key` and `packages_file`. Each provider's settings live in its own table,
`[boxd]`, `[e2b]`, `[koyeb]` and `[boat]`, and the local provider starts only with
`local.enabled = true`.

`router-api` reads each `[runner]` build once at start and refuses to start when a configured
provider needs one that is unset or unreadable. `make build` in `bugnet-run` writes
`dist/bugnet-run-linux-amd64`.

Money is integer micro-USD and times are UTC.

## Job output

Every frame `bugnet-run` emits goes into the job's Valkey stream until the job's output has cost
`limits.job_output_budget` of stream memory, or the placement's jobs
`limits.placement_output_budget` together. Past that the router follows the job with
`bugnet-run attach --tail`, stores its newest 64 KiB per output every 10 s until another quarter
of the job's budget is spent, and once more when the job ends. Once the placement's budget is
spent, a job's last 8 KiB per output, the tail `Exec` returns, is still stored when it ends, from
a separate reserve per placement of `limits.placement_tail_reserve` that only these final tails
use and that expires with the streams, so a small command run later still shows its output.

A `skip` frame names the offsets the stored output goes on from. `WatchJob` sends it as `skip`,
`Exec` sets `truncated` and the job row records `output_truncated`. The whole output stays in the
sandbox in `/tmp/bugnet/jobs/<job>/out` and `err`, which `ReadFile` reads. A placement runs at
most `limits.max_running_jobs` jobs at once, and `Exec` answers `ResourceExhausted` past that.

## Deployment

Valkey holds the job streams, leases and shared counters, and nothing in it may be evicted. Run it
with `maxmemory` set and `maxmemory-policy noeviction`, so a full Valkey refuses writes, which
the router logs, instead of dropping streams or leases, and alert on its memory well before it
reaches `maxmemory`.

The output budgets bound what the streams hold. The aggregate bound is
`limits.placement_output_budget` times the placements running at once, plus
`limits.placement_tail_reserve` for each, counting a placement until its streams expire 10
minutes after its deadline. With the defaults that is 40 MiB a placement, so 100 placements at
once take up to about 4 GiB. Size `maxmemory` from that with headroom, since Valkey's own memory
for a stream can run up to half again over the budget's estimate.

## Events

`router-worker` posts each outbox event to `events_url` as JSON `{id, type, created_at,
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
reads its key from the environment and its settings from its table in the settings file, and
`providers` lists the ones a deployment uses in preference order.

- `local` runs sandboxes as host directories and processes, for tests and development only.
- `boxd` runs isolated boxd machines, with `BOXD_API_KEY`, `boxd.region` and `boxd.endpoint`.
- `e2b` runs E2B sandboxes from templates built with `deploy/e2b`, with `E2B_API_KEY`, one
  `[[e2b.templates]]` table per size, `e2b.domain` and `e2b.max_lifetime`. Without templates it
  has no sizes and is never chosen.
- `koyeb` runs Koyeb sandbox services, one app each, with the RTX 4000 SFF Ada for GPU packages,
  with `KOYEB_API_TOKEN`, `koyeb.region` and `[koyeb.instance_types]`.
- `boat` runs Boat sandboxes in the EU without the account's secrets, snapshots or monitoring,
  with `BOAT_API_KEY` and `[boat.types]`. `large` needs a first payment and `xlarge` the $100
  plan, so `[boat.types]` can leave them out. The fee of a start past the plan's included starts
  comes from `GET /limits`. When the key cannot read it, the router prices the fee from the
  starts every replica counts in Valkey under `valkey.prefix`, kept for the day-long window, and
  from each process's own count while Valkey does not answer.
