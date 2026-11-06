# bugnet-router

The Bugnet sandbox router. It takes a paid order token, places a sandbox on one of several
providers, runs commands in it and ends it when the paid time is up.

Two binaries share one Postgres database. `router-api` serves the Connect API, runs the
placement saga and streams command output. `router-worker` ends placements at their deadline,
sweeps stray sandboxes and posts signed events to `events_url`. Postgres is the system of record,
Valkey carries output streams and job leases, and River runs durable jobs. Both read their
settings from a TOML file and their secrets from the environment.

`router-api` serves `SandboxRouterService` from `bugnet-proto`. Its previews, transfers, SSH keys,
secrets and `FetchSource` RPCs answer `Unimplemented` until the router builds them.

A `Placement`'s `ended_at` is when it became terminated or failed, and never moves after that. It
is unset before. A placement that fails at once, with no provider for its order, ends at its
`created_at`.

A terminating or terminated placement's `termination_reason` is a stable code such as `stop` or
`deadline`. It is empty in every other state. A failed placement says why in `failure_reason` alone.

## Layout

| Path | What it holds |
| --- | --- |
| `cmd/router-api` | API server, `router-api migrate` applies migrations and exits |
| `cmd/router-worker` | Deadline, watch, sweep and outbox jobs, `router-worker outbox-redrive` sends stuck events at once |
| `cmd/bugnet-dev` | Developer CLI for keys, tokens, placements and commands |
| `internal/api` | Connect handlers, health and readiness |
| `internal/ratelimit` | Per-placement operation limit in Valkey |
| `internal/vkclient` | Valkey connections at `host:port` or a `unix://` socket path |
| `internal/vkinfo` | Valkey's memory and `maxmemory-policy` from `INFO memory` |
| `internal/saga` | Start, extend and stop of placements |
| `internal/placement` | Provider choice by price, margin and health |
| `internal/jobs`, `internal/streams` | Commands through `bugnet-run`, output streams, leases |
| `internal/worker` | River workers for deadlines, the sweep and the outbox |
| `internal/store`, `migrations` | Postgres access and schema |
| `internal/provider`, `internal/providers` | Provider port, adapters and the registry |
| `internal/config`, `internal/catalog` | Settings file, secrets and the package catalog |
| `internal/bytesize` | Byte sizes such as `8MiB` in settings and the catalog |
| `internal/ordertoken`, `internal/handle` | Order tokens and sandbox handles |
| `test/e2e` | End-to-end tests over the built binaries |
| `config` | `example.toml` with every setting, `dev.toml` and the catalog `dev-up.sh` runs with |
| `scripts` | `dev-up.sh` for a local stack and `live-test.sh` for the live provider tests |
| `deploy/e2b` | E2B template definitions, one template per size |

## Prerequisites

- Go 1.27
- Postgres 18 on `localhost:5432`, the current user a superuser
- Valkey 9 on `localhost:6379`, at its default `maxmemory-policy noeviction`
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
costs a little on the provider's account. The boxd run also starts `python3 -m http.server 8000`
in a machine and checks that its public default proxy does not serve it.

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
settings file with `order_token_public_key` from `bin/bugnet-dev pubkey` in front, in place of
any top-level `order_token_public_key` the file has, migrates the database and starts one
`router-api` and one `router-worker` with `-config bin/dev.toml` on the local provider. Logs go
to `bin/logs/`. Ctrl-C stops both. Delete `.env.keys` to get new secrets. Both `.env` and
`.env.keys` are ignored by git.

The local provider runs commands as host processes under `bin/local/`. Use it only with code
you trust. To try a real provider, replace `local` with it in `providers` in `config/dev.toml`,
as in `providers = ["e2b"]`, and paste its key into `.env`. The router tries the cheapest
provider first, and `local` costs nothing and fits every package, so a provider listed beside it
is chosen only when `local` fails. Real providers also need the Linux build of `bugnet-run` that
`runner.linux_amd64` names, which `make build` in `../bugnet-run` writes.

Drive the router from another terminal in the repository root:

```sh
set -a; . ./.env.keys; set +a
bin/bugnet-dev create --package dev-small-10m
bin/bugnet-dev exec --handle sbx_... -- sh -c 'echo hello; sleep 1; echo done'
bin/bugnet-dev extensions --handle sbx_...
bin/bugnet-dev check-extend --handle sbx_... --package dev-small-10m-more
bin/bugnet-dev extend --handle sbx_... --package dev-small-10m-more
bin/bugnet-dev stop --handle sbx_...
```

`bugnet-dev` reads `config/dev.toml`, or the file given with `--config FILE` before the command,
for the API address and the catalog, and needs only `BUGNET_INTERNAL_TOKEN` and
`BUGNET_ORDER_PRIVATE_KEY` from the environment. `create` signs an order token with
`BUGNET_ORDER_PRIVATE_KEY`, priced from the catalog, and waits until the sandbox is ready.
`exec` streams the output and exits with the command's exit code. If the stream breaks, it
prints the `bugnet-dev watch` command that resumes from the last cursor, with the same
`--config` when one was given. `extensions` prints a line for each package that can extend the
placement, with its minutes, its price and whether `extend` would take it now or the reason it
would not. `check-extend` prints the placement, then as `deadline_after` and `price_micros` the
deadline `extend` would give it and the package's price, with no order and no change.
`bugnet-dev token` prints a token for other clients, `bugnet-dev pubkey` the public key that
verifies its tokens, and `bugnet-dev` with no arguments lists every command.

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
| `BUGNET_EDGE_TOKEN`, `BUGNET_SECRETS_TOKEN`, `BUGNET_SSH_TOKEN`, `BUGNET_EGRESS_TOKEN` | `router-api` and `router-worker`, one bearer token per edge process, each empty or 32 characters or more |
| `BUGNET_EGRESS_FETCH_TOKEN` | `router-api` and `router-worker`, the `bnf_` token the router's private fetch opens egress tunnels with, empty or 32 characters or more |

An empty caller token admits no calls, so a router without edges serves only the gateway. No two
of `BUGNET_INTERNAL_TOKEN` and the caller tokens may be equal, since each admits one caller to
its own service.

`router-api migrate` and `router-worker outbox-redrive` read only `BUGNET_DATABASE_URL` and need
no settings file.

## Settings

`router-api -config FILE` and `router-worker -config FILE` read every other setting from a TOML
file. [`config/example.toml`](config/example.toml) lists each key with what it means, each at
its default except `[names]` and the two `[[e2b.templates]]` tables, since there are no names and
E2B has no templates by default.
[`config/dev.toml`](config/dev.toml) is the file `dev-up.sh` runs with. The file is decoded
strictly, so an unknown or misspelt key, or one in the wrong case, is an error naming the key and
its line, and the keys of `koyeb.instance_types` and `boat.types` must be size names of
lowercase letters, digits and hyphens. Relative paths resolve against the working directory the
binary starts in. Serving needs `order_token_public_key` and `packages_file`. Each provider's
settings live in its own table, `[boxd]`, `[e2b]`, `[koyeb]` and `[boat]`, and the local
provider starts only with `local.enabled = true`.

Each router that shares a provider account needs its own `deployment_id`. Every sandbox carries
its router's deployment, and a router's sweep destroys the sandboxes of its deployment that its
own database does not need, so two routers with the same `deployment_id` destroy each other's
sandboxes. `config/dev.toml` uses `dev`, the default is `main`.

`router-api` reads each `[runner]` build once at start and refuses to start when a configured
provider needs one that is unset or unreadable. `make build` in `bugnet-run` writes
`dist/bugnet-run-linux-amd64`.

Byte sizes are a whole number of bytes, or one ending in `KiB`, `MiB` or `GiB`, from 1 byte to
1 TiB. `valkey.addr` takes `host:port`, or `unix://` and an absolute socket path, as
`unix:///run/valkey/valkey.sock` on the VPS.

### Data plane

These tables hold the data plane's settings, each at its default in `config/example.toml`. The
features that read them land one by one, and until then the settings only load and check.

| Table | What it holds |
| --- | --- |
| `[names]` | The public names the edges serve, empty by default. `example.toml` shows the production set and `dev.toml` the `.dev` live-test set, with SSH on port 2200 |
| `[edge_budget]` | Bytes, pace, request rate and concurrency of one placement's previews, transfers and SSH together, and of its egress apart |
| `[previews]` | `providers` whose gate route passed its live test, the in-sandbox gate port 29417, previews per placement and the interstitial |
| `[transfers]` | The start window, per-file cap and duration, per-placement counts, claims per URL and Koyeb's part buffers |
| `[ssh]` | `providers` whose SSH live test passed, the gateway's host keys, sshd ports and the per-placement limits and timeouts |
| `[secrets]` | `providers` whose secret live test passed, the egress proxy's addresses, CA and box keys, and the link and put limits |
| `[fetch]` | The private repository fetch's byte cap and the egress proxy's listener |
| `[hooks]` | `providers` whose webhooks are verified |

`boxd.gate_route` (`subdomain` or `default`), `boat.ssh_path` (`gate` or `native`) and
`local.sshd_path` choose each provider's route. Every provider list takes known provider names,
each once. `secrets.providers` never takes `local`, whose sandboxes share the host.
`secrets.box_public_keys` takes `{ id, key }` tables, newest first, each key standard base64 of a
32-byte X25519 public key.

`worker.dataplane_workers` (8) sets the River workers of the `dataplane` queue, which keeps slow
data-plane jobs away from deadlines, the sweep and the outbox.

A package in the catalog may carry its own edge budget and a restricted egress list:

```json
{"id": "small-10m", "minutes": 10, "edge": {"bytes": "4GiB", "transfer_max_bytes": "4GiB"},
 "egress": {"allow": ["huggingface.co"]}}
```

`edge` takes `bytes`, `bytes_per_second`, `requests_per_second`, `requests_burst`,
`queued_requests`, `websockets`, `transfer_max_bytes`, `egress_bytes`, `egress_bytes_per_second`
and `egress_requests_in_flight`, each optional, sizes as strings. A field left out keeps the
router's setting. `egress.allow` lists exact lowercase host names, at least one. An extension must
carry the same `egress.allow` as the package it extends, since a sandbox keeps the egress rules
it was created with.

Money is integer micro-USD and times are UTC.

## Prices and availability

`Estimate` lists every start package by price, each with `available` where a provider could host
it now for the order's requirements. `PricePackage` takes one start package ID and the same
requirements and answers with that package as `Estimate` would list it. The gateway asks it right
before Payments charges a paid start. It writes nothing, spends no rate-limit token, and leaves
`fits`, `extensions_needed` and `short_on` unset. An unknown package answers `InvalidArgument`
with reason `PACKAGE_UNKNOWN`, and one with `extension_of` set with reason `NOT_START_PACKAGE`.

An unavailable start package's `unavailable_reason` is always one of these codes, never error
text.

| Code | When |
| --- | --- |
| `no_candidate` | A configured provider has the capabilities needed, but none can take it now |
| `feature_unavailable` | No configured provider offers the SSH, previews, secrets or GPU needed |
| `internal` | Any other error, logged at error level with the detail |

A capable provider can still refuse a package for its health, its sizes, the margin or its
maximum lifetime, which gives `no_candidate`. The capability check ignores health, so
`feature_unavailable` means waiting will not help. `ListExtensions` has its own codes, below.

## Extensions

`ExtendPlacement` takes an extend order token for the placement's handle and moves the deadline by
the package's minutes, once per order, for the placement's own package or one whose `extension_of`
names it. It refuses a placement that is not ready with `FailedPrecondition` and the
`google.rpc.ErrorInfo` reason `NOT_READY`, as every call does that needs a ready placement, and an
extension that would outlast the provider's maximum lifetime with `FailedPrecondition` and reason
`EXCEEDS_LIFETIME`. The router keeps ended placements, so extending a handle past its deadline
answers `NOT_READY`, and only a handle it never issued answers `NotFound`. `CheckExtendPlacement`
takes a handle and a package ID and answers as `ExtendPlacement` would for a fresh order of that
package now, with the same `NotFound`, `InvalidArgument` or `FailedPrecondition`, or else the
placement as it is, the deadline it would have and the package with its price, changing nothing.
The gateway asks it before it takes payment. `ListExtensions` takes a handle and lists the
placement's own package and every package whose `extension_of` names it, by minutes, then price,
then ID, each with its price and `available` where `CheckExtendPlacement` would accept it, else
`unavailable_reason` `not_ready` or `exceeds_lifetime`, the `ErrorInfo` reasons in lowercase. A
placement that is not ready lists every package as `not_ready`, a handle the router never issued
answers `NotFound`, and the listing changes nothing.

`PermissionDenied`, `InvalidArgument`, `NotFound` and `FailedPrecondition` from `ExtendPlacement`
mean that call recorded nothing. Any other error, or a reply that never arrives, may follow a
committed extension. A retry with the same order token then answers with the placement if the
order was applied, since a repeated order is answered before the ready and lifetime checks, and
refuses as above if it was not. Once the token has expired a retry answers `PermissionDenied`
either way, and only a `placement.extended` event with the order's `extend_order_id` tells.

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

`bugnet-run`'s exit frame says whether the kernel's OOM killer ended a process of the job and the
job's peak memory. `WatchJob`'s exit carries both as `oom_killed` and `peak_memory_mib`, and
`Exec` sets `oom_killed` when it saw the exit frame. A job that ended before `Exec` read its
stream reports no OOM kill, since the job row does not keep it.

## Operation limit

Each placement's `Exec`, `CancelJob`, `ReadFile`, `WriteFile` and `ListFiles` calls spend one
token from a bucket that every `router-api` replica shares, `ops:<placement id>` under
`valkey.prefix`. It holds `limits.sandbox_ops_burst` tokens and gains
`limits.sandbox_ops_per_second` a second by the Valkey server's clock, 20 and 10 by default, and
expires once it is full again. A call that finds it empty answers `ResourceExhausted` with reason
`RATE_LIMITED` and a `google.rpc.RetryInfo` holding the wait until the next token. Other calls
spend nothing, nor does a request the router itself finds invalid, an unknown handle, or an `Exec`
or file call on a placement that is not ready. A request only the provider refuses has spent its
token. When Valkey cannot answer within 500 ms, the call goes through and the router logs a
warning. `StopPlacement` spends nothing, and stops that race on one placement in one process wait
for the destroy already in flight instead of calling the provider again.

## Error reasons

Each refusal below carries a `google.rpc.ErrorInfo` in domain `bugnet.router`, so the gateway
tells them apart without another call. No message or detail
holds a handle or a job ID.

| Code | Reason | When |
| --- | --- | --- |
| `NotFound` | `HANDLE_UNKNOWN` | A call taking a handle gets a malformed one or one never issued |
| `NotFound` | `JOB_UNKNOWN` | `WatchJob` or `CancelJob` names a job the handle's placement lacks |
| `NotFound` | `FILE_NOT_FOUND` | `ReadFile`, `WriteFile` or `ListFiles` names a path that does not exist |
| `InvalidArgument` | `PACKAGE_UNKNOWN` | `PricePackage` names a package the catalog lacks |
| `InvalidArgument` | `NOT_START_PACKAGE` | `PricePackage` names a package with `extension_of` set |
| `FailedPrecondition` | `NOT_READY` | The placement is placing, ending or ended |
| `FailedPrecondition` | `EXCEEDS_LIFETIME` | The extension would outlast the provider's limit |
| `ResourceExhausted` | `RATE_LIMITED` | The placement's operation bucket is empty |

A provider failure carries the provider's code in lowercase instead, such as `rate_limited`. A
call that its caller cancels or lets run out of time answers `Canceled` or `DeadlineExceeded`,
with only the context error as its message.

An `Exec` whose `idempotency_key` another call holds waits for that call to record the job. When
it records none within the wait, `Exec` answers `Unavailable` with no reason, and a retry with the
same key runs the command once.

## Deployment

Valkey holds the job streams, leases and shared counters, and nothing in it may be evicted. Run it
with `maxmemory` set and `maxmemory-policy noeviction`, so a full Valkey refuses writes, which
the router logs, instead of dropping streams or leases. `router-api` and `router-worker` refuse to
start with `valkey: maxmemory-policy is X, the router needs noeviction` when `INFO memory`
reports any other policy, even when it lacks another field, and start with a warning when it
reports no `maxmemory` or cannot be read in full.

The output budgets bound what the streams hold. The aggregate bound is
`limits.placement_output_budget` times the placements running at once, plus
`limits.placement_tail_reserve` for each, counting a placement until its streams expire 10
minutes after its deadline. With the defaults that is 40 MiB a placement, so 100 placements at
once take up to about 4 GiB. Size `maxmemory` from that with headroom, since Valkey's own memory
for a stream can run up to half again over the budget's estimate.

Point an uptime monitor at the worker's `GET /monitor` on `worker.listen_addr` every minute,
expecting 200. A 503 lists one problem a line, counts only, within one 2 s timeout: Postgres or
Valkey unreachable, placements ready or terminating over 3 minutes past their deadline and stops
still terminating after 3 minutes, which mean the worker runs but no longer ends sandboxes, stuck
outbox events, Valkey memory at 80% of `maxmemory` or more, and a `maxmemory-policy` other than
`noeviction`. A failed connection means the worker is down. When `INFO memory` fails or times out
while Valkey answers, `/monitor` reports `valkey memory is unknown`. When an ACL or
`rename-command` refuses `INFO`, it reports `valkey refuses INFO, so its memory and policy go
unchecked`, since that also leaves the start-up policy check blind, so leave `INFO` allowed. When it answers without a field, as a managed Valkey may, `/monitor` checks the
fields it got and logs a warning. `/healthz` is a bare liveness answer and `/readyz` checks only
that Postgres and Valkey answer.

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
which is the signal to alert on, and `/monitor` reports their count. `router-worker
outbox-redrive` (which reads only `BUGNET_DATABASE_URL`) gives every stuck event its attempts back
so it is sent at once, for example after the events endpoint is fixed.

## Providers

Every provider has an adapter: boxd, E2B, Koyeb and Boat, beside `local` for development. Each
reads its key from the environment and its settings from its table in the settings file, and
`providers` lists the ones a deployment uses. A placement tries the providers that can host the
package cheapest first, a cost being the package's minutes in whole billing increments plus any
start fee. A provider whose recent starts took over 10 s to get ready at the 95th percentile
goes behind the faster ones. The order of `providers` only breaks ties in cost.

- `local` runs sandboxes as host directories and processes, for tests and development only.
- `boxd` runs isolated boxd machines, with `BOXD_API_KEY`, `boxd.region` and `boxd.endpoint`.
  Every machine's public default proxy is pinned to the gate port 29417 before the router uses
  it, so no app inside is public on its own. boxd's own SSH and auto hibernation are turned off
  too. A failed pin or SSH call destroys the machine, and the placement tries the next provider.
  A refused hibernation call only warns, and the ref records it as `hibernate_off` `0`.
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

The registry decides which providers offer previews, SSH and secrets, and the placement engine and
the API read capabilities only through it. A provider offers previews when it is listed in
`previews.providers` and its adapter has a gate route, SSH when it is listed in `ssh.providers`
and has a gate route, or with `boat.ssh_path = "native"` an SSH server of its own, and secrets
when it is listed in `secrets.providers`. A listed provider whose adapter lacks what the feature
needs stops both binaries at start, naming it. No adapter has a gate route yet and every list is
empty, so no provider offers SSH, previews or secrets. An order that requires any of them finds
no provider. `Estimate` marks every package, and `PricePackage` the one it prices,
`feature_unavailable` for it.

Commands run as root on E2B and Koyeb, as `boxd` on boxd, as `user` on Boat, and as the user
running the router on `local`.
