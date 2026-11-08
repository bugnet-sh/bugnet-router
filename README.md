# bugnet-router

The Bugnet sandbox router. It takes a paid order token, places a sandbox on one of several
providers, runs commands in it and ends it when the paid time is up.

Two binaries share one Postgres database. `router-api` serves the Connect API, runs the
placement saga and streams command output. `router-worker` ends placements at their deadline,
sweeps stray sandboxes and posts signed events to `events_url`. Postgres is the system of record,
Valkey carries output streams and job leases, and River runs durable jobs. Both read their
settings from a TOML file and their secrets from the environment.

`router-api` serves `SandboxRouterService` from `bugnet-proto`, and one more Connect service for
each other caller (see [Caller services](#caller-services)). Its previews, SSH keys, secrets and
`FetchSource` RPCs, and every other RPC not built yet, answer `Unimplemented` until the router
builds them. Transfers are built (see [Transfers](#transfers)).

A `Placement`'s `ended_at` is when it became terminated or failed, and never moves after that. It
is unset before. A placement that fails at once, with no provider for its order, ends at its
`created_at`.

A terminating or terminated placement's `termination_reason` is a stable code such as `stop` or
`deadline`. It is empty in every other state. A failed placement says why in `failure_reason` alone.

A `Placement`'s `paid_micros` sums what the start order and every applied extension paid for the
package, the x402 allowance left out. It is on every answer, from the first.

## Layout

| Path | What it holds |
| --- | --- |
| `cmd/router-api` | API server, `router-api migrate` applies migrations and exits |
| `cmd/router-worker` | Deadline, watch, sweep and outbox jobs, `router-worker outbox-redrive` sends stuck events at once |
| `cmd/bugnet-dev` | Developer CLI for keys, tokens, placements and commands |
| `internal/api` | Connect handlers, health and readiness |
| `internal/ratelimit` | Token buckets in Valkey, the per-placement operation limit and named buckets with fractional rates |
| `internal/vkclient` | Valkey connections at `host:port` or a `unix://` socket path |
| `internal/vkinfo` | Valkey's memory and `maxmemory-policy` from `INFO memory` |
| `internal/saga` | Start, extend and stop of placements |
| `internal/placement` | Provider choice by price, margin and health |
| `internal/jobs`, `internal/streams` | Commands through `bugnet-run`, output streams, leases |
| `internal/worker` | River workers for deadlines, the sweep and the outbox |
| `internal/store`, `migrations` | Postgres access and schema |
| `internal/provider`, `internal/providers` | Provider port, adapters, the registry and the contract suites every adapter runs |
| `internal/config`, `internal/catalog` | Settings file, secrets and the package catalog |
| `internal/hostname` | The one exact host name rule that `[names]` and a package's `egress.allow` share |
| `internal/hostrules` | The one set of host rules that secret hosts and x402 hosts share |
| `internal/x402` | x402 service checks, service ids, path rules, event codes and statuses |
| `internal/solana` | The one base58 decoder, for Solana keys and signatures |
| `internal/bytesize` | Byte sizes such as `8MiB` in settings and the catalog |
| `internal/ordertoken`, `internal/handle` | Order tokens and sandbox handles |
| `internal/wallet`, `internal/wallets` | Payer addresses and the payer decision, wallet blocks, strikes and buckets |
| `internal/dpkeys` | Data-plane tokens, keys and grants derived from the root keys, and sealed provider credentials |
| `internal/budget`, `internal/edgeevents` | Edge and egress budgets in Valkey, and the edge event stream |
| `internal/transfer` | Transfer records and claims in Valkey, path resolution and staging through `bugnet-run` |
| `test/e2e` | End-to-end tests over the built binaries |
| `test/live` | The live stack and the data-plane live rows `scripts/live-test.sh` runs |
| `config` | `example.toml` with every setting, `dev.toml`, the catalog and the pool list `dev-up.sh` runs with, `dev-front.toml` for the live stack behind the VPS |
| `scripts` | `dev-up.sh` for a local stack and `live-test.sh` for the live provider tests |
| `deploy/e2b` | E2B template definitions, one template per size |
| `deploy/vps` | The VPS front: public names, site settings, the HAProxy template, the dev front |

## Prerequisites

- Go 1.27
- Postgres 18 on `localhost:5432`, the current user a superuser
- Valkey 9 on `localhost:6379`, at its default `maxmemory-policy noeviction`
- `bugnet-run` and `bugnet-proto` checked out next to this repository, `go.mod` points at them
  with `replace` until they are published, and `bugnet-edges` beside them for `test/live`

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

`test/live` holds the live rows, which skip unless `scripts/live-test.sh` names providers. Its
own tests run without any provider and check the fronted mode's refusals. The test that builds
and starts the whole stack on the local provider runs only with `BUGNET_TEST_STACK=1`, since it
builds five binaries, `bugnet-edge` among them from `../bugnet-edges`:

```sh
BUGNET_TEST_STACK=1 go test ./test/live/ -run TestStackStartsOnTheLocalProvider -v
```

That test also sends a 3-byte file through the edge with `curl -T`, fetches it back, and sees the
used URL answer 410.

`make test`, `make lint` and `make build` wrap the usual commands.

## Live provider tests

Live tests call the real providers with the keys in `.env` (see `.env.example`), take every
other setting from `config/dev.toml`, create real sandboxes and remove them again, which costs a
little on the provider's account. `scripts/live-test.sh` runs them, one suite at a time:

```sh
scripts/live-test.sh e2b
scripts/live-test.sh boxd koyeb boat
scripts/live-test.sh --suite dataplane --run 'LiveDataplane/T1' e2b
scripts/live-test.sh --suite dataplane --front vps e2b boxd
scripts/live-test.sh --suite bench --router http://127.0.0.1:8080
```

| Suite | Runs | Providers | Keys checked |
|---|---|---|---|
| `adapters`, the default | Each provider's adapter live tests, `go test -run Live` | At least one | Each named provider's, from `.env` |
| `dataplane` | `TestLiveDataplane` in `test/live` | At least one | Each named provider's, from `.env` |
| `control` | `TestLiveControl` in `test/live` | Zero or more | Each named provider's, from `.env` |
| `leak` | `TestLiveLeak` in `test/live` | At least one | Each named provider's, from `.env` |
| `bench` | `bugnet-dev bench` against `--router URL` | Zero or more | None. It sources no `.env` and needs `BUGNET_OPERATOR_TOKEN` set |

The script fails with the variable's name when a key is empty. `--run REGEX` replaces the suite's
test pattern, and `--timeout DURATION` its time limit: 20 minutes for each provider's adapter
tests, 90 minutes for the `test/live` suites. A row that needs another key checks it itself and
fails at once naming the variable, never its value. Every suite but `bench` ends with
`bugnet-dev owned` for each provider named, and fails while one still holds a sandbox under
Bugnet's labels (`bugnet`, `bugnet-live` or `bugnet-contract`).

The `test/live` suites start a stack on this Mac for each provider in turn: `router-api`,
`router-worker` and `bugnet-edge`, built from the workspace, on a fresh database and Valkey
prefix, with fresh secrets from `bugnet-dev keys` and the router's settings from
`config/dev.toml` on that provider alone. Each row is a subtest named by its id and provider, such
as `TestLiveDataplane/T1/e2b`. Rows that need the VPS fail at once in the local mode, naming
`--front vps`.

The transfer rows each make their own sandbox and stop it at the end. They drive `bugnet-dev
create`, `exec`, `transfer` and `stop`, and move files with `curl` as a user would.

| Row | What it does |
|---|---|
| T1 | 0 B, 1 B, 1 MiB + 1, 5 MiB + 1 and 101 MiB up and back, checked by SHA-256 on both ends |
| T2 | 1 GiB up and down on `dev-xfer-30m` (30 minutes, a 4 GiB edge budget), timed |
| T3 | A 20 MiB download cut at half, then finished with `curl -C -` |
| T4 | An upload cut at half, then one with a wrong `--sha256` (422), the old file unchanged |
| T5 | `/dev/urandom` and a FIFO refused at `transfer` as not regular files |
| T6 | `/opt/bugnet-test` and a root-owned 0600 file in `/tmp`, both ways |
| T7 | Logs the filesystems of `/tmp` and the home directory, which must not be RAM |
| BOAT5 | Boat only: an upload the exec user cannot write and a download it cannot read |

The gate rows read the placement from the stack's database and build its route with the adapter,
as the router does, so they reach the gate the way the edges will.

| Row | What it does |
|---|---|
| G1 | The placement records `gate_ready_at`, and `/__bugnet/health` answers through the route within 3 tries 2 s apart |
| G2 | The route with the provider's credential but no gate key, or a wrong one, gets the gate's bare 404 |
| E2B1 | E2B only: the traffic token from create, then from `connect {"timeout":1}` with `endAt` unchanged |
| E2B2 | E2B only: `Bugnet-Port: 49983` gets the gate's 400 |
| E2B5 | E2B only: logs whether the template boots systemd, and then a gate unit exists |
| BOXD2 | boxd only: median health and `curl` past bot protection through the route `boxd.gate_route` names, and a log of whether the other route answers |
| KOYEB1 | Koyeb only: health after `bind_port`, and again after a second `PrepareGate` unbinds and binds |
| BOAT1 | Boat only: health through `POST /host` with the runtime key |
| BOAT2 | Boat only: raw TCP to the machine's own address on the gate port is refused or gets a 404 |

`TestLiveControl` holds CP 15's rows. N1, on every provider, checks `harden` left
`/etc/bugnet/firewall.rules` and that port 25 on a public host, `169.254.169.254:80` and
`10.0.0.1:80` stay closed while `exec`, files and the gate route work. N2, on E2B only, removes
the rules inside the VM and checks `network.denyOut` still closes `10.0.0.1:80`, logging whether
port 25 opens and the VM's IPv6 default route. `169.254.169.254` on E2B is Firecracker's MMDS,
answered inside the VM before `denyOut` sees it, so only `harden` holds it and N2 logs it when
it opens. N1 also probes `[fd00:ec2::254]:80`.

| | `--front local`, the default | `--front vps`, behind the dev front |
|---|---|---|
| Ports | Free loopback ports | Edge 8443, secrets route 8445, egress 9443, SSH 2223, from `config/dev-front.toml` |
| Names | The dev names, reached with `curl --resolve` through a loopback front that adds HAProxy's PROXY v2 header | The dev names through the VPS |
| TLS | ACME off, a fresh self-signed pair | Let's Encrypt for the exact names over the tunnel, the wildcard `make dev-certs` copied |
| `secrets.egress_ips` | `127.0.0.1` | `vps.egress_ips` in `config/dev-front.toml` |

The fronted mode keeps its ACME caches in `~/.local/state/bugnet-dev/acme/<service>/`, so
repeated runs reuse their certificates and stay inside Let's Encrypt's limits. It refuses to start
when one of its four ports is taken, when the wildcard pair in
`~/.local/state/bugnet-dev/preview-dev/` is missing or expired, or when
`https://transfer.dev.bugnet.sh/` does not answer with the certificate this stack's edge holds,
and it says which. The live stack needs the Linux build of `bugnet-run` from
`make -C ../bugnet-run build`.

`bugnet-dev owned PROVIDER` prints the count and ids on its own, reading the provider's key from
the environment, so `set -a; . ./.env; set +a` first.

The adapter tests of each provider run the contract against the real service, and skip without
the provider's key. The boxd run also starts `python3 -m http.server 8000` in a machine and checks
that its public default proxy does not serve it.

Every provider's run streams files of 0 B to 101 MiB up and back, compares their SHA-256 with
`sha256sum` in the sandbox, checks a device or FIFO never reads as a regular file and logs the
filesystem of `/tmp` and the home directory. Three runs add their provider's file rows:

| Test | Rows |
|---|---|
| `TestLiveBoxdFileRows` | A cancelled upload keeps the old file, 1 MiB chunks, `FsRename` over a hard-linked and a root-owned target |
| `TestLiveKoyebFileRows` | The `/files` and `/files/uploads` routes, a 1 GiB download through Koyeb's cuts, two 8 MiB parts sent at 0.3 MB/s |
| `TestLiveBoatFileRows` | `Range` on signed download URLs, download and upload URLs used past their `expiresAt` |

The Koyeb run reads 1 GiB, so give it `--timeout 90m` on a slow uplink.

## Running locally

```sh
createdb bugnet_router_dev
cp .env.example .env
scripts/dev-up.sh
```

`.env` holds only secrets, the database URL and the provider keys. Every other setting comes
from `config/dev.toml`, or from the file given as `dev-up.sh`'s argument. `dev-up.sh` builds the
binaries into `bin/`, writes fresh router secrets to `.env.keys` with
`bin/bugnet-dev keys > .env.keys` when that file is missing, adds any secret `bugnet-dev keys`
prints that an older `.env.keys` lacks, keeping its other values, writes `bin/dev.toml` as the
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
bin/bugnet-dev transfer --handle sbx_... --upload data.csv --path ~/data.csv
bin/bugnet-dev transfer --handle sbx_... --download /root/out.csv
bin/bugnet-dev preview --handle sbx_... --port 5173
BUGNET_OPERATOR_TOKEN=... bin/bugnet-dev revoke-preview <preview id> --reason 'phishing page'
bin/bugnet-dev stop --handle sbx_...
bin/bugnet-dev create --package dev-small-10m --payer <base58> \
    --fee-payer DevFaci1itatorAddress1111111111111111111111
bin/bugnet-dev wallet show <base58>
bin/bugnet-dev wallet block <base58> --reason 'test' --until 1h
bin/bugnet-dev wallet unblock <base58> --note 'done'
bin/bugnet-dev wallet list --blocked
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
`transfer` mints a transfer URL and prints it with its window, size, cap and the `curl` command
that uses it: an upload of a local file to `--path`, `~/<file name>` when none is given, with an
optional `--sha256`, or a download of a sandbox path.
`preview` prints the preview link of one port, its id, the paid deadline and whether something
listened on the port when the router asked. `revoke-preview` calls `OperatorService` with
`BUGNET_OPERATOR_TOKEN`, the actor from `--actor` or else `USER`, and kills every link of that
preview for good. `token` and `create` take `--payer`, `--fee-payer` and `--allowance-micros`,
which set those claims. Without `--amount` the allowance is added to the catalog price, so the
package stays paid. `config/dev.toml` turns the payer claim on with one test facilitator address,
so `create --payer` takes the wallet path, and raises the anonymous cap, since most dev tokens
carry no payer. `wallet block`, `unblock`, `show` and `list` call `OperatorService` with
`BUGNET_OPERATOR_TOKEN`, the actor from `--actor` or else `USER`. `--until` takes an RFC 3339
time or a duration from now, and a block without it lasts until an operator lifts it.
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
| `BUGNET_DATA_PLANE_KEYS` | `router-api` and `router-worker`, required, root keys as `<id>:<base64 of 32 bytes>` entries separated by commas, newest first |
| `BUGNET_EVENT_SIGNING_KEY` | `router-worker`, which requires it, and `router-api` when set, base64 Ed25519 seed that signs events |
| `BOXD_API_KEY`, `E2B_API_KEY`, `KOYEB_API_TOKEN`, `BOAT_API_KEY` | the binaries for each provider in `providers`, and the live tests |
| `BUGNET_ORDER_PRIVATE_KEY` | `bugnet-dev` only, base64 Ed25519 seed it signs order tokens with |
| `BUGNET_EDGE_TOKEN`, `BUGNET_SECRETS_TOKEN`, `BUGNET_SSH_TOKEN`, `BUGNET_EGRESS_TOKEN` | `router-api` and `router-worker`, one bearer token per edge process, each empty or 32 characters or more |
| `BUGNET_EGRESS_FETCH_TOKEN` | `router-api` and `router-worker`, the `bnf_` token the router's private fetch opens egress tunnels with, empty or 32 characters or more |
| `BUGNET_OPERATOR_TOKEN` | `router-api`, and `bugnet-dev`'s operator commands, the bearer token of `OperatorService`, empty or 32 characters or more |
| `BUGNET_PAYMENTS_ROUTER_TOKEN` | `router-api`, and Payments in its own environment with the same value, the bearer token of `PaymentsService`, empty or 32 characters or more |
| `BUGNET_HOOK_PATH_SECRET` | The edge only, base64url of 32 bytes in the provider webhook path |

An empty caller token admits no calls, so a router without edges serves only the gateway. No two
of `BUGNET_INTERNAL_TOKEN` and the caller tokens may be equal, since each admits one caller to
its own service. `bugnet-dev keys` prints fresh values for all of them, and `dev-up.sh` adds the
ones an older `.env.keys` lacks.

`BUGNET_DATA_PLANE_KEYS` derives the data plane's preview tokens, gate keys, SSH grant ids,
transfer and link tokens, put grants, pairing codes and placeholders, so the router stores none
of them. A root id is 1 to 32 lowercase letters, digits or hyphens, and `bugnet-dev keys` prints
one root named `k1`. Each placement records the id of the root its gate key and placeholders come
from. To rotate, put a new root first in the list. New placements use it, and preview tokens, put
grants and SSH grant ids made under any listed root still verify. Restart `router-api` and
`router-worker` together with the same list, since a process without the new root cannot open or
check what the other made under it. Remove an old root only once every placement that recorded it
has ended.

The store seals the provider credentials in `provider_ref` (E2B's `envd_token` and
`traffic_token`, Koyeb's `secret`, Boat's `_token`) with AES-256-GCM under a key derived from the
newest root, as `v1.<root id>.<base64url>`, so a copy of the database without these keys reveals
none of them. Every other field, `id` included, stays plaintext. A field written before sealing
reads as plaintext. A sealed field no listed root opens reads as absent, and the store logs a
warning that names the placement and the field. A destroy needs only the sandbox id, so a stop or
a deadline still ends the placement and frees its slot. Only a call that uses the credential
fails.

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
| `[hardening]` | The mail ports and the private, link-local and metadata ranges `bugnet-run harden` blocks outbound, and the mining pool list |

`boxd.gate_route` (`default` when unset, or `subdomain`), `boat.ssh_path` (`gate` or `native`)
and `local.sshd_path` choose each provider's route. A `bn` subdomain proxy made after boot can
lack its certificate for a while, so boxd's default is the machine's own name. Every provider
list takes known provider names, each once. `secrets.providers` never takes `local`, whose
sandboxes share the host.
`router-api` refuses to start when `secrets.providers` lists a provider while
`secrets.ca_cert_file` is empty, so no placement with secrets lacks the CA. It reads the CA file
once at start and refuses one that is not PEM certificates alone.
`secrets.box_public_keys` takes `{ id, key }` tables, newest first, each key standard base64 of a
32-byte X25519 public key.

`[e2b]`, `[boxd]`, `[koyeb]` and `[boat]` each take `harden_required` (`true`), which fails a
boot over to the next provider when `harden` fails there, and `harden_allow` (`[]`), the
provider's own control addresses, open before any block. Ranges and addresses are IP addresses
or CIDRs. `hardening.pool_list` names a file of mining pool addresses or CIDRs, one per line, at
most 10000, with blank lines and `#` lines skipped. `router-api` reads it at start and refuses a
malformed one, naming the line by number. A missing or empty list logs a warning, `/monitor`
reports it, and every other rule still applies. `dev.toml` points at
`config/dev-mining-pools.txt`, a documentation range standing in for real pools.

`worker.dataplane_workers` (8) sets the River workers of the `dataplane` queue, which keeps slow
data-plane jobs away from deadlines, the sweep and the outbox. It takes 1 to 10000, the most
River runs on one queue.

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

A host name here and in `[names]` is never an IP address, and never ends in a label that is all
digits or `0x` and hex digits, such as `127.1` or `a.0x1f`. `inet_aton`, curl and browsers read
those short forms as IPv4 addresses.

Money is integer micro-USD and times are UTC.

### x402

`[x402]` holds the settings of the x402 host kind, through which code in a sandbox pays x402 APIs
from an allowance Payments holds. The features that read them land one by one, and until then
the settings only load and check.

| Key | Default | Takes |
| --- | --- | --- |
| `enabled` | `false` | The kill switch. `false` refuses `EnableX402` and `CheckX402` and tells egress never to pay |
| `max_services` | `5` | x402 services per placement, 1 to 32 |
| `max_price_micros` | `500000` | The most one paid call may cost, 1 to 500000 micro-USD |
| `unpaid_requests_per_host` | `300` | Requests Bugnet did not pay, per placement and host, 1 to 4294967295 |
| `unpaid_bytes_per_host` | `"50MiB"` | Bytes both ways of those requests, per placement and host, a byte size, 1 byte to 1 TiB |
| `shared_hosting_suffixes` | 16 names | The gateway's list of shared-hosting suffixes |
| `allowed_shared_hosts` | `[]` | Hosts under a shared-hosting suffix that may still be enabled |
| `blocked_hosts` | `[]` | Hosts refused at enable and dropped from `ResolveTunnel` |

`enabled` stays `false` in `config/example.toml` and `config/dev.toml` until the x402
switch-on. The three lists take lowercase host names, each once, and an error names the entry's
position, never its text. `shared_hosting_suffixes` holds the same 16 names as bugnet-proto's
`vectors/x402/shared-hosting-suffixes.json`. A host under one of them is refused unless it is in
`allowed_shared_hosts`, which the deploy writes from the reviewed hosts of the gateway's
`index.json`, never by hand. `blocked_hosts` is the operator's list.

### Wallets

`[wallets]` holds the payer claim, the per-wallet limits, blocks and strikes (see
[Wallets](#wallets-1)). Counts and rates take 1 to 1000000, durations a positive Go duration.

| Key | Default | Takes |
| --- | --- | --- |
| `use_payer` | `false` | `false` counts every order as having no payer. `true` needs `BUGNET_PAYMENTS_ROUTER_TOKEN` and `emit_events` |
| `require_payer` | `false` | `true` refuses start orders that count as having no payer, and needs `use_payer` |
| `max_concurrent` | `5` | Placements placing, ready or terminating per wallet |
| `starts_per_hour`, `starts_burst` | `30`, `5` | A start bucket per wallet |
| `starts_per_day` | `200` | A second start bucket per wallet, its burst the same |
| `anonymous_max_concurrent` | `20` | Every placement without a payer, together |
| `ops_per_second`, `ops_burst` | `20`, `40` | User operations per wallet |
| `refusals_per_hour` | `10` | Refused orders per wallet before a block |
| `refusal_block_for` | `"1h"` | How long that block lasts |
| `facilitator_addresses` | `[]` | Canonical base58 addresses that count as no payer and are never blocked |
| `emit_events` | `false` | `wallet.blocked` and `wallet.unblocked` to Payments |
| `strike_threshold`, `strike_window`, `strike_block_for` | `2`, `"24h"`, `"168h"` | Strikes inside the window that block a wallet, and for how long |

`[boat]`, `[e2b]`, `[boxd]` and `[koyeb]` each take `ops_per_second`, the user operations a
second on that provider account, 0 for no bucket. Boat's is 50, the others 0.

`router-api` refuses to start with `use_payer = true` while `BUGNET_PAYMENTS_ROUTER_TOKEN` is
empty or `emit_events` is false, since Payments could then neither ask `CheckPayer` nor hear of a
block. With `use_payer = false` it logs once at start that every order counts as having no payer.

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

Each placement's `Exec`, `WatchJob`, `CancelJob`, `ReadFile`, `WriteFile`, `ListFiles`,
`CreateTransfer`, `CreatePreview` and `OpenTransfer` calls spend one token from a bucket that every
`router-api` replica shares, `ops:<placement id>` under `valkey.prefix`. It holds
`limits.sandbox_ops_burst` tokens and gains `limits.sandbox_ops_per_second` a second by the Valkey
server's clock, 20 and 10 by default, and expires once it is full again. A call that finds it empty
answers `ResourceExhausted` with reason `RATE_LIMITED` and a `google.rpc.RetryInfo` holding the wait
until the next token. Other calls spend nothing, nor does a request the router itself finds invalid,
an unknown handle, or an `Exec` or file call on a placement that is not ready. A request only the
provider refuses has spent its token, and so has a `CreatePreview` of a refused port. When Valkey
cannot answer within 500 ms, the call goes through and the router logs a warning. `StopPlacement`
spends nothing, and stops that race on one placement in one process wait for the destroy already in
flight instead of calling the provider again.

The same calls but `OpenTransfer`, which a stranger holding the URL can make, also spend from the
payer's bucket, `rl:wallet:ops:<payer>`, at `wallets.ops_per_second` with a burst of
`wallets.ops_burst`, for a placement with a payer, and from the provider account's bucket,
`rl:acct:<provider>`, at `<provider>.ops_per_second` with a burst of the same, when that is above 0.
One script takes a token from each only when all have one, so a refusal takes nothing, and answers
`RATE_LIMITED` with the longest wait. Control calls, `StopPlacement` among them, never take from
these buckets, so a busy account still ends sandboxes.

## Wallets

An order token may carry `payer`, the base58 Solana address that paid, `fee_payer`, the payment
requirements' `extra.feePayer`, `tx`, the payment's transaction signature, and on a start token
`allowance_micros`, the x402 allowance inside `amount_micros`. A set address must be the one
canonical base58 text of 32 bytes, and `tx` must decode to 64 bytes, else the token is refused
with `PermissionDenied`. `allowance_micros` runs from 0 to `amount_micros`, and an extend token
carrying it is refused. The underpaid check and `orders.amount_micros` use `amount_micros` less
the allowance. `tx` is checked and kept nowhere yet.

The router decides the payer an order counts under in this order: `wallets.use_payer` false, no
payer, a payer equal to the token's `fee_payer`, a payer in `wallets.facilitator_addresses` or seen
as an earlier order's `fee_payer`, or no `fee_payer` with an empty facilitator list, each count as
no payer. The last logs at error level at most once a minute and shows on `/monitor` for an hour,
through the Valkey key `wallet:unguarded`, which a Valkey restart or flush clears early. Otherwise
the payer counts, and the placement keeps it. Every order keeps the payer claim as the token named
it, whatever the decision, and operator calls decide an extension's payer again.

`CreatePlacement` then runs these checks under the payer's transaction lock, or one lock for
every order without a payer, in the transaction that records the order. A retried order answers
as it did, with no check run again.

| Check | Answer | Recorded |
| --- | --- | --- |
| No payer with `require_payer` | `PermissionDenied`, `PAYER_REQUIRED` | Nothing |
| A blocked payer | `PermissionDenied`, `WALLET_BLOCKED` | Nothing |
| `max_concurrent` live placements, or `anonymous_max_concurrent` without a payer | `ResourceExhausted`, `WALLET_BUSY` | A failed, refundable placement and `placement.failed` |
| An empty start bucket, a payer only | `ResourceExhausted`, `WALLET_RATE_LIMITED` with `RetryInfo` | The same |

Each `WALLET_BUSY` or `WALLET_RATE_LIMITED` of a payer takes a token from
`rl:wallet:refusals:<payer>`, and the one that finds it empty also blocks the payer for
`refusal_block_for` with reason `refusals`. When Valkey does not answer within 500 ms, the start
buckets and the refusal bucket go unchecked with a warning, and the concurrency cap still holds.
`ExtendPlacement` refuses a blocked payer's new extension with `WALLET_BLOCKED` and records
nothing. Extensions count against no bucket.

`PaymentsService.CheckPayer` answers what `CreatePlacement` or `ExtendPlacement` would say about
a payer now, as `OK`, `BLOCKED` with `blocked_until`, `BUSY` or `RATE_LIMITED` with
`retry_after`, under no lock, writing nothing and reading the start buckets without taking a
token. An extension is checked for a block only. A bad address answers `InvalidArgument` with
`PAYER_INVALID`. It is advisory, and the check at `CreatePlacement` stays the binding one.

`OperatorService`'s `BlockWallet`, `UnblockWallet`, `GetWallet` and `ListWallets` need an `actor` on
every change, written as `operator:<actor>` on the wallet and its event. A block waits for any order
of that payer in flight, never shortens a block in force, and is refused for a facilitator address
with `FailedPrecondition` and `FACILITATOR_ADDRESS`. Unblocking a wallet with no block in force
changes nothing and writes no event. `stop_live` answers `FailedPrecondition` with
`FEATURE_UNAVAILABLE` until the job that ends live placements lands. `ListWallets` takes up to 1000
a page, 100 when `limit` is 0.

## Error reasons

Each refusal below carries a `google.rpc.ErrorInfo` in domain `bugnet.router`, so the gateway
tells them apart without another call. No message or detail
holds a handle or a job ID.

| Code | Reason | When |
| --- | --- | --- |
| `NotFound` | `HANDLE_UNKNOWN` | A call taking a handle gets a malformed one or one never issued |
| `NotFound` | `JOB_UNKNOWN` | `WatchJob` or `CancelJob` names a job the handle's placement lacks |
| `NotFound` | `FILE_NOT_FOUND` | `ReadFile`, `WriteFile`, `ListFiles` or a download names a path that does not exist |
| `NotFound` | `TOKEN_UNKNOWN` | `OpenTransfer` gets a token with no record |
| `NotFound` | `PREVIEW_UNKNOWN` | A preview id the router never made, or one that is malformed |
| `PermissionDenied` | `TOKEN_INVALID` | A preview token that belongs to no generation of the preview |
| `InvalidArgument` | `PORT_REFUSED` | A preview of a port outside 1 to 65535 or on the gate's deny list |
| `InvalidArgument` | `PATH_REFUSED` | A transfer path that is empty, relative other than `~` or `~/rest`, or holds NUL or a newline |
| `InvalidArgument` | `IS_DIRECTORY` | A download of a directory, or an upload onto one or onto a path ending in `/` |
| `InvalidArgument` | `NOT_REGULAR` | A download of a FIFO, device or socket |
| `InvalidArgument` | `SIZE_REQUIRED` | An upload without `size_bytes` |
| `InvalidArgument` | `SIZE_MISMATCH` | An upload stream that ends short of its size or runs past it |
| `FailedPrecondition` | `TOKEN_USED` | A transfer URL completed once, or spent its 5 claims or its bytes |
| `FailedPrecondition` | `TOKEN_EXPIRED` | A transfer URL past its start window, also for a keyed `CreateTransfer` |
| `FailedPrecondition` | `IN_USE` | Another stream holds the transfer |
| `FailedPrecondition` | `FILE_CHANGED` | The file's size or modification time moved since its URL was made |
| `FailedPrecondition` | `FEATURE_UNAVAILABLE` | `CreateTransfer` on a router without `names.transfer_host`, `CreatePreview` without `names.preview_domain` or on a provider not in `previews.providers` |
| `FailedPrecondition` | `PREVIEW_REVOKED` | An operator revoked the preview, or `CreatePreview` names its port |
| `OutOfRange` | `OVER_CAP` | A file over the transfer cap |
| `OutOfRange` | `RANGE` | A read outside the file |
| `ResourceExhausted` | `TOO_MANY_TRANSFERS` | A 17th unexpired transfer URL, or a 3rd active transfer, of one placement |
| `ResourceExhausted` | `TOO_MANY_PREVIEWS` | A preview past `previews.max_per_placement` live ones |
| `ResourceExhausted` | `BUDGET_SPENT` | The placement spent its edge budget |
| `ResourceExhausted` | `NO_SPACE` | The sandbox's disk is full |
| `DataLoss` | `CHECKSUM_MISMATCH` | An upload's SHA-256 differs from the one its URL was made with |
| `Unavailable` | `GATE_UNAVAILABLE` | The sandbox's gate did not answer, after one repair, or its route is not prepared |
| `InvalidArgument` | `PACKAGE_UNKNOWN` | `PricePackage` names a package the catalog lacks |
| `InvalidArgument` | `NOT_START_PACKAGE` | `PricePackage` names a package with `extension_of` set |
| `FailedPrecondition` | `NOT_READY` | The placement is placing, ending or ended |
| `FailedPrecondition` | `EXCEEDS_LIFETIME` | The extension would outlast the provider's limit |
| `ResourceExhausted` | `RATE_LIMITED` | The placement's, the payer's or the provider account's operation bucket is empty |
| `InvalidArgument` | `PAYER_INVALID` | `CheckPayer` or an operator call names an address that is not canonical base58 of 32 bytes |
| `PermissionDenied` | `PAYER_REQUIRED` | A start order without a payer while `wallets.require_payer` is set |
| `PermissionDenied` | `WALLET_BLOCKED` | An order of a blocked payer |
| `ResourceExhausted` | `WALLET_BUSY` | A start at the wallet's cap on live placements, or the anonymous cap |
| `ResourceExhausted` | `WALLET_RATE_LIMITED` | A start while one of the payer's start buckets is empty, with `RetryInfo` |
| `FailedPrecondition` | `FACILITATOR_ADDRESS` | A block of a facilitator address |

A provider failure carries the provider's code in lowercase instead, such as `rate_limited`. A
call that its caller cancels or lets run out of time answers `Canceled` or `DeadlineExceeded`,
with only the context error as its message.

An `Exec` whose `idempotency_key` another call holds waits for that call to record the job. When
it records none within the wait, `Exec` answers `Unavailable` with no reason, and a retry with the
same key runs the command once.

## Caller services

`router-api` serves seven Connect services on its one listener. Each admits only its own bearer
token, and a service whose token is empty admits no calls, so a router without edges serves only
the gateway.

| Service | Caller | Token | Built so far |
| --- | --- | --- | --- |
| `SandboxRouterService` | The MCP gateway, `bugnet-dev` | `BUGNET_INTERNAL_TOKEN` | Placements, commands, files, `CreateTransfer` and `CreatePreview` |
| `EdgeService` | `bugnet-edge` | `BUGNET_EDGE_TOKEN` | `ReportEdgeUsage`, `CheckEdgeLeases`, `WatchEdgeEvents`, `OpenTransfer`, `ReadTransfer`, `WriteTransfer`, `ResolvePreview`, `RepairPreview` |
| `SecretsRouteService` | `bugnet-secrets` | `BUGNET_SECRETS_TOKEN` | None |
| `SshGatewayService` | `bugnet-ssh` | `BUGNET_SSH_TOKEN` | `ReportSshUsage`, `WatchSshEvents` |
| `EgressService` | `bugnet-egress` | `BUGNET_EGRESS_TOKEN` | `ReportEgressUsage`, `CheckEgressLeases`, `WatchEgressEvents` |
| `OperatorService` | `bugnet-dev`'s operator commands | `BUGNET_OPERATOR_TOKEN` | `RevokePreview`, `BlockWallet`, `UnblockWallet`, `GetWallet`, `ListWallets` |
| `PaymentsService` | Payments | `BUGNET_PAYMENTS_ROUTER_TOKEN` | `GetOrder`, `CheckPayer` |

### Budgets

Each placement has an edge budget, shared by its previews, transfers and SSH, and an egress
budget for its secret tunnels. The transaction that makes it ready sets both from the start
package, each extension's transaction adds its own package's, and a placement records the id of
the `BUGNET_DATA_PLANE_KEYS` root it was created under in `key_id`.

| Budget | Bytes | Pace each way |
| --- | --- | --- |
| Edge | The package's `edge.bytes`, or `edge_budget.bytes_per_10_minutes` times its minutes over 10 | `edge.bytes_per_second`, or `edge_budget.bytes_per_second` |
| Egress | `edge.egress_bytes`, or `edge_budget.egress_bytes_per_10_minutes` scaled the same way | `edge.egress_bytes_per_second`, or `edge_budget.egress_bytes_per_second` |

The pace is the start package's. The edges report the bytes they carry every few seconds, and
`router-api` adds them to `edge:used:<placement>` or `egress:used:<placement>` under
`valkey.prefix`. Every report answers each named placement's bytes left, 0 once spent, and, for
the edge budget, the pace to keep: the full figure while only the reporting process carried the
placement's bytes in the last 10 s, half while `bugnet-edge` and `bugnet-ssh` both did. A
placement that is not ready, or an id that is unknown or malformed, gets no entry. The first report
that reaches a budget writes `BudgetSpent`, once across every replica. An extension writes its
grown budgets to `edge:budget:<placement>` and `egress:budget:<placement>` and clears the spent
marks in one step, so a later crossing writes it again. A report compares against the larger of
that figure and the budget it read, so a report that read the budget before an extension never
spends the grown one. A report counted between an extension's commit and that step crossed before
the extension, and the step clears its mark. The used counters, budget figures and spent marks
expire an hour after the deadline, and
`edge:seen:<placement>`, which records which process carried bytes for the pace, 30 s after the
last report that carried bytes. An extension moves the used counters to an hour after the new
deadline, even for a placement that reports nothing, and no report moves an expiry earlier.

`CheckEdgeLeases` and `CheckEgressLeases` answer each named placement that is still ready with its
deadline and bytes left, and the egress check its `secrets_version`. Ended placements are absent.
`CheckEdgeLeases` also answers, among the previews the edge holds, each one that was revoked, whose
generation moved, or that the router never made, so a lost `PreviewRevoked` is repaired.

### Edge events

The edges learn of stops, extensions and spent budgets from one Valkey stream, `edge:events` under
`valkey.prefix`, kept to about 100,000 entries. `router-api` and `router-worker` append to it right
after the commit that causes an event, retrying a failed append 3 times before logging
`edge event lost`, and the edges' lease checks repair a lost one.

| Event | Sent when |
| --- | --- |
| `PlacementEnded` | A stop (`STOPPED`) or the deadline (`EXPIRED`) moves a placement out of ready, before its sandbox is destroyed |
| `DeadlineMoved` | An extension commits, after both spent marks are cleared, with the new deadline and both budgets' bytes left |
| `BudgetSpent` | A report first reaches a placement's edge or egress budget |
| `PreviewRevoked` | `RevokePreview` commits, and again on each repeat of it |

`WatchEdgeEvents`, `WatchSshEvents` and `WatchEgressEvents` serve the same stream. Over Connect and
gRPC each sends its response headers as soon as it has fixed where it starts, so the caller's open
returns before the first event and no event published after the open is missed. Over gRPC-Web the
headers wait for the first event, since gRPC-Web puts an error that comes before any event in the
headers. Each then sends every event after the cursor, from now when the cursor is empty, each with
the stream id to resume from. A cursor the stream does not hold, because the stream was trimmed past
it or lost with Valkey's data, gets one `Resync` first, after which the caller re-checks every lease
it holds. A cursor that is not a stream id answers `InvalidArgument`.

### Previews

`CreatePreview` answers the link to one port of a ready placement,
`https://<id>.<names.preview_domain>/?bugnet_token=bnp_...`, with the same token for the
`Bugnet-Token` header, the paid deadline and whether something accepted on the port when the
router asked (DP 8.2.10).

- **Ids and tokens.** A preview id is 16 lowercase base32 characters of 80 random bits, one per
  placement and port, in the `previews` table. Its token is derived from the id and the row's
  generation under the placement's `key_id` root, so the router stores none and a repeated call
  answers the same link.
- **Checks.** In order: the handle (`HANDLE_UNKNOWN`), a ready placement (`NOT_READY`), the
  provider in `previews.providers` and a router with `names.preview_domain` and data-plane keys
  (`FEATURE_UNAVAILABLE`), one operation token, then a port of 1 to 65535 off the gate's deny list
  (`PORT_REFUSED`). A placement holds at most `previews.max_per_placement` live previews
  (`TOO_MANY_PREVIEWS`), and a revoked port gets no new one (`PREVIEW_REVOKED`). A transaction
  holding a Postgres advisory lock per placement keeps the cap across replicas.
- **The gate.** A placement whose gate never answered at boot has start saga steps 2 and 3 run
  again, `PrepareGate` and the health checks, under a Postgres advisory lock per placement, so
  racing calls on every replica run them one replica at a time, and only once, logging
  `gate opened` once, when they succeed. Then the gate's health is checked for 2 s. When the steps
  or that check fail, the gate is repaired once and checked again for up to 5 s
  (`GATE_UNAVAILABLE`), and a gate that then answers gets its gate time. A placement that stops
  meanwhile answers `NOT_READY`.
- **Repairs.** `RepairPreview`, and `CreatePreview`'s repair, run `setup --repair` through `Exec`
  and `PrepareGate` again under the same advisory lock, storing the route's meta, at most once a
  minute per placement: `gate:repair:<placement>` under `valkey.prefix` is set with `NX` for 60 s,
  and a call that finds it set answers `repaired: false`. A failed repair answers
  `GATE_UNAVAILABLE` and keeps that guard for its minute.
- **Resolving.** `ResolvePreview` recomputes the token under every listed root and compares in
  constant time, then needs the placement ready and before its deadline (`NOT_READY`) and edge
  budget left (`BUDGET_SPENT`). It answers the provider's route with `Bugnet-Gate` added, the edge
  budget, `previews.interstitial`, the source label and when the sandbox became ready. It spends no
  operation token. A token of a generation a revocation moved past answers `PREVIEW_REVOKED`, any
  other wrong token `TOKEN_INVALID`.
- **Revoking.** `OperatorService`'s `RevokePreview` needs an `actor` and moves the preview's
  generation and sets `revoked_at` once, writes `PreviewRevoked`, and logs the actor, preview,
  placement and reason. A refused call is logged too, with its code, the actor when valid and the
  preview only when its id is well formed. Other ports of the placement keep working.

### Transfers

`CreateTransfer` mints a one-use URL, `https://<names.transfer_host>/<bnx_ token>`, for one file
going into (`UPLOAD`) or out of (`DOWNLOAD`) a ready placement. The edge serves it with
`OpenTransfer`, `ReadTransfer` and `WriteTransfer` (DP 8.3).

- **Paths.** `~` and `~/rest` mean the exec user's home: `/root` for root, `/home/<user>`
  otherwise, and the local provider's own `/root`. Any other relative path, an empty one, or one
  with NUL or a newline answers `PATH_REFUSED`. `realpath -m` then resolves the path in the
  sandbox, under `sudo -n` where the exec user is not root, so links behave the same everywhere.
- **Staging.** Where the exec user is not root (boxd, Boat), a path outside its home and `/tmp`,
  or one inside them that it cannot read (download) or write in place (upload), goes through
  `<home>/.bugnet/xfer/<transfer id>/`. A download is copied there with
  `sudo -n bugnet-run stage-out`, an upload is moved into place with `sudo -n bugnet-run place`,
  and the directory is removed afterwards. Staging directories and temporary files older than an
  hour are removed at the next `CreateTransfer` into the placement.
- **Checks.** An upload needs `size_bytes`, 0 included, and is refused over the cap or onto a
  directory or a path ending in `/`. A download is refused for a directory, anything but a regular
  file, or a file over the cap, and its ETag pins its size and modification time. With
  `want_sha256` the answer carries the file's SHA-256, from `sha256sum` in the sandbox, and
  `OpenTransfer` hands the same digest to the edge for its `X-Bugnet-Sha256` header. The cap is
  the package's `edge.transfer_max_bytes`, else `transfers.max_bytes`.
- **Tokens.** A call with an `idempotency_key` derives its token from the placement and the key,
  so a retry answers the same URL, `TOKEN_USED` once it completed and `TOKEN_EXPIRED` once its
  window passed. Without a key the token is random. Each token's SHA-256 goes into
  `transfer_index` in Postgres with its placement, for abuse reports.
- **Records.** Each URL is a Valkey hash, `xfer:<sha256 hex>`, alive for `transfers.url_ttl`
  plus `transfers.max_duration`, and at least until its last lease ends, so a late resume keeps
  its record. A placement holds at most `transfers.max_tokens_per_placement`
  unexpired URLs (`xfer:open:<placement>`) and `transfers.max_active_per_placement` active ones
  (`xfer:active:<placement>`). `OpenTransfer` claims a URL in one script under a 30 s lease,
  checking its `transfers.max_claims` claims and its byte budget of twice its size plus 64 MiB.
  A failed stream hands it back, a lapsed lease frees it, and a completed one makes it used. A
  read of a range that stops short of the file's end hands it back too, so the rest can follow. A
  download that started may resume after its window, an upload only within it. `OpenTransfer`
  with `probe` describes the file without claiming, and refuses a placement that spent its edge
  budget with `BUDGET_SPENT`.
- **Streams.** `ReadTransfer` reads from an offset in messages of at most 256 KiB, checks the file
  again at the end and only then sends its last bytes with `verified`. `WriteTransfer` hashes and
  counts the bytes and commits only on a clean close with exactly the declared size and a matching
  SHA-256, so a cut or wrong upload leaves the old file. A stream ends after
  `transfers.idle_timeout` without a byte from the edge. Time the provider spends reading,
  writing, committing or staging never counts as idle. But one provider call that runs past 5 x
  `transfers.idle_timeout` ends the stream as stalled, which frees the URL and its active slot at
  once for a resume or a retry. A stream also ends at `transfers.max_duration`, at the
  placement's deadline, and at once when the placement ends or spends its edge budget, whichever
  replica served it, from one reader of the edge events per process. A stream reads its placement
  and the edge spent mark again once it watches the events and every 10 s after. So an event sent
  before it watched, or lost on the way, still ends it. An extension moves its end.
- **Logs** name a transfer by the first 8 hex characters of its token's SHA-256, never its path.

### Orders for Payments

`GetOrder` tells Payments what became of an order, so it can refund one that no event reached.
An order the router never took answers `redeemed` false, and Payments refunds it in full. Any
other answers `redeemed` true with the order's `kind` (`start` or `extend`), its placement's
state now, and the placement's `failure_reason`, `termination_reason` and `ended_at` as
`Placement` carries them. `refundable` is the placement's, for a start order only, since the
router records an extension only once it applied it. An empty `order_id` answers
`InvalidArgument`. `CheckPayer` is in [Wallets](#wallets-1).

## Deployment

With `api.reuse_port = true`, `router-api` binds `api.listen_addr` with `SO_REUSEPORT`, so a new
process starts beside the old one. Production turns it on. It is off by default, so a second
`router-api` on a shared machine fails with address in use instead of taking connections meant
for another stack.

On SIGTERM the old one stops accepting, ends long polls and watch streams, and hands its jobs to
peers at once, then keeps serving its open transfer streams for up to `transfers.max_duration`
before it cuts what is left and exits. A download cut then resumes, and an upload restarts and uses
one of its claims. Its systemd unit sets `TimeoutStopSec` above `transfers.max_duration`, such as
`31min`, since systemd otherwise kills it 90 s after SIGTERM.

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
outbox events, Valkey memory at 80% of `maxmemory` or more, a `maxmemory-policy` other than
`noeviction`, a mining pool list that is empty, missing or unreadable, and a payer claim counted
as absent in the last hour because no `fee_payer` came and `wallets.facilitator_addresses` is
empty. A failed connection
means the worker is down. When `INFO memory` fails or times out while Valkey answers,
`/monitor` reports `valkey memory is unknown`. When an ACL or `rename-command` refuses `INFO`,
it reports `valkey refuses INFO, so its memory and policy go unchecked`, since that also leaves
the start-up policy check blind, so leave `INFO` allowed. When it answers without a field, as a
managed Valkey may, `/monitor` checks the fields it got and logs a warning. `/healthz` is a bare
liveness answer and `/readyz` checks only that Postgres and Valkey answer.

## The VPS front

The founder's VPS is the public front (DP 15). HAProxy splits TCP 443 by SNI and never decrypts.
Until the production deploy, the VPS fronts the Mac through a reverse SSH tunnel for live tests
(DP 15.6). `deploy/vps` holds its files.

| Path | What it holds |
| --- | --- |
| `names.toml` | Every public name on 443, `exact` or `suffix`, its backend, production and dev |
| `site.toml` | The VPS's addresses and the founder's answers, decoded strictly |
| `haproxy.cfg.tmpl` | DP 15.2's config, its bind lines and optional frontends from `site.toml` |

### Maps and settings

```sh
go run ./cmd/bugnet-dev deploy maps --names deploy/vps/names.toml --site deploy/vps/site.toml \
  --out DIR [--front]
```

It writes six files into `DIR`:

| File | Goes to | Holds |
| --- | --- | --- |
| `sni.map` | `/etc/haproxy/` | Each `exact` name and its backend, production first |
| `end.map` | `/etc/haproxy/` | Each `suffix`, with a leading dot, and its backend |
| `haproxy.cfg` | `/etc/haproxy/` | DP 15.2's config |
| `certs.toml` | `/etc/bugnet/` | `bugnet-certs` settings, each suffix's wildcard, its DNS host |
| `certs-staging.toml` | `/etc/bugnet/` | The same against Let's Encrypt staging |
| `deploy.env` | Read by `front.sh` | The admin SSH port and the wildcard directories |

- A new public name is one line in `names.toml`. A suffix line names its certificate directory
  under `/var/lib/bugnet-certs/` with `cert`.
- A staging pair goes under `/var/lib/bugnet-certs/staging/`, never into a live directory. A
  staging certificate that is not yet due would keep the real one from being issued.
- `--front` writes the dev front's files. `haproxy.cfg` has no `frontend ssh`, since port 22
  waits for the production deploy. The `bugnet-certs` files hold the dev wildcard only, with
  `cert_metrics = []`, since no edge runs on the VPS then.
- It refuses, naming the key or the line: an unknown key in either file, a name that is not a
  lowercase host name, an unknown kind or backend, a name listed twice as one kind, a suffix
  without `cert`, an address of the wrong family, an admin SSH port of 80, 443 or 2200, and an
  empty `ipv4`.

### The founder's answers

Each answer changes a value in `site.toml`, never a script.

| Question | Answer | `site.toml` | HAProxy |
| --- | --- | --- | --- |
| DP 1 | (a) admin SSH on 2222 | `admin_ssh_port = 2222` | `frontend ssh` binds `:22`, `:::22` |
| DP 1 | (b) a second IPv4 | `admin_ssh_port = 22`, `ssh_gateway_ip` | It binds 22 there only |
| DP 2 | (a) or (b) | `dns_provider`, `dns_env` | No change |
| DP 11 | The proxy shares the IP | `egress_ip = ""` | Unknown names go to `egress` |
| DP 11 | The proxy's own IPv4 | `egress_ip` | `frontend egress_ip` sends all to `egress` |

- Under DP 1 (b), admin sshd must listen on `ipv4` alone, not on every address, or HAProxy cannot
  bind 22 on the second one. The dev front needs no `ssh_gateway_ip`, since it binds no 22.
- With `egress_ip`, `frontend tls` binds `ipv4:443` instead of `:443`. The two frontends split
  its 14,000 connections evenly, so the sum stays under the global 20,000.
- `dns_env` is `["CF_DNS_API_TOKEN"]` by default, Cloudflare's variable. Another `dns_provider`
  needs its own list. The values go in `/etc/bugnet/certs.env` on the VPS and nowhere else.

### The dev front

`deploy/vps/front.sh` sets up the dev front, as root on the VPS. Every VPS live test waits on it.
Running it again changes nothing. It:

1. checks its bundle, and that admin SSH listens on `admin_ssh_port`, before it changes anything;
2. installs `haproxy` and `ufw`, and refuses when another process holds port 443 or 2200;
3. installs the maps and `haproxy.cfg`, runs `haproxy -c` and puts the old files back when that
   fails, then reloads HAProxy, which keeps open connections, and waits until it holds both ports;
4. makes the `tunnel` user and its sshd block, checked with `sshd -t` (`deploy/vps/tunnel.md`);
5. installs `bugnet-certs` in `/opt/bugnet/certs/<sha256 prefix>/` with `current` pointing at it,
   its two settings files, its unit and its timer, which runs it twice a day. It makes
   `/etc/bugnet/certs.env` empty, root's and 0600 when it is missing, and never reads it;
6. resets ufw to allow TCP 80, 443, 2200 and `admin_ssh_port` only, denying UDP 443 too. A
   ufw that already holds exactly those rules is left alone, since a reset lifts it briefly.

The dev wildcard's directory is `bugnet-certs:<reader> 2750`. `<reader>` is the primary group of
the admin user who ran `sudo`, or `BUGNET_CERTS_READER` when set. The setgid bit gives each new
pair that group, so `make dev-certs` in bugnet-edges can copy it as that user.

The steps, from bugnet-router on the Mac. `<admin>` is the VPS's admin user, `<vps>` its IPv4,
`<port>` its admin SSH port (2222, or 22 under DP question 1 b), and `<arch>` `amd64` when
`uname -m` on the VPS says `x86_64`, `arm64` when it says `aarch64`.

```sh
[ -e ~/.ssh/bugnet_tunnel ] || ssh-keygen -t ed25519 -N "" -C mac -f ~/.ssh/bugnet_tunnel
out=$(mktemp -d)/bugnet-front
go run ./cmd/bugnet-dev deploy maps --front --names deploy/vps/names.toml \
  --site deploy/vps/site.toml --out "$out"
cp -R deploy/vps/front.sh deploy/vps/systemd "$out"/
cp ~/.ssh/bugnet_tunnel.pub "$out"/tunnel.pub
(cd ../bugnet-edges && CGO_ENABLED=0 GOOS=linux GOARCH=<arch> \
  go build -o "$out"/bugnet-certs ./cmd/bugnet-certs)
scp -P <port> -r "$out" <admin>@<vps>:
ssh -t -p <port> <admin>@<vps> sudo bugnet-front/front.sh bugnet-front/bugnet-certs
```

Then on the VPS, once the DNS token is in `/etc/bugnet/certs.env`, one run against Let's Encrypt
staging. It exits 0 with a pair in `/var/lib/bugnet-certs/staging/preview-dev/`:

```sh
sudo systemd-run --wait --pipe --quiet --collect \
  -p User=bugnet-certs -p Group=bugnet-certs -p EnvironmentFile=/etc/bugnet/certs.env \
  -p StateDirectory=bugnet-certs -p StateDirectoryMode=0755 \
  /opt/bugnet/certs/current/bugnet-certs -config /etc/bugnet/certs-staging.toml
```

Once it passes, the real run. It exits 0 with the pair in `/var/lib/bugnet-certs/preview-dev/`,
and a second run logs `certificate not due`:

```sh
sudo systemctl start bugnet-certs.service; sudo journalctl -u bugnet-certs.service -n 20
```

Last, `make dev-certs VPS=<admin>@<vps> VPS_SSH_PORT=<port>` in bugnet-edges, and autossh as
`deploy/vps/tunnel.md` says.

`deploy/vps/test/vm-test.sh front` runs `front.sh` in a throwaway Ubuntu 24.04 VM on the Mac
with `limactl` (`brew install lima`). It needs no VPS, provider or DNS token. It checks that:

- a bundle with `frontend ssh`, or with admin SSH where nothing listens, is refused untouched;
- a second run changes nothing;
- `haproxy -c` passes and HAProxy listens on 443 over both families and on 2200, never on 22;
- `tunnel` has no shell, and may listen on the four tunnel ports and nowhere else, with no `-L`,
  `-W` or command, while other users keep their sshd settings;
- ufw allows TCP 80, 443, 2200 and the admin port alone, and drops UDP 443;
- the unit, its user and `certs.toml` work, since a run without the token stops at `dns_env`, and
  the staging command hands back that exit status;
- a file `bugnet-certs` writes in the dev wildcard's directory takes the reader's group, and the
  admin user can read it.

`KEEP_VM=1` keeps the VM for a look afterwards. The first run downloads Ubuntu's cloud image.

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
| `reason` | A stable code such as `stop`, `deadline`, `no_candidate`, `wallet_busy`, `wallet_rate_limited`, `provider_capacity`, `boot_failed` or `placing_stalled` |

With `wallets.emit_events`, `wallet.blocked` carries `{payer, until, reason, placement_id}`, with
`until` and `placement_id` null when there is none, and `wallet.unblocked` carries `{payer}`.

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

E2B's create blocks the IPv4 entries of `hardening.deny_ranges` outside the VM with
`network.denyOut`. `harden` blocks the IPv6 entries inside the VM, all but `fe80::/10`, which
stays open for envd. It also blocks `169.254.169.254/32` when an entry holds it: Firecracker's
MMDS answers there inside the VM, before `denyOut` sees the packet. A package with
`egress.allow` is restricted: it lands only on a provider with egress control at create. On
E2B it reaches the egress proxy's IPv4 addresses alone, through `network.allowOut` under a
`0.0.0.0/0` deny, so `router-api` refuses to start when `providers` lists `e2b`, a package has
`egress.allow` and `secrets.egress_ips` holds no IPv4 address. On boxd the
machine's `egress` list holds the package's hosts, `names.egress_host` and the proxy's IPv4
addresses. Koyeb, Boat and `local` never take one: Boat has no egress control, and Koyeb's
network policy waits for live test N1.

The registry decides which providers offer previews, SSH and secrets, and the placement engine and
the API read capabilities only through it. A provider offers previews when it is listed in
`previews.providers` and its adapter has a gate route, SSH when it is listed in `ssh.providers`
and has a gate route, or with `boat.ssh_path = "native"` an SSH server of its own, and secrets
when it is listed in `secrets.providers`. A listed provider whose adapter lacks what the feature
needs stops both binaries at start, naming it. Every list is empty until a live test passes, so
no provider offers SSH, previews or secrets yet. An order that requires any of them finds no
provider. `Estimate` marks every package, and `PricePackage` the one it prices,
`feature_unavailable` for it.

Every adapter has a gate route, the way the edges reach the `bugnet-run` gate inside a sandbox.
`PrepareGate` opens it once and returns the `provider_ref` meta to keep. `GateRoute` builds it
from the ref alone and never calls the provider. The router adds the gate key itself.

| Adapter | `PrepareGate` | Route | Credential | Meta kept |
| --- | --- | --- | --- | --- |
| E2B | `connect {"timeout":1}`, only for a ref without the token | `https://<port>-<id>.<domain>` | `e2b-traffic-access-token` header | `traffic_token` |
| boxd, `subdomain` | `CreateProxy bn`, or `SetProxyPort bn` when it exists | `https://bn.<name>.<access domain>` | None | `gate_host` |
| boxd, `default` | Pins the default proxy, turns bot protection off | `https://<name>.<access domain>` | None | `gate_host` |
| Koyeb | `bind_port`, with `unbind_port` and a second bind on 409 | The public URL, `/koyeb-sandbox/` blocked | `X-Routing-Key` in pool mode | `executor_url`, `routing_key` |
| Boat | `POST /host` | `https://<sub>-<port>.on.boat.dev` | `_port_auth` cookie | `gate_host`, `_token` |
| local | None, the ports come from `Create` | `http://127.0.0.1:<gate_port>` | None | `gate_port`, `sshd_port` |

E2B's create keeps `traffic_token`, since every sandbox is created closed to public traffic.
Boat's hosted URL is split into its host and token and never stored or logged whole. A local
sandbox gets two free loopback ports at create, kept in its `meta.json`, since local sandboxes
share the host. A route whose meta is missing fails as `invalid`, wrapping
`provider.ErrGateUnprepared`.

Koyeb's executor binds one port at a time and cannot say which, so a repeat `PrepareGate`
unbinds the live gate for a moment before binding it again. When the second bind also gets 409,
another client bound a port in between, and `PrepareGate` fails as `internal` without failover
so the caller retries it.

Commands run as root on E2B and Koyeb, as `boxd` on boxd, as `user` on Boat, and as the user
running the router on `local`.

On `local`, the runner's root commands (`setup`, `gate`, `ssh-up`, `egress apply` and
`forward`) get `--root <sandbox>/fs`, so `/etc/bugnet` is the sandbox's own, and their `--spec`
and `--config` paths map into the sandbox as `--cwd` does. A root command the router sends
through `sudo -n` runs as the host user there. Create hard-links `local.runner` at the
sandbox's `/usr/local/lib/bugnet/bugnet-run`, so `setup` finds it in place, and `Destroy` kills
the gate, forwarder, sshd and sampler whose pid files under `/run/bugnet` name the sandbox.

Every adapter streams whole files in and out of sandboxes, for transfers, through
`StatFile`, `OpenReader` and `OpenWriter` on `provider.Provider`. A read stops at the size the
file had when it opened and fails if the file ends sooner. No write makes new bytes visible:
the target changes only on `Commit`, after exactly the declared size, and an abort or the end of
the writer's context leaves it as it was. `StatFile` never follows a final symlink, and only a
regular file reads as `Regular`. `contract.Streams` checks every adapter the same way. A full
disk reads as `no_space` on `local`, E2B, boxd and Koyeb. Boat's API names no full-disk answer,
so a full disk there fails as another error.

| Adapter | Reads | Writes |
|---|---|---|
| `local` | `O_NOFOLLOW` open, checked against the `Lstat` | `.<name>.bugnet-<8 hex>.part` beside the target, renamed on `Commit` |
| E2B | One ranged `GET /files` | `POST /files` to the temp file, then `chmod` and `Move` |
| boxd | `FsRead` with the length, its byte count checked | `FsWrite` in 1 MiB chunks to the temp file, then `FsRename` |
| Koyeb | Ranged `GET /files`, reissued with `If-Range` when cut, 3 empty tries in a row fail | Parts of `transfers.koyeb_part` in memory, one `PUT /files` or an upload session completed on `Commit` |
| Boat | A ranged `GET` of a signed URL, bytes skipped on a 200 of the stat's length | A `PUT` of the temp file to a signed URL, then `permissions` and `move` |

Transfer clients ask for `identity` encoding over HTTP/1.1, so lengths stay exact. boxd streams use
a second gRPC connection, so a long stream never shares TCP with `Exec`. Each Koyeb part is sent
again up to 3 times, after the answer's `Retry-After` (at most 30 s) or else a pause of 1 s that
doubles each time. All part buffers of one process stay under `transfers.koyeb_buffer_total`, a
writer waiting for room. A transfer stream counts the write that sends a part, with its retries and
its wait for room, as one provider call. So each part must reach Koyeb within 5 x
`transfers.idle_timeout`, or the stream ends as stalled. At 0.3 MB/s an 8 MiB part takes about 28 s,
and a 100 MiB part about 350 s, past the default 300 s. A Koyeb part or read that gives up after a
lost connection or a 502 and up reads as `not_found` once Koyeb shows the sandbox gone. Koyeb's
`GET /files/stat` follows a final symlink, so `StatFile` reads the directory's listing instead.
Boat's file API reaches only `/home/user`, `/tmp` and the paths under them, so any other path is
refused before a call. `StatFile` describes the two directories themselves, which a writer refuses.
Boat's file info has no type, so `StatFile` asks the shell whether a path is a regular file. Boat
answers a file call on a missing path with a 400 `sandbox_direct_failed` whose message starts
`No such file or directory`, which the adapter reads as `not_found`.
