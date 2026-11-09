# bugnet-router

The Bugnet sandbox router. It takes a paid order token, places a sandbox on one of several
providers, runs commands in it and ends it when the paid time is up.

Two binaries share one Postgres database. `router-api` serves the Connect API, runs the
placement saga and streams command output. `router-worker` ends placements at their deadline,
sweeps stray sandboxes and posts signed events to `events_url`. Postgres is the system of record,
Valkey carries output streams and job leases, and River runs durable jobs. Both read their
settings from a TOML file and their secrets from the environment.

`router-api` serves `SandboxRouterService` from `bugnet-proto`, and one more Connect service for
each other caller (see [Caller services](#caller-services)). Its SSH keys RPCs,
and every other RPC not built yet, answer `Unimplemented` until the router builds them.
Transfers, previews and secrets are built (see [Transfers](#transfers),
[Previews](#previews) and [Sealed secrets](#sealed-secrets)).

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
| `cmd/router-worker` | Deadline, watch, sweep, outbox, `apply_secrets`, session jobs, canaries, the health rollup, `reconcile` and `hook_hint`, benchmark waves and retention, `router-worker outbox-redrive` sends stuck events at once |
| `cmd/bugnet-dev` | Developer CLI for keys, tokens, placements, commands, internal certificates, benchmarks and the placement simulator |
| `internal/api` | Connect handlers, health and readiness |
| `internal/ratelimit` | Token buckets in Valkey, the per-placement operation limit and named buckets with fractional rates |
| `internal/vkclient` | Valkey connections at `host:port` or a `unix://` socket path |
| `internal/vkinfo` | Valkey's memory and `maxmemory-policy` from `INFO memory` |
| `internal/saga` | Start, extend and stop of placements |
| `internal/placement` | Provider choice by price, margin and health |
| `internal/jobs`, `internal/streams` | Commands through `bugnet-run`, output streams, leases |
| `internal/worker` | River workers for deadlines, the sweep, the outbox, the session copy, session expiry, limit polls and account checks |
| `internal/probe` | The probe that starts, times and removes one sandbox, for canaries and benchmarks |
| `internal/bench`, `internal/simulate` | Nearest-rank percentiles, benchmark cost estimates and start reports, and the placement simulator that replays recorded start samples |
| `internal/reconcile` | `Classify` (the signal classes of CP 6.3), the correlation `Guard` and `Reconcile`, which ends the placements their provider lost |
| `internal/health` | The provider health the replicas share through Postgres, its rollup rules and canary schedule, and the requests for an account check or a limits poll |
| `internal/store`, `migrations` | Postgres access and schema |
| `internal/provider`, `internal/providers` | Provider port, adapters, the registry and the contract suites every adapter runs |
| `internal/config`, `internal/catalog` | Settings file, secrets and the package catalog |
| `internal/hostname` | The one exact host name rule that `[names]` and a package's `egress.allow` share |
| `internal/hostrules` | The one set of host rules that secret hosts and x402 hosts share |
| `internal/x402` | x402 service checks, service ids, path rules, event codes and statuses |
| `internal/solana` | The one base58 decoder, for Solana keys and signatures |
| `internal/bytesize` | Byte sizes such as `8MiB` in settings and the catalog |
| `internal/ordertoken`, `internal/handle` | Order tokens and sandbox handles |
| `internal/estimator` and its packages | The resource estimator: the source parser, facts, versioned rules, canonical JSON, the GitHub and Hugging Face fetchers with their caches and budget guard, and the service `Estimate` calls |
| `internal/wallet`, `internal/wallets` | Payer addresses and the payer decision, wallet blocks, strikes and buckets |
| `internal/stats` | The public stats feed, its Payments totals call, its routes and the source functions of values later tasks build |
| `internal/dpkeys` | Data-plane tokens, keys and grants derived from the root keys, and sealed provider credentials |
| `internal/budget`, `internal/edgeevents` | Edge and egress budgets in Valkey, and the edge event stream |
| `internal/transfer` | Transfer records and claims in Valkey, path resolution and staging through `bugnet-run` |
| `internal/sessions` | The session page: cleaning and redaction, event writes, `record_json` and `live_json` |
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
scripts/live-test.sh --suite sessions e2b
scripts/live-test.sh --suite bench --router http://127.0.0.1:8080
```

| Suite | Runs | Providers | Keys checked |
|---|---|---|---|
| `adapters`, the default | Each provider's adapter live tests, `go test -run Live` | At least one | Each named provider's, from `.env` |
| `dataplane` | `TestLiveDataplane` in `test/live` | At least one | Each named provider's, from `.env` |
| `control` | `TestLiveControl` in `test/live` | Zero or more | Each named provider's, from `.env` |
| `leak` | `TestLiveLeak` in `test/live` | At least one | Each named provider's, from `.env` |
| `sessions` | `TestLiveSessions` in `test/live`, rows SESS1 to SESS6, with `sessions.keep` at 3 minutes | At least one | Each named provider's, from `.env` |
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

The SSH rows start `bugnet-ssh` beside the edge, on a free port locally and on the tunnel's port
2223 with `--front vps`. They drive `bugnet-dev ssh-key` and the host's `ssh`, `scp`, `sftp` and
`rsync`.

| Row | What it does |
|---|---|
| S1 | `ssh host id -un` answers the exec user |
| S2 | A PTY shell, `scp`, `scp -O`, `sftp`, `rsync` and `ssh -L` to a loopback port |
| S3 | `ssh -D` to a remote host, `-R` and `-A` refused |
| S4 | A 15-minute idle session, then a 60-minute one on `dev-small-75m` (50 minutes on E2B Hobby, 60 with `BUGNET_LIVE_E2B_PRO=1`) |
| S5 | `scp` of 1 GiB, the rate logged |
| S6 | `ssh host env` shows the CA variables |
| S7 | A session closes at the deadline, at `StopPlacement` and at `--remove` |
| B1 | A 64 MiB budget (`dev-edge-64m`) across a preview, a transfer and `scp`, all cut by 80 MiB |
| B2 | Spend that budget, extend with `dev-edge-64m-more`, load the same preview link again |
| E2B3, BOXD4, KOYEB4, KOYEB6, BOAT6 | Listeners, install time, a session past a minute, port 2022 and a reboot |

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

The preview rows start a small Python app in the sandbox on port 8001 (`/headers`, `/big`,
`/sse`, `/poll`, `/static.css` and a raw echo WebSocket), make its link with `bugnet-dev preview`
and reach it through the stack's edge with the `Bugnet-Token` header, as an agent's fetch does.
The stack lists its provider in `previews.providers`, so the rows can show it belongs there. A
row that needs python3 or node and finds none says so.

| Row | What it does |
|---|---|
| P1 | `python3 -m http.server 8000` lists its directory, and the app sees the preview's `Host` |
| P2 | A WebSocket on `dev-small-40m` still echoes after 30 minutes |
| P3 | SSE and a long poll of 3 minutes each, logging any cut and when |
| P4 | 50 MiB in about 10 s at the 5 MiB/s pace, failing under 8 s |
| P5 | Vite 5.4.11 with 300 modules loads, then an edit reaches the HMR socket. Skips without node |
| P8 | `revoke-preview`, `stop` and the deadline of `dev-small-1m` each close a live WebSocket within 1 s |
| G3 | Killing the gate gives the edge's 502 page, and a repair heals the preview within 90 s |
| E2B4 | E2B only: the app gets no `e2b-*`, `Bugnet-*` or `x-routing-key` header and no preview cookie |
| E2B6 | E2B only: logs 200 MiB straight through E2B's route, its own speed |
| E2B7 | E2B only: with the gate gone the visitor sees only the edge's 502 page |
| E2B8 | E2B only: 1,000 requests in a minute through one preview draw no 429 |
| BOXD3 | boxd only: the app sees the preview's `Host`, logged with `X-Forwarded-For` and the gate's sockets |
| KOYEB2 | Koyeb only: a WebSocket through the `/` route. A failure keeps Koyeb out of previews and SSH |
| KOYEB3 | Koyeb only: logs where SSE is cut, and fails when Koyeb's CDN serves a gate answer from cache |
| BOAT3 | Boat only: `_port_auth` reaches neither the app nor the visitor, and logs a WebSocket's life up to 20 minutes |
| BOAT4 | Boat only: 12,000 requests in a minute straight at the route. No 429 lets `previews.boat_requests_per_minute` drop to 0 |
| X1 | Needs `--front vps`. Curl, requests, httpx, Node, Go, git, pip and uv reach a secret host and trust the Bugnet CA. A client the sandbox lacks is logged as skipped, and fewer than four fail the row |
| X2 | Needs `--front vps` and `BUGNET_LIVE_X2_HF_URL`, `_HF_TOKEN`, `_GIT_URL` and `_GIT_TOKEN`, throwaway tokens. A gated file downloads and a private repository clones, and `env` shows only placeholders |
| X3 | Needs `--front vps`. A throwaway bound to `httpbin.org` shows in its echo, and the placeholder sent to `postman-echo.com` shows as the placeholder |
| X4 | Needs `--front vps`. After the forwarder is killed by its pid file, the secret host works again within 45 s |
| X5 | Needs `--front vps`. A tunnel from outside with no token, then with a placeholder as the token, gets 401, and the placeholder at the echo host stays inert |
| X6 | Needs `--front vps`. `curl -X TRACE` to a secret host through the tunnel answers 405 |
| KOYEB5 | Koyeb only, needs `--front vps`. The `/etc/hosts` block maps the secret hosts and a second secret rewrites it in place, the mount entry logged |
| SRC1 | Boat only, needs `--front vps`. Five sandboxes each get a stored source that holds Boat's `ip`, and their own tunnels are admitted. Logs each `ip`, `sshEndpoint` and interface addresses, and how many had an IPv4 of their own |
| SRC2 | Boat only, needs `--front vps`. A sandbox's tunnel token and placeholder, run through `bugnet-run forward` on the Mac and in a second sandbox, get 403 at the upgrade while the sandbox's own tunnels keep working |
| SRC3 | Boat only, needs `--front vps`. After `systemctl reboot` the secret host works within 70 s of the sandbox being back, and the log says whether `ip` changed |
| SRC4 | Boat only, needs `--front vps` and `BUGNET_LIVE_SOURCE_MODE=report`. SRC2's copies are served and logged as `tunnel_source_unmatched` |
| SRC5 | Boat only, needs `--front vps` and the VPS's IPv6. A second sandbox takes the first one's IPv6 address and dials with it bound. A 403, no route or no answer passes, and a served tunnel is evidence for the founder |

The SRC rows run the dev router with `secrets.source_check.modes.boat` set to
`BUGNET_LIVE_SOURCE_MODE` (`enforce` when empty), so SRC1 to SRC3 and SRC5 run with the default and
SRC4 in its own run. `BUGNET_LIVE_EGRESS_FAMILIES` (a comma list) sets `boat.egress_families` for a
run that tests one family. A row skips when the mode is not its own.

`TestLiveControl` holds CP 15's rows. N1, on every provider, checks `harden` left
`/etc/bugnet/firewall.rules` and that port 25 on a public host, `169.254.169.254:80` and
`10.0.0.1:80` stay closed while `exec`, files and the gate route work. N2, on E2B only, removes
the rules inside the VM and checks `network.denyOut` still closes `10.0.0.1:80`, logging whether
port 25 opens and the VM's IPv6 default route. `169.254.169.254` on E2B is Firecracker's MMDS,
answered inside the VM before `denyOut` sees it, so only `harden` holds it and N2 logs it when
it opens. N1 also probes `[fd00:ec2::254]:80`.

| | `--front local`, the default | `--front vps`, behind the dev front |
|---|---|---|
| Ports | Free loopback ports | Edge 8443, secrets route 8445, sessions route 8448, egress 9443, SSH 2223, from `config/dev-front.toml` |
| Names | The dev names, reached with `curl --resolve` through a loopback front that adds HAProxy's PROXY v2 header | The dev names through the VPS |
| TLS | ACME off, a fresh self-signed pair | Let's Encrypt for the exact names over the tunnel, the wildcard `make dev-certs` copied |
| `secrets.egress_ips` | `127.0.0.1` | `vps.egress_ips` in `config/dev-front.toml` |
| `bugnet-egress` | A free loopback port, which `fetch.egress_addr` follows, with the self-signed pair, which `fetch.egress_roots_file` trusts | Port 9443, Let's Encrypt over the tunnel |
| `secrets.ca_cert_file` | A fresh CA for the stack | The dev CA in `egress.ca_dir` of `config/dev-front.toml` when it exists, which E2B needs |

The fronted mode keeps its ACME caches in `~/.local/state/bugnet-dev/acme/<service>/`, so
repeated runs reuse their certificates and stay inside Let's Encrypt's limits. It refuses to start
when one of its five ports is taken, when the wildcard pair in
`~/.local/state/bugnet-dev/preview-dev/` is missing or expired, or when
`https://transfer.dev.bugnet.sh/` does not answer with the certificate this stack's edge holds,
and it says which. The live stack needs the Linux build of `bugnet-run` from
`make -C ../bugnet-run build`.

`bugnet-dev hooks register e2b|boat` registers this deployment's endpoint
`https://<names.hooks_host>/hooks/<provider>/<path secret>` and prints the signing secret once,
with `team_id` for E2B. `hooks list` hides the path secret, `hooks rotate` prints a new secret
(move the old one to `..._PREVIOUS` for a day) and `hooks delete PROVIDER ID` removes an endpoint.
A verified webhook queues a `hook_hint` job per provider and sandbox.

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
printf %s "$HF_TOKEN" | bin/bugnet-dev secret --handle sbx_... --name HF_TOKEN --host huggingface.co
bin/bugnet-dev ssh-key --handle sbx_... --public-key ~/.ssh/id_ed25519.pub
bin/bugnet-dev ssh-key --handle sbx_... --remove SHA256:...
bin/bugnet-dev x402 check --handle sbx_... \
    --service "GET,api.example.com,/search/,10000,$PAY_TO,/search"
bin/bugnet-dev x402 enable --handle sbx_... --wait \
    --service "GET,api.example.com,/search/,10000,$PAY_TO,/search"
bin/bugnet-dev x402 list --handle sbx_...
bin/bugnet-dev session rotate --handle sbx_...
BUGNET_OPERATOR_TOKEN=... bin/bugnet-dev session hold --placement <placement id>
bin/bugnet-dev fetch-source --handle sbx_...
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
preview for good. `secret` reads a value from stdin, seals it locally with `crypto_box_seal` to the
newest key `GetSecretSealingKey` lists, bound to the placement, name, hosts and header names as DP
10.2 says, and puts it with `PutSecret`. It prints the secret's state, never the value. It trusts
the router's key list, as a dev tool may, where the page and the bridge pin their keys. `ssh-key`
registers the public key in `--public-key FILE` on a placement and prints its alias, the gateway's
host and port, the key's fingerprint, the `known_hosts` lines, the `ssh` command, the paid deadline
and an `ssh_config` stanza, one `config=` line each. It refuses a file holding a private key before
calling the router. `--identity-file` and `--known-hosts-file` name the paths the command and stanza
use. `get` and `create` print the placement's `ssh_alias` once it has one. `token` and `create` take
`--payer`, `--fee-payer`, `--tx` and `--allowance-micros`, which set those claims. Without
`--amount` the allowance is added to the catalog price, so the package stays paid. `config/dev.toml`
turns the payer claim on with one test facilitator address, so `create --payer` takes the wallet
path, and raises the anonymous cap, since most dev tokens carry no payer. `wallet block`, `unblock`,
`show` and `list` call `OperatorService` with `BUGNET_OPERATOR_TOKEN`, the actor from `--actor` or
else `USER`. `--until` takes an RFC 3339 time or a duration from now, and a block without it lasts
until an operator lifts it.
`x402 check` runs `CheckX402` on a change without making it. Each `--service` is
`method,host,prefix,max_price_micros,pay_to[,paid_path]`, and a service without a paid path is
indexed. `--disable ID` names a service id to disable. Without `--handle` it runs only the checks
that need no placement. It prints `passed=true`, or exits 1 with the code, the reason and the
refused entry's `service_id` and `host`. `x402 enable` calls `EnableX402` with the same flags, adds `--wait` to wait up to
`secrets.apply_wait` for the added services, and `--request-id R` (a fresh id when empty), then
prints one line for every service of the placement, disabled ones too. `x402 list` prints
`enabled`, then one line per service with its state, pins and counts, then one line per refusal
reason with its count and last time.
`x402 seller-check --names A,B` checks the mainnet test seller's two public names over public DNS
and pays nothing (`deploy/testseller/README.md`). It prints a line per check and exits 1 at the
first failure, naming it.
`get` prints the placement's `session_url` while its record lives. `session rotate` ends that link and
prints the new one. `session hold` prints a held record as `OperatorService` serves it, with
`BUGNET_OPERATOR_TOKEN`.
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
| `BUGNET_DATABASE_URL` | `router-api`, `router-worker`, `router-api migrate` and `router-worker outbox-redrive`, may hold a password, and sets `pool_max_conns=48` for `router-worker` |
| `BUGNET_INTERNAL_TOKEN` | `router-api`, `router-worker` and `bugnet-dev`, the API's bearer token, 32 characters or more |
| `BUGNET_HANDLE_KEY` | `router-api` and `router-worker`, base64 of 32 bytes or more that derives handles |
| `BUGNET_DATA_PLANE_KEYS` | `router-api` and `router-worker`, required, root keys as `<id>:<base64 of 32 bytes>` entries separated by commas, newest first |
| `BUGNET_EVENT_SIGNING_KEY` | `router-worker`, which requires it, and `router-api` when set, base64 Ed25519 seed that signs events |
| `BOXD_API_KEY`, `E2B_API_KEY`, `KOYEB_API_TOKEN`, `BOAT_API_KEY` | the binaries for each provider in `providers`, and the live tests |
| `BUGNET_ORDER_PRIVATE_KEY` | `bugnet-dev` only, base64 Ed25519 seed it signs order tokens with |
| `BUGNET_EDGE_TOKEN`, `BUGNET_SECRETS_TOKEN`, `BUGNET_SSH_TOKEN`, `BUGNET_EGRESS_TOKEN` | `router-api` and `router-worker`, one bearer token per edge process, each empty or 32 characters or more |
| `BUGNET_EGRESS_FETCH_TOKEN` | `router-api` and `router-worker`, the `bnf_` token the router's private fetch opens egress tunnels with, empty or 32 characters or more |
| `BUGNET_OPERATOR_TOKEN` | `router-api`, and `bugnet-dev`'s operator commands, the bearer token of `OperatorService`, empty or 32 characters or more |
| `BUGNET_PAYMENTS_ROUTER_TOKEN` | `router-api`, and Payments in its own environment with the same value, the bearer token of `PaymentsService`, empty or 32 characters or more. `router-worker` sends it to Payments' totals call while the stats feed is on |
| `BUGNET_SESSIONS_TOKEN` | `router-api`, and `bugnet-edge --role sessions`, the bearer token of `SessionPageService`, empty or 32 characters or more |
| `BUGNET_HOOK_PATH_SECRET` | The edge, and `bugnet-dev hooks`, base64url of 32 bytes in the provider webhook path |
| `E2B_WEBHOOK_SECRET`, `BOAT_WEBHOOK_SECRET` | `router-api`, the signing secret `bugnet-dev hooks register` prints once, needed for each provider in `hooks.providers`. The same name with `_PREVIOUS` holds the replaced secret for a day after `hooks rotate` |
| `BOAT_ADMIN_API_KEY` | `bugnet-dev hooks` for Boat only, typed at its prompt with echo off when unset, never in a service |

An empty caller token admits no calls, so a router without edges serves only the gateway. No two
of `BUGNET_INTERNAL_TOKEN` and the caller tokens may be equal, since each admits one caller to
its own service. `bugnet-dev keys` prints fresh values for all of them, and `dev-up.sh` adds the
ones an older `.env.keys` lacks. `bugnet-dev keys --production` leaves out
`BUGNET_ORDER_PRIVATE_KEY`, which Payments holds in production, and `bugnet-dev pubkey --event`
prints the public half of `BUGNET_EVENT_SIGNING_KEY` from the environment for Payments.

`BUGNET_DATA_PLANE_KEYS` derives the data plane's preview tokens, gate keys, SSH grant ids,
transfer and link tokens, put grants, pairing codes and placeholders, so the router stores none
of them. A root id is 1 to 32 lowercase letters, digits or hyphens, and `bugnet-dev keys` prints
one root named `k1`. Each placement records the id of the root its gate key and placeholders come
from. To rotate, put a new root first in the list. New placements use it, and preview tokens, put
grants and SSH grant ids made under any listed root still verify. Restart `router-api` and
`router-worker` together with the same list, since a process without the new root cannot open or
check what the other made under it. Remove an old root only 48 hours after the last placement that
recorded it ended, since its session page link derives from that root until the record expires.

The store seals the provider credentials in `provider_ref` (E2B's `envd_token` and
`traffic_token`, Koyeb's `secret`, Boat's `_token`) with AES-256-GCM under a key derived from the
newest root, as `v1.<root id>.<base64url>`, so a copy of the database without these keys reveals
none of them. Every other field, `id` included, stays plaintext. A field written before sealing
reads as plaintext. A sealed field no listed root opens reads as absent, and the store logs a
warning that names the placement and the field. A destroy needs only the sandbox id, so a stop or
a deadline still ends the placement and frees its slot. Only a call that uses the credential
fails.

`router-api migrate` and `router-worker outbox-redrive` read only `BUGNET_DATABASE_URL` and need
no settings file. `migrate` drops `pool_max_conns` and the other pool settings from the URL, so
the production URL serves both.

Each River worker of `router-worker` holds a Postgres connection while it runs a job, so the pool
needs more connections than its workers together: `worker.default_workers` (20) and
`worker.dataplane_workers` (8) and `health.probe_workers` (4), 32 in all. Put `pool_max_conns=48` in `BUGNET_DATABASE_URL`, for example
`postgres://host:5432/bugnet_router?sslmode=require&pool_max_conns=48`. pgx's own default is 4
or the number of CPUs, whichever is larger. At start `router-worker` logs a warning naming
`pool_max_conns` when the pool's size is not above its workers.

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
| `[secrets.source_check]` | The source check's mode per provider (`off`, `report` or `enforce`), `on_unknown` (`allow` or `refuse`), the read interval and its timeout |
| `[fetch]` | The private repository fetch's byte cap, the egress proxy's listener and the PEM roots its outer certificate is checked against, the system roots when empty |
| `[bootstrap]` | Where `FetchSource` puts repositories under the exec user's home, how long a silent private fetch lives, and its attempts per placement and fetches per replica |
| `[hooks]` | `providers` whose webhooks are verified (`e2b`, `boat`), `boat_window` for the timestamp check and `hint_unique` for folding hints of one sandbox. `router-api` refuses to start when a listed provider has no secret or no verifier |
| `[hardening]` | The mail ports and the private, link-local and metadata ranges `bugnet-run harden` blocks outbound, and the mining pool list |
| `[limits_poll]` | How often each provider's limits and account standing are read, the slots placements leave for probes, and the missed polls before `/monitor` reports old limits |
| `[bench]` | The estimate over which `bench start` needs `--yes`, and the most runs one benchmark may ask for |
| `[abuse]` | How provider signals are classed: the class of E2B's `admin` kill and of a Boat sandbox degraded past `degraded_grace`, the word lists matched in Boat's stop text and Koyeb's instance events, the hourly `provider_failed` alert, the correlation guard (`correlated_max` ends in `correlated_window`) and the `late_window` in which a provider's abuse signal still blocks the payers of an ended placement |
| `[reconcile]` | `ready_grace` (a listing may lag a fresh create), `max_per_sweep` (reconcile jobs one sweep inserts per provider) and `lifetime_drift` |
| `[health]` | How often each provider gets a canary, the probe workers, the size a canary takes, the window and bounds that mark a provider `down` or `slow`, and when `/monitor` reports a provider down |

`boxd.gate_route` (`default` when unset, or `subdomain`), `boat.ssh_path` (`gate` or `native`)
and `local.sshd_path` choose each provider's route. `boat.egress_families` (`ipv6`, `ipv4`) and
`boat.egress_ipv6_bits` (64 to 128) shape the egress source Boat reports. Both routers refuse to
start when `secrets.source_check.modes` names a provider that is not listed or reports no
address, or when `boat.egress_families` is empty while Boat's mode is not `off`. A `bn` subdomain proxy made after boot can
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
data-plane jobs away from deadlines, the sweep and the outbox. `worker.default_workers` (20) sets
the default queue's, which runs deadlines, the sweep, the watch, the outbox and the wallet jobs.
Each takes 1 to 10000, the most River runs on one queue.

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

A package with `gpu_count` above 0 names its GPU in `resources.gpu_model`, as the public stats
feed and the landing's pricing show it, such as `"NVIDIA L4"`, and `gpu_memory_gib` is each GPU's
own memory. The name is 1 to 64 printable characters with no space at either end. A package
without a GPU has no `gpu_model`, and an extension carries the same one as the package it extends.

A host name here and in `[names]` is never an IP address, and never ends in a label that is all
digits or `0x` and hex digits, such as `127.1` or `a.0x1f`. `inet_aton`, curl and browsers read
those short forms as IPv4 addresses.

Money is integer micro-USD and times are UTC.

### Session page settings

`[sessions]` and `[sessions.live]` size the session page (SP 6 and 7).

| Key | Default | Takes |
| --- | --- | --- |
| `base_url` | `""` | The page's origin, `https://sessions.<domain>`, or `http` to a loopback host. No link while empty |
| `keep` | `"48h"` | How long a page lives after its sandbox ends, at least 1h |
| `abuse_hold` | `"720h"` | How long an abuse or operator end keeps the record for operators |
| `max_events` | `5000` | Events per record, 1 to 100000 |
| `max_jobs` | `1000` | Jobs per record, 1 to 10000, later ones given no row and counted in `jobsDropped` |
| `job_output` | `"64KiB"` | The newest output kept of each output of a job |
| `placement_output` | `"4MiB"` | The output kept per record, earlier jobs keeping 8 KiB per output past it, up to 1 MiB of such tails |
| `disk_budget` | `"8GiB"` | The three session tables together, `/monitor` warning at 80% |
| `mcp_tools` | 24 names | The gateway's tool names an `mcp` event may carry |
| `live.enabled` | `true` | The live view's kill switch, read at start, so `false` takes a restart, whose shutdown ends every open stream |
| `live.max_readers` | `256` | Live streams one `router-api` serves at once, 1 to 4096 |
| `live.placement_bytes` | `"256MiB"` | Live bytes per placement |
| `live.bytes_per_second` | `"1MiB"` | The pace of each live stream, an idle one saving up at most one second of it |
| `live.replay` | `"256KiB"` | What a first connection replays of a job |

`config/dev.toml` sets `base_url` to `https://sessions.dev.bugnet.sh`.

### x402

`[x402]` holds the settings of the x402 host kind, through which code in a sandbox pays x402 APIs
from an allowance Payments holds. `CheckX402` and `ListX402` read them today, and the other
features that read them land one by one.

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
| `abuse_block_for` | `""` | How long an abuse end blocks its payers, empty until an operator lifts it |
| `stop_live_on_abuse` | `true` | An abuse end also ends every other live placement of each payer it blocks |
| `refund_stopped_live` | `false` | Those `wallet_blocked` ends refund their unused time |
| `failed_refunds_per_day` | `2` | Refundable `provider_failed` ends per start payer in 24 h |
| `anonymous_failed_refunds_per_day` | `20` | The same for every placement without a payer, together |

`[boat]`, `[e2b]`, `[boxd]` and `[koyeb]` each take `ops_per_second`, the user operations a
second on that provider account, 0 for no bucket. Boat's is 50, the others 0.

`router-api` refuses to start with `use_payer = true` while `BUGNET_PAYMENTS_ROUTER_TOKEN` is
empty or `emit_events` is false, since Payments could then neither ask `CheckPayer` nor hear of a
block. With `use_payer = false` it logs once at start that every order counts as having no payer.

### Stats

`[stats]` holds the public stats feed (see [Public stats feed](#public-stats-feed)). It is off
while `listen_addr` is empty, the default and `config/dev.toml`'s value. Production sets
`"127.0.0.1:8082"`.

| Key | Default | Takes |
| --- | --- | --- |
| `listen_addr` | `""` | A loopback IP address and port for router-api's feed listener, or empty |
| `payments_url` | `"http://127.0.0.1:8447"` | Payments' internal API, `http://` to a loopback address and port while the feed is on |
| `payments_timeout` | `"5s"` | 1ms to 15s for Payments' totals call |
| `row_cache` | `"5s"` | 1ms to 1m, how long router-api serves the snapshot row before reading it again |
| `sessions_cache` | `256` | 1 to 100000 cached session pages per router-api |
| `max_queries` | `4` | 1 to 64 session page queries at once per router-api |
| `per_source_per_minute`, `per_source_burst` | `60`, `20` | 1 to 1000000, a bucket per client address |
| `max_sources` | `10000` | 1 to 1000000 per-source buckets per router-api |

`CheckX402` checks a change of a placement's x402 services without making it, in this order: the
handle, `enabled`, the state (placing or ready), the capability, each `add` entry in request
order, the `disable` ids, then the counts. While placing, the capability is the order's
`requirements.secrets`; from ready, it is the provider's place in `secrets.providers`. An entry's
own checks are the service rules of `internal/x402`, a repeated service id or one also in
`disable`, a host a secret already reaches and a host outside the package's `egress.allow`. A
change that brings in a service or a host may not end over `max_services` live services or 32
hosts with the secret hosts, where `applying`, `active` and `failed` rows are live. A change that
only keeps, changes or disables services passes the counts, so a placement over a lowered
`max_services` can still shrink. Without a handle it runs `enabled`, each entry's service rules,
a repeated id and more than `max_services` entries, and any `disable` id is unknown. With a
handle and no change it runs the checks up to the capability.

`ListX402` answers in any state, with `enabled`, every row with its counts and the refusal counts
by reason with their last time. A row that is not `disabled` whose host is in `blocked_hosts`, or
under a shared-hosting suffix and not in `allowed_shared_hosts`, shows as `FAILED` with
`HOST_BLOCKED`, and the stored row stays as it was. Neither call spends an operation token.

Egress reports each placement's unpaid use of its x402 hosts in `ReportEgressUsage`'s
`x402_unpaid`: the requests Bugnet did not pay and their bytes since its last report, by host.
That field may come alone, with an empty `usage`, and it never changes the bytes left that the
call answers. `router-api` adds the deltas to one Valkey hash per placement under
`valkey.prefix`.

| Key | Type | Expires | Holds |
| --- | --- | --- | --- |
| `egress:x402:<placement>` | Hash | Deadline + 1 h | `<host>:req` and `<host>:bytes`, the unpaid totals of each host |

- One script adds a placement's deltas, so reports from two replicas never lose a count.
- The hash holds at most 64 fields, two for each of 32 hosts. A report drops a new field past
  that, and `router-api` logs the placement and the count of fields dropped, never a host.
- Hosts go in sorted order, so a full hash drops the same hosts every time.
- One report adds at most 2^40 to a field. A placement that is not ready, an id that is unknown
  or malformed, a host that is not a valid lowercase name and a host with no use are skipped.
- The expiry only moves later, and an extension moves it with the used counters.

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

## Estimator

`Estimate` also reads what the agent says it will run: `source`, `workload`, `entry_command`,
`expected_minutes` and `needs`. It answers the floor (vCPU, memory, disk, GPU, minutes and bytes
to download), the start packages that cover it with their extensions, one recommended package, the
reasons and the warnings, each built from typed values and never from repository text. `needs`
merge into `requirements`. Free quotes are bounded three ways by Valkey buckets, `rl:est:all` for
calls, `rl:est:new` for sources nobody cached and `rl:est:stored` for new estimate rows, and a
replica uses an in-process bucket at the same rate when Valkey takes over 100 ms.

A GitHub source is read at one commit. With `BUGNET_GITHUB_TOKEN` it takes one GraphQL query whose
text is a constant, every agent value travelling as a variable, and one tree call. Without a
token it takes the tree call and at most `estimator.max_files` files from the raw host. At start
the router reads the token's limits, and a token that can list a private repository is never used.
A private repository reads as not found. A Hugging Face source takes the Hub's model call, a
second for GGUF files and `config.json` unless the model is gated. All of a quote's fetches share
`estimator.fetch_budget`. A fetch past it goes on for `estimator.background_budget` to fill the
cache, and the quote answers from defaults with `partial` set and a `SOURCE_UNREAD` warning.

Each distinct answer is stored with its inputs, the facts they name and the catalog, and a repeated
quote with the same inputs and availability returns the same `estimate_id`. `CreatePlacement`
takes that id when the estimate was served within `estimator.max_estimate_age` and copies the
source onto the placement as `source_kind`, `source_canonical`, `source_ref`, `source_revision`
and `source_read`. A GitHub or Hugging Face source that was read also sets `source_label`, as
`github.com/<owner>/<repo>@<7 hex>` or the model id. An unknown, stale or malformed id is ignored.

`bugnet-dev estimate --replay ID` fetches the stored estimate through `OperatorService`'s
`GetEstimate`, runs the rules locally on its inputs and catalog bytes, and compares the floor and
the recommendation byte for byte. An estimate made under another `rules.Version` is replayed from
that version's git tag.

### FetchSource

`FetchSource(handle)` puts a placement's GitHub source in its sandbox at `~/<bootstrap.dir>/<repo>`.
A source the estimator read, or failed to read for a reason other than not found, is cloned by a
job under the key `_bootstrap:<attempt>`: `git fetch --depth 1` of the commit, then a full fetch.
A failed clone runs again under the next key. A private source (not found) needs an active secret
whose hosts include `api.github.com`, else the call answers `SECRET_REQUIRED` and the placement's
`bootstrap.state` reads `NEEDS_SECRET`. The router then fetches the tarball itself through the
egress proxy, which swaps the secret's placeholder, follows one redirect to `codeload.github.com`
through `internal/netguard`, spools it, counts every byte in the placement's egress budget, writes
it to `~/.bugnet/src.tar.gz` and starts a job that unpacks and deletes it. At most
`bootstrap.max_private_attempts` private fetches run per placement and `bootstrap.max_concurrent`
per replica. A fetch silent for `bootstrap.heartbeat_stale` reads as `INTERRUPTED`. Failures reach
the gateway as `Bootstrap.state` `FAILED` with a `failure_reason`, never as an error. Idempotency
keys that start with `_` are the router's own, and `Exec` refuses them with
`IDEMPOTENCY_KEY_RESERVED`. `bugnet-dev fetch-source --handle H` calls it.

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
`CreateTransfer`, `CreatePreview`, `AddSshKey`, `OpenTransfer`, `CreateSecretLink`,
`CreateSecretPutGrant`, `PutSecret` and `PutBridgeSecret` calls spend one token from a bucket that
every `router-api` replica shares, `ops:<placement id>` under `valkey.prefix`. It holds
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
the allowance. Each order keeps its checked `tx` in `orders.tx`, which the public stats feed shows
beside the start order's payer.

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
changes nothing and writes no event, and logs only at debug level. `stop_live` also inserts a
`wallet_stop_live` job in the block's transaction. `ListWallets` takes up to 1000 a page, 100 when
`limit` is 0.

### Ending a placement for a reason

`Saga.End` ends a ready placement for one of the reasons below, and `Saga.FlagEnded` records
abuse on one that already ended. Reconciliation and the operator's end calls use them.

| Reason | `EndReason` to the edges | Refundable | Wallet action |
| --- | --- | --- | --- |
| `abuse` | `ABUSE` | No | Blocks every payer, start and extension, for `abuse_block_for` |
| `control_lost` | `CONTROL_LOST` | No | One strike on every payer |
| `provider_failed` | `PROVIDER_FAILED` | Under the daily cap | A strike on every payer past the cap |
| `sandbox_exited` | `SANDBOX_EXITED` | No | None |
| `wallet_blocked` | `ABUSE` | `refund_stopped_live` | None |
| `operator` | `OPERATOR` | The operator's choice | None |

An end takes the wallet lock of every payer of the placement, then the placement's row, the order
`ExtendPlacement` takes them in. A payer that extends the placement before the row is locked
starts the end again, so its block is never missed. A terminating placement keeps its reason,
except that `abuse` replaces a stop in flight. A held end (the correlation guard) takes no wallet
action, records `correlated: true` in its evidence and never counts against the daily cap.
Every end for `abuse`, `control_lost`, `provider_failed` or `sandbox_exited` writes one
`provider_signals` row, its `source` taken from the evidence's `source` field and its
`held_class` from `held_class`. With `stop_live_on_abuse`, each payer an abuse end blocks gets a
`wallet_stop_live` job in the same transaction.

`FlagEnded` takes `abuse` only and a terminated or failed placement. It changes neither the
reason nor the money, blocks every payer not already blocked for abuse, writes a `note` event for
those that are, and answers the payers it blocked.

`wallet_stop_live{payer}` (150 s) ends the payer's placements that are ready, as start or
extension payer, with reason `wallet_blocked`, at most 5 a run, then inserts itself again. While
one is still placing it looks again 15 s later, up to 20 times. `wallet_expiry` runs every 10
minutes (30 s) and lifts blocks whose `blocked_until` has passed, each with an `unblock` event of
reason `expired` and actor `router`, and `wallet.unblocked` when events are on.

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
| `NotFound` | `ESTIMATE_UNKNOWN` | `GetEstimate` names an estimate that is not stored |
| `FailedPrecondition` | `NO_SOURCE` | `FetchSource` on a placement with no GitHub source |
| `FailedPrecondition` | `TOOL_MISSING` | `FetchSource` where the provider's image manifest has no `git`, or no `tar` for a private source |
| `FailedPrecondition` | `SECRET_REQUIRED` | `FetchSource` of a private source with no active GitHub secret |
| `InvalidArgument` | `IDEMPOTENCY_KEY_RESERVED` | `Exec` with an idempotency key that starts with `_` |
| `NotFound` | `PREVIEW_UNKNOWN` | A preview id the router never made, or one that is malformed |
| `PermissionDenied` | `TOKEN_INVALID` | A preview token that belongs to no generation of the preview |
| `InvalidArgument` | `SOURCE_INVALID` | An `Estimate` source that parses as nothing, or a URL with userinfo |
| `InvalidArgument` | `SECRET_IN_ARGUMENT` | An `Estimate` source or command holding a value of a secret shape |
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
| `InvalidArgument` | `KEY_INVALID` | `AddSshKey` gets a certificate, DSA, RSA under 3072 bits or a line with options, or `OpenSshUpstream` a gateway key that is not plain ed25519 |
| `InvalidArgument` | `PATH_REFUSED` | An `AddSshKey` `identity_file` or `known_hosts_file` outside `^(~\|\$TMPDIR)?[A-Za-z0-9._/-]{1,200}$`, or starting with `~` but not `~/` |
| `FailedPrecondition` | `FEATURE_UNAVAILABLE` | `AddSshKey` on a provider not in `ssh.providers`, or on a router without `names.ssh_host`, `ssh.host_keys` or data-plane keys |
| `ResourceExhausted` | `TOO_MANY_KEYS` | A key past `ssh.max_keys_per_placement` live ones |
| `NotFound` | `GRANT_UNKNOWN` | `AuthorizeSshKey` finds no live key on a ready placement for the alias, or a grant's key, placement or deadline no longer stands |
| `PermissionDenied` | `GRANT_INVALID` | A grant id the router's roots never made |
| `ResourceExhausted` | `TOO_MANY_CONNECTIONS` | A connection lease past `ssh.max_connections_per_placement` live ones |
| `Unavailable` | `SSH_PREPARING` | `OpenSshUpstream` waited `ssh.prepare_timeout` for the sandbox's sshd, or `ssh-up` failed |
| `InvalidArgument` | `PACKAGE_UNKNOWN` | `PricePackage` names a package the catalog lacks |
| `InvalidArgument` | `NOT_START_PACKAGE` | `PricePackage` names a package with `extension_of` set |
| `FailedPrecondition` | `NOT_READY` | The placement is placing, ending or ended |
| `FailedPrecondition` | `EXCEEDS_LIFETIME` | The extension would outlast the provider's limit |
| `ResourceExhausted` | `RATE_LIMITED` | The placement's operation bucket is empty, or its link or put bucket, or one of `Estimate`'s three buckets |
| `InvalidArgument` | `NAME_RESERVED` | A secret name that is malformed or that the sandbox's tools read, such as `PATH` or `LD_*` |
| `InvalidArgument` | `HOST_REFUSED` | A secret host that is not one exact lowercase name, or is a public suffix, a shared-hosting name, a provider's or Bugnet's own, or repeats |
| `InvalidArgument` | `HEADER_REFUSED` | A header name not lowercase, repeated, or one the proxy or a browser sets, such as `cookie` |
| `InvalidArgument` | `SEALED_TOO_LARGE` | A sealed value that is empty or over 70 KiB |
| `InvalidArgument` | `KEY_UNKNOWN` | A value sealed to a key not in `secrets.box_public_keys` |
| `FailedPrecondition` | `HOST_NOT_ALLOWED` | A custom host while `secrets.custom_hosts` is false, or a host outside the package's `egress.allow` |
| `FailedPrecondition` | `HOST_IN_USE` | A secret host that a live x402 service of the placement uses |
| `FailedPrecondition` | `LINK_LOCKED` | A secret link refused 5 values |
| `NotFound` | `LINK_UNKNOWN` | A secret link that is unknown, used or expired, or whose placement left ready |
| `PermissionDenied` | `PUT_GRANT_INVALID` | A put grant that is expired, forged or names another placement |
| `NotFound` | `SESSION_UNKNOWN` | A session page call with an unknown id or a wrong key alike, a live view of a placement not running, or a hold no record has |
| `FailedPrecondition` | `FEATURE_UNAVAILABLE` | `WatchSessionJob` while `sessions.live.enabled` is `false` |
| `ResourceExhausted` | `TOO_MANY_SECRETS` | A 21st secret, a 33rd host in all with the live x402 hosts, an 11th host or a 9th header name of one secret |
| `ResourceExhausted` | `RATE_LIMITED` | The placement's, the payer's or the provider account's operation bucket is empty |
| `InvalidArgument` | `PAYER_INVALID` | `CheckPayer` or an operator call names an address that is not canonical base58 of 32 bytes |
| `PermissionDenied` | `PAYER_REQUIRED` | A start order without a payer while `wallets.require_payer` is set |
| `PermissionDenied` | `WALLET_BLOCKED` | An order of a blocked payer |
| `ResourceExhausted` | `WALLET_BUSY` | A start at the wallet's cap on live placements, or the anonymous cap |
| `ResourceExhausted` | `WALLET_RATE_LIMITED` | A start while one of the payer's start buckets is empty, with `RetryInfo` |
| `FailedPrecondition` | `FACILITATOR_ADDRESS` | A block of a facilitator address |

| `ResourceExhausted` | `TOO_MANY_SECRETS` | A 21st secret, a 33rd host in all, an 11th host or a 9th header name of one secret, or an x402 change past 32 hosts with the secret hosts |
| `InvalidArgument` | `PREFIX_REFUSED` | An x402 prefix not `^/.+/$`, over 200 characters, holding `?` or `#` or failing the path rules, or a paid path missing, unneeded, outside its prefix or failing the same rules |
| `InvalidArgument` | `SERVICE_DUPLICATE` | An x402 change naming one service id twice in `add`, or in both `add` and `disable` |
| `InvalidArgument` | `METHOD_REFUSED` | An x402 service method other than `GET` or `POST` |
| `InvalidArgument` | `PRICE_REFUSED` | An x402 price cap at or under 0, or over `x402.max_price_micros` |
| `InvalidArgument` | `PAYTO_REFUSED` | An x402 `pay_to` that is not canonical base58 of 32 bytes |
| `FailedPrecondition` | `X402_DISABLED` | `CheckX402` while `x402.enabled` is false |
| `FailedPrecondition` | `HOST_IN_USE` | An x402 host that is already a secret's host |
| `ResourceExhausted` | `TOO_MANY_SERVICES` | An x402 change past `x402.max_services` live services |
| `NotFound` | `SERVICE_UNKNOWN` | An x402 `disable` id the placement lacks, or any id without a handle |
| `NotFound` | `ORDER_UNKNOWN` | A `LookupPlacement` start order that names no placement, an extend order's id included, or an `AppendAllowanceEvent` order that is no placement's start order |

`HOST_REFUSED` and `HOST_NOT_ALLOWED` also answer an x402 host, by the same host rules with
`allowed_shared_hosts` and `blocked_hosts` added. `FEATURE_UNAVAILABLE` also answers `CheckX402`
on a placing placement whose order did not ask for secrets, or a ready one on a provider not in
`secrets.providers`. The per-service x402 refusals, `HOST_REFUSED`, `HOST_NOT_ALLOWED`,
`PREFIX_REFUSED`, `SERVICE_DUPLICATE`, `METHOD_REFUSED`, `PRICE_REFUSED`, `PAYTO_REFUSED` and
`HOST_IN_USE`, carry ErrorInfo metadata `service_id` and `host` of the first failing `add` entry.
`SERVICE_UNKNOWN` carries only `service_id`, the first unknown id. Every other refusal carries
no metadata.

A provider failure carries the provider's code in lowercase instead, such as `rate_limited`. A
call that its caller cancels or lets run out of time answers `Canceled` or `DeadlineExceeded`,
with only the context error as its message.

An `Exec` whose `idempotency_key` another call holds waits for that call to record the job. When
it records none within the wait, `Exec` answers `Unavailable` with no reason, and a retry with the
same key runs the command once.

## Caller services

`router-api` serves eight Connect services on its one listener. Each admits only its own bearer
token, and a service whose token is empty admits no calls, so a router without edges serves only
the gateway.

| Service | Caller | Token | Built so far |
| --- | --- | --- | --- |
| `SandboxRouterService` | The MCP gateway, `bugnet-dev` | `BUGNET_INTERNAL_TOKEN` | Placements, commands, files, `CreateTransfer`, `CreatePreview`, the secret RPCs, `AddSshKey`, `RemoveSshKey`, `AppendSessionEvents` and `RotateSessionLink` |
| `EdgeService` | `bugnet-edge` | `BUGNET_EDGE_TOKEN` | `ReportEdgeUsage`, `CheckEdgeLeases`, `WatchEdgeEvents`, `OpenTransfer`, `ReadTransfer`, `WriteTransfer`, `ResolvePreview`, `RepairPreview` |
| `SecretsRouteService` | `bugnet-secrets` | `BUGNET_SECRETS_TOKEN` | `GetSecretLink`, `SubmitSecretLink`, `GetBridgeSealingKey`, `PutBridgeSecret` |
| `SshGatewayService` | `bugnet-ssh` | `BUGNET_SSH_TOKEN` | Every RPC |
| `EgressService` | `bugnet-egress` | `BUGNET_EGRESS_TOKEN` | `ResolveTunnel`, `ReportEgressUsage`, `CheckEgressLeases`, `WatchEgressEvents` |
| `OperatorService` | `bugnet-dev`'s operator commands | `BUGNET_OPERATOR_TOKEN` | `RevokePreview`, `BlockWallet`, `UnblockWallet`, `GetWallet`, `ListWallets`, `GetEstimate`, `GetSessionHold` |
| `PaymentsService` | Payments | `BUGNET_PAYMENTS_ROUTER_TOKEN` | `GetOrder`, `CheckPayer`, `LookupPlacement`, `AppendAllowanceEvent` |
| `SessionPageService` | `bugnet-edge --role sessions` | `BUGNET_SESSIONS_TOKEN` | `GetSessionRecord`, `DeleteSessionRecord`, `WatchSessionJob` |

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
last report that carried bytes. An extension moves the used counters and the unpaid x402 use
(`egress:x402:<placement>`, under x402) to an hour after the new deadline, even for a placement
that reports nothing, and no report moves an expiry earlier.

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
| `SshKeyRemoved` | `RemoveSshKey` marks a live key removed |

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

### Sealed secrets

The router never sees a secret's value. The page and the bridge seal it to the egress proxy's
X25519 key, and the router stores the box and the SHA-256 of its placeholder (DP 10.2 to 10.4).

- **Checks.** Every write needs a ready placement (`NOT_READY`) on a provider in
  `secrets.providers`, and a router with data-plane keys, `secrets.box_public_keys`,
  `secrets.egress_ips` and `names.egress_host` (`FEATURE_UNAVAILABLE`). Links also need
  `names.secrets_host`. The spec's rules are in `internal/secrets`, and the host rules are the
  same set x402 hosts use, from `internal/hostrules`, without `allowed_shared_hosts` and
  `blocked_hosts`.
- **Links.** `CreateSecretLink` derives its `bnsl_` token from the request id, so a repeat answers
  the same link, and a used, expired or locked one answers `TOKEN_USED`, `TOKEN_EXPIRED` or
  `LINK_LOCKED`. It keeps a suggested host only when it is in the preset of the suggested name.
  Two calls with one new request id store and spend for one link. A link lives
  `secrets.link_ttl`, at most `secrets.links_per_hour` an hour per placement (`rl:links:<id>`).
  `GetSecretLink` never uses it, `SubmitSecretLink` uses it in the write's transaction, a repeat
  of the same `submission_id` answers again, and 5 refused values lock it, a lock that the
  write's transaction checks again.
- **Puts.** `PutSecret` (with the handle) and `PutBridgeSecret` (with a `bnsp_` grant from
  `CreateSecretPutGrant`, valid `secrets.put_grant_ttl`) share one implementation, keyed by the
  idempotency key, at most `secrets.puts_per_hour` an hour per placement together
  (`rl:puts:<id>`). A `fetch_only` secret is `ACTIVE` at once.
- **The row lock.** Each write takes the placement's row lock in the one transaction that checks
  its counts before it spends any limit, stores its row and inserts its `apply_secrets` job, and
  commits before it waits up to `secrets.apply_wait` for the secret to leave `APPLYING`.
  `DeleteSecret` removes the row, moves `secrets_version` and inserts the job, and a name the
  placement lacks changes nothing.
- **`apply_secrets`.** A River job on the `dataplane` queue, 90 s, never unique. It runs under
  `pg_try_advisory_xact_lock` of `bugnet:apply:<placement>` and snoozes 2 s while another run holds
  it. With the lock, it applies the whole current set: it mints a `bnt_` tunnel token when the
  placement has none, writes `/tmp/bugnet/egress-spec.json` and runs `bugnet-run egress apply` as
  root. Then it marks only the secrets it loaded, each still at the `updated_at` it read,
  `ACTIVE` or `FAILED` with the runner's error in upper case (`PORT_443_TAKEN`), moves
  `secrets_version` and writes `SecretsChanged`. A run with nothing applying and no pending
  change exits at once. A pending change (a delete, or a fetch-only put over a loaded secret)
  raises the lag between `secrets_version` and `secrets_applied_version`. A run clears only the
  lag it read when it started, so a change that commits during the run stays pending for its own
  job. A token a failed write may not have reached is forgotten, so the next run mints another,
  and `token_missing` mints one at once, both writes within 75 s.
- **The proxy's terms.** `ResolveTunnel` answers a `bnt_` token's digest, or the fetcher's
  `fetch_placement_id`, with the placement's deadline, CA version, secrets and paces. A sandbox
  never gets `fetch_only` secrets, and `custom_hosts` lists each secret's hosts outside every
  preset (empty, with those hosts dropped from the secret, when `secrets.custom_hosts` is false).
  It answers `TOKEN_UNKNOWN` for an unknown token, `NOT_READY` for a placement that is not ready
  or has no CA version yet, and `BUDGET_SPENT` once the egress budget is spent.
- **The source check.** Where a provider's API reports a sandbox's own address, only Boat, the
  proxy accepts a tunnel only from it. `ResolveTunnel` takes the PROXY v2 source as `source_ip`
  and answers `source_check`, a mode (`report` or `enforce`) and at most one allowed prefix. The
  prefix comes from Boat's `ip` and is stored on the placement (`egress_source`), never from the
  source the proxy saw. A source outside the stored prefix makes one provider read, at most once
  per `refresh_interval` a placement across replicas (`egress:srcread:<id>`). A new address moves
  `secrets_version`, writes `SecretsChanged` and inserts `apply_secrets`. `apply_secrets` reads
  the address too unless a read is fresh, and writes `source` (`family` and `bind`) into the
  spec when one is stored. With `on_unknown = "refuse"` and no address, it fails the secrets with
  `SOURCE_UNKNOWN` and writes nothing. `/monitor` of `router-worker` lists the ready placements
  with secrets and no usable address. `boat.egress_ipv6_bits` stays 128 unless each Boat sandbox
  owns its own `/64`.
- **Deletion.** The transaction ending a placement deletes its `secrets`, `secret_links` and
  `ssh_gateway_keys` rows and clears its tunnel token. The sweep deletes leftovers once the
  deadline is an hour past, and the `previews`, `ssh_keys` and `ssh_hosts` rows 7 days after
  the end.

### SSH

A placement whose provider is in `ssh.providers` gets an alias at ready, `bn-` and 10 lowercase
base32 characters, in `placements.ssh_alias`, which `GetPlacement` and `WatchPlacement` answer as
`ssh_alias`. An order that asks for SSH runs `bugnet-run ssh-up` during boot, after the gate and
before the sampler, and a failure there fails the attempt over. The SSH user is always the
provider's exec user, `root` when it has none, never what `ssh-up` prints (DP 9).

- **Keys.** `AddSshKey` checks the handle, a ready placement, the provider's SSH and a router
  with `names.ssh_host`, `ssh.host_keys` and data-plane keys, spends one operation token, then
  checks the paths and the key. It keeps the key's wire bytes and SHA256 fingerprint, at most
  `ssh.max_keys_per_placement` live ones, a repeated key changing nothing. When the sandbox's sshd
  is not ready yet, it enqueues the `prepare_ssh` job, once per placement while one waits or runs,
  on the `dataplane` queue with a 150 s timeout and 3 attempts. The answer carries the `ssh`
  command, one `known_hosts` line per host key (`[host]:port` when `names.ssh_port` is not 22, and
  then `-p` in the command) and an `ssh_config` stanza whose multiplexing lines,
  `ControlMaster auto`, `ControlPath` beside the identity file and `ControlPersist 60`, the gateway
  drops for Windows. `$TMPDIR` in a path is written `${TMPDIR}` in the stanza, the form ssh_config
  expands. `RemoveSshKey` marks a key removed and writes `SshKeyRemoved`, and spends nothing.
- **Grants.** `AuthorizeSshKey` answers a `g1.` grant id derived from the placement and the key's
  fingerprint, the deadline and the per-connection caps of 10 sessions and 32 forwards. It writes
  and counts nothing, since bugnet-ssh runs it for key queries before any signature. Every denial
  is the same `GRANT_UNKNOWN`. No SSH RPC spends an operation token.
- **Leases.** `AcquireSshConnection` refuses a spent edge budget (`BUDGET_SPENT`), then adds a
  lease to the sorted set `ssh:conns:<placement>` under `valkey.prefix`, scored by its expiry
  `ssh.connection_lease` ahead on Valkey's clock, at most `ssh.max_connections_per_placement` live
  ones, in one Lua script that first drops lapsed leases. `RenewSshConnection` moves a lease's
  expiry, or takes it again after a lapse while there is room, and `ReleaseSshConnection` removes
  it, also after the placement ended. The set expires an hour after the deadline.
- **Upstream.** `OpenSshUpstream` waits up to `ssh.prepare_timeout` for the sshd record, asking
  for `prepare_ssh` again, then answers `SSH_PREPARING`. It runs `ssh-up` once per gateway key,
  under a Postgres advisory lock per placement, with the key in `authorize` behind
  `restrict,pty,port-forwarding` and loopback `permitopen` options, and records it in
  `ssh_gateway_keys`, which the transaction ending the placement empties. The answer is a
  WebSocket to the gate's `/__bugnet/ssh` with the route's headers, cookies and `Bugnet-Gate`, or,
  with `boat.ssh_path = "native"`, a TCP address from Boat's `POST /sshkey`, sent once per key.
- **Checks.** `CheckSshGrants` answers the deadline of each grant whose placement is ready and in
  its paid time and whose key is still live, and leaves the rest out.

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

`LookupPlacement` tells Payments which placement an x402 allowance account belongs to. It takes
one key, the handle's SHA-256 as 64 lower-case hex or the placement's start order id.

| Field | Value |
| --- | --- |
| `placement_id`, `start_order_id`, `package_id` | The placement's, in every state |
| `state` | `placing`, `ready`, `terminating`, `terminated` or `failed`, as `GetOrder` says |
| `deadline` | The deadline, unset before the placement was ready |
| `ended_at` | When it ended as `terminated` or `failed`, unset before |
| `x402_capable` | Before ready, whether the order asked for secrets; from ready, whether the provider is in `secrets.providers` |

A missing key, an empty start order id or a hash that is not 64 lower-case hex answers
`InvalidArgument`. An unknown hash answers `NotFound` `HANDLE_UNKNOWN`. A start order id that
names no placement, such as an extend order's, answers `NotFound` `ORDER_UNKNOWN`.

## Public stats feed

bugnet.sh/stats and the landing's pricing read the feed (control-plane 20, the website's contract
in `docs/website/site-and-stats.md` section 7). With `stats.listen_addr` set:

- `router-worker` runs `stats_snapshot` every 30 s and at start, on the default queue, with a
  25 s timeout and one attempt. It reads the router's figures in one read-only repeatable-read
  transaction, calls Payments' `GET /internal/v1/stats/totals` with
  `BUGNET_PAYMENTS_ROUTER_TOKEN`, checks the answer, merges both with the catalog's start
  packages and the limits, and writes the one row of `stats_snapshot`.
- A failed Payments call keeps the last good Payments part and its time, so `generatedAt` shows
  how old the money is. Without any Payments part the row holds no body.
- `daily` leaves out Payments' days over a day before the router's first order, or over 90 days
  back while it has none, so one stray early day never zero-fills every day since. The totals keep
  them, and each snapshot that drops some warns with their count.
- `router-api` serves `GET` and `HEAD` of `/api/stats` and `/api/stats/sessions` on that loopback
  listener, HTTP/1.1 only, no token. Caddy routes `/api/stats/payments*` to Payments first.
- `/api/stats` serves the stored body as is, the row read at most once per `stats.row_cache`, and
  the last body while Postgres fails. No body answers 503 `unavailable`.
- `/api/stats/sessions?before=<cursor>` reads 50 placements of the last 30 days a page, newest
  first, cached 30 s per cursor, at most `stats.max_queries` queries at once. A query waits 2 s
  for a slot, then answers 503 `busy`. A bad cursor answers 400 `bad_cursor`, as does one whose
  time is outside 1970 to 9999. A cursor older than 30 days answers an empty page with no query.
- Each client address has a bucket of `stats.per_source_per_minute` with bursts of
  `stats.per_source_burst`, the address being the last `X-Forwarded-For` entry or the peer, IPv6
  by /64. An empty bucket answers 429 `rate_limited`.
- A session's `wallet` is its start order's payer only after the payer guard's steps 2 to 4, and
  only when it decodes to 32 bytes. Its `tx` must decode to 64 bytes. Raw reasons go out as one
  word each, and abuse, blocks, lost control and an operator's end as `stopped_by_bugnet`.
- `/monitor` on `router-worker` shows `stats snapshot N s old` past 2 minutes, and the Payments
  part's age past 5 minutes, naming an empty token as the cause.

Values that later tasks build go out as the contract's unknown form until each lands, each from
one function:

| Value | Until | Goes out as | Function |
| --- | --- | --- | --- |
| `startP50Ms`, `startP95Ms`, `failed7d` | Task 33's start samples | Built | `statsStartSamples` in `internal/store/stats.go` reads kind `placement` samples, the percentiles `null` under 5 |
| `spentMicros` in the totals and each day | Task 35's cost meter | `null` | `statsSpent` in `internal/store/stats.go` |
| `wallet.bytes_per_day` | Task 29's byte cap | Left out | `walletBytesPerDay` in `internal/stats/sources.go` |

### Session page

Every placement gets a session record (SP 6) in the transaction that creates it, so
`Placement.session_url` is in the first answer, placing included. The link is
`<sessions.base_url>/s/<id>#k=<key>`: `id` is 16 random lower-case base32 characters, and `key`
is `bnk_` and the base64url of HMAC-SHA256 under the `sessions` purpose key of the placement's
root, over the id and the record's generation. Every `Placement` answer carries the link until
the record expires or its page is closed, then none. No link is given while `sessions.base_url`
is empty, and the record is still kept. A placement with no `key_id` gets no link either, and a
log line, since a key from the current root would change at the next root rotation.

- **Events** (SP 6.2) go through one writer, which cleans every string, cuts it to its length and
  logs a failure without failing the call that caused it. The saga writes `placement` (placed,
  ready, extended, ended) and `payment` (start, extend), the jobs service `job` (start, end), the
  API `file`, `preview` (issued, revoked) and `transfer`. A record keeps `sessions.max_events`
  events, past which only `placement`, `payment` and `allowance` rows land and the rest are
  counted in `eventsDropped`. A record its user deleted takes no event.
- **Commands** (D-f): each new job gets its `seq` with the first 1 KiB of its command, cleaned and
  redacted. A record shows `sessions.max_jobs` jobs; a later job gets no row, no `job` events and
  no copy, and is counted in `jobsDropped` on the record and the snapshot.
- **The copy:** every transaction that ends a placement inserts `session_copy` on the `dataplane`
  queue. It reads each job's stream through the page's cleaner, keeps the newest
  `sessions.job_output` of each output and at most `sessions.placement_output` a record, newest
  jobs first, then 8 KiB per output up to 1 MiB of such tails a record, earlier jobs keeping no
  text and counting it in `cutBytes`, and removes the streams with `UNLINK`: every job the
  `jobs` table lists for the placement, also after Delete now or once the record expired. Missing
  streams set the output `expired`, some missing `partial`, and 10 minutes after the end the copy
  gives up as `expired`. At 100% of `sessions.disk_budget` a copy keeps 8 KiB per output and sets
  `partial`.
- **Expiry:** `session_expiry` runs at start and hourly, deleting up to 10,000 records whose page
  ended and whose hold is empty or past. `abuse`, `wallet_blocked` and `operator` ends close the
  page at once and hold the record `sessions.abuse_hold` for `GetSessionHold`.
- **Cleaning and redaction** follow bugnet-gateway's `cleanText` rule for rule, against its vectors
  in `internal/sessions/testdata`, and then redact Bugnet and vendor token shapes, `Authorization`
  and `Proxy-Authorization` values whatever their scheme, `Bearer` and `Basic` values, URL
  passwords, private key blocks, and the placement's gate key and provider credentials, as
  `[redacted <kind>]`, in output and in the strings an event takes from the sandbox (a file path, a
  transfer's name, a secret host). A token split across frames is still caught, and a line naming an
  `Authorization` header is held until its end, with at most 16 KiB held back per output. A full
  16 KiB is shown up to the token, lead word or open `Authorization` value it would cut, which stays
  held; one run of token characters that fills it is dropped with the rest of its line. Output that
  goes on after a gap, a `skip` that moves it or a replay start inside a line drops the held word
  before it and its text up to the next line break, so neither part of a cut token is shown. It then
  drops the lines a private key block it may have gone on inside could have (base64 and `=` only,
  `Proc-Type:`, `DEK-Info:`, a blank line, and the block's `END` line) up to the first other line,
  as a replay that starts at a line's start does too. A key block longer than 16 KiB that a gap cuts
  stays hidden until its `END` line.
- **The live view** (`WatchSessionJob`) runs on its own Valkey client, its blocking pool sized
  for `sessions.live.max_readers`, and counts its bytes in `live:used:<placement>`, never in the
  edge budget. A first connection replays the newest `sessions.live.replay` after a `skip`. A
  stream ends on the job's exit, the placement's end, a closed or rotated link, 30 minutes or
  shutdown, re-reading its record and placement every 5 s. Its cursor is the stream entry and
  both output offsets, sealed with an HMAC under the `sessions` purpose key over the record id,
  generation and `seq`. A cursor without a valid seal starts over as a first connection, so a
  page cannot resume at an entry boundary inside a token.
- **The snapshot** (`live_json`) serves the placement's deadline, and while it is placing the
  earliest deadline its package's minutes allow, now plus those minutes.
- `AppendSessionEvents` takes at most 50 `mcp` events and leaves out each with a tool outside
  `sessions.mcp_tools` or a malformed outcome, until 10 minutes after the end. It stores a call's
  events in one transaction, so a retried call never stores one twice.
  `AppendX402Events` stores at most 50 `x402` events once per `ref`, leaves out each of a bad shape
  and answers its index in `rejected`, and moves the `x402_services` and `x402_refusals` counters
  of each stored or cap-dropped row, until 10 minutes after the end.
  `AppendAllowanceEvent` stores one event per phase of the start order's record until it
  expires: `paid` and `refunded` with their `tx`, and `kept` (an unspent amount under the refund
  minimum) with `amount_micros` 0, `kept_micros` above 0 and no `tx`. `RotateSessionLink` moves
  the generation only when it names the current link.
- The payment event's `tx` is served empty, and its amount the order's whole amount, until router
  Task 28's `Tx` and `AllowanceMicros` claims land (`paymentClaims` in
  `internal/saga/session.go`). An `allowance` event names USDC's mint as `USDC` and leaves any
  other mint out, so no base58 key reaches the page.

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
`noeviction`, a mining pool list that is empty, missing or unreadable, a payer claim counted
as absent in the last hour because no `fee_payer` came and `wallets.facilitator_addresses` is
empty, and session records at 80% of `sessions.disk_budget` or more. A failed connection
means the worker is down. When `INFO memory` fails or times out while Valkey answers,
`/monitor` reports `valkey memory is unknown`. When an ACL or `rename-command` refuses `INFO`,
it reports `valkey refuses INFO, so its memory and policy go unchecked`, since that also leaves
the start-up policy check blind, so leave `INFO` allowed. When it answers without a field, as a
managed Valkey may, `/monitor` checks the fields it got and logs a warning. `/healthz` is a bare
liveness answer and `/readyz` checks only that Postgres and Valkey answer.

## The VPS front

The founder's VPS is the public front (DP 15). HAProxy splits TCP 443 by SNI and never decrypts.
Until the production deploy, the VPS fronts the Mac through a reverse SSH tunnel for live tests
(DP 15.6). `deploy/vps` holds its files. `deploy/vps/README.md` is the production deploy's
runbook: `ship.sh` on the Mac, `deploy.sh`, `check.sh` and `bugnet-ops` on the VPS.

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
| `deploy.env` | Read by `front.sh`, `deploy.sh` | The SSH ports, the wildcard directories, `gateway_users` |

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
| DP 1 | (c), answered 2026-11-07 | `admin_ssh_port = 22`, `ssh_gateway_port = 2222` | It binds `:2222`, `:::2222` |
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
7. loads `nftables/dev-front.nft` through `bugnet-nft.service`, a oneshot that loads every file in
   `/etc/bugnet/nftables/` at boot and never flushes the ruleset. Only `haproxy` may connect to
   `127.0.0.1:19443`, the tunnel's VPS end, so no other local user can write a PROXY header that
   names a sandbox's address (DP 10.12.7.1). The production deploy adds `egress.nft` the same way,
   for `127.0.0.1:9443` and the users `haproxy` and `bugnet-router`.

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
cp -R deploy/vps/front.sh deploy/vps/systemd deploy/vps/nftables "$out"/
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
- `tunnel` has no shell, and may listen on the five tunnel ports and nowhere else, with no `-L`,
  `-W` or command, while other users keep their sshd settings;
- ufw allows TCP 80, 443, 2200 and the admin port alone, and drops UDP 443;
- `nft list table inet bugnet_dev_front_lo` shows the rule, and with a listener on
  `127.0.0.1:19443` the user `haproxy` connects while `nobody` and root are refused, again after a
  reboot of the VM;
- the unit, its user and `certs.toml` work, since a run without the token stops at `dns_env`, and
  the staging command hands back that exit status;
- a file `bugnet-certs` writes in the dev wildcard's directory takes the reader's group, and the
  admin user can read it.

`KEEP_VM=1` keeps the VM for a look afterwards. The first run downloads Ubuntu's cloud image.

### Internal mTLS for x402

Egress calls Payments' x402 signer over mutual TLS 1.3 on `127.0.0.1:8449` (XS 6.10). Both sides
trust one internal CA. `bugnet-dev mtls init` makes it, offline, with Go's `crypto/x509`, so
renewal can sign a public key read from a certificate. Every file is plain PEM that `openssl`
reads.

```sh
bin/bugnet-dev mtls init --out "$HOME/bugnet-mtls"
d="$HOME/bugnet-mtls/etc/bugnet"
bin/bugnet-dev mtls init --renew --out "$HOME/bugnet-mtls" \
    --from "$d/egress-x402-client.pem,$d/payments/x402-server.pem"
bin/bugnet-dev mtls init --dev --out "$TMPDIR/x402-mtls"
```

| Key | Curve | Life | Name and use |
| --- | --- | --- | --- |
| Internal CA | ECDSA P-256 | 10 years | CA only, its key never on the VPS |
| Payments' server pair | ECDSA P-256 | 1 year | SAN `bugnet-payments.internal`, server auth |
| Egress's client pair | ECDSA P-256 | 1 year | SAN `bugnet-egress.internal`, client auth |

`init` keeps `internal-ca.key` (0600) and `internal-ca.pem` in `--state`, by default
`~/.local/state/bugnet-dev/mtls` (0700). It refuses when `--state` already holds a CA and names
the directory. Under `--out` it writes the files laid out as on the VPS:

| File under `--out` | Mode | Goes to |
| --- | --- | --- |
| `etc/bugnet/internal-ca.pem` | 0644 | The same path, read by egress and Payments |
| `etc/bugnet/egress-x402-client.pem` | 0644 | The same path |
| `etc/bugnet/keys/egress-x402-client.key` | 0600 | The same path, root only, a credential |
| `etc/bugnet/payments/x402-server.pem` | 0644 | The same path, in the Payments user's directory |
| `payments.env.add` | 0600 | Its one line appended to `/etc/bugnet/payments.env` |

The env line is `BUGNET_X402_TLS_KEY=` and the base64 of Payments' server key PEM. Egress loads
its client key as the systemd credential `egress_x402_client_key`. `init` prints each path it
wrote and a `delete_once_copied=` line for the two files that hold a service key. It never prints
a key or the env line. Copy the files over the admin SSH, then delete those two files from the
Mac.

Renew a month before a certificate expires. `--renew --from CLIENT_PEM,SERVER_PEM` reads the two
current certificates, checks the kept CA issued them for their names, and issues new 1-year
certificates for the same public keys and names with new serials. A certificate never outlives
the CA: with less than a year left, it ends when the CA does. It writes only the two `.pem` files
under `--out` and prints `not_after=`. Copy them to the VPS and send both services `SIGHUP`. A new
key pair needs a Payments restart and an egress roll instead.

`--dev --out DIR` makes a separate development CA and both pairs for `make x402-local`, flat in DIR
under the names egress and Payments read by default: `internal-ca.pem`, `egress-x402-client.pem`,
`egress_x402_client_key` (0600), `x402-server.pem` and `payments.env.add` (0600). DIR serves as
egress's `$CREDENTIALS_DIRECTORY`. The development CA key is not kept, so each run makes a new CA
and replaces the five files. `--dev` takes no `--state`, `--renew` or `--from`.

## The x402 local run

`make x402-local` runs the x402 path on this Mac with nothing mocked: the real `router-api`,
`router-worker`, `bugnet-egress` and bugnet-payments, a fake Solana chain, the fake seller and a
sandbox of the `local` provider. It needs Postgres, Valkey, `tsx`, Node 26 and a bugnet-payments
checkout (`BUGNET_PAYMENTS_DIR`, default the one beside this repository).

It builds `router-api`, `router-worker` and `bugnet-dev` here and `bugnet-egress` in `bugnet-edges`
with `-tags localrun`, then runs `go test ./test/x402local/` with `BUGNET_TEST_X402_LOCAL=1`.
Without that variable the tests skip. One subtest per mode:

- `ok`: Payments takes a $0.10 allowance on a start order, a sandbox is placed with its token, two
  services are enabled, and one call of $0.001 is paid. The reply is 200 with
  `Bugnet-X402-Status: paid`, `Bugnet-X402-Paid-Usd: 0.001` and `Bugnet-X402-Left-Usd: 0.099`,
  Payments confirms the payment on the fake chain, and `ListX402` counts one paid call.
- `free`: 300 free calls pass and the 301st is 403 `unpaid_cap`.

A `localrun` build differs from a normal one in four places, each of them off in a normal build:
`secrets.providers` may list `local`, a local sandbox gets the Bugnet CA, `[localrun]
forward_listen` and `egress_port` replace the forwarder's address and the proxy's port in the
egress spec, and egress reads `[localrun]` `upstream_addrs` and `upstream_roots_file`. A normal
build refuses all four.

The forwarder cannot listen on 127.77.0.1 or trust the proxy's certificate on a Mac, so the run
starts the sandbox's own `bugnet-run forward --root` on a loopback port, with the proxy's
certificate as its `roots_file`, and the calls go to it. `BUGNET_X402_LOCAL_KEEP=1` keeps the run's
directory and sandboxes.

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
| `reason` | A stable code such as `stop`, `deadline`, `abuse`, `provider_failed`, `no_candidate`, `wallet_busy`, `wallet_rate_limited`, `provider_capacity`, `boot_failed` or `placing_stalled` |
| `unused_seconds` | On `placement.terminated` only, `max(0, deadline - terminated_at)` in whole seconds when refundable, else 0 |
| `refunds` | On `placement.terminated` only, `[{order_id, refund_micros}]` when refundable, else `[]` |

`refunds` takes the unused seconds from the newest order back to the start order. Each order
refunds its share at its own price a second, `amount_micros / seconds_bought` rounded down to
whole micros, where `amount_micros` already leaves out a start order's x402 allowance. An order
from before `orders.seconds_bought` was kept stops the list there.

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

### Limits and account standing

`limits_poll{provider}` reads each account's cap and use into `provider_capacity` every 60 s, and
Boat's every 15 s. A placement reserves a slot only while `in_use + external_in_use +
limits_poll.reserve_slots < max_concurrent`, so sandboxes other deployments hold on the account
count, and one slot stays free for a probe. `max_concurrent` is the smaller of the provider's cap
and the provider table's `max_concurrent`, 50 when neither is set. A create that meets the
account's own cap (`CodeQuota`) asks for a poll at once. E2B's cap cannot be read with a key, so
`e2b.max_concurrent` (20) is its cap, and `e2b.team_id` (printed by `bugnet-dev hooks register
e2b`) lets its account check read the team's metrics. `koyeb.organization_id` is read from the
account when empty.

`account_check{provider}` runs every minute and reads the account when 5 minutes have passed
since its last check, or 1 minute while the provider is blocked. A create, a call of the API or a
sweep that sees `abuse_lock`, `unfunded` or `auth` asks for a check at once, at most one every 30
s. A blocked provider (`banned`, `unfunded` or `auth`) is skipped by every replica's placement
within 10 s, and each change writes a `provider_signals` row. A check that cannot read the account
changes nothing. Boat's `GET /me` or `GET /limits` answering 403 means the key lacks
`account.read`, which blocks nothing.

Every provider table takes `creates_per_second`, 0 for no bucket (E2B's is 1). The saga takes a
token from `rl:create:<provider>` before each create and fails over when none comes within 1 s.
A `rate_limited` answer with a Retry-After pauses the provider's creates for that long, at most 5
minutes. A rate limit or a quota refusal never counts against the provider's health.

`/monitor` reports a blocked provider, limits not polled for `stale_after` intervals, and Boat's
`RateLimit` header with under 20% of its window left.

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

### Start samples, shared health and canaries

Every attempt of the saga that called the provider writes one `start_samples` row of kind
`placement` in its own transaction with `synchronous_commit = off`, off the attempt's path. It
holds `create_ms`, `ready_ms` (create to the provider answering ready), `runner_ms` (upload and
ping, null where the runner is baked in), `gate_ms`, the outcome, the provider's error code and an
estimated cost. An attempt skipped for an empty create bucket or a pause writes none. The outcome
is `ok` or `failed`, or `capacity`, `rate_limited` or `quota`, which say nothing against the
provider and are left out of its health. A failed write only logs.

`canary_tick` runs every minute and queues a `probe` job for each provider that is due. It is due
when its last canary is older than `health.canary_every` (5 minutes, spread over 80 to 120 percent
by a hash of the provider and the last canary), `canary_every_down` (1 minute) while it is down, or
`boat_canary_every` (30 minutes) for a provider that charges per start, and no placement got ready
there within `passive_window`, its account is not blocked, and the next start of the canary's size
is free. The canary's size is the cheapest one covering `canary_vcpu` and `canary_memory_mib`.

A probe (`internal/probe`) takes a slot with no margin and records it in `probe_slots`, takes a
create token, creates the size with the labels `bugnet=1`, `bugnet_deployment`,
`bugnet_probe=<kind>` and `probe=<run id>`, no `placement` label, a 5-minute lifetime and the key
`probe-<run id>`, boots it with `Saga.Boot` (so `harden` and the gate run as for a placement),
times the runner's `ping` once cold and three times warm, writes and reads 4 KiB, destroys it and
stores one sample. `router-worker` loads the `[runner]` builds and `hardening.pool_list` for this,
as `router-api` does. The probes queue has `health.probe_workers` workers, a probe runs once and
times out at 2 minutes.

`health_rollup` runs every 30 s. Over the last `health.window` (15 minutes) a provider is `down`
when its last two counted canaries failed, or it has at least `min_samples` samples and a Wilson
lower bound under `down_below`, `slow` when its ready p95 is over `slow_ready_p95`, else `ok`.
When canary failures alone would mark every one of two or more providers down, the rollup keeps
each state and logs at error level. Every replica reads `provider_health` every 10 s: a provider
is skipped when it is `down` or blocked, and the shared ready p95 stands with at least 5 samples.
The local breaker still closes a provider at 3 failures in 60 s without the rollup.

`/monitor` also reports `provider X is down for N minutes` past `down_alert_after`, `provider X
has had no canary for N minutes` when a canary is 3 intervals late and no placement got ready
there, and `provider X has a probe sandbox N minutes old` past 15 minutes, from the sweep's
listing. Start samples older than `samples_keep` are deleted by the retention job.

### Benchmarks, the placement simulator and retention

`bugnet-dev bench start --provider P [--size S] [--runs 30] [--concurrency 3] [--yes]
[--allow-paid-starts]` calls `StartBench` with `BUGNET_OPERATOR_TOKEN`. The router checks that the
provider is configured and its account is not blocked (`PROVIDER_BLOCKED`), takes the named size
or the provider's smallest, and estimates the cost as runs times 30 s at the hourly rate, rounded
up to the billing increment, plus the provider's start fee. Over `bench.confirm_above_micros`
(500000, $0.50) it refuses with `COST_UNCONFIRMED` and the estimate in the error's
`estimated_cost_micros` metadata unless `confirm_cost_micros` covers it. `bugnet-dev` prints the
estimate and sends it back with `--yes`. `runs` is 1 to `bench.max_runs` (200) and `concurrency`
is 1 to `health.probe_workers`. The call writes a `benches` row and queues `bench{bench_id,
wave: 0}`.

A `bench` job runs one wave. The first time it sees a wave it queues up to `concurrency` `probe`
jobs of kind `bench` on the probes queue, numbered from `wave * concurrency`, and snoozes 15 s at
a time while any is out. When none is, it counts the samples stored under the benchmark and queues
the next wave, or finishes the row once the runs are stored, so a probe that was rescued and lost
is made up by the next wave. A benchmark probe waits up to 10 s for a create token, so a provider's
create rate holds. It stores a `capacity` sample for a provider with no free slot and a
`rate_limited` sample for a paused one, and the benchmark goes on. On a provider that charges per
start, a wave has one probe and the benchmark ends when the next start would cost a fee, unless
`--allow-paid-starts` is set. The worker's own keys do the work, so the numbers come from where
the worker runs.

`bugnet-dev bench report [--bench-id ID | --since 24h] [--kind bench,canary,placement]
[--provider P] [--json]` calls `ReportStarts`, which groups the samples by provider, size and kind.
Each table gives the ok, failed and refused counts, the success rate with its 95 percent Wilson
lower bound, and the nearest-rank p50, p90 and p99 of create, ready, runner, gate, first command,
warm command, destroy and total (ready, runner, gate and first command added), then the measured
cost beside the estimate. Refused counts capacity, rate limit and quota answers, which are not
failures.

`bugnet-dev bench router --package P [--runs 20] [--concurrency 2] [--json]` orders through
`CreatePlacement` with dev order tokens, runs `true` through `Exec` and stops the placement, and
times it on the client. It reports `start_path: cold`. It needs `BUGNET_INTERNAL_TOKEN` and
`BUGNET_ORDER_PRIVATE_KEY`, so it runs against a development router only.

`bugnet-dev simulate [--since 7d] [--budget-seconds 30[,N...]] [--providers A,B] [--orders 1000]
[--json]` replays the recorded start samples (`internal/simulate`, pure Go) through the saga's
failover rules: providers are tried in the order given, at most 3 attempts, within the placing
budget, and each attempt draws a recorded start of the provider it tries. It prints per budget the
share of orders ready, the share that failed over, the share refunded for no candidate, and the p50
and p90 time to ready. `ReportStarts` answers aggregates only, so `simulate` reads the samples
from Postgres at `BUGNET_DATABASE_URL`.

The `retention` job runs at start and daily at 04:00 UTC, one at a time, with a 120 s timeout. It
deletes up to 10,000 rows of a table a batch and queues itself again when a table still had a full
batch after 90 s of work.

| Rows | Deleted after |
| --- | --- |
| `start_samples` | `health.samples_keep` (30 days) |
| `benches` | 90 days |
| `transfer_index` | 30 days |
| `probe_slots` | 10 minutes, the slot's capacity given back |
| `usage_observations` | `estimator.observations_keep` (90 days) |
| `estimates` that no placement or observation refers to | `estimator.estimates_keep` (7 days) after `served_at` |
| `source_facts` that no kept estimate refers to | `estimator.source_facts_keep` (7 days) after `used_at` |

`wallets`, `wallet_events`, `catalogs` and `provider_cost_daily` are kept.

### Reconciliation

`Provider.Inspect` reads one sandbox into a `Status`: a `State` (`starting`, `running`, `degraded`,
`stopped`, `failed` or `gone`), the provider's own words in `Reason`, E2B's kill reason in
`KillReason` for a sandbox it no longer has, and `ExpiresAt`, the provider-side lifetime. A sandbox
the provider answers 404 for is `gone` with a nil error. An unknown state or a failed call is an
error, and an error never ends a placement. `Summary.State` and `Summary.ExpiresAt` carry the same
from a listing where it says them. Koyeb cannot move a lifetime, so its drift is never fixed.

`internal/reconcile` holds three pieces. `Classify` is the table of CP 6.3 as a pure function of
the provider, the `Status`, the placement (`Live`) and the `[abuse]` settings. `Guard` is the
correlation guard of CP 6.4: under a per-provider advisory lock it holds an end back when more than
`correlated_max` placements of the provider got `abuse`, `control_lost` or `sandbox_exited` inside
`correlated_window`, when the provider's account is blocked, or when its newest canary in the
window failed. A held end becomes a refundable `provider_failed` with `correlated: true` in its
evidence, and its wallet actions are recorded in `provider_signals` as `held`. `Reconcile` inspects
a placement's sandbox, classifies the reading and ends the placement through `Saga.End`, or flags
an ended one through `Saga.FlagEnded`. Only an `abuse` verdict acts on a placement that is
terminating or ended.

- A degraded sandbox sets `placements.degraded_since`. Past `degraded_grace` the setting's class
  applies, and before it a scheduled `reconcile` job looks again. A healthy reading clears it.
- A boxd machine `hibernated` or `suspended` is `provider_failed` only when its ref says
  `hibernate_off = 1`, else `sandbox_exited`, since an idle machine hibernates on its own.
- A word list matches as a lower-case substring. Boat's abuse words win over its provider words.
  Koyeb's exit words win over its platform words, since a user causes an exit.
- E2B's `request` kill on a placement still `ready` is `provider_failed` and raises the alert
  `/monitor` shows. No sandbox holds an API key, so no guest can cause it.
- A lifetime that differs from the deadline plus five minutes by more than `lifetime_drift` is set
  again under the placement's row lock, which an extension holds until its new deadline commits,
  and at most once per placement every 10 minutes (`reconcile:life:<placement>`).

The sweep inserts a `reconcile{placement}` job for each placement ready on a provider for longer
than `ready_grace` that its complete listing lacks or shows `degraded`, `stopped`, `failed` or
`gone`. It inserts at most `max_per_sweep` new jobs per provider, and a listing cut short by an
error inserts none. The job is unique per placement for 10 s and times out at 90 s. The
`hook_hint` job (default queue, River's 60 s) finds the placement by the event's `placement`
label, else by `(provider, provider_ref ->> 'id')` and then `placement_attempts.sandbox_id`. It
drops a hint whose labels name another deployment, reconciles a ready or terminating placement,
and for one that ended inside `late_window` acts only on an `abuse` verdict. With the worker
registered, `hooks.providers` may list `e2b` and `boat`.

`/monitor` adds `provider X: N sandboxes killed that no router asked for` and `provider X: N
placements ended as provider_failed in the last hour` over `abuse.lost_alert_per_hour`.

The live rows are `TestLiveControl/G1` (what `Inspect` reads after `sudo poweroff` and `sudo kill
-9 1`), `R1` (`Inspect` after a destroy and `ExpiresAt` after `SetLifetime`), `H2` (E2B's kill
event), and `H1` and `H3` (E2B's and Boat's deliveries, which need `--front vps` and the provider's
webhook secret in the environment).
