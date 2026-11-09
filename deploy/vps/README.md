# The production deploy on the VPS

The runbook of DP 15 (data-plane plan Task 39, with x402 plan Task X35). Every command is exact.
Secrets are made and kept on the VPS only; the Mac never makes, reads or prints one.

| File | What it is |
| --- | --- |
| `ship.sh` | Mac side: builds linux/amd64 binaries, renders the maps, copies the bundle, runs `deploy.sh` |
| `deploy.sh` | VPS side, as root: `keys`, `mtls`, a deploy, `x402-index` |
| `check.sh` | The deploy check, installed as `/opt/bugnet/check.sh` |
| `bugnet-ops` | `bugnet-dev` as `bugnet-router` with the router's env, `/usr/local/bin/bugnet-ops` |
| `config/` | The production settings: router, edge, secrets route, sessions, SSH gateway, egress |
| `systemd/` | One templated unit per service, `router-worker.service`, the Valkey drop-in, bugnet-certs |
| `postgres/`, `valkey/` | DP 15.3's fragments |
| `site.toml`, `names.toml` | The VPS's facts and every public name, read by `bugnet-dev deploy maps` |
| `front.sh`, `tunnel.md` | The dev front of DP 15.6, for live tests from the Mac |

## 1. Layout (DP 15.1)

| Unit | User | Listens on |
| --- | --- | --- |
| `haproxy` | `haproxy` | `:443`, `:2222` (SSH gateway), `:2200` (dev SSH) |
| `bugnet-edge@<v>` | `bugnet-edge` | `127.0.0.1:8443`, `:80`, metrics `127.0.0.1:9101` |
| `bugnet-secrets@<v>` | `bugnet-secrets` | `127.0.0.1:8445`, metrics `9104` |
| `bugnet-sessions@<v>` | `bugnet-sessions` | `127.0.0.1:8448`, metrics `9105` |
| `bugnet-ssh@<v>` | `bugnet-ssh` | `127.0.0.1:2223`, metrics `9102` |
| `bugnet-egress@<v>` | `bugnet-egress` | `127.0.0.1:9443`, metrics `9103` |
| `router-api@<v>` | `bugnet-router` | `127.0.0.1:8080`, stats `127.0.0.1:8082` |
| `router-worker` | `bugnet-router` | `127.0.0.1:8081` |
| `bugnet-certs.timer` | `bugnet-certs` | Nothing |
| `postgresql` | `postgres` | `/var/run/postgresql` only |
| `valkey-server` (or `redis-server`) | `valkey` (`redis`) | `/run/valkey/valkey.sock` only |

Binaries live in `/opt/bugnet/<router|edge|ssh|egress|certs|run>/<version>/`, with `current`
pointing at the version that passed. `bugnet-dev` sits beside `router-api`. Ubuntu 24.04 has no
`valkey` package, so `deploy.sh` then installs `redis-server` and runs it in group `valkey` with
the same socket, so every name above stays. ufw allows TCP 22, 80, 443, 2200 and 2222 and denies
the rest, UDP 443 included.

## 2. The split (DP 15.2)

`ship.sh` runs `bugnet-dev deploy maps` with `names.toml` and `site.toml` and ships the result in
`maps/`. Production adds `frontend ssh` on `ssh_gateway_port` (2222, DP 1 answer c) and the
production wildcard. `deploy.sh` installs the maps and runs `haproxy -c` before every reload; a
refused config leaves the old one.

## 3. Units and data stores (DP 15.3)

- Env files `/etc/bugnet/<service>.env` are `root:root 0600`. Keys in `/etc/bugnet/keys/` are
  `root:root 0600` and reach a service only as a systemd credential.
- Postgres takes peer logins on its socket only: `bugnet-router` reaches `bugnet_router` and
  `bugnet`, `bugnet-payments` reaches `bugnet_payments`. `deploy.sh` makes both roles and
  databases.
- Valkey: `port 0`, the socket `770` in group `valkey`, `/run/valkey` `0750`, `noeviction`,
  `maxmemory 1gb` (raise it in `valkey/bugnet.conf` with the VPS's memory).
- `BUGNET_DATABASE_URL` is
  `postgres://bugnet_router@/bugnet?host=/var/run/postgresql&pool_max_conns=48`.

`check.sh` proves after every deploy: only `bugnet-router` opens either store, and nobody reaches
them over TCP; `pool_max_conns` is at least the River workers plus 8 (36 today); no unit failed,
`bugnet-certs` included; every `/readyz` answers; the stats feed answers (a warning only); and the
x402 checks of section 9.

## 4. DNS and certificates (DP 15.4)

At bugnet.sh's DNS host, all "DNS only", TTL 300, `<IPv4>` being `site.toml`'s `ipv4`:

```text
*.preview.bugnet.sh  preview.bugnet.sh  transfer.bugnet.sh  hooks.bugnet.sh
secrets.bugnet.sh    sessions.bugnet.sh ssh.bugnet.sh       egress.bugnet.sh
mcp.bugnet.sh        bugnet.sh          www.bugnet.sh                      A  <IPv4>
preview.bugnet.sh    CAA   0 issue "letsencrypt.org"
preview.bugnet.sh    CAA   0 issuewild "letsencrypt.org; validationmethods=dns-01"
```

With IPv6, each A gets an AAAA. No CAA at the apex until DP 13 is answered. `bugnet-certs` issues
`*.preview.bugnet.sh` by DNS-01 with `CF_DNS_API_TOKEN`; `deploy.sh` issues the first pair before
the edge starts, since the edge needs it. The edge, the secrets route, the sessions role and
egress take theirs by TLS-ALPN-01 through the split; `deploy.sh` opens each name once after a
deploy so they are issued before `bugnet-certs` checks them.

## 5. Keys made once (DP 15.5)

`ship.sh --keys` runs `deploy.sh keys` on the VPS. It makes, once each:
`/etc/bugnet/keys/ssh_host_ed25519_key`, the egress CA (`egress-ca.key`, and `egress-ca.pem`
copied to `/etc/bugnet/`), the box key `egress_box_k1`, and `/etc/bugnet/router.env` from
`bugnet-dev keys --production` plus `BUGNET_DATABASE_URL`. It copies each caller's token into its
own env file without printing it, then prints only public values:

- the SSH host key line, for `ssh.host_keys` in `config/router.toml`;
- the box key, for `secrets.box_public_keys`, and to the gateway team to pin;
- the event public key, for Payments' `router.event_public_key` (`sudo bugnet-ops pubkey --event`
  prints it again later).

Back up `/etc/bugnet/keys/` off the VPS, encrypted. The SSH host key must never change.

## 6. Secrets: the env files, by name

| File | Names | From |
| --- | --- | --- |
| `router.env` | `BUGNET_DATABASE_URL`, `BUGNET_INTERNAL_TOKEN`, `BUGNET_HANDLE_KEY`, `BUGNET_DATA_PLANE_KEYS`, `BUGNET_EVENT_SIGNING_KEY`, `BUGNET_EDGE_TOKEN`, `BUGNET_SECRETS_TOKEN`, `BUGNET_SSH_TOKEN`, `BUGNET_EGRESS_TOKEN`, `BUGNET_EGRESS_FETCH_TOKEN`, `BUGNET_OPERATOR_TOKEN`, `BUGNET_PAYMENTS_ROUTER_TOKEN`, `BUGNET_SESSIONS_TOKEN`, `BUGNET_HOOK_PATH_SECRET` | `deploy.sh keys` |
| `router.env` | `E2B_API_KEY`, `BOAT_API_KEY`, `BOXD_API_KEY`, `BUGNET_GITHUB_TOKEN` (`BUGNET_HF_TOKEN` optional) | The founder's accounts, by hand |
| `edge.env` | `BUGNET_EDGE_TOKEN`, `BUGNET_HOOK_PATH_SECRET` | `deploy.sh keys` |
| `secrets-route.env` | `BUGNET_SECRETS_TOKEN` | `deploy.sh keys` |
| `sessions.env` | `BUGNET_SESSIONS_TOKEN` | `deploy.sh keys` |
| `ssh.env` | `BUGNET_SSH_TOKEN` | `deploy.sh keys` |
| `egress.env` | `BUGNET_EGRESS_TOKEN`, `BUGNET_EGRESS_FETCH_TOKEN` | `deploy.sh keys` |
| `certs.env` | `CF_DNS_API_TOKEN` | The founder (DP 15.8 step 3) |
| `payments.env` | `BUGNET_X402_TLS_KEY` | `deploy.sh mtls`; the rest is Payments' own |
| `payments.env` | `BUGNET_PAYMENTS_ROUTER_TOKEN` | The router's value, copied below |
| `gateway.env` | `BUGNET_INTERNAL_TOKEN` | The router's value, copied below |

Add a value by hand with `sudoedit /etc/bugnet/router.env`, one `NAME=value` a line, no quotes, no
comment after a value. Copy a shared token on the VPS without printing it:

```sh
sudo sh -c 'grep "^BUGNET_PAYMENTS_ROUTER_TOKEN=" /etc/bugnet/router.env >> /etc/bugnet/payments.env'
sudo sh -c 'grep "^BUGNET_INTERNAL_TOKEN=" /etc/bugnet/router.env >> /etc/bugnet/gateway.env'
```

Hand-offs by team:

- From Payments: `order_token_public_key` (`pnpm cli keys pubkey`), the fee payer's address for
  `wallets.facilitator_addresses` (`pnpm cli keys show`). Both go in `config/router.toml`.
- To Payments: the event public key, `BUGNET_PAYMENTS_ROUTER_TOKEN` (copied above), the x402 mTLS
  files (section 9).
- From the gateway team: their Unix users for `site.toml`'s `gateway_users` (`bugnet-gateway`
  once it exists; `check.sh` fails on a listed user that does not exist), and their entry page
  bundle, copied to `/srv/bugnet/secrets-page/`.
- To the gateway team: the box public key, `BUGNET_INTERNAL_TOKEN` (copied above).
- From bugnet-website: the session page build, copied to `/srv/bugnet/session-page/`. Until then
  a placeholder page serves. The sessions role reads it at start: `sudo systemctl restart
  'bugnet-sessions@*'` after a copy.

## 7. First deploy

On the Mac, in `bugnet-router`, with the checkouts beside it on the commits to ship:

1. The founder gives the VPS's IPv4 (and IPv6), and an admin user with sudo on port 22. Write
   them into `deploy/vps/site.toml`'s `ipv4` and `ipv6`, and commit.
2. The founder adds the DNS records of section 4, and puts the DNS token on the VPS:
   `sudo install -d -m 0755 /etc/bugnet && sudo install -m 0600 /dev/null /etc/bugnet/certs.env
   && sudoedit /etc/bugnet/certs.env` with the line `CF_DNS_API_TOKEN=<token>`.
3. Keys: `deploy/vps/ship.sh --keys <admin>@<IPv4>`. Paste the printed lines into
   `config/router.toml` (`ssh.host_keys`, `secrets.box_public_keys`), and send the event key to
   Payments.
4. Provider keys: on the VPS, `sudoedit /etc/bugnet/router.env`, adding `E2B_API_KEY`,
   `BOAT_API_KEY`, `BOXD_API_KEY` and `BUGNET_GITHUB_TOKEN`. Copy the two shared tokens (section
   6).
5. x402 mTLS: `go run ./cmd/bugnet-dev mtls init --out ~/bugnet-mtls`, then
   `deploy/vps/ship.sh --mtls ~/bugnet-mtls <admin>@<IPv4>`, then
   `rm -rf ~/bugnet-mtls`. The CA key stays in `~/.local/state/bugnet-dev/mtls`.
6. Payments' values into `config/router.toml`: `order_token_public_key` and
   `wallets.facilitator_addresses`. Commit.
7. Deploy: `deploy/vps/ship.sh <admin>@<IPv4>`. `deploy.sh` stops before changing anything when
   a key, an env name or `order_token_public_key` is missing, and lists each. Expected: `check.sh`
   passes and the last line is `deployed <version>`.
8. From the Mac: `curl -sI https://preview.bugnet.sh/` answers with HSTS.
9. Secrets and x402 on Boat and boxd: in `config/router.toml` set
   `secrets.providers = ["boat", "boxd"]`, `ca_cert_file = "/etc/bugnet/egress-ca.pem"` and
   `egress_ips = ["<IPv4>", "<IPv6>"]`, then ship again (step 7). x402 pays only on a provider in
   `secrets.providers`. E2B joins once its templates carry the production CA (Task 39 step 8).
10. Once Payments runs on 8447, 8449 and 8450, and the gateway has its release, the reviewed
    index (section 9).

With `ipv4` set, `ship.sh` takes the host and port from `site.toml`, so the host may be left out.

## 8. Later deploys and drains (DP 15.7)

```sh
deploy/vps/ship.sh <admin>@<IPv4>                 # build, copy, deploy, check
deploy/vps/ship.sh --copy-only <admin>@<IPv4>     # copy only, then run the printed command
deploy/vps/ship.sh --check-heads                  # check the four checkouts, build nothing
```

Before building, `ship.sh` checks the four checkouts it builds from (`bugnet-proto`, `bugnet-run`,
`bugnet-edges` beside `bugnet-router`): each is clean, on the router's branch, and at that branch's
tip, `origin/<branch>` for the canonical checkout and the canonical checkout's branch for another
worktree. On a mismatch it prints one line per checkout (repo, path, branch, commit, expected
commit, what differs) and stops. `--allow-mixed` ships anyway with a warning. The bundle's
`COMMITS`, `~/bugnet-deploy/<version>/COMMITS` on the VPS, records each `repo:sha` with its branch
and result.

`deploy.sh` migrates the database with the new `router-api`, then starts each new instance
beside the old one (`SO_REUSEPORT`), waits until its `/readyz` answers ten times in a row, and
only then stops the old ones, which drain on their own: the edge 5 minutes, the secrets route and
sessions 1 minute, egress 5 minutes, router-api up to 31 minutes, the SSH gateway up to 2 hours.
`router-worker` restarts, and River retries its jobs. A new instance that never gets ready stops
every new instance, and the old ones keep serving. A failed `check.sh` fails the deploy. To go
back: `cd ~/bugnet-deploy/<old version> && sudo ./deploy.sh <old version> ./bin`.

Settings live in `config/`, so a settings change is a commit and a ship, never an edit on the
VPS. The previous copy of each changed file is kept in `/etc/bugnet/previous/`. Operator commands:
`sudo bugnet-ops <bugnet-dev arguments>`, for example `sudo bugnet-ops wallet show <payer>`.

## 9. x402: files, credential, ports and checks (X35)

| Path | Owner and mode | Read by |
| --- | --- | --- |
| `/etc/bugnet/internal-ca.pem` | root 0644 | Egress and Payments |
| `/etc/bugnet/egress-x402-client.pem` | root 0644 | Egress |
| `/etc/bugnet/keys/egress-x402-client.key` | root 0600 | systemd, as `egress_x402_client_key` |
| `/etc/bugnet/payments/` | Payments' user, 0755 | Payments |
| `/etc/bugnet/payments/x402-server.pem`, `x402-index.json` | 0644 | Payments |
| `BUGNET_X402_TLS_KEY` in `/etc/bugnet/payments.env` | root 0600 | Payments |

- Ports, all on `127.0.0.1`: 8447 Payments internal, 8449 the signer (mTLS), 8450 Payments public.
  No HAProxy rule reaches 8447 or 8449. Egress calls `https://127.0.0.1:8449` as
  `bugnet-payments.internal`. 8446 is the website's alone.
- `deploy.sh mtls` owns `/etc/bugnet/payments/` by `bugnet-payments` when that user exists, root
  otherwise; Payments' deploy chowns it later.
- `check.sh` checks: 8447, 8449 and 8450 on loopback only, 8446 held by `bugnet-website`, no
  HAProxy line to 8447 or 8449, the egress unit's credential line, the modes above,
  `openssl x509 -checkend 2592000` on both leaves and the CA, egress `payments_timeout` (3s) above
  Payments' `sign_timeout` (2s), and `allowed_shared_hosts` equal to
  `bugnet-dev x402 allowed-hosts` for the deployed index.
- The reviewed index, in XS 7.16's order: Payments, the gateway, then the router. Copy the
  gateway release's `packages/gateway/src/x402/index.json` to the VPS, then:

  ```sh
  sudo ~/bugnet-deploy/<version>/deploy.sh x402-index ~/index.json            # Payments, SIGHUP
  # the gateway team deploys its release
  sudo ~/bugnet-deploy/<version>/deploy.sh x402-index --router ~/index.json   # router-api rolls
  ```

  Later deploys keep the router's line, generated from `/etc/bugnet/router-x402-index.json`.
- Renewal, a month before expiry (egress logs a line a day from 30 days): on the Mac,
  `go run ./cmd/bugnet-dev mtls init --renew --from <client.pem>,<server.pem> --out
  ~/bugnet-mtls`, copy `internal-ca.pem`, `egress-x402-client.pem` and `x402-server.pem` to their
  paths, then `sudo systemctl reload 'bugnet-egress@*'` and SIGHUP Payments. Then
  `sudo /opt/bugnet/check.sh x402`.
- A new key pair: `mtls init` again, `ship.sh --mtls`, replace `BUGNET_X402_TLS_KEY` in
  `payments.env` with `sudoedit` (`deploy.sh mtls` never overwrites it), restart Payments, and
  ship once more, which rolls egress (DP 15.7).

## 10. Live tests from the Mac (DP 15.6)

Before and beside production, the VPS fronts the Mac on the dev names through a reverse SSH
tunnel: `front.sh` and `tunnel.md`. The dev front renders with `--front` and its own site file;
the test VPS keeps admin SSH on 2222, so pass a site file with `admin_ssh_port = 2222` there.

## 11. Founder steps and open answers (DP 15.8)

1. The VPS's IPv4, IPv6 and SSH access (section 7 step 1).
2. The DNS records of section 4, and the DNS token (step 2).
3. Admin SSH stays on 22 (DP 1 answered c on 2026-11-07); the SSH gateway takes 2222.
4. Everything else is `deploy.sh`'s work.

Until an answer comes, the default stays:

| Question | Default | What an answer changes |
| --- | --- | --- |
| DP 3, the abuse contact | `support@bugnet.org` | `previews.abuse_contact` in `config/edge.toml` |
| DP 12, the plan's traffic | `edge_budget` defaults, edge `40MiB/s` | `edge_budget.bytes_per_10_minutes`, each `total_bytes_per_second` |
| DP 13, CAA at the apex | No apex records | Records once every issuer is named |
| DP 14, Payments' site | No `pay.bugnet.sh` line | A `names.toml` line and a backend, if served here |
| DP 15, IPv6 | Empty `ipv6` | `site.toml` `ipv6`, AAAA records, `secrets.egress_ips` |
| DP 18, the trace backend | No traces | Waits for Tasks 42 and 42B |
| CP 22, 23, image builds | Dev templates | The registry in `pins.conf`, the Boat image key at its prompt |

## 12. Not in this deploy yet

Each waits for its task to merge on `feat/router-v1-rest`; the deploy gains it then.

- `bugnet-nft.service`, `egress.nft` and `dev-front.nft` (Task 43B): `check.sh` skips the
  loopback rule of DP 10.12.7.1 (who may connect to `127.0.0.1:9443` and `:19443`).
- Task 43, the source-address check in the router: no setting yet.
- `[telemetry]` and `BUGNET_OTLP_AUTHORIZATION` (Tasks 42, 42B): no traces.
- `[health]` probe workers and canaries (Task 33), and `[pools]` (Tasks 40 to 40D): `check.sh`'s
  pool floor counts them once their tables exist, 44 then.
- Tasks 32 (limit polling), 34 (reconcile), 35 (capacity and spend), 36 (abuse tools), 37 (the
  leak test) and 38 (benchmarks and retention): no settings or units.
- Tasks 41 and 41B, the base image: no `[images]` blocks; E2B keeps the dev templates and stays
  out of `secrets.providers`.
- Task 29 (the wallet byte cap) and Task 30 (the estimator's learning loop): defaults only.
- Task 31, webhooks: `hooks.providers = []`, so no webhook secrets.
- The mining pool list: none is published, so `/etc/bugnet/mining-pools.txt` is absent and the
  router logs that harden blocks no pools.
- `vm-test.sh deploy`: not written; the controller runs the deploy against a VM or the VPS.
