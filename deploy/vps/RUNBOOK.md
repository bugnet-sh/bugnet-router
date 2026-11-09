# Production runbook

How to deploy and redeploy Bugnet by hand, in order, with what each command prints. Every
command comes from the scripts beside this file, the Payments and website deploy notes, or the
production deploy of 2026-11-09. [README.md](README.md) explains why each piece is built the way
it is; this file is the order of operations.

Production is one VPS: Hostinger, Ubuntu 26.04.1 on x86_64, 1 vCPU, about 4 GB RAM, 48 GB disk,
`62.72.21.74` and `2a02:4780:c:ab47::1`. It is also the test VPS that fronts the Mac on the
`*.dev.bugnet.sh` names ([tunnel.md](tunnel.md)).

Placeholders used below: `<v>` a version, `<old>` the version before it, `bugnet@62.72.21.74`
the admin login.

## Owners

When this file says "Ask bugnet-<owner>", the fact is not written down anywhere yet.

| Owner | Owns |
| --- | --- |
| bugnet-d7 | Router, edges, egress, sessions and secrets roles, `ship.sh`, `deploy.sh`, `check.sh` |
| bugnet-22 | Payments and `vps-install.sh` |
| bugnet-ea | MCP gateway, the x402 index, the bridge `@bugnet/mcp`, the secret page |
| bugnet-7f | The website `bugnet.sh` and the session page |
| bugnet-65 | Demo filming; it sets production freezes |

## Live versions on 2026-11-09

| Part | Version |
| --- | --- |
| bugnet-router, bugnet-edges, bugnet-run | `b1ce86f`, `5a65a06`, `944471e` (redeployed 04:08Z) |
| Gateway | `b61aa2b` |
| Payments | `25cbc55` |
| Website | `c90a99c` |
| Secret page, session page | bundles handed over: `cf4988b` (bugnet-gateway), `9e11d2a` (bugnet-website) |

## 1. What runs where

Every port is on `127.0.0.1` unless it says otherwise. HAProxy splits TCP 443 by SNI
([names.toml](names.toml)) and passes the raw TLS on with PROXY v2.

| Service | Unit | Port | Public host |
| --- | --- | --- | --- |
| Admin SSH | the system's SSH server | `:2222` | `62.72.21.74:2222`, admins only |
| SNI split | `haproxy` | `:443`, `:2201` SSH gateway, `:2200` dev SSH | every name below |
| Edge | `bugnet-edge@<v>` | 8443, `:80` redirect, metrics 9101 | `preview.bugnet.sh`, `*.preview.bugnet.sh`, `transfer.bugnet.sh`, `hooks.bugnet.sh` |
| Secrets route | `bugnet-secrets@<v>` | 8445, metrics 9104 | `secrets.bugnet.sh` |
| Sessions role | `bugnet-sessions@<v>` | 8448, metrics 9105 | `sessions.bugnet.sh` |
| SSH gateway | `bugnet-ssh@<v>` | 2223, metrics 9102 | `ssh.bugnet.sh:2201` (`ssh.providers` is empty) |
| Egress | `bugnet-egress@<v>` | 9443, metrics 9103 | `egress.bugnet.sh` |
| Router API | `router-api@<v>` | 8080, stats 8082 | none |
| Router worker | `router-worker` | 8081 | none |
| Certificates | `bugnet-certs.timer` | none | keeps `*.preview.bugnet.sh` |
| Payments | `bugnet-payments@<v>` | 8447 internal, 8450 site, 8449 x402 signer | `pay.bugnet.sh` through the website |
| MCP gateway | `bugnet-gateway@<v>` | 8444 TLS front, 8787 plain (local checks) | `mcp.bugnet.sh` |
| x402 crawl | `bugnet-x402-crawl@<v>.timer` | none | none |
| Website (Caddy) | `bugnet-website@<v>` | 8446 | `bugnet.sh`, `www.bugnet.sh`, `pay.bugnet.sh`, `bugnet.org`, `www.bugnet.org` |
| Postgres | `postgresql` | socket only | none |
| Valkey | `valkey-server` or `redis-server` | `/run/valkey/valkey.sock` only | none |

- The website's Caddy sends `/api/stats/payments*` to 8450 (Payments) and other `/api/stats*` to
  8082 (router stats). Both must answer.
- Files: binaries in `/opt/bugnet/<router|edge|ssh|egress|certs|run>/<v>/` with a `current`
  link, Payments in `/opt/bugnet/payments/<v>`, the gateway in `/opt/bugnet/gateway/<v>`, the
  website in `/opt/bugnet/website/<v>`, settings in `/etc/bugnet/`, keys in `/etc/bugnet/keys/`.
- The dev front (tunnel user, dev backends 18443 to 19443, port 2200) shares this VPS.
- Not deployed yet: `bugnet-nft.service` (README section 12), so `check.sh` skips the loopback
  rule for 9443.

**Ports differ from the README.** README sections 7 and 11 say admin SSH on 22 and the SSH
gateway on 2222: that was the plan for a separate production server. Production is the test VPS,
so [site.toml](site.toml) wins: admin SSH on 2222, the SSH gateway on 2201.

## 2. First-time server setup

### 2.1 Done by hand once (founder)

On 2026-11-07 the founder made, on the VPS:

1. The admin user `bugnet`, with passwordless sudo and the Mac's key `~/.ssh/bugnet_deploy`.
2. Admin SSH on port 2222 only.
3. ufw left inactive. `deploy.sh` turns it on later.

The exact lines used for steps 1 and 2: Ask bugnet-d7.

Check from the Mac. It must log in with no password prompt:

```sh
ssh -p 2222 -i ~/.ssh/bugnet_deploy bugnet@62.72.21.74
```

`ship.sh` calls plain `ssh` and `scp` with no `-i`, so this key must also be offered by default
(ssh agent or `~/.ssh/config`). How the Mac is set up for that: Ask bugnet-d7.

### 2.2 Users and packages: made by the scripts

| Script | Installs |
| --- | --- |
| `deploy.sh` (router) | `haproxy postgresql ufw curl openssl` and `valkey-server` (or `redis-server` where the release has no valkey package); users `bugnet-router bugnet-edge bugnet-secrets bugnet-sessions bugnet-ssh bugnet-egress bugnet-certs`, group `valkey`; Postgres roles and databases `bugnet_router`/`bugnet` and `bugnet_payments`/`bugnet_payments` |
| `vps-install.sh` (Payments) | Node 26 at `/usr/bin/node` (NodeSource), pnpm 12.6.0, user `bugnet-payments` |
| Website README | Caddy from its apt repository, user `bugnet-website` |

The gateway user is made by hand. `check.sh` fails while a user in `site.toml`'s
`gateway_users` does not exist, so make it before the first router deploy. Same form as the
Payments README:

```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin bugnet-gateway
```

### 2.3 The Mac

`ship.sh` builds from four checkouts side by side: `bugnet-router`, `bugnet-edges`, `bugnet-run`,
`bugnet-proto`. It needs Go, `make`, `git`, `ssh` and `scp`. Payments and the gateway need Node 26
and pnpm 12.6.0.

## 3. DNS

All of `bugnet.sh` is at Cloudflare. Every record is **DNS only (grey cloud)**: HAProxy needs
the raw TLS, and Cloudflare's proxy would cap uploads at 100 MB, cut long connections and read
every request.

### 3.1 Import the production records

1. Open https://dash.cloudflare.com, click **bugnet.sh**, then **DNS, Records, Import and
   Export, Import DNS records**.
2. Choose the zone file below (kept as `docs/founder-decisions/bugnet-sh-production.zone` in the
   Bugnet workspace).
3. Make sure **Proxy imported DNS records is unchecked**, then click **Import**.
4. The list gains 26 records (12 names with A and AAAA, plus 2 CAA), all grey. Click any orange
   cloud to switch it to DNS only.

```text
$ORIGIN bugnet.sh.
$TTL 300
@           300 IN A     62.72.21.74
@           300 IN AAAA  2a02:4780:c:ab47::1
www         300 IN A     62.72.21.74
www         300 IN AAAA  2a02:4780:c:ab47::1
preview     300 IN A     62.72.21.74
preview     300 IN AAAA  2a02:4780:c:ab47::1
*.preview   300 IN A     62.72.21.74
*.preview   300 IN AAAA  2a02:4780:c:ab47::1
transfer    300 IN A     62.72.21.74
transfer    300 IN AAAA  2a02:4780:c:ab47::1
secrets     300 IN A     62.72.21.74
secrets     300 IN AAAA  2a02:4780:c:ab47::1
sessions    300 IN A     62.72.21.74
sessions    300 IN AAAA  2a02:4780:c:ab47::1
hooks       300 IN A     62.72.21.74
hooks       300 IN AAAA  2a02:4780:c:ab47::1
ssh         300 IN A     62.72.21.74
ssh         300 IN AAAA  2a02:4780:c:ab47::1
egress      300 IN A     62.72.21.74
egress      300 IN AAAA  2a02:4780:c:ab47::1
mcp         300 IN A     62.72.21.74
mcp         300 IN AAAA  2a02:4780:c:ab47::1
pay         300 IN A     62.72.21.74
pay         300 IN AAAA  2a02:4780:c:ab47::1
preview     300 IN CAA   0 issue "letsencrypt.org"
preview     300 IN CAA   0 issuewild "letsencrypt.org; validationmethods=dns-01"
```

Check from the Mac, a few minutes later. It prints `62.72.21.74`:

```sh
dig +short transfer.bugnet.sh
```

The 16 dev records (`*.dev.bugnet.sh`) were imported on 2026-11-08 from
`docs/founder-decisions/cloudflare-test-vps-records.txt` and stay.

### 3.2 bugnet.org

`names.toml` sends `bugnet.org` and `www.bugnet.org` to the website backend, and its Caddy gets
their certificate through the split, then redirects every path to `https://bugnet.sh`. So both
names must point at the VPS, DNS only. Their MX records stay with the mail host
(`support@bugnet.org`, `admin@bugnet.org`). The exact bugnet.org records as added: Ask bugnet-7f.

### 3.3 The DNS token (once per server)

`bugnet-certs` proves control of `bugnet.sh` by DNS-01 for the `*.preview.bugnet.sh` wildcard.
This VPS has had its token since 2026-11-08. For a new server:

1. https://dash.cloudflare.com/profile/api-tokens, **Create Token**, **Edit zone DNS**, **Use
   template**.
2. **Zone Resources:** Include, Specific zone, `bugnet.sh`.
3. **Client IP Address Filtering:** Is in, `62.72.21.74` and `2a02:4780:c:ab47::1`.
4. **Continue to summary**, **Create Token**, copy it once.
5. On the VPS:

   ```sh
   sudo install -d -m 0755 /etc/bugnet
   sudo install -m 0600 /dev/null /etc/bugnet/certs.env
   sudoedit /etc/bugnet/certs.env
   ```

   Type one line, `CF_DNS_API_TOKEN=` and the token. Never paste it anywhere else.
6. Check: `sudo ls -l /etc/bugnet/certs.env` shows `-rw-------` and `root root`.

## 4. Keys and public values

No private value appears in this file, in chat, or on screen. Private values are made on the
VPS, copied between files with `grep >>` (never printed), and edited with `sudoedit`.

### 4.1 Public values

| Public value | What it is | Where it goes | Print it again |
| --- | --- | --- | --- |
| `ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ5Uj5xh80oFhcYJCELNpDMzCVBjPg+RXdw/Dw6D0iK2` | The SSH gateway's host key. It must never change | `config/router.toml` `ssh.host_keys` | `ship.sh --keys` output |
| `izQzPuN7A8R6WyD4+wT2h1lMf7VnvlsJU1gFWapvi1k=` | Box key `k1`: the bridge and the secret page seal secrets to it, egress opens them | `config/router.toml` `secrets.box_public_keys`; pinned in bugnet-gateway `packages/seal/src/pinned.ts` | `ship.sh --keys` output |
| The router event public key | Checks the events the router signs for Payments | Payments `[router] event_public_key` (`vps-install.sh --event-key`) | `sudo bugnet-ops pubkey --event` |
| `loAtatGXwc4DPBvSFHHjSShoEDhB/eam3sZCFuYHzHw=` | Payments' order token public key: the router checks order tokens with it | `config/router.toml` `order_token_public_key` | `sudo ./vps-install.sh --pubkeys` |
| `AxsYqFSYfxJKJgj8iTCJhVWLi1mphAx6Sh3cMLMdqyyr` | Payments' fee payer, pays Solana fees | `config/router.toml` `wallets.facilitator_addresses`; fund 0.3 SOL | `--pubkeys` (`fee_payer=`) |
| `4JJ6XsUTFKQSCdJ6Qo8AQENvexWLtm5uHLeinbv4AScD` | The founder's cold wallet, the payment recipient. Its key never touches the server | Payments `[solana] recipient` (`--recipient`) | `--pubkeys` (`recipient=`) |
| `GWYeTu1JdQFJJGDx4Lobqka13iNxJk4qfvvWdJ2tDuqd` | Payments' hot refund wallet | fund $25 USDC | `--pubkeys` (`refund=`) |
| `DG9LtTcLRYgKHCvDkoxd4DcVLfCndQFFBnzVf8ACmwzA` | Payments' x402 allowance wallet, pays x402 sellers | Payments `[x402] allowance_wallet` (set by the script); fund $0.01 USDC | `--pubkeys` (`allowance=`) |
| `/etc/bugnet/egress-ca.pem` | The egress CA certificate sandboxes trust | `secrets.ca_cert_file`; the provider images | the file |
| `/etc/bugnet/internal-ca.pem`, `egress-x402-client.pem`, `payments/x402-server.pem` | x402 mTLS between egress and the Payments signer | installed by `deploy.sh mtls` | the files |

The wallet addresses are as printed on 2026-11-09; confirm with `--pubkeys` before funding.

### 4.2 Private files, by name only

- `/etc/bugnet/keys/`: `ssh_host_ed25519_key`, `egress-ca.key`, `egress_box_k1`,
  `egress-x402-client.key` (root 0600, reach services only as systemd credentials).
- `/etc/bugnet/<name>.env` (root 0600): `router`, `edge`, `secrets-route`, `sessions`, `ssh`,
  `egress`, `certs`, `payments`, `gateway`. The names each holds: README section 6.
- On the Mac: the x402 internal CA key in `~/.local/state/bugnet-dev/mtls`.
- With the founder: the cold wallet key, the Cloudflare token, the provider API keys, the Helius
  RPC key.

Back up `/etc/bugnet/keys/` off the VPS, encrypted. How it is backed up today: Ask bugnet-d7.

A new box key means a new gateway pin, then a new bridge and secret page, before the router lists
it (bugnet-gateway `CONTRIBUTING.md`, Sealing keys).

## 5. Router, edges, sessions, secrets and egress

One `ship.sh` run ships all of them: it builds `router-api`, `router-worker`, `bugnet-dev`,
`bugnet-edge` (also the secrets and sessions roles), `bugnet-egress`, `bugnet-ssh`,
`bugnet-certs` and the `bugnet-run` runner, renders the HAProxy maps, copies the bundle to
`~/bugnet-deploy/<v>/` on the VPS and runs `deploy.sh` there.

### 5.1 Before every ship

1. **Freeze.** Ask the founder or bugnet-65 whether a filming freeze is on. During one, no
   restart and no redeploy.
2. **Same head in all four repos.** Put `bugnet-router`, `bugnet-edges`, `bugnet-run` and
   `bugnet-proto` on the integration head you mean to ship, the same branch in each (production's
   commits are on `main` and `feat/router-v1-rest`). A stale sibling broke production once
   (section 11.1).
3. **Clean trees.** Commit everything. `ship.sh` builds what is checked out.

### 5.2 First time only

On the Mac, in `bugnet-router`:

1. Keys, made on the VPS:

   ```sh
   deploy/vps/ship.sh --keys bugnet@62.72.21.74
   ```

   It prints, after the build and copy lines:

   ```text
   deploy.sh: made the SSH gateway's host key, which must never change (DP 15.5)
   deploy.sh: made the egress CA
   deploy.sh: made the box key k1
   deploy.sh: wrote the router's secrets and tokens to /etc/bugnet/router.env

   Public values for the hand-offs (DP 15.5, Task 39 step 7):
     router ssh.host_keys:            ssh-ed25519 AAAA...
     router secrets.box_public_keys:  { id = "k1", key = "..." }
                                      and to the gateway team, to pin in the bridge and page
     Payments router.event_public_key: <base64>
     egress CA certificate:           /etc/bugnet/egress-ca.pem (public; scp it to the Mac for the image)

   Still to add by hand, with sudoedit (names only, README.md "Secrets"):
     E2B_API_KEY in /etc/bugnet/router.env
     ...
   Back up /etc/bugnet/keys off the VPS, encrypted. The SSH host key must never change.
   ```

   Paste the first two values into `config/router.toml` and commit. Send the event key to
   Payments (section 6.2) and the box key to bugnet-ea.

2. Provider keys, on the VPS. Add `E2B_API_KEY`, `BOAT_API_KEY`, `BOXD_API_KEY` and
   `BUGNET_GITHUB_TOKEN`, one `NAME=value` a line, no quotes, no comment after a value:

   ```sh
   sudoedit /etc/bugnet/router.env
   ```

3. x402 mTLS, on the Mac:

   ```sh
   go run ./cmd/bugnet-dev mtls init --out ~/bugnet-mtls
   deploy/vps/ship.sh --mtls ~/bugnet-mtls bugnet@62.72.21.74
   rm -rf ~/bugnet-mtls
   ```

   Expected on the VPS:

   ```text
   deploy.sh: appended BUGNET_X402_TLS_KEY to /etc/bugnet/payments.env
   deploy.sh: installed the x402 mTLS files and deleted this copy of both keys
   ```

   The CA key stays in `~/.local/state/bugnet-dev/mtls` on the Mac. The deploy notes also
   mention making this CA on the VPS; which way production used: Ask bugnet-d7.

4. Install Payments (section 6.2) so its public values exist. Put `order_token_public_key` and
   the fee payer in `wallets.facilitator_addresses` into `config/router.toml`, and commit.
   Production already has both. On a brand-new server there is a loop: `vps-install.sh` needs
   Postgres, which the router's first deploy installs, and that deploy refuses to start without
   `order_token_public_key`. How production got past it: Ask bugnet-d7.

5. Make the gateway user (section 2.2), then ship (5.3).

### 5.3 Ship

```sh
deploy/vps/ship.sh bugnet@62.72.21.74
```

`ship.sh` takes port 2222 from `site.toml`. The version defaults to
`<UTC date>-<bugnet-router short commit>`. Expected output, in order:

```text
bugnet-router <branch> <commit>
bugnet-edges <branch> <commit>
bugnet-run <branch> <commit>
bugnet-proto <branch> <commit>
ship.sh: building linux/amd64 binaries
ship.sh: rendering the HAProxy and bugnet-certs files from names.toml and .../deploy/vps/site.toml
ship.sh: copying the bundle to bugnet@62.72.21.74:bugnet-deploy/<v> over port 2222
ship.sh: running on the VPS: cd ~/bugnet-deploy/<v> && sudo ./deploy.sh <v> ./bin
deploy.sh: postgres takes peer logins on its socket only
deploy.sh: <valkey-server|redis-server>.service listens on /run/valkey/valkey.sock only
deploy.sh: installed <v> under /opt/bugnet
deploy.sh: haproxy serves 443, 2200 and 2201
deploy.sh: ufw already allows TCP 80 443 2200 2201 2222 alone
deploy.sh: migrating the database with router-api <v>
deploy.sh: starting router-api@<v>
deploy.sh: starting bugnet-edge@<v>
deploy.sh: starting bugnet-secrets@<v>
deploy.sh: starting bugnet-sessions@<v>
deploy.sh: starting bugnet-ssh@<v>
deploy.sh: starting bugnet-egress@<v>
deploy.sh: stopping router-api@<old>, which drains on its own
... one stopping line per unit ...
deploy.sh: running check.sh
check.sh: ok only bugnet-router opens the data stores, users: haproxy and the services bugnet-gateway
check.sh: ok pool_max_conns 48 is at least the workers' floor of 36
check.sh: ok no unit failed
check.sh: ok every service answers /readyz
check.sh: warning skipped: who may connect to 127.0.0.1:9443 and :19443 waits for bugnet-nft (Task 43B)
check.sh: ok egress payments_timeout 3s is above Payments' sign_timeout 2s
check.sh: ok x402 ports, files and settings checked
check.sh: ok the stats feed answers on 8082
deploy.sh: deployed <v>
```

- **Read the first four lines before the build ends.** All four must be the head you meant, with
  no `(uncommitted changes)`. If not, press Ctrl-C.
- The last line must be `deployed <v>`. Anything else means the deploy failed.
- On the first deploy, `ufw allows TCP 80 443 2200 2201 2222 and denies the rest, UDP 443
  included` replaces the `already` line, and `issuing the first wildcard pair into ...` can take a
  few minutes.
- `deploy.sh` stops before changing anything when a key, an env name or
  `order_token_public_key` is missing, and lists each under `deploy.sh: missing, see README.md
  "First deploy":`.
- A new instance that never gets ready stops every new instance; the old ones keep serving:
  `deploy.sh: <unit>@<v> never became ready, so every new instance stopped and the old ones
  serve`.
- Old instances drain on their own: edge 5 min, secrets and sessions 1 min, egress 5 min,
  router-api up to 31 min, SSH gateway up to 2 h. `router-worker` restarts.

Copy without running, then run the printed command on the VPS yourself:

```sh
deploy/vps/ship.sh --copy-only bugnet@62.72.21.74
```

### 5.4 Check

On the Mac. It answers with a `strict-transport-security` header:

```sh
curl -sI https://preview.bugnet.sh/
```

On the VPS, any time. Each check prints `check.sh: ok ...`; a failure prints
`check.sh: FAIL ...` and exits 1:

```sh
sudo /opt/bugnet/check.sh
sudo /opt/bugnet/check.sh x402
```

Names: `all`, `stores`, `pool`, `units`, `ready`, `x402`, `timeouts`, `stats`.

### 5.5 Page bundles

- **Session page** (bugnet-7f): the build in bugnet-website
  `packages/session-page/release/dist` goes into `/srv/bugnet/session-page/`. The sessions role
  reads it at start:

  ```sh
  sudo systemctl restart 'bugnet-sessions@*'
  ```

- **Secret page** (bugnet-ea): `pnpm --filter @bugnet/secret-page build` in bugnet-gateway, into
  `/srv/bugnet/secrets-page/`. Whether the secrets route needs a restart after a copy: Ask
  bugnet-d7.

Until a bundle arrives, `deploy.sh` serves a placeholder page. The copy command used for both:
Ask bugnet-7f and bugnet-ea.

### 5.6 Settings and operator commands

- Settings live in `config/`. A change is a commit and a ship, never an edit on the VPS.
  `deploy.sh` keeps the previous copy of each changed file in `/etc/bugnet/previous/`.
- `allowed_shared_hosts` in `[x402]` is written by `deploy.sh x402-index --router`, never by
  hand (section 7.1).
- E2B stays out of `secrets.providers` (so no x402 on E2B) until its templates carry the
  production egress CA.
- Operator commands run as `bugnet-router` with the router's environment, printing no secret:

  ```sh
  sudo bugnet-ops wallet show <payer>
  sudo bugnet-ops pubkey --event
  ```

## 6. Payments

Payments runs as `bugnet-payments@<v>` from `/opt/bugnet/payments/<v>`, with
`/etc/bugnet/payments.toml` (no secrets) and `/etc/bugnet/payments.env` (root 0600). Full notes:
bugnet-payments `deploy/README.md`. Start order: the router first, then Payments.

### 6.1 Build the release tarball (Mac)

In a bugnet-payments checkout at the release commit, with `../bugnet-proto` beside it:

```sh
pnpm install --frozen-lockfile
pnpm build
v=$(git rev-parse --short HEAD)
# On macOS, without these tar adds hidden ._ metadata files that unpack on Linux as real files
COPYFILE_DISABLE=1 tar --no-mac-metadata --no-xattrs -czf "bugnet-payments-$v.tgz" \
    dist migrations config vendor patches deploy package.json pnpm-lock.yaml pnpm-workspace.yaml
tar tzf "bugnet-payments-$v.tgz" | grep -c '/\._\|^\._'
```

The last line must print `0`. Any other number: rebuild (section 11.3). bugnet-22 keeps built
tarballs in `~/.local/state/bugnet-paid/release/`.

Copy the tarball and `deploy/vps-install.sh` from the same commit to the VPS, for example with
the `scp -P` form `ship.sh` uses:

```sh
scp -P 2222 "bugnet-payments-$v.tgz" deploy/vps-install.sh bugnet@62.72.21.74:
```

### 6.2 First install

On the VPS, as root. Run it after `ship.sh --keys`, so `/etc/bugnet/router.env` exists:

```sh
sudo ./vps-install.sh bugnet-payments-<v>.tgz \
    --recipient 4JJ6XsUTFKQSCdJ6Qo8AQENvexWLtm5uHLeinbv4AScD \
    --event-key <router event public key>
```

The router URL defaults to `http://127.0.0.1:8080`, router-api's address, so `--router-url` is
not needed. It prints addresses and names only:

```text
== installing Node 26
== node v26..., pnpm 12.6.0
== created user bugnet-payments
== unpacking <v> into /opt/bugnet/payments/<v>
== installing packages
== generating any missing keys in /etc/bugnet/payments.env (addresses only)
== BUGNET_PAYMENTS_ROUTER_TOKEN copied from /etc/bugnet/router.env
== wrote /etc/bugnet/payments.toml
== wrote an empty x402 index; pass --index with the gateway's index.json to fill it
== bugnet-payments@<v> enabled, not started (run --start once the settings are complete)
== public values (no secrets)
order_token_public_key=...
fee_payer=...
refund=...
allowance=...
recipient=4JJ6XsUTFKQSCdJ6Qo8AQENvexWLtm5uHLeinbv4AScD
== next: fund the wallets, set BUGNET_SOLANA_RPC_URL in /etc/bugnet/payments.env (sudoedit), run --mtls, then --start
```

The script drops the generated `BUGNET_RECIPIENT_KEY`: the recipient is the cold wallet.

Then, in order:

1. **Router values.** `order_token_public_key` and `fee_payer` go into `config/router.toml`
   (section 5.2 step 4). `sudo ./vps-install.sh --pubkeys` prints them again.
2. **Fund the wallets (founder).** From the founder's own wallet, on Solana mainnet:
   - `fee_payer`: 0.3 SOL. Boot refuses below 0.1 SOL.
   - `refund`: $25 USDC.
   - `allowance`: $0.01 USDC.
   - The recipient's USDC account must already exist.

   The Payments README creates missing USDC accounts with `pnpm cli keys fund
   --create-refund-ata`, `--create-allowance-ata` and `--create-recipient-ata <address>`. How
   they were run against the keys on the VPS: Ask bugnet-22.
3. **RPC URL.** A Helius free plan (https://dashboard.helius.dev/signup?plan=free). Add the line
   `BUGNET_SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=<KEY>`:

   ```sh
   sudoedit /etc/bugnet/payments.env
   ```

4. **mTLS.** Done by `ship.sh --mtls` (section 5.2 step 3). Without it, use the staging folder
   of `bugnet-dev mtls init`:

   ```sh
   sudo ./vps-install.sh --mtls <staging dir>
   ```

5. **Gateway token.** Copy `BUGNET_PAYMENTS_TOKEN` to the gateway without printing it (the
   README's copy pattern):

   ```sh
   sudo sh -c 'grep "^BUGNET_PAYMENTS_TOKEN=" /etc/bugnet/payments.env >> /etc/bugnet/gateway.env'
   ```

6. **Start**, once the router is deployed:

   ```sh
   sudo ./vps-install.sh --start
   ```

   Expected: `== bugnet-payments@<v> is up on 8447 and 8450`. It refuses to start and names each
   gap (`/etc/bugnet/payments.env has no <NAME>`, `missing <file>`, `fill the <...> values in
   /etc/bugnet/payments.toml`).

### 6.3 Upgrade

On the Mac, build and copy the new tarball (6.1). On the VPS, with that release's
`vps-install.sh`:

```sh
sudo ./vps-install.sh bugnet-payments-<new>.tgz
sudo ./vps-install.sh --start
```

Expected from `--start`:

```text
== stopped bugnet-payments@<old>.service
== bugnet-payments@<new> is up on 8447 and 8450
```

Two versions cannot share the ports, so `--start` stops the old one first. Meanwhile egress
answers `payments_unavailable`. Existing keys, `payments.toml` and the index are kept.

### 6.4 Check

```sh
curl -fsS http://127.0.0.1:8447/healthz
curl -fsS http://127.0.0.1:8450/healthz
journalctl -u "bugnet-payments@<v>" -o cat | grep '"level":"alarm"'
sudo /opt/bugnet/check.sh x402
```

A failed boot check is one log line, `"msg":"boot check failed"`, whose `reason` names the fix.
Operator commands (`pcli`): Payments README section 8. x402 incidents: its section 11.

## 7. Gateway release and the x402 index

### 7.1 The reviewed x402 index

`packages/gateway/src/x402/index.json` in the gateway release is the one reviewed list of x402
services. Payments, the router and the gateway each keep a copy, updated in this order (x402
spec 7.16, Payments README section 10, gateway `CONTRIBUTING.md`):

1. Payments, then `SIGHUP`.
2. The router's `allowed_shared_hosts`, which rolls router-api.
3. The gateway release.

README section 9 and `deploy.sh`'s hint (`deploy the gateway, then run x402-index --router`) put
the gateway before the router. Follow the order above.

Copy the release's `index.json` to the VPS as `~/index.json`, then:

```sh
sudo ~/bugnet-deploy/<v>/deploy.sh x402-index ~/index.json
sudo ~/bugnet-deploy/<v>/deploy.sh x402-index --router ~/index.json
```

`<v>` is the router version deployed now. Expected:

```text
deploy.sh: Payments has the index; deploy the gateway, then run x402-index --router
deploy.sh: router.toml: allowed_shared_hosts = [...]
deploy.sh: starting router-api@<v>_r<timestamp>
deploy.sh: stopping router-api@<v>, which drains on its own
check.sh: ...
```

A broken file stops at the first command and changes nothing. Later router deploys keep the line,
generated from `/etc/bugnet/router-x402-index.json`.

### 7.2 Install or upgrade the gateway

The release is a self-contained esbuild bundle, no `node_modules`: a folder holding `deploy/`
(the units) and `packages/gateway/` (`dist/main.js`, `dist/x402-crawl.js`, `package.json`,
`src/x402/index.json`). How bugnet-ea builds it: Ask bugnet-ea. It needs Node 26 at
`/usr/bin/node`, which the Payments install puts there.

First time only:

1. `mcp.bugnet.sh` resolves to the VPS (section 3). The gateway gets its certificate by
   TLS-ALPN-01 through HAProxy's passthrough.
2. User `bugnet-gateway` (section 2.2).
3. Settings: the private `docs/mcp-gateway/gateway-production.toml` (Bugnet workspace) becomes
   `/etc/bugnet/gateway.toml`, owner `root:bugnet-gateway`, mode 0640.
4. Secrets, root 0600:

   ```sh
   sudo install -o root -g root -m 0600 /dev/null /etc/bugnet/gateway.env
   sudo sh -c 'grep "^BUGNET_INTERNAL_TOKEN=" /etc/bugnet/router.env >> /etc/bugnet/gateway.env'
   ```

   Plus `BUGNET_PAYMENTS_TOKEN` (section 6.2 step 5), and `BUGNET_QUOTE_KEY`, a new base64
   value of 32 or more bytes (how it was made: Ask bugnet-ea). Optional: `GITHUB_TOKEN` and
   `HF_TOKEN`, read access to public data only.

Every release:

1. Copy the release folder to `/opt/bugnet/gateway/<v>/` (the notes say `rsync -a`; the exact
   command: Ask bugnet-ea).
2. Install the units from it, same form as Payments:

   ```sh
   sudo cp deploy/bugnet-gateway@.service deploy/bugnet-x402-crawl@.service \
       deploy/bugnet-x402-crawl@.timer /etc/systemd/system/
   sudo systemctl daemon-reload
   ```

3. The index (7.1), steps 1 and 2.
4. Start the new version and its crawl timer, then stop the previous version's:

   ```sh
   sudo systemctl enable --now bugnet-gateway@<v>
   sudo systemctl enable --now bugnet-x402-crawl@<v>.timer
   sudo systemctl disable --now bugnet-gateway@<old>
   sudo systemctl disable --now bugnet-x402-crawl@<old>.timer
   ```

   Whether two gateway versions can run side by side on 8444 and 8787: Ask bugnet-ea.
5. Check. It answers 200:

   ```sh
   curl -fsS https://mcp.bugnet.sh/healthz
   ```

## 8. Website

Owner: bugnet-7f, who runs every website deploy. Runbook: `internal/deploy/website/README.md` in
the Bugnet workspace (Caddy setup, `deploy.sh --dry-run` first, `BUGNET_DEPLOY_PORT=2222`, roll
back). The router side it needs is already in place: the `names.toml` lines for `bugnet.sh`,
`www.bugnet.sh`, `pay.bugnet.sh`, `bugnet.org` and `www.bugnet.org`, and `backend website` on
`127.0.0.1:8446`. `pay.bugnet.sh` is served by the website's Caddy, which ends TLS and proxies to
Payments on 8450.

## 9. npm: `@bugnet/mcp`

The bridge is bugnet-gateway's `packages/bridge`, published as `@bugnet/mcp` (version 0.1.2 on
gateway `main`). Its `prepack` builds `dist/main.js` and refuses a development box key. Founder
steps:

1. An npm account with 2FA, and the npm org `bugnet` (made 2026-11-09).
2. Log in:

   ```sh
   npm login
   ```

3. In bugnet-gateway at the release commit, in `packages/bridge`, a dry run, then the publish:

   ```sh
   pnpm publish --access public --dry-run
   pnpm publish --access public
   ```

4. **The staged-review hold.** npm listed a stray `0.0.0-stage` version beside 0.1.0 on
   2026-11-09. What holds a publish for review and how to release it: Ask bugnet-ea.

## 10. Rollback

### 10.1 Router, edges, sessions, secrets, egress

Every shipped bundle stays in `~/bugnet-deploy/<version>/` on the VPS, and
`/etc/bugnet/deployed` holds `current=<v>` and `previous=<old>`. Deploy the old bundle again; it
rolls every unit and puts back that version's settings:

```sh
cd ~/bugnet-deploy/<old> && sudo ./deploy.sh <old> ./bin
```

The last line must be `deploy.sh: deployed <old>`. A version that is already running gets a fresh
instance named `<old>_r<timestamp>`. Before rolling back across a database migration: Ask
bugnet-d7.

### 10.2 Payments

With the old tarball still on the VPS, the script switches back (it skips unpacking a version
already in `/opt/bugnet/payments/`):

```sh
sudo ./vps-install.sh bugnet-payments-<old>.tgz
sudo ./vps-install.sh --start
```

Or by hand, which leaves `/etc/bugnet/payments.release` naming the newer version, so a later
`--start` would start it again:

```sh
sudo systemctl disable --now bugnet-payments@<new>
sudo systemctl enable --now bugnet-payments@<old>
curl -fsS http://127.0.0.1:8447/healthz
curl -fsS http://127.0.0.1:8450/healthz
```

Before rolling back across a Payments migration: Ask bugnet-22.

### 10.3 Gateway

```sh
sudo systemctl disable --now bugnet-gateway@<new>
sudo systemctl disable --now bugnet-x402-crawl@<new>.timer
sudo systemctl enable --now bugnet-gateway@<old>
sudo systemctl enable --now bugnet-x402-crawl@<old>.timer
curl -fsS https://mcp.bugnet.sh/healthz
```

If the index changed with that release, put the old `index.json` back in the 7.1 order.

### 10.4 Website, pages

- Website: the Roll back section of `internal/deploy/website/README.md` (bugnet-7f).
- Session and secret pages: copy the previous build back (5.5). Where previous builds are kept:
  Ask bugnet-7f and bugnet-ea.

## 11. Incidents and lessons (2026-11-09)

### 11.1 `bad_spec`: mixed repo versions

- **What happened.** The first production x402 test, off camera, failed with `bad_spec`. The
  router (with Task 43B) sends the egress spec field `source`, but `ship.sh` had built
  `bugnet-run` from a stale branch in the controller's lane (`d0aabdc`).
- **Fix.** Redeployed at 04:08Z with router `b1ce86f`, edges `5a65a06`, run `944471e`. The
  rerun passed.
- **Lesson.** Build all four sibling repos at the same integration head, and read `ship.sh`'s
  first four lines every time (5.3). A guard inside `ship.sh` is a follow-up.

### 11.2 Payments code folder 0700

- **What happened.** `vps-install.sh` at `4ca57b3` runs with `umask 077` for the secrets, so
  `/opt/bugnet/payments/<v>` came out 0700: root-owned and closed to `bugnet-payments`, the user
  the service runs as.
- **Fix.** `03d8ed4`: the script creates the code folders 0755 and runs
  `chmod -R go-w,a+rX` on the version folder.
- **Lesson.** Use `vps-install.sh` from `03d8ed4` or later. A script that sets a strict umask must
  set code folder modes itself.

### 11.3 `._` files in the Payments migrations

- **What happened.** The `4ca57b3` tarball, built on macOS, carried 31 AppleDouble `._` files.
  On Linux they unpack as real files, and the ones under `migrations/` broke the migrations. They
  were removed by hand before the start.
- **Fix.** `2514668`: migrations run only `NNNNN_name.sql` files, and the tarball is built with
  `COPYFILE_DISABLE=1 tar --no-mac-metadata --no-xattrs` (6.1).
- **Lesson.** Always run the `grep -c` check of 6.1; it must print `0`.

### 11.4 Phantom Lighthouse refusals

- **What happened.** On a public site, Phantom adds Lighthouse program assertion instructions
  around the USDC transfer, some naming the fee payer, and Payments refused those payments.
  `bed54a3` accepted them after Payments' own 4 instructions, but the next payment was refused
  (`instructions_length`): Phantom adds them before the transfer too.
- **Fix.** `20454a2` finds the transfer after leading assertions and passed a real Phantom payment
  on the test host. Review found a hole: Lighthouse `MemoryWrite` (discriminator 0) and
  `MemoryClose` (1) take a signer payer, so letting them name the fee payer lets a buyer make it
  fund memory accounts. The founder ruled that only Lighthouse assertions (discriminator 2 or
  more) may name the fee payer. `25cbc55` does that and is live.
- **Open.** The next Payments release applies the same rule to the skipped leading Lighthouse
  instructions.
- **Lesson.** Test any change to the payment checks with a real Phantom payment on the test host
  before production. Refused payments log their transaction (`1c48a36`); payment headers are
  redacted, so payloads are never stored.

## 12. Final checklist

Before:

- [ ] No filming freeze (founder or bugnet-65).
- [ ] The four Go repos on one integration head, trees clean.
- [ ] Payments tarball: `grep -c` printed `0`.

Router side:

- [ ] `ship.sh` ended with `deploy.sh: deployed <v>`.
- [ ] Every `check.sh` line is `ok`, apart from the `bugnet-nft` warning.
- [ ] `curl -sI https://preview.bugnet.sh/` shows `strict-transport-security`.

Payments, gateway, website:

- [ ] `vps-install.sh --start` printed `is up on 8447 and 8450`.
- [ ] Wallets funded: fee payer 0.3 SOL, refund $25 USDC, allowance $0.01 USDC.
- [ ] Index in order: Payments, router, gateway.
- [ ] `curl -fsS https://mcp.bugnet.sh/healthz` answers.
- [ ] Website deployed by bugnet-7f; `pay.bugnet.sh` and `bugnet.org` answer through it.

After:

- [ ] `sudo /opt/bugnet/check.sh x402` passes.
- [ ] No secret value was printed, pasted or committed.
- [ ] `/etc/bugnet/keys/` backed up off the VPS, encrypted, after any key change.
