# The tunnel from the VPS to the Mac

Before the production deploy, the VPS fronts the Mac for live tests (DP 15.6, ruling R1). HAProxy
on the VPS sends the dev names to five loopback ports. A reverse SSH tunnel from the Mac listens
on those ports and carries each connection to the Mac's own services. No relay code runs anywhere.

```text
user, sandbox ─► VPS HAProxy ─► 127.0.0.1:18443 / 18445 / 18448 / 19443 / 12223
              ─► ssh -R ─► Mac 8443 / 8445 / 8448 / 9443 / 2223
```

| VPS port | HAProxy backend | Mac port | Mac service |
|---|---|---|---|
| 18443 | `edge_dev` | 8443 | `bugnet-edge` |
| 18445 | `secrets_dev` | 8445 | `bugnet-edge --role secrets` |
| 18448 | `sessions_dev` | 8448 | `bugnet-edge --role sessions` |
| 19443 | `egress_dev` | 9443 | `bugnet-egress` |
| 12223 | `ssh_dev`, from port 2200 | 2223 | `bugnet-ssh` |

router-api stays on `127.0.0.1:8080` on the Mac and is never forwarded. Each Mac service accepts
PROXY v2 from `127.0.0.1`, which is where the tunnel exits.

## The key, once on the Mac

```sh
ssh-keygen -t ed25519 -N "" -C mac -f ~/.ssh/bugnet_tunnel
```

The public half goes into the front bundle as `tunnel.pub` (bugnet-router README, The dev front).
The private half never leaves the Mac. It opens nothing but these five listeners.

## The `tunnel` user on the VPS

`front.sh` makes it. Running `front.sh` again changes nothing.

- It is a system user with `/usr/sbin/nologin` as its shell, and a password field of `*`. That
  field allows no password but is not locked, so sshd accepts the key with or without PAM.
- `/home/tunnel/.ssh/authorized_keys` belongs to root, group `tunnel`, mode 0640, so the user
  cannot change its own limits. It holds DP 15.6's line, shown here in five parts:

  ```text
  restrict,port-forwarding,
  permitlisten="127.0.0.1:18443",permitlisten="127.0.0.1:18445",
  permitlisten="127.0.0.1:18448",permitlisten="127.0.0.1:19443",
  permitlisten="127.0.0.1:12223"
  ssh-ed25519 AAAA... mac
  ```

  In the file they are one line, joined with no spaces after the commas and one space before
  `ssh-ed25519`.

- `/etc/ssh/sshd_config.d/bugnet-tunnel.conf` holds the `Match User tunnel` block. It allows
  remote forwarding only, to the same five listeners, and nothing else: no local forwarding, no
  `-W`, no terminal, no agent, no X11, no command. It ends a session whose Mac stops answering.
- The five ports are read from the dev backends of `haproxy.cfg`, so the tunnel may listen only
  where HAProxy dials.
- `front.sh` checks the block with `sshd -t` before it reloads sshd, and keeps the old settings
  when that fails.

## Keeping the tunnel up from the Mac

Install autossh once with `brew install autossh`. Then start every service on loopback with the
dev names (the live stack's fronted mode), and run:

```sh
autossh -M 0 -N -p <admin_ssh_port> -i ~/.ssh/bugnet_tunnel \
  -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes \
  -R 127.0.0.1:18443:127.0.0.1:8443 \
  -R 127.0.0.1:18445:127.0.0.1:8445 \
  -R 127.0.0.1:18448:127.0.0.1:8448 \
  -R 127.0.0.1:19443:127.0.0.1:9443 \
  -R 127.0.0.1:12223:127.0.0.1:2223 \
  tunnel@<VPS IPv4>
```

- `<admin_ssh_port>` is `site.toml`'s value: 2222 under DP question 1 (a), 22 under (b).
- The first connection asks whether to trust the VPS's host key. Compare its fingerprint with
  `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub`, run on the VPS over the admin SSH.
- `ExitOnForwardFailure=yes` makes ssh exit when a port is still held, for example by a session
  the VPS has not dropped yet. autossh then starts it again.
- A dead session ends on both sides in about 45 s, 15 s times 3 missed replies. On the Mac,
  `ServerAliveInterval` and `ServerAliveCountMax` end ssh. On the VPS, the tunnel block's
  `ClientAliveInterval 15` and `ClientAliveCountMax 3` end the session and free the five ports.
- Ctrl-C stops it.

## Checking it

On the VPS, while autossh runs:

```sh
ss -ltn | grep -E '127\.0\.0\.1:(18443|18445|18448|19443|12223) '
```

All five ports show, on `127.0.0.1` only. With the Mac's edge running, a request to a dev name
reaches it:

```sh
curl -sI https://preview.dev.bugnet.sh/
```

## Limits in dev

- On the Mac any local process can reach `127.0.0.1:9443`, so the egress source check proves less
  in dev than on the VPS (DP 10.12.7.1).
- On the VPS, until the production deploy loads the loopback rule (`bugnet-nft.service`), any
  local user can connect to the tunnel's five ports. The dev front runs no other service, so only
  the admin users can.
