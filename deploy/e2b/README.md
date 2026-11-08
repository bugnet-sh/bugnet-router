# E2B templates

E2B fixes a sandbox's vCPUs and memory when its template is built, so the router sells each
size as a template of its own. `template.ts` defines the image, E2B's Debian base image with
`root` as the default user. `build.ts` builds the templates `templates.json` lists with the E2B
SDK's build system, production ones or dev ones:

| Template | vCPU | Memory | CA baked in | Built by |
| --- | --- | --- | --- | --- |
| `bugnet-2x2` | 2 | 2048 MiB | production | `pnpm run build` |
| `bugnet-4x8` | 4 | 8192 MiB | production | `pnpm run build` |
| `bugnet-dev-2x2` | 2 | 2048 MiB | dev | `pnpm run build:dev` |
| `bugnet-dev-4x8` | 4 | 8192 MiB | dev | `pnpm run build:dev` |

The router uploads `bugnet-run` into every sandbox, so the image needs nothing else. The build
disables and masks the image's own `ssh.service` and `ssh.socket` and deletes its
`/etc/ssh/ssh_host_*` keys, so the only sshd in a sandbox is the one `bugnet-run ssh-up` starts on
127.0.0.1 behind the gate (DP 6.9).

The build also bakes the Bugnet CA (DP 6.9, 10.6). The certificate lands byte for byte in
`/usr/local/share/ca-certificates/bugnet-egress-v<version>.crt` and `update-ca-certificates` runs,
so `bugnet-run setup` finds the file by the SHA-256 the router sends and answers `checked`.
`templates.json` names, per template, which CA it takes. The production CA is
`ca.production.cert_file`, empty until the production CA exists. The dev CA is made once on the
Mac, outside every repository, with
`bugnet-egress ca init --out ~/.local/state/bugnet-dev/egress-ca` in `bugnet-edges`. A file that
holds a private key is refused.

A sandbox started from a template without the CA answers `ca_mismatch` once the router has
`secrets.ca_cert_file`. So rebuild the templates first, then point the router at the certificate
with the same `ca.<name>.version` as `secrets.ca_version`.

## Building

Templates belong to one E2B project, and a project lives in one region. Build them with that
project's API key, using Node.js 22.19 or later and pnpm:

```sh
cd deploy/e2b
pnpm install --frozen-lockfile
E2B_API_KEY=e2b_... pnpm run build       # production templates
E2B_API_KEY=e2b_... pnpm run build:dev   # dev templates, with the dev CA
```

For the EU region (EU-West-1), once E2B has enabled it for the account, use the EU project's
key and add `E2B_DOMAIN=e2b-juliett.dev`. The router then needs `domain = "e2b-juliett.dev"` in
the `[e2b]` table of its settings file too.

The build prints each template's ID. A sandbox can name a template by ID or by its name, and the
name stays the same across rebuilds. Give the router both sizes in its settings file, one
`[[e2b.templates]]` table each, with the disk the plan grants, 10 GiB on Hobby and 20 GiB on
Pro. `config/example.toml` holds the production names. A dev router names `bugnet-dev-2x2` and
`bugnet-dev-4x8` in the same tables:

```toml
[[e2b.templates]]
size = "2x2"
template = "bugnet-2x2"
vcpu = 2
memory_mib = 2048
disk_gib = 10

[[e2b.templates]]
size = "4x8"
template = "bugnet-4x8"
vcpu = 4
memory_mib = 8192
disk_gib = 10
```

`template` takes the name or the ID. The numbers must match the build, since the router prices
and picks sizes by them. The router's E2B key is `E2B_API_KEY` in its environment.

Rebuild after changing `template.ts`, or to pick up a newer kernel, which E2B fixes at build
time.
