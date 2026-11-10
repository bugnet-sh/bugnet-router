# E2B templates

E2B fixes a sandbox's vCPUs and memory when its template is built, so the router sells each
size as a template of its own. `template.ts` starts every template from the Bugnet base image
by digest (`../image`) and runs `setup-image.sh finish --target e2b`, which masks E2B's own sshd
again and rewrites the manifest after E2B's provisioning. `build.ts` builds one template per size
`templates.json` lists:

| Template | vCPU | Memory |
| --- | --- | --- |
| `bugnet-<v>-2x2` | 2 | 2048 MiB |
| `bugnet-<v>-4x8` | 4 | 8192 MiB |

`<v>` is the image version with dashes, such as `2026-11-1`. The dev channel names them
`bugnet-dev-<v>-<size>`. The CA is baked in the image, so the templates carry no certificate of
their own. `build.ts` reads `IMAGE_REF`, `IMAGE_VERSION` and `IMAGE_CHANNEL` from its environment,
and writes its part of the build record to `E2B_RECORD` when set.

## Building

Templates belong to one E2B project, and a project lives in one region. Build them with that
project's API key, using Node.js 22.19 or later and pnpm:

```sh
cd deploy/e2b
pnpm install --frozen-lockfile
IMAGE_REF=... IMAGE_VERSION=2026.11.1 E2B_API_KEY=e2b_... pnpm run build  # prod channel
IMAGE_REF=... IMAGE_VERSION=2026.11.1 E2B_API_KEY=e2b_... pnpm run build:dev  # dev channel
```

For the EU region (EU-West-1), once E2B has enabled it for the account, use the EU project's
key and add `E2B_DOMAIN=e2b-juliett.dev`. The router then needs `domain = "e2b-juliett.dev"` in
the `[e2b]` table of its settings file too.

The build prints each template's ID. A sandbox can name a template by ID or by its name, and the
name stays the same across rebuilds. Give the router both sizes in its settings file, one
`[[e2b.templates]]` table each, with the disk the plan grants, 10 GiB on Hobby and 20 GiB on
Pro. `config/example.toml` holds the production names. A router on the image names `bugnet-<v>-2x2` and
`bugnet-<v>-4x8`, or the `bugnet-dev-<v>` ones, in the same tables:

```toml
[[e2b.templates]]
size = "2x2"
template = "bugnet-2026-11-1-2x2"
vcpu = 2
memory_mib = 2048
disk_gib = 10

[[e2b.templates]]
size = "4x8"
template = "bugnet-2026-11-1-4x8"
vcpu = 4
memory_mib = 8192
disk_gib = 10
```

`template` takes the name or the ID. The numbers must match the build, since the router prices
and picks sizes by them. The router's E2B key is `E2B_API_KEY` in its environment.

Rebuild after changing `template.ts`, or to pick up a newer kernel, which E2B fixes at build
time.
