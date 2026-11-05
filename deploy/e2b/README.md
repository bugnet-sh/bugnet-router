# E2B templates

E2B fixes a sandbox's vCPUs and memory when its template is built, so the router sells each
size as a template of its own. `template.ts` defines the image, E2B's Debian base image with
`root` as the default user. `build.ts` builds it twice with the E2B SDK's build system:

| Template | vCPU | Memory |
| --- | --- | --- |
| `bugnet-2x2` | 2 | 2048 MiB |
| `bugnet-4x8` | 4 | 8192 MiB |

The router uploads `bugnet-run` into every sandbox, so the image needs nothing else.

## Building

Templates belong to one E2B project, and a project lives in one region. Build them with that
project's API key, using Node.js 22.19 or later and pnpm:

```sh
cd deploy/e2b
pnpm install --frozen-lockfile
E2B_API_KEY=e2b_... pnpm run build
```

For the EU region (EU-West-1), once E2B has enabled it for the account, use the EU project's
key and add `E2B_DOMAIN=e2b-juliett.dev`. The router then needs `domain = "e2b-juliett.dev"` in
the `[e2b]` table of its settings file too.

The build prints each template's ID. A sandbox can name a template by ID or by its name, and the
name stays the same across rebuilds. Give the router both sizes in its settings file, one
`[[e2b.templates]]` table each, with the disk the plan grants, 10 GiB on Hobby and 20 GiB on
Pro. `config/example.toml` and `config/dev.toml` already hold these:

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
