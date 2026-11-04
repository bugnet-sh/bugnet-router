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
key and add `E2B_DOMAIN=e2b-juliett.dev`. The router then needs
`BUGNET_E2B_DOMAIN=e2b-juliett.dev` too.

The build prints each template's ID. A sandbox can name a template by ID or by its name, and the
name stays the same across rebuilds. Give the router both sizes with the disk the plan grants,
10 GiB on Hobby and 20 GiB on Pro:

```sh
BUGNET_E2B_TEMPLATES=2x2=bugnet-2x2:2:2048:10,4x8=bugnet-4x8:4:8192:10
```

Each entry is `size=templateID:vcpu:memoryMiB:diskGiB`. The numbers must match the build, since
the router prices and picks sizes by them.

Rebuild after changing `template.ts`, or to pick up a newer kernel, which E2B fixes at build
time.
