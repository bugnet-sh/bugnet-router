# E2B API specs

These files come unchanged from [github.com/e2b-dev/E2B](https://github.com/e2b-dev/E2B) at
release `e2b@2.62.2`, under the Apache License 2.0 in `LICENSE`. The project ships no NOTICE
file.

| | |
| --- | --- |
| Release | `e2b@2.62.2`, published 2026-11-03 |
| Commit | `9301cd84757b0e3f3fccbd349e85ff2bd403a1f4` |

Each file sits at the same path under `spec/` upstream. Their SHA-256 sums, which
`shasum -a 256 -c` checks from this directory:

```text
99fccbc60e6d0cae998703e1f5f7ad727a3cf86b013fe37d9545e8610673ff98  openapi.yml
8edd9358c7dbfcad96796b3f0ed8d14c262b8b14d6bc7d5e84d468941511b8e0  envd/process/process.proto
5024f6f6c9a62cab94e29a209389313fd259c8eaa847b5093356308dc2f711b9  envd/filesystem/filesystem.proto
```

`openapi.yml` is E2B's REST API. `../api` holds the client oapi-codegen generates from the whole
file. The protos are envd's Process and Filesystem services, and `../envdpb` holds the Go and
Connect code generated from them. They have no `option go_package`, so `../buf.gen.yaml` maps
them with `M` options. envd's HTTP API (`spec/envd/envd.yaml`) is not vendored, since the
adapter calls its `/health` and `/files` endpoints with plain HTTP requests.

## Regenerating

From `internal/provider/e2b`, with oapi-codegen v2.8.2, protoc-gen-go v1.36.12,
protoc-gen-connect-go v1.21.1 and buf 1.75.0 on `PATH`:

```sh
oapi-codegen -config oapi-codegen.yaml spec/openapi.yml
buf generate
```

To update, replace the files with those of a newer release, update the tables above, regenerate
and commit the generated code with them.
