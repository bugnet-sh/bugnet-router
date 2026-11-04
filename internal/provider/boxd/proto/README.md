# boxd API proto

`api.proto` is boxd's public gRPC API (`boxd.api.v1.BoxdApi`), vendored from
[github.com/boxd-sh/sdk](https://github.com/boxd-sh/sdk) under the MIT license in `LICENSE`.

| | |
|---|---|
| Source file | `proto/api.proto` |
| Commit | `9fc94e185819f2de28054b199e91a45efddf5f7f` (2026-11-02) |
| SHA-256 of the upstream file | `d7d4a75c9b226694cd9072ba80a0e0b70899bc4d4ff85cc0be9cffd719651a24` |

The only change is a three-line header naming the license and the commit. The file has no
`option go_package`, so `../buf.gen.yaml` maps it to `boxdpb` with an `M` option.

## Regenerating

From `internal/provider/boxd`, with `protoc-gen-go` v1.36.12 and `protoc-gen-go-grpc` v1.6.0 on
`PATH`:

```sh
buf generate
```

To update, replace the body of `api.proto` with the file at a newer commit, keep the header with
the new commit, update the table above, regenerate and commit the generated code with it.
