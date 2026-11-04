# Boat API spec

`boat-v1.yaml` is Boat's public OpenAPI document, stored unchanged as served on the date below.

| | |
| --- | --- |
| Source | <https://docs.boat.dev/openapi/boat-v1.yaml> |
| Fetched | 2026-11-04, `Last-Modified: Tue, 03 Nov 2026 13:46:31 GMT` |
| Version | OpenAPI 3.1.0, `info.version` 1.0.0, 349,088 bytes |
| SHA-256 | `9f2abcf0735ba6230fba1b40ac9a11fe9022348997152821f3b99a1f7c2421fc` |

`shasum -a 256 -c` checks it from this directory:

```text
9f2abcf0735ba6230fba1b40ac9a11fe9022348997152821f3b99a1f7c2421fc  boat-v1.yaml
```

## Terms

The document states no license. Boat's [Terms of Service](https://boat.dev/terms) (version
2026-10-12, Dedale AI, Corp.) cover its APIs and documentation and say Dedale or its licensors
own them. The file is kept here only to generate and check this client.

## Regenerating

`../api` holds the client oapi-codegen generates for the operations the adapter calls. With
default options the output does not compile, since schema names collide with the response
types named after lowercase operation ids, so `resolve-type-name-collisions` is on. The
`webhooks` section refers to schemas pruned with the unused operations, so `overlay.yaml`
drops it while generating and the document stays as fetched.

From `internal/provider/boat`, with oapi-codegen v2.8.2 on `PATH`:

```sh
oapi-codegen -config oapi-codegen.yaml spec/boat-v1.yaml
```

To update, replace `boat-v1.yaml` with a newer copy, update the table above, regenerate and
commit the generated code with it.
