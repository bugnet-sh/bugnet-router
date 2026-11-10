# The Bugnet base image

One definition builds the image every provider starts from (CP 19). `setup-image.sh` is the one
script. The Dockerfile runs it for E2B and Koyeb, and a builder sandbox runs it for boxd and Boat.

| File | What it holds |
|---|---|
| `VERSION` | The image version, such as `2026.11.1` |
| `pins.conf` | Every pin, `KEY=VALUE` lines, no secret |
| `Dockerfile` | The pinned base image, the payload and `setup-image.sh build --target oci` |
| `setup-image.sh` | `build`, `finish` and `probe`, run as root |
| `build.sh` | The payload, its scan, the OCI build, the image scan, the push and the record |
| `inputs.sha256` | The hash of every input and the version it was taken at |
| `builds/` | One JSON record per channel and version, committed |

## Building

```sh
make -C ../bugnet-run build
deploy/image/build.sh --channel dev --ca ~/path/bugnet-egress-v1.crt
```

The CA file is named `bugnet-egress-v<N>.crt`, stays outside every repository, and its SHA-256 is
the pin `CA_SHA256_<CHANNEL>_V<N>` in `pins.conf`. `--no-push` stops after the image scan. The
payload holds `setup-image.sh`, `pins.conf`, `VERSION`, the `bugnet-run` build and each CA.

## Inputs and versions

The inputs are `Dockerfile`, `setup-image.sh`, `pins.conf`, `../e2b/template.ts` and
`../e2b/build.ts`. `make image-inputs` rewrites `inputs.sha256` once `VERSION` moved, or while no
`builds/<channel>-<VERSION>.json` exists. After a version's first build an input change needs a
new `VERSION`, and `TestImageInputsNeedANewVersion` fails until `make image-inputs` ran.

## Never in the image

No key, token or password of Bugnet's or a provider's, no CA key, no `/etc/bugnet/gate.key`,
`egress.token`, `secrets.env` or `ssh/`, no host keys and no firewall rules. Three scans enforce
it: the payload scan, the image scan in `build.sh` and the nightly leak test.
