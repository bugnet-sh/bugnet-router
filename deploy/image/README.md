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

## Artifacts on each provider

One image, four artifacts (CP 19.5). E2B builds a template per size from the OCI digest, Koyeb
pulls the digest at each deploy, boxd saves a snapshot per size and Boat saves one snapshot.
Only the controller builds, since every step calls a provider or the registry with a key.

| Provider | Artifact | Built by | A create names it as |
|---|---|---|---|
| E2B | `bugnet-<v>-<size>`, `bugnet-dev-<v>-<size>` | `deploy/e2b` | The template |
| Koyeb | The OCI digest | Nothing more | `docker.image` |
| boxd | `bugnet-<channel>-<v>-<size>` | `bugnet-dev image build` | `spec.base.snapshot` |
| Boat | `bugnet-<channel>-<v>` | `bugnet-dev image build` | `from` |

`<v>` is the version with dashes. Each artifact name is refused when it exists, since a second
save would replace an artifact live sandboxes may need.

### Build, check and roll out

1. `deploy/image/build.sh --channel dev --ca FILE` makes the payload and the OCI image, and starts
   `builds/dev-<version>.json` with the digest.
2. `scripts/image.sh build e2b|boxd|boat|koyeb --channel dev`, one provider at a time. Each adds
   its refs to the record. Boat asks for its image key at `Boat image key:` with echo off. The key
   lives in memory only, joins the scan's needles, and the founder revokes it afterwards.
3. `scripts/image.sh check e2b boxd koyeb boat --channel dev` starts fresh sandboxes from each
   artifact and prints the settings block of one that passes. A failing artifact is deleted.
4. `scripts/live-test.sh --suite image e2b boxd koyeb boat` runs IMG1 to IMG10 and the rows of
   each provider.
5. Paste each passing provider's block into the router's settings, add the version at the front
   of `images.accept`, and restart one replica at a time. A provider whose rows fail stays on its
   own image and out of `images.accept` until CP question 25 is answered.
6. Watch a day of canaries and `image_version` on the start samples, then drop the old version
   from `images.accept`.

The Bugnet boxd org holds no org-level variables or secrets, and every Boat builder is `noEnv`.

### Retire and roll back

Rolling back puts the previous version's blocks back from its record and restarts. To retire:
`scripts/image.sh retire <provider> --version V`. It reads `BUGNET_OPERATOR_TOKEN`, asks
`ListImageUse`, and refuses while the version is on `images.accept`, any ready or terminating
placement runs on it, or a placement became ready on it in the last hour. Keep the current and
the previous version per channel, which is at most 4 Boat saved snapshots. Registry tags stay.

### The build record

`builds/<channel>-<version>.json` holds `version`, `channel`, `inputs_sha256`, `runner_sha256`,
`oci.ref`, and a section per provider: `e2b.templates` (name, size and id), `boxd.refs` (size to
snapshot), `koyeb.ref` and `boat.ref`, each with the `os` and `tools` that `image check` measured.
It names no secret.
