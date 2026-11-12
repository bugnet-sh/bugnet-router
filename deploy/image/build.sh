#!/usr/bin/env bash
# Builds the Bugnet base image for one channel
#   build.sh --channel dev|prod --ca FILE... [--no-push]
# The payload, its scan, the OCI build, the image scan, then the push and the digest.
# --no-push stops after the image scan.
set -euo pipefail

# A CA pin may come from the environment while pins.conf holds none, which the container tests use
ENV_PINS="$(env | grep '^CA_SHA256_' || true)"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck disable=SC1091
. "$HERE/pins.conf"

# BUGNET_BASE_IMAGE replaces BASE_IMAGE, for the container tests and the Koyeb fallback
BASE_IMAGE="${BUGNET_BASE_IMAGE:-$BASE_IMAGE}"

die() { echo "build: $*" >&2; exit 1; }

CHANNEL="" PUSH=1 CAS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --channel) CHANNEL="${2:-}"; shift 2 ;;
        --ca) CAS+=("${2:-}"); shift 2 ;;
        --no-push) PUSH=0; shift ;;
        *) die "unknown argument $1" ;;
    esac
done
case "$CHANNEL" in dev | prod) ;; *) die "--channel must be dev or prod" ;; esac
[ "${#CAS[@]}" -gt 0 ] || die "give at least one --ca FILE"

VERSION="$(tr -d '[:space:]' <"$HERE/VERSION")"
RUNNER="$ROOT/../bugnet-run/dist/bugnet-run-linux-amd64"
[ -f "$RUNNER" ] || die "build bugnet-run first, make build in bugnet-run writes $RUNNER"

# The line of inputs.sha256 must hold the current hash, so no input moved after it was written
INPUTS="$(cut -d' ' -f1 <"$HERE/inputs.sha256")"
(cd "$ROOT" && go run ./internal/image/inputs >/dev/null) || die "run make image-inputs"
[ "$INPUTS" = "$(cut -d' ' -f1 <"$HERE/inputs.sha256")" ] || die "run make image-inputs"

DIST="$HERE/dist"
PAYLOAD="$DIST/payload"
rm -rf "$PAYLOAD"
mkdir -p "$PAYLOAD"
cp "$HERE/setup-image.sh" "$HERE/pins.conf" "$HERE/VERSION" "$PAYLOAD/"
cp "$RUNNER" "$PAYLOAD/bugnet-run-linux-amd64"
# The bare base image has no CA bundle for apt's first fetch from the snapshot, so it borrows ours
for bundle in /etc/ssl/certs/ca-certificates.crt /etc/ssl/cert.pem; do
    if [ -s "$bundle" ]; then
        cp "$bundle" "$PAYLOAD/apt-ca.pem"
        break
    fi
done
[ -s "$PAYLOAD/apt-ca.pem" ] || die "this host has no CA bundle to lend apt"

upper="$(echo "$CHANNEL" | tr '[:lower:]' '[:upper:]')"
for ca in "${CAS[@]}"; do
    [ -f "$ca" ] || die "$ca is not a file"
    if grep -q "PRIVATE KEY" "$ca"; then die "$ca holds a private key"; fi
    n="$(basename "$ca" | sed -n 's/^bugnet-egress-v\([0-9][0-9]*\)\.crt$/\1/p')"
    [ -n "$n" ] || die "name the CA file bugnet-egress-v<N>.crt"
    want_var="CA_SHA256_${upper}_V${n}"
    want="${!want_var:-}"
    if [ -z "$want" ]; then
        want="$(printf '%s\n' "$ENV_PINS" | sed -n "s/^${want_var}=//p" | head -n 1)"
        [ -z "$want" ] || echo "${want_var}=${want}" >>"$PAYLOAD/pins.conf"
    fi
    [ -n "$want" ] || die "pins.conf has no $want_var"
    got="$(shasum -a 256 "$ca" | cut -d' ' -f1)"
    [ "$got" = "$want" ] || die "$ca does not match $want_var"
    cp "$ca" "$PAYLOAD/"
done

tar -C "$PAYLOAD" -czf "$DIST/payload-$CHANNEL-$VERSION.tar.gz" .

(cd "$ROOT" && go run ./cmd/bugnet-dev image scan --dir "$PAYLOAD")

# BUGNET_LOCAL_TAG replaces the local tag, so container test runs on one Docker never share it
TAG="${BUGNET_LOCAL_TAG:-bugnet-base:$VERSION-$CHANNEL}"
(cd "$HERE" && docker buildx build --platform linux/amd64 --provenance=false --load \
    --build-arg "BASE_IMAGE=$BASE_IMAGE" --build-arg "CHANNEL=$CHANNEL" \
    --build-arg "INPUTS=$INPUTS" -t "$TAG" -f Dockerfile .)

size="$(docker image inspect "$TAG" --format '{{.Size}}')"
[ "$size" -le "$MAX_BYTES" ] || die "the image is $size bytes, over MAX_BYTES"

(cd "$ROOT" && go run ./cmd/bugnet-dev image scan --oci "$TAG")

if [ "$PUSH" = 0 ]; then
    echo "$TAG"
    exit 0
fi

REMOTE="$REGISTRY:$VERSION-$CHANNEL"
docker tag "$TAG" "$REMOTE"
docker push "$REMOTE"
# The local tag shares the digest, so the record takes the one under the registry's name
DIGEST="$(docker image inspect "$REMOTE" --format '{{range .RepoDigests}}{{println .}}{{end}}' |
    grep -m1 "^$REGISTRY@sha256:" || true)"
[ -n "$DIGEST" ] || die "the push left no digest under $REGISTRY"
RECORD="$HERE/builds/$CHANNEL-$VERSION.json"
[ ! -e "$RECORD" ] || die "$RECORD exists, a version is built once per channel"
jq -n --arg version "$VERSION" --arg channel "$CHANNEL" --arg inputs "$INPUTS" \
    --arg ref "$DIGEST" --arg runner "$(shasum -a 256 "$RUNNER" | cut -d' ' -f1)" \
    '{version:$version,channel:$channel,inputs_sha256:$inputs,runner_sha256:{amd64:$runner},
      oci:{ref:$ref}}' >"$RECORD"
echo "$DIGEST"
