#!/usr/bin/env bash
# Builds, checks and retires the Bugnet base image artifacts on each provider, CP 19.7.
#
# scripts/image.sh build PROVIDER --channel dev|prod [--env FILE]
# scripts/image.sh check PROVIDER... --channel dev|prod [--env FILE]
# scripts/image.sh retire PROVIDER --version V [--channel dev|prod] [--env FILE]
#
# PROVIDER is e2b, boxd, koyeb or boat, and build takes one. Run deploy/image/build.sh first, which
# starts deploy/image/builds/<channel>-<version>.json with the OCI digest. build adds a provider's
# artifacts to that record, check starts fresh sandboxes from them and prints the settings block
# to paste, and retire deletes an old version's artifacts. Keys come from .env, or from the file
# --env names, and an empty one fails by name without its value ever being printed. Boat's image
# key is typed at a prompt by bugnet-dev, never read from a file. Every step calls a provider, so
# only the controller runs this, and each step costs a little on the provider's account.
# shellcheck disable=SC1090
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
	echo "usage: scripts/image.sh build PROVIDER --channel dev|prod [--env FILE]" >&2
	echo "       scripts/image.sh check PROVIDER... --channel dev|prod [--env FILE]" >&2
	echo "       scripts/image.sh retire PROVIDER --version V [--channel dev|prod] [--env FILE]" >&2
	exit 2
}

die() {
	echo "image: $*" >&2
	exit 1
}

[ $# -gt 0 ] || usage
command=$1
shift
case $command in
build | check | retire) ;;
*) usage ;;
esac

channel=
version=
envfile=.env
names=()
while [ $# -gt 0 ]; do
	case $1 in
	--channel | --version | --env)
		[ -n "${2:-}" ] || die "$1 needs a value"
		case $1 in
		--channel) channel=$2 ;;
		--version) version=$2 ;;
		--env) envfile=$2 ;;
		esac
		shift 2
		;;
	--*) die "unknown option $1" ;;
	*)
		names+=("$1")
		shift
		;;
	esac
done

for p in ${names[@]+"${names[@]}"}; do
	case $p in
	e2b | boxd | koyeb | boat) ;;
	*) die "unknown provider $p, want e2b, boxd, koyeb or boat" ;;
	esac
done
[ ${#names[@]} -gt 0 ] || usage
if [ "$command" != check ] && [ ${#names[@]} -ne 1 ]; then
	die "$command takes one provider"
fi
case $command in
build | check)
	case $channel in dev | prod) ;; *) die "--channel must be dev or prod" ;; esac
	;;
retire)
	[[ $version =~ ^[0-9]{4}\.[0-9]{2}\.[0-9]+$ ]] || die "--version must look like 2026.11.1"
	[ -z "$channel" ] || case $channel in dev | prod) ;; *) die "--channel must be dev or prod" ;; esac
	;;
esac

[ -f "$envfile" ] || die "$envfile is missing, copy .env.example to .env and paste the keys there"
set -a
. "$envfile"
set +a

# need fails naming a key that is empty, never printing a value
need() {
	local key
	for key in "$@"; do
		[ -n "${!key:-}" ] || die "$key is empty in $envfile, paste the key there"
	done
}

# keyOf names the environment variable of a provider's runtime key
keyOf() {
	case $1 in
	e2b) echo E2B_API_KEY ;;
	boxd) echo BOXD_API_KEY ;;
	koyeb) echo KOYEB_API_TOKEN ;;
	boat) echo BOAT_API_KEY ;;
	esac
}

: "${version:=$(tr -d '[:space:]' <deploy/image/VERSION)}"
record=deploy/image/builds/$channel-$version.json

# merge adds a JSON object under one key of the build record
merge() { # merge KEY JSON
	local tmp
	tmp=$(mktemp)
	jq --argjson part "$2" --arg key "$1" '.[$key] = ((.[$key] // {}) + $part)' "$record" >"$tmp"
	mv "$tmp" "$record"
}

need_record() {
	[ -f "$record" ] || die "$record is missing, run deploy/image/build.sh --channel $channel first"
}

build() {
	need_record
	local payload=deploy/image/dist/payload-$channel-$version.tar.gz
	case $1 in
	e2b)
		need E2B_API_KEY
		local ref part
		ref=$(jq -r '.oci.ref // empty' "$record")
		[ -n "$ref" ] || die "$record names no OCI digest"
		part=$(mktemp)
		local script=build
		[ "$channel" = dev ] && script=build:dev
		(cd deploy/e2b && IMAGE_REF=$ref IMAGE_VERSION=$version IMAGE_CHANNEL=$channel \
			E2B_RECORD=$part pnpm run "$script")
		merge e2b "$(jq '{templates: .templates}' "$part")"
		rm -f "$part"
		;;
	koyeb)
		# Koyeb pulls the registry image itself, so there is nothing to build
		[ -n "$(jq -r '.oci.ref // empty' "$record")" ] || die "$record names no OCI digest"
		merge koyeb "$(jq '{ref: .oci.ref}' "$record")"
		;;
	boxd)
		need BOXD_API_KEY
		local extra=()
		local from
		from=$(jq -r '.boxd.from_oci // empty' "$record")
		if [ -n "$from" ]; then extra+=(--from-oci "$from"); fi
		go run ./cmd/bugnet-dev image build --provider boxd --channel "$channel" \
			--version "$version" --payload "$payload" --record "$record" ${extra[@]+"${extra[@]}"}
		;;
	boat)
		go run ./cmd/bugnet-dev image build --provider boat --channel "$channel" \
			--version "$version" --payload "$payload" --record "$record"
		;;
	esac
}

check() {
	need_record
	need "$(keyOf "$1")"
	local args=()
	case $1 in
	e2b) mapfile -t args < <(jq -r '.e2b.templates[] | "--ref\n" + .size + "=" + .id' "$record") ;;
	boxd) mapfile -t args < <(jq -r '.boxd.refs | to_entries[] | "--ref\n" + .key + "=" + .value' "$record") ;;
	koyeb | boat) args=(--ref "$(jq -r --arg p "$1" '.[$p].ref // empty' "$record")") ;;
	esac
	[ ${#args[@]} -gt 0 ] || die "$record names no $1 artifact, run build $1 first"
	go run ./cmd/bugnet-dev image check --provider "$1" --channel "$channel" \
		--version "$version" --record "$record" "${args[@]}"
}

retire() {
	local ch=${channel:-}
	need "$(keyOf "$1")" BUGNET_OPERATOR_TOKEN
	go run ./cmd/bugnet-dev image retire --provider "$1" --version "$version" \
		${ch:+--channel "$ch"}
}

status=0
case $command in
build) build "${names[0]}" || status=1 ;;
check)
	for p in "${names[@]}"; do
		if ! check "$p"; then
			echo "image: the check of $p failed" >&2
			status=1
		fi
	done
	;;
retire) retire "${names[0]}" || status=1 ;;
esac
exit $status
