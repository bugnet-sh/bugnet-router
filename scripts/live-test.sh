#!/usr/bin/env bash
# Runs live tests against the real providers named, each one of boxd, e2b, koyeb or boat.
#
# scripts/live-test.sh [--timeout DURATION]
#   [--suite adapters|dataplane|control|leak|sessions|image|bench] [--front local|vps]
#   [--run REGEX] [--router URL] [PROVIDER...]
#
# adapters, the default, runs each provider's adapter live tests, 20 minutes each unless
# --timeout says otherwise. dataplane, control, leak and sessions run TestLiveDataplane,
# TestLiveControl, TestLiveLeak and TestLiveSessions in test/live on a stack on this Mac, 90
# minutes unless --timeout says otherwise, behind the VPS's dev front with --front vps. image runs
# TestLiveImage, CP 19.15.3's rows against the artifacts of deploy/image/builds, with no stack.
# --run replaces the suite's test pattern. Keys come from .env and every other setting from config/dev.toml. These suites end with bugnet-dev
# owned for each provider named and fail while one still holds a Bugnet sandbox. bench sources no
# .env, checks no provider key, and drives bugnet-dev bench against --router with
# BUGNET_OPERATOR_TOKEN from the environment. Live tests create real sandboxes and remove them
# again, which costs a little on the provider's account.
# shellcheck disable=SC1091
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
	echo "usage: scripts/live-test.sh [--timeout DURATION]" \
		"[--suite adapters|dataplane|control|leak|sessions|image|bench] [--front local|vps]" \
		"[--run REGEX] [--router URL] [PROVIDER...]" >&2
	exit 2
}

# need refuses an option given without its value
need() {
	if [ -z "${2:-}" ]; then
		echo "live-test: $1 needs a value" >&2
		usage
	fi
}

timeout=
suite=adapters
front=
pattern=
router=
duration='^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$'
while [ $# -gt 0 ]; do
	case $1 in
	--timeout)
		if [[ ! ${2:-} =~ $duration ]]; then
			echo "live-test: --timeout needs a Go duration such as 90m or 1h30m" >&2
			exit 2
		fi
		timeout=$2
		shift 2
		;;
	--suite)
		need "$1" "${2:-}"
		case $2 in
		adapters | dataplane | control | leak | sessions | image | bench) suite=$2 ;;
		*)
			echo "live-test: unknown suite $2, want adapters, dataplane, control, leak," \
				"sessions, image or bench" >&2
			exit 2
			;;
		esac
		shift 2
		;;
	--front)
		need "$1" "${2:-}"
		case $2 in
		local | vps) front=$2 ;;
		*)
			echo "live-test: --front must be local or vps" >&2
			exit 2
			;;
		esac
		shift 2
		;;
	--run)
		need "$1" "${2:-}"
		pattern=$2
		shift 2
		;;
	--router)
		need "$1" "${2:-}"
		router=$2
		shift 2
		;;
	--*)
		echo "live-test: unknown option $1" >&2
		usage
		;;
	*) break ;;
	esac
done

keys=()
for provider in "$@"; do
	case $provider in
	boxd) keys+=(BOXD_API_KEY) ;;
	e2b) keys+=(E2B_API_KEY) ;;
	koyeb) keys+=(KOYEB_API_TOKEN) ;;
	boat) keys+=(BOAT_API_KEY) ;;
	*)
		echo "live-test: unknown provider $provider, want boxd, e2b, koyeb or boat" >&2
		exit 2
		;;
	esac
done

case $suite in
adapters | dataplane | leak | sessions | image)
	if [ $# -eq 0 ]; then
		echo "live-test: the $suite suite needs at least one provider" >&2
		usage
	fi
	;;
esac
if [ -n "$front" ]; then
	case $suite in
	adapters | image | bench)
		echo "live-test: --front goes with the dataplane, control, leak and sessions suites" >&2
		exit 2
		;;
	esac
fi
if [ -n "$router" ] && [ "$suite" != bench ]; then
	echo "live-test: --router goes with the bench suite" >&2
	exit 2
fi

if [ "$suite" = bench ]; then
	if [ -z "$router" ]; then
		echo "live-test: the bench suite needs --router URL, the router it drives" >&2
		exit 2
	fi
	if [ -z "${BUGNET_OPERATOR_TOKEN:-}" ]; then
		echo "live-test: the bench suite needs BUGNET_OPERATOR_TOKEN set in the environment" >&2
		exit 1
	fi
	exec go run ./cmd/bugnet-dev bench --router "$router" "$@"
fi

if [ ! -f .env ]; then
	echo "live-test: .env is missing, copy .env.example to .env and paste the keys there" >&2
	exit 1
fi
set -a
. ./.env
set +a
for key in ${keys[@]+"${keys[@]}"}; do
	if [ -z "${!key:-}" ]; then
		echo "live-test: $key is empty in .env, paste the provider's key there" >&2
		exit 1
	fi
done

status=0
if [ "$suite" = adapters ]; then
	for provider in "$@"; do
		if ! go test -count=1 -v -timeout "${timeout:-20m}" -run "${pattern:-Live}" \
			"./internal/provider/$provider/"; then
			status=1
		fi
	done
else
	case $suite in
	dataplane) run=TestLiveDataplane ;;
	control) run=TestLiveControl ;;
	leak) run=TestLiveLeak ;;
	sessions) run=TestLiveSessions ;;
	image) run=TestLiveImage ;;
	esac
	list=$(
		IFS=,
		echo "$*"
	)
	if ! go test -count=1 -v -timeout "${timeout:-90m}" -run "${pattern:-$run}" ./test/live/ \
		-args -providers="$list" -front="${front:-local}"; then
		status=1
	fi
fi

# Every run ends with no Bugnet sandbox left on any provider it touched
for provider in "$@"; do
	if ! out=$(go run ./cmd/bugnet-dev owned "$provider"); then
		echo "live-test: bugnet-dev owned $provider failed" >&2
		status=1
		continue
	fi
	if ! grep -qx 'count=0' <<<"$out"; then
		echo "live-test: $provider still holds Bugnet sandboxes:" >&2
		echo "$out" >&2
		status=1
	fi
done
exit $status
