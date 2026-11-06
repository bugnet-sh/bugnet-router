#!/usr/bin/env bash
# Runs the live tests of each provider named, boxd, e2b, koyeb or boat, against the real service.
# A first argument --timeout DURATION replaces go test's 20 minute limit for each provider.
#
# Keys come from .env and every other setting from config/dev.toml. The tests create real
# sandboxes and remove them again, which costs a little on the provider's account.
# shellcheck disable=SC1091
set -euo pipefail

cd "$(dirname "$0")/.."
timeout=20m
if [ "${1:-}" = "--timeout" ]; then
	if [ -z "${2:-}" ]; then
		echo "live-test: --timeout needs a duration such as 90m" >&2
		exit 2
	fi
	timeout=$2
	shift 2
fi
if [ $# -eq 0 ]; then
	echo "usage: scripts/live-test.sh [--timeout DURATION] PROVIDER..., each one of boxd," \
		"e2b, koyeb or boat" >&2
	exit 2
fi
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
if [ ! -f .env ]; then
	echo "live-test: .env is missing, copy .env.example to .env and paste the keys there" >&2
	exit 1
fi

set -a
. ./.env
set +a
for key in "${keys[@]}"; do
	if [ -z "${!key:-}" ]; then
		echo "live-test: $key is empty in .env, paste the provider's key there" >&2
		exit 1
	fi
done

for provider in "$@"; do
	go test -count=1 -v -timeout "$timeout" -run Live "./internal/provider/$provider/"
done
