#!/usr/bin/env bash
# Runs migrate, then one router-api and one router-worker on the local provider until Ctrl-C.
#
# Settings come from the TOML file given as the first argument, config/dev.toml by default, with
# a path relative to the repository root. Secrets come from .env and from .env.keys, which this
# script writes with bugnet-dev keys when it is missing. Both processes read bin/dev.toml, the
# settings with the generated order_token_public_key in front, and with any top-level
# order_token_public_key line of the settings file dropped, so a copy of config/example.toml
# works too. Binaries are built into bin/ and every process logs to bin/logs/. Ctrl-C sends
# SIGTERM to both processes and waits for them to drain.
# shellcheck disable=SC1091,SC2329
set -euo pipefail
# Job control puts each process in its own group, so Ctrl-C reaches only this script
set -m

cd "$(dirname "$0")/.."
config=${1:-config/dev.toml}
if [ ! -f "$config" ]; then
	echo "dev-up: settings file $config is missing" >&2
	exit 1
fi
if [ ! -f .env ]; then
	echo "dev-up: .env is missing, copy .env.example to .env first" >&2
	exit 1
fi

mkdir -p bin/logs
echo "dev-up: building into bin/"
go build -o bin/ github.com/oynozan/bugnet-run/cmd/bugnet-run \
	./cmd/router-api ./cmd/router-worker ./cmd/bugnet-dev

if [ ! -f .env.keys ]; then
	bin/bugnet-dev keys >.env.keys
	echo "dev-up: wrote fresh secrets to .env.keys"
fi
set -a
. ./.env
. ./.env.keys
set +a

pubkey=$(bin/bugnet-dev pubkey)
# TOML top-level keys must come before every table, so the key goes first
{
	printf 'order_token_public_key = "%s"\n\n' "$pubkey"
	awk '/^[[:space:]]*\[/ { table = 1 }
		table || !/^[[:space:]]*["\047]?order_token_public_key["\047]?[[:space:]]*=/' "$config"
} >bin/dev.toml

if ! bin/router-api migrate >>bin/logs/migrate.log 2>&1; then
	echo "dev-up: migrate failed, see bin/logs/migrate.log" >&2
	exit 1
fi

api_pid=
worker_pid=
stop() {
	trap - EXIT INT TERM
	echo "dev-up: stopping"
	for pid in "$api_pid" "$worker_pid"; do
		[ -z "$pid" ] || kill -TERM "$pid" 2>/dev/null || true
	done
	wait
	echo "dev-up: stopped"
}
trap stop EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

bin/router-api -config bin/dev.toml >>bin/logs/router-api.log 2>&1 &
api_pid=$!
bin/router-worker -config bin/dev.toml >>bin/logs/router-worker.log 2>&1 &
worker_pid=$!
echo "dev-up: router-api and router-worker run with bin/dev.toml, logs in bin/logs, Ctrl-C stops"

while kill -0 "$api_pid" 2>/dev/null && kill -0 "$worker_pid" 2>/dev/null; do
	# A background sleep keeps wait interruptible, so Ctrl-C stops at once
	sleep 1 &
	wait $! || true
done
echo "dev-up: a process exited, see bin/logs" >&2
exit 1
