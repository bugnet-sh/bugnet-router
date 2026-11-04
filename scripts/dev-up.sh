#!/usr/bin/env bash
# Runs migrate, then one router-api and one router-worker on the local provider until Ctrl-C.
#
# Settings come from the env file given as the first argument, .env by default. Secrets come
# from .env.keys, which this script writes with bugnet-dev keys when it is missing. Binaries are
# built into bin/ and every process logs to bin/logs/. Ctrl-C sends SIGTERM to both processes
# and waits for them to drain.
# shellcheck disable=SC1090,SC1091,SC2329
set -euo pipefail
# Job control puts each process in its own group, so Ctrl-C reaches only this script
set -m

cd "$(dirname "$0")/.."
env_file=${1:-.env}
if [ ! -f "$env_file" ]; then
	echo "dev-up: $env_file is missing, copy .env.example to .env first" >&2
	exit 1
fi
case $env_file in
*/*) ;;
*) env_file=./$env_file ;;
esac

mkdir -p bin/logs
echo "dev-up: building into bin/"
go build -o bin/ github.com/oynozan/bugnet-run/cmd/bugnet-run \
	./cmd/router-api ./cmd/router-worker ./cmd/bugnet-dev

if [ ! -f .env.keys ]; then
	bin/bugnet-dev keys >.env.keys
	echo "dev-up: wrote fresh secrets to .env.keys"
fi
set -a
. "$env_file"
. ./.env.keys
set +a
export BUGNET_PROVIDERS=${BUGNET_PROVIDERS:-local}
export BUGNET_ENABLE_LOCAL_PROVIDER=${BUGNET_ENABLE_LOCAL_PROVIDER:-1}
export BUGNET_LOCAL_RUNNER=${BUGNET_LOCAL_RUNNER:-$PWD/bin/bugnet-run}
export BUGNET_LOCAL_ROOT=${BUGNET_LOCAL_ROOT:-$PWD/bin/local}

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

bin/router-api >>bin/logs/router-api.log 2>&1 &
api_pid=$!
bin/router-worker >>bin/logs/router-worker.log 2>&1 &
worker_pid=$!
echo "dev-up: router-api on ${BUGNET_LISTEN_ADDR:-:8080}," \
	"router-worker on ${BUGNET_WORKER_LISTEN_ADDR:-:8081}, logs in bin/logs, Ctrl-C stops"

while kill -0 "$api_pid" 2>/dev/null && kill -0 "$worker_pid" 2>/dev/null; do
	# A background sleep keeps wait interruptible, so Ctrl-C stops at once
	sleep 1 &
	wait $! || true
done
echo "dev-up: a process exited, see bin/logs" >&2
exit 1
