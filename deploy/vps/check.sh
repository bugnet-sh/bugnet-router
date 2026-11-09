#!/usr/bin/env bash
# The deploy check (DP 15.3, data-plane plan Task 39, x402 plan Task X35). deploy.sh runs it after
# every deploy and installs it as /opt/bugnet/check.sh, so it can run alone too:
#
#   sudo /opt/bugnet/check.sh             every check, exit 1 when one fails
#   sudo /opt/bugnet/check.sh timeouts    one check by name, see main at the end
#
# BUGNET_ETC points it at another settings directory, for the tests. It never prints a secret.
set -uo pipefail

etc=${BUGNET_ETC:-/etc/bugnet}
failed=0

bad() {
	printf 'check.sh: FAIL %s\n' "$*" >&2
	failed=1
}

ok() {
	printf 'check.sh: ok %s\n' "$*"
}

warn() {
	printf 'check.sh: warning %s\n' "$*" >&2
}

env_value() {
	sed -n "s/^$2=//p" "$1" | tail -n 1
}

# toml_get prints a plain value of KEY in [SECTION] of a TOML file, "" for the top level
toml_get() {
	awk -v want="$2" -v key="$3" '
		/^[[:space:]]*\[/ {
			s = $0; sub(/[[:space:]]*#.*$/, "", s); gsub(/[[:space:]]|\[|\]/, "", s); sec = s; next
		}
		sec == want && $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
			v = $0; sub(/^[^=]*=[[:space:]]*/, "", v); sub(/[[:space:]]+#.*$/, "", v)
			gsub(/^"|"[[:space:]]*$/, "", v); print v; exit
		}' "$1"
}

# duration_ms turns a Go or Payments duration such as 3s, 500ms or 1.5m into milliseconds
duration_ms() {
	awk -v d="$1" 'BEGIN {
		if (!match(d, /^[0-9]+(\.[0-9]+)?/)) exit 1
		n = substr(d, 1, RLENGTH); u = substr(d, RLENGTH + 1)
		m = (u == "ms") ? 1 : (u == "s") ? 1000 : (u == "m") ? 60000 : (u == "h") ? 3600000 : 0
		if (m == 0) exit 1
		printf "%d\n", n * m
	}'
}

# as runs a command as a Unix user, with no shell and no password
as() {
	local u=$1
	shift
	runuser -u "$u" -- "$@"
}

vkcli() {
	if command -v valkey-cli >/dev/null; then valkey-cli "$@"; else redis-cli "$@"; fi
}

# Only bugnet-router opens Postgres and Valkey; nobody reaches either over TCP (DP 15.3)
check_stores() {
	local gateway_users u
	gateway_users=$(env_value "$etc/deploy.env" GATEWAY_USERS)
	[[ $(as bugnet-router psql -Xw -h /var/run/postgresql -U bugnet_router -d bugnet -Atc \
		'select 1' 2>/dev/null) == 1 ]] || bad "bugnet-router cannot open Postgres"
	[[ $(as bugnet-router "$(command -v valkey-cli || command -v redis-cli)" \
		-s /run/valkey/valkey.sock PING 2>/dev/null) == PONG ]] ||
		bad "bugnet-router cannot open Valkey"
	for u in haproxy bugnet-edge bugnet-secrets bugnet-sessions bugnet-certs bugnet-ssh \
		bugnet-egress $gateway_users; do
		if ! id -u "$u" >/dev/null 2>&1; then
			bad "user $u does not exist, so its refusal proves nothing"
			continue
		fi
		[[ $(as "$u" "$(command -v valkey-cli || command -v redis-cli)" \
			-s /run/valkey/valkey.sock PING 2>/dev/null) != PONG ]] ||
			bad "$u opens the Valkey socket"
		! as "$u" psql -Xw -h /var/run/postgresql -U bugnet_router -d bugnet -Atc 'select 1' \
			>/dev/null 2>&1 || bad "$u opens the router's database"
	done
	if id -u bugnet-payments >/dev/null 2>&1; then
		! as bugnet-payments psql -Xw -h /var/run/postgresql -U bugnet_router -d bugnet -Atc \
			'select 1' >/dev/null 2>&1 || bad "bugnet-payments opens the router's database"
		! as bugnet-router psql -Xw -h /var/run/postgresql -U bugnet_payments \
			-d bugnet_payments -Atc 'select 1' >/dev/null 2>&1 ||
			bad "bugnet-router opens Payments' database"
	fi
	[[ $(vkcli -h 127.0.0.1 -p 6379 PING 2>/dev/null) != PONG ]] || bad "Valkey answers on TCP"
	! psql -Xw -h 127.0.0.1 -U bugnet_router -d bugnet -Atc 'select 1' >/dev/null 2>&1 ||
		bad "Postgres answers on TCP"
	ok "only bugnet-router opens the data stores, users: haproxy and the services $gateway_users"
}

# The pool must hold every River worker plus 8 for River itself (CP 13.4, 18.14)
check_pool() {
	local url max def dp probes pools floor
	url=$(env_value "$etc/router.env" BUGNET_DATABASE_URL)
	max=$(printf '%s' "$url" | sed -n 's/.*[?&]pool_max_conns=\([0-9]*\).*/\1/p')
	def=$(toml_get "$etc/router.toml" worker default_workers)
	dp=$(toml_get "$etc/router.toml" worker dataplane_workers)
	probes=$(toml_get "$etc/router.toml" health probe_workers)
	pools=$(toml_get "$etc/router.toml" pools workers)
	# The health and pools tables join once their tasks land; until then their workers are 0
	floor=$((${def:-20} + ${dp:-8} + ${probes:-0} + ${pools:-0} + 8))
	if [[ -z $max ]]; then
		bad "BUGNET_DATABASE_URL in $etc/router.env sets no pool_max_conns, want $floor or more"
	elif ((max < floor)); then
		bad "pool_max_conns is $max, below the workers' floor of $floor"
	else
		ok "pool_max_conns $max is at least the workers' floor of $floor"
	fi
}

# No unit of the deploy has failed, bugnet-certs' 14-day alert included (Task 10)
check_units() {
	local failed_units
	failed_units=$(systemctl list-units --failed --plain --no-legend 'bugnet-*' 'router-*' \
		haproxy.service 'postgresql*' valkey-server.service redis-server.service |
		awk '{print $1}')
	if [[ -n $failed_units ]]; then
		bad "failed units: $(echo "$failed_units" | tr '\n' ' ')"
	else
		ok "no unit failed"
	fi
}

check_ready() {
	local unit port inst
	for unit in router-api:8080 bugnet-edge:9101 bugnet-secrets:9104 bugnet-sessions:9105 \
		bugnet-ssh:9102 bugnet-egress:9103; do
		port=${unit#*:}
		unit=${unit%%:*}
		inst=$(systemctl list-units --plain --no-legend --state=active "$unit@*" | awk 'NR==1')
		if [[ -z $inst ]]; then
			bad "no $unit instance is active"
		elif ! curl -fsS -o /dev/null --max-time 3 "http://127.0.0.1:$port/readyz"; then
			bad "$unit's /readyz on $port does not answer 200"
		fi
	done
	curl -fsS -o /dev/null --max-time 3 http://127.0.0.1:8081/readyz ||
		bad "router-worker's /readyz on 8081 does not answer 200"
	ok "every service answers /readyz"
}

# A stats outage must not block a deploy, so this one only warns (CP 20.9)
check_stats() {
	for _ in $(seq 30); do
		if curl -fsS -o /dev/null --max-time 2 http://127.0.0.1:8082/api/stats; then
			ok "the stats feed answers on 8082"
			return
		fi
		sleep 2
	done
	warn "http://127.0.0.1:8082/api/stats did not answer 200 within 60 s"
}

# The loopback rule of DP 10.12.7.1 (9443 and 19443) needs bugnet-nft, which waits for Task 43B
check_loopback() {
	warn "skipped: who may connect to 127.0.0.1:9443 and :19443 waits for bugnet-nft (Task 43B)"
}

# Egress's payments_timeout stays above Payments' sign_timeout (XS 6.17)
check_timeouts() {
	local egress payments e p
	egress=$(toml_get "$etc/egress.toml" x402 payments_timeout)
	if [[ ! -f $etc/payments.toml ]]; then
		warn "no $etc/payments.toml, so the timeouts were not compared"
		return
	fi
	payments=$(toml_get "$etc/payments.toml" x402 sign_timeout)
	e=$(duration_ms "${egress:-3s}") || { bad "egress payments_timeout $egress"; return; }
	p=$(duration_ms "${payments:-2s}") || { bad "Payments sign_timeout $payments"; return; }
	if ((e > p)); then
		ok "egress payments_timeout ${egress:-3s} is above Payments' sign_timeout ${payments:-2s}"
	else
		bad "egress payments_timeout ${egress:-3s} must be above Payments' sign_timeout" \
			"${payments:-2s}"
	fi
}

# mode_is checks a file's owner and mode
mode_is() {
	local got
	got=$(stat -c '%U %a' "$1" 2>/dev/null) || { bad "$1 is missing"; return; }
	[[ $got == "$2" ]] || bad "$1 is $got, want $2"
}

# x402's ports, files and settings (x402 plan Task X35)
check_x402() {
	local p listeners pid user line want
	for p in 8447 8449 8450; do
		listeners=$(ss -Hltn "sport = :$p" | awk '{print $4}')
		if [[ -z $listeners ]]; then
			warn "nothing listens on $p yet (Payments)"
		elif grep -vqE '^(127\.0\.0\.1|\[::1\]):' <<<"$listeners"; then
			bad "$p listens beyond loopback: $(echo "$listeners" | tr '\n' ' ')"
		fi
	done
	pid=$(ss -Hltnp "sport = :8446" | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -n 1)
	if [[ -n $pid ]]; then
		user=$(ps -o user= -p "$pid" | tr -d ' ')
		[[ $user == bugnet-website ]] || bad "8446 is the website's, but $user holds it"
	fi
	if grep -qE '127\.0\.0\.1:(8447|8449)([^0-9]|$)' /etc/haproxy/haproxy.cfg /etc/haproxy/*.map \
		2>/dev/null; then
		bad "haproxy reaches 8447 or 8449, which no HAProxy rule may target"
	fi
	line='LoadCredential=egress_x402_client_key:/etc/bugnet/keys/egress-x402-client.key'
	grep -qxF "$line" /etc/systemd/system/bugnet-egress@.service ||
		bad "bugnet-egress@.service lacks $line"
	mode_is "$etc/keys/egress-x402-client.key" "root 600"
	mode_is "$etc/internal-ca.pem" "root 644"
	mode_is "$etc/egress-x402-client.pem" "root 644"
	if [[ -f $etc/payments/x402-server.pem ]]; then
		[[ $(stat -c '%a' "$etc/payments/x402-server.pem") == 644 ]] ||
			bad "$etc/payments/x402-server.pem is not 0644"
	else
		warn "no $etc/payments/x402-server.pem yet"
	fi
	for p in "$etc/internal-ca.pem" "$etc/egress-x402-client.pem" \
		"$etc/payments/x402-server.pem"; do
		[[ -f $p ]] || continue
		openssl x509 -checkend 2592000 -noout -in "$p" >/dev/null ||
			bad "$p expires within 30 days, renew it (README)"
	done
	check_timeouts
	if [[ -f $etc/router-x402-index.json ]]; then
		want=$(/opt/bugnet/router/current/bugnet-dev x402 allowed-hosts \
			--index "$etc/router-x402-index.json" --config "$etc/router.toml")
		grep -qxF "$want" "$etc/router.toml" ||
			bad "router.toml's allowed_shared_hosts differs from the index's: $want"
	else
		warn "no reviewed index yet, run deploy.sh x402-index --router once the gateway has it"
	fi
	ok "x402 ports, files and settings checked"
}

main() {
	cd / || exit 1
	case ${1:-all} in
	all)
		check_stores
		check_pool
		check_units
		check_ready
		check_loopback
		check_x402
		check_stats
		;;
	stores) check_stores ;;
	pool) check_pool ;;
	units) check_units ;;
	ready) check_ready ;;
	x402) check_x402 ;;
	timeouts) check_timeouts ;;
	stats) check_stats ;;
	*)
		echo "usage: check.sh [all|stores|pool|units|ready|x402|timeouts|stats]" >&2
		exit 2
		;;
	esac
	exit "$failed"
}

main "$@"
