#!/usr/bin/env bash
# The production deploy on the VPS (DP 15, data-plane plan Task 39, x402 plan Task X35). Run it as
# root from the bundle ship.sh copies, which holds this script, check.sh, bugnet-ops, systemd/,
# postgres/, valkey/, config/, maps/ (what `bugnet-dev deploy maps` wrote) and bin/.
#
#   sudo ./deploy.sh keys <binaries dir>          the keys made once (DP 15.5), then the env files
#   sudo ./deploy.sh mtls <mtls init dir>         egress's and Payments' x402 mTLS files (XS 6.17)
#   sudo ./deploy.sh <version> <binaries dir>     a deploy: start new, wait /readyz, stop old, check
#   sudo ./deploy.sh x402-index <index.json>      the reviewed index to Payments, then SIGHUP
#   sudo ./deploy.sh x402-index --router <index.json>
#                                                 allowed_shared_hosts from it, then roll router-api
#
# Every step is idempotent. It never prints a secret. README.md is the runbook.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
etc=/etc/bugnet
keys=$etc/keys
opt=/opt/bugnet
certs_root=/var/lib/bugnet-certs
deployed=$etc/deployed
usage="usage: deploy.sh <version> <binaries dir> | keys <binaries dir> | mtls <dir> |
       x402-index [--router] <index.json>"

# Each templated unit, the /opt/bugnet directory its binary sits in, and its /readyz port
units=(router-api bugnet-edge bugnet-secrets bugnet-sessions bugnet-ssh bugnet-egress)
declare -A dir_of=(
	[router-api]=router [bugnet-edge]=edge [bugnet-secrets]=edge [bugnet-sessions]=edge
	[bugnet-ssh]=ssh [bugnet-egress]=egress
)
declare -A ready_of=(
	[router-api]=8080 [bugnet-edge]=9101 [bugnet-secrets]=9104 [bugnet-sessions]=9105
	[bugnet-ssh]=9102 [bugnet-egress]=9103
)
# The binaries each /opt/bugnet directory holds, bugnet-dev beside router-api
declare -A bins_of=(
	[router]="router-api router-worker bugnet-dev" [edge]=bugnet-edge [ssh]=bugnet-ssh
	[egress]=bugnet-egress [certs]=bugnet-certs [run]=bugnet-run-linux-amd64
)
service_users=(bugnet-router bugnet-edge bugnet-secrets bugnet-sessions bugnet-ssh bugnet-egress
	bugnet-certs)

# The secrets each env file must hold, by name only (DP 15.3, 15.5, CP 13.6)
declare -A env_names=(
	[router]="BUGNET_DATABASE_URL BUGNET_INTERNAL_TOKEN BUGNET_HANDLE_KEY BUGNET_DATA_PLANE_KEYS
		BUGNET_EVENT_SIGNING_KEY BUGNET_EDGE_TOKEN BUGNET_SECRETS_TOKEN BUGNET_SSH_TOKEN
		BUGNET_EGRESS_TOKEN BUGNET_EGRESS_FETCH_TOKEN BUGNET_OPERATOR_TOKEN
		BUGNET_PAYMENTS_ROUTER_TOKEN BUGNET_SESSIONS_TOKEN"
	[edge]="BUGNET_EDGE_TOKEN BUGNET_HOOK_PATH_SECRET"
	[secrets-route]=BUGNET_SECRETS_TOKEN
	[sessions]=BUGNET_SESSIONS_TOKEN
	[ssh]=BUGNET_SSH_TOKEN
	[egress]="BUGNET_EGRESS_TOKEN BUGNET_EGRESS_FETCH_TOKEN"
)
declare -A provider_env=([boat]=BOAT_API_KEY [e2b]=E2B_API_KEY [boxd]=BOXD_API_KEY
	[koyeb]=KOYEB_API_TOKEN)
database_url='postgres://bugnet_router@/bugnet?host=/var/run/postgresql&pool_max_conns=48'

die() {
	printf 'deploy.sh: %s\n' "$*" >&2
	exit 1
}

say() {
	printf 'deploy.sh: %s\n' "$*"
}

need_root() {
	[[ $EUID -eq 0 ]] || die "run it as root, with sudo"
}

# env_value prints KEY's value from a KEY=value file, never running the file
env_value() {
	sed -n "s/^$2=//p" "$1" | tail -n 1
}

has_name() {
	[[ -f $1 ]] && grep -q "^$2=." "$1"
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

#### Inputs

check_bin_dir() {
	local d=$1 f
	[[ -d $d ]] || die "$d is not a directory; $usage"
	for f in router-api router-worker bugnet-dev bugnet-edge bugnet-egress bugnet-ssh bugnet-certs \
		bugnet-run-linux-amd64; do
		[[ -f $d/$f ]] || die "$d/$f is missing; build the bundle with ship.sh"
	done
	"$d/bugnet-dev" --help >/dev/null 2>&1 ||
		die "$d/bugnet-dev does not run on this $(uname -m) machine; ship.sh builds linux/amd64"
}

check_bundle() {
	local f
	for f in check.sh bugnet-ops maps/sni.map maps/end.map maps/haproxy.cfg maps/certs.toml \
		maps/certs-staging.toml maps/deploy.env postgres/bugnet.conf postgres/pg_hba.conf \
		postgres/pg_ident.conf valkey/bugnet.conf config/router.toml config/edge.toml \
		config/secrets-route.toml config/sessions.toml config/ssh.toml config/egress.toml \
		config/packages.json systemd/valkey-server.service.d/bugnet.conf \
		systemd/bugnet-certs.service systemd/bugnet-certs.timer systemd/router-worker.service; do
		[[ -f $here/$f ]] || die "$here/$f is missing; build the bundle with ship.sh"
	done
	for f in "${units[@]}"; do
		[[ -f $here/systemd/$f@.service ]] || die "$here/systemd/$f@.service is missing"
	done
	grep -q '^frontend ssh$' "$here/maps/haproxy.cfg" ||
		die "maps/haproxy.cfg has no frontend ssh: render it without --front"
	[[ -n $(env_value "$here/maps/deploy.env" PRODUCTION_CERT_DIRS) ]] ||
		die "maps/deploy.env lists no production wildcard: render it without --front"
	admin_port=$(env_value "$here/maps/deploy.env" ADMIN_SSH_PORT)
	gateway_port=$(env_value "$here/maps/deploy.env" SSH_GATEWAY_PORT)
	[[ $admin_port =~ ^[0-9]+$ && $gateway_port =~ ^[0-9]+$ ]] ||
		die "maps/deploy.env lacks ADMIN_SSH_PORT or SSH_GATEWAY_PORT"
}

# preflight lists everything the services need that a person makes, then stops on any missing
preflight() {
	local missing=() f n p providers
	for f in ssh_host_ed25519_key egress-ca.key egress_box_k1 egress-x402-client.key; do
		[[ -f $keys/$f ]] || missing+=("$keys/$f")
	done
	for f in egress-ca.pem internal-ca.pem egress-x402-client.pem; do
		[[ -f $etc/$f ]] || missing+=("$etc/$f")
	done
	for f in "${!env_names[@]}"; do
		for n in ${env_names[$f]}; do
			has_name "$etc/$f.env" "$n" || missing+=("$n in $etc/$f.env")
		done
	done
	providers=$(toml_get "$here/config/router.toml" "" providers | tr -d '[]",')
	for p in $providers; do
		n=${provider_env[$p]:-}
		[[ -z $n ]] || has_name "$etc/router.env" "$n" || missing+=("$n in $etc/router.env")
	done
	for n in $(toml_get "$here/maps/certs.toml" "" dns_env | tr -d '[]",'); do
		has_name "$etc/certs.env" "$n" || missing+=("$n in $etc/certs.env (DP 15.8 step 3)")
	done
	[[ -n $(toml_get "$here/config/router.toml" "" order_token_public_key) ]] ||
		missing+=("order_token_public_key in config/router.toml, from Payments")
	if ((${#missing[@]} > 0)); then
		printf 'deploy.sh: missing, see README.md "First deploy":\n' >&2
		printf '  %s\n' "${missing[@]}" >&2
		exit 1
	fi

	# ufw closes every other port, so admin SSH must already listen where site.toml says
	[[ -n $(ss -Hltn "sport = :$admin_port") ]] ||
		die "nothing listens on admin_ssh_port $admin_port; correct site.toml"
	local others
	if [[ $gateway_port != "$admin_port" ]]; then
		others=$(ss -Hltnp "sport = :$gateway_port" | grep -v '"haproxy"' || true)
		[[ -z $others ]] || die "port $gateway_port, site.toml's ssh_gateway_port, is held: $others"
	fi
}

#### Host setup

install_packages() {
	local want=(haproxy postgresql ufw curl openssl) missing=() p
	if dpkg-query -W -f='${Status}' valkey-server 2>/dev/null | grep -q 'ok installed' ||
		{ apt-cache policy valkey-server 2>/dev/null | grep -q 'Candidate: [0-9]'; }; then
		want+=(valkey-server valkey-tools)
	elif dpkg-query -W -f='${Status}' redis-server 2>/dev/null | grep -q 'ok installed'; then
		want+=(redis-server redis-tools)
	else
		apt-get update -qq
		if apt-cache policy valkey-server | grep -q 'Candidate: [0-9]'; then
			want+=(valkey-server valkey-tools)
		else
			# Ubuntu 24.04 has no valkey package, its redis-server takes the same settings
			want+=(redis-server redis-tools)
		fi
	fi
	for p in "${want[@]}"; do
		[[ $(dpkg-query -W -f='${Status}' "$p" 2>/dev/null) == *'ok installed' ]] || missing+=("$p")
	done
	if ((${#missing[@]} > 0)); then
		say "installing ${missing[*]}"
		apt-get update -qq
		DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
			"${missing[@]}"
	fi
	if [[ " ${want[*]} " == *' valkey-server '* ]]; then
		vk_unit=valkey-server.service vk_conf=/etc/valkey/valkey.conf
	else
		vk_unit=redis-server.service vk_conf=/etc/redis/redis.conf
	fi
}

install_users() {
	local u
	getent group valkey >/dev/null || groupadd --system valkey
	for u in "${service_users[@]}"; do
		if ! id -u "$u" >/dev/null 2>&1; then
			useradd --system --user-group --no-create-home --home-dir /nonexistent \
				--shell /usr/sbin/nologin "$u"
		fi
	done
	# bugnet-router is the one member of valkey beside valkey itself (DP 15.3)
	usermod -aG valkey bugnet-router
	install -d -o root -g root -m 0755 "$etc" "$opt" /srv/bugnet
	install -d -o root -g root -m 0700 "$keys"
}

# install_if_changed copies src to dst with the owner and mode given, and says if it changed
install_if_changed() {
	local src=$1 dst=$2 owner=$3 mode=$4
	if [[ -f $dst ]] && cmp -s "$src" "$dst"; then
		chown "$owner" "$dst"
		chmod "$mode" "$dst"
		return 1
	fi
	install -o "${owner%%:*}" -g "${owner##*:}" -m "$mode" "$src" "$dst"
}

install_postgres() {
	local pgdir changed=0
	pgdir=$(find /etc/postgresql -mindepth 2 -maxdepth 2 -name main -type d | sort -V | tail -n 1)
	[[ -n $pgdir ]] || die "no /etc/postgresql/<version>/main after installing postgresql"
	install -d -o postgres -g postgres -m 0755 "$pgdir/conf.d"
	install_if_changed "$here/postgres/bugnet.conf" "$pgdir/conf.d/bugnet.conf" \
		postgres:postgres 0644 && changed=1
	install_if_changed "$here/postgres/pg_hba.conf" "$pgdir/pg_hba.conf" postgres:postgres 0640 &&
		changed=1
	install_if_changed "$here/postgres/pg_ident.conf" "$pgdir/pg_ident.conf" \
		postgres:postgres 0640 && changed=1
	systemctl enable --quiet postgresql
	if ((changed)) || ! systemctl is-active --quiet postgresql; then
		systemctl restart postgresql
	fi
	for _ in $(seq 60); do
		pg_isready -q -h /var/run/postgresql && break
		sleep 0.5
	done
	pg_isready -q -h /var/run/postgresql || die "postgres does not answer on its socket"
	local role db
	for role in bugnet_router:bugnet bugnet_payments:bugnet_payments; do
		db=${role#*:}
		role=${role%%:*}
		[[ $(runuser -u postgres -- psql -XAtc \
			"select 1 from pg_roles where rolname = '$role'") == 1 ]] ||
			runuser -u postgres -- psql -Xq -c "create role $role login"
		[[ $(runuser -u postgres -- psql -XAtc \
			"select 1 from pg_database where datname = '$db'") == 1 ]] ||
			runuser -u postgres -- createdb --owner "$role" "$db"
	done
	say "postgres takes peer logins on its socket only"
}

install_valkey() {
	local changed=0 dropin unit_dir=/etc/systemd/system/$vk_unit.d
	install_if_changed "$here/valkey/bugnet.conf" "$etc/valkey.conf" root:root 0644 && changed=1
	if ! grep -qxF "include $etc/valkey.conf" "$vk_conf"; then
		printf '\n# The Bugnet settings, last so they win (deploy.sh)\ninclude %s\n' \
			"$etc/valkey.conf" >>"$vk_conf"
		changed=1
	fi
	dropin=$(mktemp)
	cat "$here/systemd/valkey-server.service.d/bugnet.conf" >"$dropin"
	if [[ $vk_unit == redis-server.service ]]; then
		# redis-server runs in group valkey with /run/valkey, so every name stays DP 15.3's
		printf 'Group=valkey\nRuntimeDirectory=valkey\n' >>"$dropin"
	fi
	install -d -o root -g root -m 0755 "$unit_dir"
	install_if_changed "$dropin" "$unit_dir/bugnet.conf" root:root 0644 && changed=1
	rm -f "$dropin"
	systemctl daemon-reload
	systemctl enable --quiet "$vk_unit"
	if ((changed)) || ! systemctl is-active --quiet "$vk_unit"; then
		systemctl restart "$vk_unit"
	fi
	for _ in $(seq 40); do
		[[ -S /run/valkey/valkey.sock ]] && break
		sleep 0.25
	done
	[[ -S /run/valkey/valkey.sock ]] || die "$vk_unit made no /run/valkey/valkey.sock"
	say "$vk_unit listens on /run/valkey/valkey.sock only"
}

install_haproxy() {
	local f p backup others
	for p in 443 2200 "$gateway_port"; do
		others=$(ss -Hltnp "sport = :$p" | grep -v '"haproxy"' || true)
		[[ -z $others ]] || die "another process holds port $p, stop it first: $others"
	done
	backup=$(mktemp -d)
	for f in haproxy.cfg sni.map end.map; do
		[[ ! -e /etc/haproxy/$f ]] || cp -p "/etc/haproxy/$f" "$backup/$f"
	done
	install -o root -g root -m 0644 "$here/maps/sni.map" "$here/maps/end.map" \
		"$here/maps/haproxy.cfg" /etc/haproxy/
	if ! haproxy -c -q -f /etc/haproxy/haproxy.cfg; then
		for f in haproxy.cfg sni.map end.map; do
			if [[ -e $backup/$f ]]; then
				cp -p "$backup/$f" "/etc/haproxy/$f"
			else
				rm -f "/etc/haproxy/$f"
			fi
		done
		rm -rf "$backup"
		die "haproxy -c refused the new config, so the old one stays"
	fi
	rm -rf "$backup"
	systemctl enable --quiet haproxy
	if systemctl is-active --quiet haproxy; then
		systemctl reload haproxy
	else
		systemctl restart haproxy
	fi
	for _ in $(seq 50); do
		if [[ $(ss -Hltnp "sport = :443") == *'"haproxy"'* &&
			$(ss -Hltnp "sport = :$gateway_port") == *'"haproxy"'* ]]; then
			say "haproxy serves 443, 2200 and $gateway_port"
			return
		fi
		sleep 0.2
	done
	die "haproxy does not listen on 443 and $gateway_port; see journalctl -u haproxy"
}

install_firewall() {
	local p want status ports
	mapfile -t ports < <(printf '%s\n' 80 443 2200 "$gateway_port" "$admin_port" | sort -nu)
	want=$(printf 'ufw allow %s/tcp\n' "${ports[@]}")$'\nufw deny 443/udp'
	status=$(LC_ALL=C ufw status verbose)
	# A reset lifts the firewall for a moment, so one that already matches stays as it is
	if [[ $status == *'Status: active'*'Default: deny (incoming), allow (outgoing)'* &&
		$(LC_ALL=C ufw show added | tail -n +2) == "$want" ]]; then
		say "ufw already allows TCP ${ports[*]} alone"
		return
	fi
	ufw --force reset >/dev/null
	ufw default deny incoming >/dev/null
	ufw default allow outgoing >/dev/null
	for p in "${ports[@]}"; do
		ufw allow "$p/tcp" >/dev/null
	done
	ufw deny 443/udp >/dev/null
	ufw --force enable >/dev/null
	say "ufw allows TCP ${ports[*]} and denies the rest, UDP 443 included"
}

#### Files

install_binaries() {
	local d b
	for d in "${!bins_of[@]}"; do
		install -d -o root -g root -m 0755 "$opt/$d" "$opt/$d/$version"
		for b in ${bins_of[$d]}; do
			install -o root -g root -m 0755 "$bin_dir/$b" "$opt/$d/$version/$b"
		done
	done
	install -o root -g root -m 0755 "$here/check.sh" "$opt/check.sh"
	install -o root -g root -m 0755 "$here/bugnet-ops" /usr/local/bin/bugnet-ops
	say "installed $version under $opt"
}

# shared_hosts_line prints allowed_shared_hosts for the index the router was rolled with
shared_hosts_line() {
	"$1" x402 allowed-hosts --index "$etc/router-x402-index.json" --config "$2"
}

install_configs() {
	local f tmp line
	for f in router edge secrets-route sessions ssh egress; do
		tmp=$(mktemp)
		cp "$here/config/$f.toml" "$tmp"
		if [[ $f == router && -f $etc/router-x402-index.json ]]; then
			line=$(shared_hosts_line "$opt/router/$version/bugnet-dev" "$tmp")
			awk -v line="$line" '
				/^[[:space:]]*\[/ { sec = $0 }
				sec ~ /^\[x402\]/ && /^allowed_shared_hosts[[:space:]]*=/ { print line; next }
				{ print }' "$here/config/$f.toml" >"$tmp"
		fi
		if [[ -f $etc/$f.toml ]] && ! cmp -s "$tmp" "$etc/$f.toml"; then
			install -d -m 0700 "$etc/previous"
			cp -p "$etc/$f.toml" "$etc/previous/$f.toml"
		fi
		install -o root -g root -m 0644 "$tmp" "$etc/$f.toml"
		rm -f "$tmp"
	done
	install -o root -g root -m 0644 "$here/config/packages.json" "$etc/packages.json"
	install -o root -g root -m 0644 "$here/maps/deploy.env" "$etc/deploy.env"
	for f in router edge secrets-route sessions ssh egress certs; do
		[[ ! -f $etc/$f.env ]] || { chown root:root "$etc/$f.env" && chmod 0600 "$etc/$f.env"; }
	done
	chmod 0600 "$keys"/*

	# The page bundles: the gateway team's entry page and bugnet-website's session page (DP 17).
	# A placeholder stands in until each team copies its build there
	install -d -o root -g root -m 0755 /srv/bugnet/secrets-page /srv/bugnet/secrets-page/assets \
		/srv/bugnet/session-page /srv/bugnet/session-page/assets
	[[ -f /srv/bugnet/secrets-page/index.html ]] ||
		echo '<!doctype html><title>Bugnet secrets</title>' >/srv/bugnet/secrets-page/index.html
	if [[ ! -f /srv/bugnet/session-page/index.html ]]; then
		echo '<!doctype html><title>Bugnet session</title>' >/srv/bugnet/session-page/index.html
		printf '%s\n' "Content-Security-Policy: default-src 'none'" 'Cache-Control: no-store' \
			'Referrer-Policy: no-referrer' 'X-Content-Type-Options: nosniff' \
			>/srv/bugnet/session-page/headers.txt
	fi
}

install_units() {
	local u
	for u in "${units[@]}"; do
		install -o root -g root -m 0644 "$here/systemd/$u@.service" /etc/systemd/system/
	done
	install -o root -g root -m 0644 "$here/systemd/router-worker.service" \
		"$here/systemd/bugnet-certs.service" "$here/systemd/bugnet-certs.timer" \
		/etc/systemd/system/
	systemctl daemon-reload
}

# install_certs keeps the preview wildcards by DNS-01 (DP 15.4) and issues the first pair, which
# the edge needs before it starts
install_certs() {
	local d
	ln -sfn "$version" "$opt/certs/current.new"
	mv -Tf "$opt/certs/current.new" "$opt/certs/current"
	install -o root -g root -m 0644 "$here/maps/certs.toml" "$here/maps/certs-staging.toml" "$etc/"
	install -d -o bugnet-certs -g bugnet-certs -m 0755 "$certs_root"
	for d in $(env_value "$here/maps/deploy.env" PRODUCTION_CERT_DIRS); do
		# setgid, so each new pair takes the edge's group (DP 8.1)
		install -d -o bugnet-certs -g bugnet-edge -m 2750 "$d"
		chown bugnet-certs:bugnet-edge "$d"
		chmod 2750 "$d"
	done
	for d in $(env_value "$here/maps/deploy.env" DEV_CERT_DIRS); do
		# front.sh gives the dev pair its reader's group; a host without the front keeps it closed
		[[ -d $d ]] || install -d -o bugnet-certs -g bugnet-certs -m 0750 "$d"
	done
	for d in $(env_value "$here/maps/deploy.env" STAGING_CERT_DIRS); do
		install -d -o bugnet-certs -g bugnet-certs -m 0750 "$(dirname "$d")" "$d"
	done
	systemctl enable --quiet --now bugnet-certs.timer
	for d in $(env_value "$here/maps/deploy.env" PRODUCTION_CERT_DIRS); do
		[[ -f $d/fullchain.pem ]] && continue
		say "issuing the first wildcard pair into $d, which takes a few minutes"
		# Its check of the edges' metrics fails while they are down, so only the pair counts here
		systemctl start bugnet-certs.service || true
		[[ -f $d/fullchain.pem ]] || die "bugnet-certs wrote no pair into $d; see" \
			"journalctl -u bugnet-certs: the DNS token in $etc/certs.env and DNS (README)"
	done
}

migrate() {
	say "migrating the database with router-api $version"
	systemd-run --wait --pipe --quiet --collect -p User=bugnet-router \
		-p EnvironmentFile="$etc/router.env" "$opt/router/$version/router-api" migrate ||
		die "router-api migrate failed, nothing was started"
}

#### Rolling

running() {
	systemctl list-units --plain --no-legend --state=active,activating,reloading "$1@*" |
		awk '{print $1}' | sed -n "s/^$1@\(.*\)\.service$/\1/p"
}

# wait_ready waits until the unit is active and its /readyz answers 200 ten times in a row.
# Old and new share the port with SO_REUSEPORT, so ten in a row reach the new one too
wait_ready() {
	local unit=$1 port=$2 streak=0
	for _ in $(seq 240); do
		if systemctl is-failed --quiet "$unit"; then
			return 1
		fi
		if systemctl is-active --quiet "$unit" &&
			curl -fsS -o /dev/null --max-time 2 "http://127.0.0.1:$port/readyz"; then
			streak=$((streak + 1))
			((streak < 10)) || return 0
		else
			streak=0
		fi
		sleep 0.5
	done
	return 1
}

# roll starts version's instance of each unit given beside the old ones, waits on each, then
# stops the old ones, which drain on their own (DP 15.7). The same version gets a fresh instance
roll() {
	local u inst d started=() old
	declare -A inst_of=()
	for u in "$@"; do
		d=${dir_of[$u]}
		inst=$version
		if running "$u" | grep -qx "$version"; then
			inst=${version}_r$(date +%s)
			ln -sfn "$version" "$opt/$d/$inst"
		fi
		inst_of[$u]=$inst
		say "starting $u@$inst"
		systemctl start "$u@$inst" || true
		started+=("$u@$inst")
		if ! wait_ready "$u@$inst" "${ready_of[$u]}"; then
			journalctl -u "$u@$inst" -n 30 --no-pager >&2 || true
			systemctl stop "${started[@]}" || true
			die "$u@$inst never became ready, so every new instance stopped and the old ones serve"
		fi
	done
	for u in "$@"; do
		systemctl enable --quiet "$u@${inst_of[$u]}"
		for old in $(running "$u"); do
			[[ $old != "${inst_of[$u]}" ]] || continue
			say "stopping $u@$old, which drains on its own"
			systemctl disable --quiet "$u@$old" || true
			systemctl stop --no-block "$u@$old"
		done
	done
}

restart_worker() {
	systemctl enable --quiet router-worker.service
	systemctl restart router-worker.service
	wait_ready router-worker.service 8081 || {
		journalctl -u router-worker -n 30 --no-pager >&2 || true
		die "router-worker never became ready"
	}
}

flip_current() {
	local d
	for d in "$@"; do
		ln -sfn "$version" "$opt/$d/current.new"
		mv -Tf "$opt/$d/current.new" "$opt/$d/current"
	done
}

# warm_names opens each name the edges issue by TLS-ALPN-01 once, so the certificate check that
# bugnet-certs runs sees every name issued (DP 15.4)
warm_names() {
	local n
	while read -r n; do
		curl -sS -o /dev/null --max-time 60 --resolve "$n:443:127.0.0.1" "https://$n/" ||
			say "warning: https://$n/ did not answer yet; its DNS record may be missing"
	done < <(awk '$2 ~ /^(edge|secrets|sessions|egress)$/ {print $1}' /etc/haproxy/sni.map)
	systemctl reset-failed bugnet-certs.service 2>/dev/null || true
	systemctl start bugnet-certs.service ||
		say "warning: bugnet-certs failed its check; check.sh fails the deploy on it"
}

#### Commands

cmd_deploy() {
	version=$1 bin_dir=$(cd "$2" && pwd)
	[[ $version =~ ^[A-Za-z0-9][A-Za-z0-9.-]{0,63}$ ]] ||
		die "the version must be letters, digits, dots and dashes; $usage"
	check_bin_dir "$bin_dir"
	# psql and runuser start in the working directory, which a service user may not enter
	cd /
	check_bundle
	preflight
	install_packages
	install_users
	install_postgres
	install_valkey
	install_binaries
	install_configs
	install_units
	install_haproxy
	install_firewall
	install_certs
	migrate

	local previous=""
	[[ ! -f $deployed ]] || previous=$(env_value "$deployed" current)
	local old_run
	old_run=$(readlink "$opt/run/current" || true)
	flip_current run
	if ! (roll "${units[@]}"); then
		[[ -z $old_run ]] || { ln -sfn "$old_run" "$opt/run/current.new" &&
			mv -Tf "$opt/run/current.new" "$opt/run/current"; }
		exit 1
	fi
	flip_current router edge ssh egress
	restart_worker
	printf 'current=%s\nprevious=%s\n' "$version" "$previous" >"$deployed"
	warm_names
	say "running check.sh"
	if ! "$opt/check.sh"; then
		die "check.sh failed, so the deploy failed. $version serves; to go back, run" \
			"deploy.sh ${previous:-<old version>} with that version's bundle"
	fi
	say "deployed $version"
}

cmd_keys() {
	local bin_dir f n pub tmp
	bin_dir=$(cd "$1" && pwd)
	check_bin_dir "$bin_dir"
	install -d -o root -g root -m 0755 "$etc"
	install -d -o root -g root -m 0700 "$keys"
	umask 077
	if [[ ! -f $keys/ssh_host_ed25519_key ]]; then
		ssh-keygen -q -t ed25519 -N "" -C bugnet-ssh -f "$keys/ssh_host_ed25519_key"
		say "made the SSH gateway's host key, which must never change (DP 15.5)"
	fi
	if [[ ! -f $keys/egress-ca.key ]]; then
		"$bin_dir/bugnet-egress" ca init --out "$keys" >/dev/null
		say "made the egress CA"
	fi
	install -o root -g root -m 0644 "$keys/egress-ca.pem" "$etc/egress-ca.pem"
	if [[ ! -f $keys/egress_box_k1 ]]; then
		"$bin_dir/bugnet-egress" box-key new --id k1 --out "$keys" |
			sed -n 's/^public_key: //p' >"$keys/egress_box_k1.pub"
		say "made the box key k1"
	fi
	touch "$etc/router.env"
	if ! has_name "$etc/router.env" BUGNET_INTERNAL_TOKEN; then
		"$bin_dir/bugnet-dev" keys --production >>"$etc/router.env"
		say "wrote the router's secrets and tokens to $etc/router.env"
	fi
	has_name "$etc/router.env" BUGNET_DATABASE_URL ||
		printf 'BUGNET_DATABASE_URL=%s\n' "$database_url" >>"$etc/router.env"
	# Each caller's token goes into its own env file too, copied without being printed
	for f in edge secrets-route sessions ssh egress certs; do
		touch "$etc/$f.env"
		for n in ${env_names[$f]:-}; do
			has_name "$etc/$f.env" "$n" || grep "^$n=" "$etc/router.env" >>"$etc/$f.env"
		done
	done
	for f in router edge secrets-route sessions ssh egress certs; do
		chown root:root "$etc/$f.env"
		chmod 0600 "$etc/$f.env"
	done
	chown root:root "$keys"/*
	chmod 0600 "$keys"/*
	tmp=$(env_value "$etc/router.env" BUGNET_EVENT_SIGNING_KEY)
	pub=$(BUGNET_EVENT_SIGNING_KEY=$tmp "$bin_dir/bugnet-dev" pubkey --event)
	cat <<EOF

Public values for the hand-offs (DP 15.5, Task 39 step 7):
  router ssh.host_keys:            $(cut -d' ' -f1,2 "$keys/ssh_host_ed25519_key.pub")
  router secrets.box_public_keys:  { id = "k1", key = "$(cat "$keys/egress_box_k1.pub")" }
                                   and to the gateway team, to pin in the bridge and page
  Payments router.event_public_key: $pub
  egress CA certificate:           $etc/egress-ca.pem (public; scp it to the Mac for the image)

Still to add by hand, with sudoedit (names only, README.md "Secrets"):
EOF
	for n in E2B_API_KEY BOAT_API_KEY BOXD_API_KEY BUGNET_GITHUB_TOKEN; do
		has_name "$etc/router.env" "$n" || printf '  %s in %s/router.env\n' "$n" "$etc"
	done
	has_name "$etc/certs.env" CF_DNS_API_TOKEN ||
		printf '  CF_DNS_API_TOKEN in %s/certs.env (DP 15.8 step 3)\n' "$etc"
	echo
	echo "Back up $keys off the VPS, encrypted. The SSH host key must never change."
}

# cmd_mtls installs what `bugnet-dev mtls init --out DIR` wrote, with XS 6.17's owners and modes,
# then deletes that copy of both service keys
cmd_mtls() {
	local src=$1 owner=root f
	for f in etc/bugnet/internal-ca.pem etc/bugnet/egress-x402-client.pem \
		etc/bugnet/keys/egress-x402-client.key etc/bugnet/payments/x402-server.pem \
		payments.env.add; do
		[[ -f $src/$f ]] || die "$src/$f is missing; copy the whole mtls init --out directory"
	done
	install -d -o root -g root -m 0755 "$etc"
	install -d -o root -g root -m 0700 "$keys"
	id -u bugnet-payments >/dev/null 2>&1 && owner=bugnet-payments
	install -d -o "$owner" -g "$owner" -m 0755 "$etc/payments"
	install -o root -g root -m 0644 "$src/etc/bugnet/internal-ca.pem" \
		"$src/etc/bugnet/egress-x402-client.pem" "$etc/"
	install -o root -g root -m 0600 "$src/etc/bugnet/keys/egress-x402-client.key" "$keys/"
	install -o "$owner" -g "$owner" -m 0644 "$src/etc/bugnet/payments/x402-server.pem" \
		"$etc/payments/"
	[[ -f $etc/payments.env ]] || install -o root -g root -m 0600 /dev/null "$etc/payments.env"
	if has_name "$etc/payments.env" BUGNET_X402_TLS_KEY; then
		say "payments.env already holds BUGNET_X402_TLS_KEY; a new key pair replaces that line" \
			"by hand with sudoedit, then Payments restarts (README)"
	else
		cat "$src/payments.env.add" >>"$etc/payments.env"
		say "appended BUGNET_X402_TLS_KEY to $etc/payments.env"
	fi
	rm -f "$src/etc/bugnet/keys/egress-x402-client.key" "$src/payments.env.add"
	say "installed the x402 mTLS files and deleted this copy of both keys"
	if [[ -n $(running bugnet-egress) ]]; then
		systemctl reload 'bugnet-egress@*'
	fi
	systemctl kill -s HUP --kill-whom=main 'bugnet-payments@*' 2>/dev/null || true
}

cmd_x402_index() {
	local router=0 index bugnet_dev
	if [[ ${1:-} == --router ]]; then
		router=1
		shift
	fi
	[[ $# -eq 1 && -f $1 ]] || die "$usage"
	index=$1
	bugnet_dev=$opt/router/current/bugnet-dev
	[[ -x $bugnet_dev ]] || die "no router is deployed yet, run a deploy first"
	# allowed-hosts parses the index, so a broken file stops here
	"$bugnet_dev" x402 allowed-hosts --index "$index" --config "$etc/router.toml" >/dev/null
	if ((router == 0)); then
		local owner=root
		id -u bugnet-payments >/dev/null 2>&1 && owner=bugnet-payments
		install -d -o "$owner" -g "$owner" -m 0755 "$etc/payments"
		install -o "$owner" -g "$owner" -m 0644 "$index" "$etc/payments/x402-index.json"
		systemctl kill -s HUP --kill-whom=main 'bugnet-payments@*' ||
			say "warning: no bugnet-payments unit took SIGHUP"
		say "Payments has the index; run x402-index --router next, then deploy the gateway"
		return
	fi
	install -o root -g root -m 0644 "$index" "$etc/router-x402-index.json"
	version=$(env_value "$deployed" current)
	[[ -n $version ]] || die "$deployed names no current version"
	local tmp line
	tmp=$(mktemp)
	line=$(shared_hosts_line "$bugnet_dev" "$etc/router.toml")
	awk -v line="$line" '
		/^[[:space:]]*\[/ { sec = $0 }
		sec ~ /^\[x402\]/ && /^allowed_shared_hosts[[:space:]]*=/ { print line; next }
		{ print }' "$etc/router.toml" >"$tmp"
	install -o root -g root -m 0644 "$tmp" "$etc/router.toml"
	rm -f "$tmp"
	say "router.toml: $line"
	roll router-api
	restart_worker
	"$opt/check.sh" || die "check.sh failed after the router took the index"
}

main() {
	need_root
	case ${1:-} in
	keys)
		[[ $# -eq 2 ]] || die "$usage"
		cmd_keys "$2"
		;;
	mtls)
		[[ $# -eq 2 && -d $2 ]] || die "$usage"
		cmd_mtls "$2"
		;;
	x402-index)
		shift
		cmd_x402_index "$@"
		;;
	*)
		[[ $# -eq 2 ]] || die "$usage"
		cmd_deploy "$1" "$2"
		;;
	esac
}

main "$@"
