#!/usr/bin/env bash
# The dev front on the VPS (DP 15.2, 15.4 and 15.6). It sets up the SNI split, the tunnel user,
# bugnet-certs for the dev wildcard, the loopback rule for the tunnel port and the firewall. Run
# it as root from its bundle directory, which holds front.sh, systemd/, nftables/, tunnel.pub and
# the files `bugnet-dev deploy maps --front` writes.
#
#   sudo ./front.sh ./bugnet-certs
#
# It is idempotent. Port 22 stays out of HAProxy until the production deploy.
set -euo pipefail

usage="usage: front.sh <bugnet-certs binary>"
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
certs_root=/var/lib/bugnet-certs
sshd_dropin=/etc/ssh/sshd_config.d/bugnet-tunnel.conf

die() {
	printf 'front.sh: %s\n' "$*" >&2
	exit 1
}

say() {
	printf 'front.sh: %s\n' "$*"
}

# env_value prints KEY's value from deploy.env, never running the file
env_value() {
	sed -n "s/^$1=//p" "$here/deploy.env" | tail -n 1
}

check_inputs() {
	local f
	[[ $# -eq 1 ]] || die "$usage"
	[[ $EUID -eq 0 ]] || die "run it as root, with sudo"
	[[ -f $1 && -x $1 ]] || die "$1 is not an executable file; $usage"
	for f in sni.map end.map haproxy.cfg certs.toml certs-staging.toml deploy.env tunnel.pub \
		systemd/bugnet-certs.service systemd/bugnet-certs.timer systemd/bugnet-nft.service \
		nftables/dev-front.nft; do
		[[ -f $here/$f ]] || die "$here/$f is missing (bugnet-router README, The dev front)"
	done
	if grep -q '^frontend ssh$' "$here/haproxy.cfg"; then
		die "haproxy.cfg has frontend ssh, which waits for the production deploy;" \
			"write it with bugnet-dev deploy maps --front"
	fi
	[[ -z $(env_value PRODUCTION_CERT_DIRS) ]] ||
		die "deploy.env lists production wildcards; write it with bugnet-dev deploy maps --front"

	admin_port=$(env_value ADMIN_SSH_PORT)
	[[ $admin_port =~ ^[0-9]{1,5}$ ]] || die "deploy.env has no ADMIN_SSH_PORT"
	read -r -a dev_dirs <<<"$(env_value DEV_CERT_DIRS)"
	read -r -a staging_dirs <<<"$(env_value STAGING_CERT_DIRS)"
	((${#dev_dirs[@]} > 0)) || die "deploy.env lists no dev wildcard directory"
	for f in "${dev_dirs[@]}" "${staging_dirs[@]}"; do
		[[ $f =~ ^$certs_root/[a-z0-9-]+(/[a-z0-9-]+)?$ ]] ||
			die "deploy.env names $f, which is not under $certs_root"
	done

	# The tunnel may listen only where HAProxy's dev backends dial (DP 15.6)
	mapfile -t listens < <(sed -n \
		's/^    server mac \(127\.0\.0\.1:[0-9]\{1,5\}\) send-proxy-v2$/\1/p' "$here/haproxy.cfg")
	((${#listens[@]} > 0)) || die "haproxy.cfg has no dev backend for the tunnel"

	local fields
	read -r -a fields <"$here/tunnel.pub" || true
	[[ $(grep -c . "$here/tunnel.pub") -eq 1 && ${#fields[@]} -ge 2 &&
		${fields[0]} == ssh-ed25519 && ${fields[1]} =~ ^AAAAC3NzaC1lZDI1NTE5[A-Za-z0-9+/]+=*$ ]] ||
		die "tunnel.pub must be one ssh-ed25519 public key line, the Mac's (tunnel.md)"
	tunnel_key="${fields[0]} ${fields[1]}"

	reader=${BUGNET_CERTS_READER:-}
	if [[ -z $reader ]]; then
		[[ -n ${SUDO_USER:-} ]] || die "run it with sudo as the admin user make dev-certs" \
			"copies as, or set BUGNET_CERTS_READER to that user's group"
		reader=$(id -gn "$SUDO_USER")
	fi
	getent group "$reader" >/dev/null || die "group $reader does not exist"

	# ufw closes every other port, so admin SSH must already listen where site.toml says
	[[ -n $(ss -Hltn "sport = :$admin_port") ]] ||
		die "nothing listens on admin_ssh_port $admin_port; move admin SSH there first" \
			"(DP 15.8 step 4), or correct site.toml"
	if [[ $admin_port != 22 && -n $(ss -Htn state established '( sport = :22 )') ]]; then
		say "warning: SSH sessions on port 22 stay open, but ufw closes 22 to new logins," \
			"which need port $admin_port"
	fi
}

install_packages() {
	local missing=() p
	for p in haproxy ufw nftables; do
		[[ $(dpkg-query -W -f='${Status}' "$p" 2>/dev/null) == *'ok installed' ]] || missing+=("$p")
	done
	if ((${#missing[@]} > 0)); then
		say "installing ${missing[*]}"
		apt-get update -qq
		DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
			"${missing[@]}"
	fi
}

haproxy_listens() {
	[[ $(ss -Hltnp "sport = :$1") == *'"haproxy"'* ]]
}

install_haproxy() {
	local f p backup others
	for p in 443 2200; do
		others=$(ss -Hltnp "sport = :$p" | grep -v '"haproxy"' || true)
		[[ -z $others ]] || die "another process holds port $p, stop it first: $others"
	done

	backup=$(mktemp -d)
	for f in haproxy.cfg sni.map end.map; do
		if [[ -e /etc/haproxy/$f ]]; then
			cp -p "/etc/haproxy/$f" "$backup/$f"
		fi
	done
	install -o root -g root -m 0644 "$here/sni.map" "$here/end.map" "$here/haproxy.cfg" \
		/etc/haproxy/
	if ! haproxy -c -f /etc/haproxy/haproxy.cfg; then
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
		if haproxy_listens 443 && haproxy_listens 2200; then
			say "haproxy serves 443 and 2200"
			return
		fi
		sleep 0.2
	done
	die "haproxy does not listen on both 443 and 2200 after the reload; see journalctl -u haproxy"
}

install_tunnel_user() {
	local line opts="restrict,port-forwarding" l tmp effective
	if ! id -u tunnel >/dev/null 2>&1; then
		useradd --system --user-group --create-home --home-dir /home/tunnel \
			--shell /usr/sbin/nologin tunnel
	fi
	# No shell, and a password field that is unusable but not locked, so sshd takes the key
	usermod --shell /usr/sbin/nologin --password '*' tunnel
	for l in "${listens[@]}"; do
		opts+=",permitlisten=\"$l\""
	done
	line="$opts $tunnel_key mac"
	install -d -o root -g tunnel -m 0750 /home/tunnel/.ssh
	tmp=$(mktemp)
	printf '%s\n' "$line" >"$tmp"
	install -o root -g tunnel -m 0640 "$tmp" /home/tunnel/.ssh/authorized_keys
	rm -f "$tmp"

	grep -Eq '^[[:space:]]*Include[[:space:]]+/etc/ssh/sshd_config\.d/\*\.conf' \
		/etc/ssh/sshd_config || die "/etc/ssh/sshd_config does not include sshd_config.d"
	tmp=$(mktemp)
	cat >"$tmp" <<EOF
# The dev front's tunnel user may only listen on the dev backends (DP 15.6), from front.sh
Match User tunnel
    AllowTcpForwarding remote
    PermitListen ${listens[*]}
    PermitOpen none
    GatewayPorts no
    AllowAgentForwarding no
    AllowStreamLocalForwarding no
    X11Forwarding no
    PermitTTY no
    PermitTunnel no
    ClientAliveInterval 15
    ClientAliveCountMax 3
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    AuthenticationMethods publickey
    ForceCommand /usr/sbin/nologin
EOF
	if ! cmp -s "$tmp" "$sshd_dropin"; then
		local old=""
		if [[ -e $sshd_dropin ]]; then
			old=$(mktemp)
			cp -p "$sshd_dropin" "$old"
		fi
		install -o root -g root -m 0644 "$tmp" "$sshd_dropin"
		if ! sshd -t; then
			if [[ -n $old ]]; then
				cp -p "$old" "$sshd_dropin"
				rm -f "$old"
			else
				rm -f "$sshd_dropin"
			fi
			rm -f "$tmp"
			die "sshd -t refused the tunnel block, so sshd keeps its old settings"
		fi
		rm -f "$old"
		systemctl try-reload-or-restart ssh.service
	fi
	rm -f "$tmp"
	effective=$(sshd -T -C user=tunnel,host=localhost,addr=127.0.0.1)
	grep -qx 'allowtcpforwarding remote' <<<"$effective" ||
		die "sshd does not apply the tunnel block"
	say "tunnel may listen on ${listens[*]}"
}

# install_loopback_rule lets only haproxy connect to the dev tunnel's VPS end (DP 10.12.7.1). The
# nftables package stays disabled, since its own service flushes the ruleset
install_loopback_rule() {
	install -d -o root -g root -m 0755 /etc/bugnet /etc/bugnet/nftables
	install -o root -g root -m 0644 "$here/nftables/dev-front.nft" /etc/bugnet/nftables/
	install -o root -g root -m 0644 "$here/systemd/bugnet-nft.service" /etc/systemd/system/
	systemctl daemon-reload
	systemctl enable --quiet bugnet-nft.service
	systemctl restart bugnet-nft.service
	nft list table inet bugnet_dev_front_lo | grep -q 'skuid "haproxy" accept' ||
		die "the loopback rule for the tunnel port did not load"
	say "only haproxy may connect to 127.0.0.1:19443"
}

install_certs() {
	local bin=$1 ver code=0 d
	if ! id -u bugnet-certs >/dev/null 2>&1; then
		useradd --system --user-group --no-create-home --home-dir "$certs_root" \
			--shell /usr/sbin/nologin bugnet-certs
	fi
	ver=$(sha256sum "$bin" | cut -c1-12)
	install -d -o root -g root -m 0755 /opt/bugnet /opt/bugnet/certs "/opt/bugnet/certs/$ver"
	install -o root -g root -m 0755 "$bin" "/opt/bugnet/certs/$ver/bugnet-certs"
	ln -sfn "$ver" /opt/bugnet/certs/current.new
	mv -Tf /opt/bugnet/certs/current.new /opt/bugnet/certs/current
	# With no arguments bugnet-certs prints its usage, so 126 or 127 means it cannot run here
	runuser -u bugnet-certs -- /opt/bugnet/certs/current/bugnet-certs >/dev/null 2>&1 || code=$?
	[[ $code -ne 126 && $code -ne 127 ]] ||
		die "bugnet-certs does not run on this $(uname -m) machine; build it for linux"

	install -d -o root -g root -m 0755 /etc/bugnet
	install -o root -g root -m 0644 "$here/certs.toml" "$here/certs-staging.toml" /etc/bugnet/
	if [[ -e /etc/bugnet/certs.env ]]; then
		chown root:root /etc/bugnet/certs.env
		chmod 0600 /etc/bugnet/certs.env
	else
		install -o root -g root -m 0600 /dev/null /etc/bugnet/certs.env
		say "put the DNS token in /etc/bugnet/certs.env under the names dns_env lists (DP 15.8)"
	fi

	# systemd chowns a state directory whose owner differs, so this one is bugnet-certs' own
	install -d -o bugnet-certs -g bugnet-certs -m 0755 "$certs_root"
	for d in "${dev_dirs[@]}"; do
		# The setgid bit gives each new pair the reader's group, so make dev-certs can copy it
		install -d -o bugnet-certs -g "$reader" -m 2750 "$d"
		chown bugnet-certs:"$reader" "$d"
		chmod 2750 "$d"
		find "$d" -maxdepth 1 -type f -name '*.pem' -exec chgrp "$reader" {} +
	done
	for d in "${staging_dirs[@]}"; do
		install -d -o bugnet-certs -g bugnet-certs -m 0750 "$(dirname "$d")" "$d"
	done

	install -o root -g root -m 0644 "$here/systemd/bugnet-certs.service" \
		"$here/systemd/bugnet-certs.timer" /etc/systemd/system/
	systemctl daemon-reload
	systemctl enable --quiet --now bugnet-certs.timer
	say "bugnet-certs $ver writes ${dev_dirs[*]} for group $reader"
}

install_firewall() {
	local p want status
	want=$(printf 'ufw allow %s/tcp\n' 80 443 2200 "$admin_port")$'\nufw deny 443/udp'
	status=$(LC_ALL=C ufw status verbose)
	# A reset lifts the firewall for a moment, so one that already matches stays as it is
	if [[ $status == *'Status: active'*'Default: deny (incoming), allow (outgoing)'* &&
		$(LC_ALL=C ufw show added | tail -n +2) == "$want" ]]; then
		say "ufw already allows TCP 80, 443, 2200 and $admin_port alone"
		return
	fi
	ufw --force reset >/dev/null
	ufw default deny incoming >/dev/null
	ufw default allow outgoing >/dev/null
	for p in 80 443 2200 "$admin_port"; do
		ufw allow "$p/tcp" >/dev/null
	done
	ufw deny 443/udp >/dev/null
	ufw --force enable >/dev/null
	say "ufw allows TCP 80, 443, 2200 and $admin_port, and denies the rest, UDP 443 included"
}

main() {
	check_inputs "$@"
	install_packages
	install_haproxy
	install_tunnel_user
	install_loopback_rule
	install_certs "$1"
	install_firewall
	say "done; start autossh on the Mac as tunnel.md says, then run bugnet-certs (README)"
}

main "$@"
