#!/usr/bin/env bash
# The checks of vm-test.sh front, run as root inside the VM after front.sh
#   front-checks.sh <bundle dir> <tunnel private key> <admin user>
set -euo pipefail
# Sorted lists and the commands' messages read the same in any locale
export LC_ALL=C

bundle=$1
key=$2
admin=$3
reader=$(id -gn "$admin")
failed=0

pass() {
	printf 'PASS %s\n' "$*"
}

fail() {
	printf 'FAIL %s\n' "$*"
	failed=1
}

# check runs a command quietly and records whether it succeeded
check() {
	local name=$1
	shift
	if "$@" >/dev/null 2>&1; then pass "$name"; else fail "$name"; fi
}

equal() {
	if [[ $2 == "$3" ]]; then pass "$1"; else fail "$1: got [$2], want [$3]"; fi
}

ssh_opts=(-i "$key" -p 22 -o BatchMode=yes -o StrictHostKeyChecking=no
	-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ExitOnForwardFailure=yes)

tunnel_ssh() {
	timeout 20 ssh "${ssh_opts[@]}" "$@" </dev/null
}

listening() {
	ss -Hltn "sport = :$1" | grep -q "127.0.0.1:$1"
}

# HAProxy
check "haproxy -c passes" haproxy -c -f /etc/haproxy/haproxy.cfg
check "haproxy is active" systemctl is-active --quiet haproxy
equal "haproxy listens on 443 twice and on 2200" \
	"$(ss -Hltnp | awk '/"haproxy"/ {print $4}' | sort -u | tr '\n' ' ')" \
	"0.0.0.0:2200 0.0.0.0:443 [::]:443 "
check "frontend ssh is left out" bash -c '! grep -q "^frontend ssh$" /etc/haproxy/haproxy.cfg'
for f in haproxy.cfg sni.map end.map; do
	check "/etc/haproxy/$f is the bundle's" cmp -s "$bundle/$f" "/etc/haproxy/$f"
done

# The tunnel user
equal "tunnel has no shell" "$(getent passwd tunnel | cut -d: -f7)" /usr/sbin/nologin
read -r -a pub <"$bundle/tunnel.pub"
ports=(18443 18445 18448 19443 12223)
opts="restrict,port-forwarding"
for p in "${ports[@]}"; do
	opts+=",permitlisten=\"127.0.0.1:$p\""
done
equal "tunnel's authorized_keys line is DP 15.6's" \
	"$(cat /home/tunnel/.ssh/authorized_keys)" "$opts ${pub[0]} ${pub[1]} mac"
equal "authorized_keys is root's, readable by tunnel" \
	"$(stat -c '%U:%G %a' /home/tunnel/.ssh /home/tunnel/.ssh/authorized_keys | tr '\n' ' ')" \
	"root:tunnel 750 root:tunnel 640 "
t=$(sshd -T -C user=tunnel,host=localhost,addr=127.0.0.1)
r=$(sshd -T -C user=root,host=localhost,addr=127.0.0.1)
equal "sshd gives tunnel remote forwarding only" \
	"$(grep -E '^(allowtcpforwarding|permitlisten|permitopen|permittty|forcecommand) ' <<<"$t" |
		sort | tr '\n' '|')" \
	"allowtcpforwarding remote|forcecommand /usr/sbin/nologin|permitlisten$(printf \
		' 127.0.0.1:%s' "${ports[@]}")|permitopen none|permittty no|"
equal "sshd drops a dead tunnel session within about 45 s" \
	"$(grep -E '^clientalive(interval|countmax) ' <<<"$t" | sort | tr '\n' '|')" \
	"clientalivecountmax 3|clientaliveinterval 15|"
equal "the tunnel block leaves other users alone" \
	"$(grep -E '^(allowtcpforwarding|x11forwarding|permittty) ' <<<"$r" | sort | tr '\n' '|')" \
	"allowtcpforwarding yes|permittty yes|x11forwarding yes|"

# The tunnel's pid is timeout's own, which hands its signal on to ssh
timeout 60 ssh "${ssh_opts[@]}" -N -R 127.0.0.1:18443:127.0.0.1:8443 \
	-R 127.0.0.1:18445:127.0.0.1:8445 -R 127.0.0.1:18448:127.0.0.1:8448 \
	-R 127.0.0.1:19443:127.0.0.1:9443 \
	-R 127.0.0.1:12223:127.0.0.1:2223 tunnel@127.0.0.1 </dev/null &
tunnel_pid=$!
up=0
for _ in $(seq 50); do
	if listening 18443 && listening 18445 && listening 18448 && listening 19443 && listening 12223; then
		up=1
		break
	fi
	sleep 0.2
done
if ((up)); then
	pass "tunnel listens on the five ports"
else
	fail "tunnel listens on the five ports"
fi
kill "$tunnel_pid" 2>/dev/null || true
wait "$tunnel_pid" 2>/dev/null || true

code=0
tunnel_ssh -N -R 127.0.0.1:18444:127.0.0.1:8443 tunnel@127.0.0.1 || code=$?
equal "tunnel may not listen on another port" "$code" 255
code=0
tunnel_ssh -N -R 0.0.0.0:18443:127.0.0.1:8443 tunnel@127.0.0.1 || code=$?
equal "tunnel may not listen beyond loopback" "$code" 255
code=0
tunnel_ssh -W 127.0.0.1:22 tunnel@127.0.0.1 || code=$?
equal "tunnel may not open a connection" "$code" 255
code=0
out=$(tunnel_ssh tunnel@127.0.0.1 true 2>&1) || code=$?
if [[ $code -ne 0 && $out == *"not available"* ]]; then
	pass "tunnel runs no command"
else
	fail "tunnel runs no command: exit $code, $out"
fi

# The firewall
v=$(ufw status verbose)
check "ufw is active" grep -q '^Status: active' <<<"$v"
check "ufw denies by default" grep -q 'Default: deny (incoming)' <<<"$v"
equal "ufw allows TCP 80, 443, 2200 and admin SSH only" \
	"$(ufw status | awk '$2 == "ALLOW" {print $1}' | sort -u | tr '\n' ' ')" \
	"22/tcp 2200/tcp 443/tcp 80/tcp "
check "ufw denies UDP 443" grep -Eq '^443/udp +DENY' <<<"$(ufw status)"
check "iptables drops UDP 443" bash -c \
	'iptables -S ufw-user-input | grep -q -- "-p udp -m udp --dport 443 -j DROP"'

# bugnet-certs
check "the timer is enabled" systemctl is-enabled --quiet bugnet-certs.timer
check "the timer is active" systemctl is-active --quiet bugnet-certs.timer
check "systemd-analyze verify passes" systemd-analyze verify \
	/etc/systemd/system/bugnet-certs.service /etc/systemd/system/bugnet-certs.timer
equal "certs.env is root's alone" "$(stat -c '%U:%G %a' /etc/bugnet/certs.env)" "root:root 600"
check "current points at a version" test -x /opt/bugnet/certs/current/bugnet-certs

# A run without the token stops at dns_env, which proves the unit, its user and the settings file
since=$(date '+%Y-%m-%d %H:%M:%S')
check "a run without the DNS token fails" bash -c '! systemctl start bugnet-certs.service'
sleep 1
check "the run names the missing variable" bash -c \
	"journalctl -u bugnet-certs.service --since '$since' | grep -q 'dns_env names CF_DNS_API_TOKEN'"
systemctl reset-failed bugnet-certs.service || true

# The README's staging run, which must hand back bugnet-certs' own exit status
code=0
out=$(systemd-run --wait --pipe --quiet --collect \
	-p User=bugnet-certs -p Group=bugnet-certs -p EnvironmentFile=/etc/bugnet/certs.env \
	-p StateDirectory=bugnet-certs -p StateDirectoryMode=0755 \
	/opt/bugnet/certs/current/bugnet-certs -config /etc/bugnet/certs-staging.toml 2>&1) || code=$?
if [[ $code -eq 1 && $out == *"dns_env names CF_DNS_API_TOKEN"* ]]; then
	pass "the staging run exits 1 naming the missing variable"
else
	fail "the staging run: exit $code, $out"
fi

equal "the state directory is bugnet-certs' own" \
	"$(stat -c '%U:%G %a' /var/lib/bugnet-certs)" "bugnet-certs:bugnet-certs 755"
equal "the dev wildcard's directory keeps the reader's group and the setgid bit" \
	"$(stat -c '%U:%G %a' /var/lib/bugnet-certs/preview-dev)" "bugnet-certs:$reader 2750"
equal "the staging directory is bugnet-certs' own" \
	"$(stat -c '%U:%G %a' /var/lib/bugnet-certs/staging/preview-dev)" \
	"bugnet-certs:bugnet-certs 750"
probe=/var/lib/bugnet-certs/preview-dev/probe.pem
runuser -u bugnet-certs -- sh -c "umask 027 && echo probe > $probe"
equal "a new file takes the reader's group" "$(stat -c '%G' "$probe")" "$reader"
check "the admin user can read it" runuser -u "$admin" -- cat "$probe"
rm -f "$probe"

if ((failed)); then
	echo "front checks failed"
	exit 1
fi
echo "front checks passed"
