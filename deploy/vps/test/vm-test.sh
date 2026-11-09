#!/usr/bin/env bash
# Runs the VPS scripts in a throwaway Ubuntu 24.04 VM on the Mac, with limactl (Lima)
#
#   deploy/vps/test/vm-test.sh front
#
# front builds the dev front's bundle, runs front.sh twice, checks the result with
# front-checks.sh and checks the loopback rule again after a reboot of the VM. KEEP_VM=1 keeps the
# VM afterwards. The first run downloads Ubuntu's cloud image.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/../../.." && pwd)
edges=$(cd "$repo/../bugnet-edges" && pwd)

die() {
	printf 'vm-test.sh: %s\n' "$*" >&2
	exit 1
}

say() {
	printf 'vm-test.sh: %s\n' "$*"
}

goarch() {
	case $(uname -m) in
	arm64 | aarch64) echo arm64 ;;
	x86_64) echo amd64 ;;
	*) die "no Go architecture for $(uname -m)" ;;
	esac
}

# bundle writes the dev front's bundle for a site file into dir, as the README's steps do
bundle() {
	local site=$1 dir=$2
	shift 2
	(cd "$repo" && go run ./cmd/bugnet-dev deploy maps --names deploy/vps/names.toml \
		--site "$site" --out "$dir" "$@" >/dev/null)
	cp "$repo/deploy/vps/front.sh" "$dir/"
	cp -R "$repo/deploy/vps/systemd" "$repo/deploy/vps/nftables" "$dir/"
	cp "$work/tunnel.pub" "$dir/tunnel.pub"
	cp "$work/bugnet-certs" "$dir/bugnet-certs"
}

# site writes a site.toml with admin SSH on port
site() {
	printf 'ipv4 = "203.0.113.7"\nadmin_ssh_port = %s\n' "$2" >"$1"
}

in_vm() {
	limactl shell --workdir / "$vm" "$@"
}

# digest is the state front.sh owns, so a second run can be compared with the first
digest() {
	in_vm sudo bash -c 'sha256sum /etc/haproxy/haproxy.cfg /etc/haproxy/*.map \
		/home/tunnel/.ssh/authorized_keys /etc/ssh/sshd_config.d/bugnet-tunnel.conf \
		/etc/bugnet/certs.toml /etc/bugnet/certs-staging.toml /etc/systemd/system/bugnet-certs.* \
		/etc/bugnet/nftables/dev-front.nft /etc/systemd/system/bugnet-nft.service
		nft list table inet bugnet_dev_front_lo
		readlink /opt/bugnet/certs/current
		stat -c "%n %U:%G %a" /var/lib/bugnet-certs /var/lib/bugnet-certs/* /etc/bugnet/certs.env
		ufw status verbose'
}

front() {
	local first second code out
	say "building bugnet-certs for linux/$(goarch)"
	(cd "$edges" && CGO_ENABLED=0 GOOS=linux GOARCH=$(goarch) \
		go build -o "$work/bugnet-certs" ./cmd/bugnet-certs)
	ssh-keygen -q -t ed25519 -N "" -C mac -f "$work/tunnel"

	site "$work/site-vm.toml" 22
	bundle "$work/site-vm.toml" "$work/front" --front
	# Two refusals, the production config and admin SSH where nothing listens
	site "$work/site-full.toml" 2222
	bundle "$work/site-full.toml" "$work/full"
	bundle "$work/site-full.toml" "$work/moved" --front

	say "starting $vm"
	limactl start --name="$vm" --plain --cpus=2 --memory=2 --timeout=15m --tty=false \
		template:ubuntu-24.04 >/dev/null
	in_vm mkdir -p /tmp/vm-test
	limactl copy -r "$work/front" "$work/full" "$work/moved" "$vm:/tmp/vm-test/"
	limactl copy "$work/tunnel" "$here/front-checks.sh" "$vm:/tmp/vm-test/"
	in_vm chmod 0600 /tmp/vm-test/tunnel
	in_vm bash -c 'chmod 0755 /tmp/vm-test/*/front.sh /tmp/vm-test/*/bugnet-certs'

	for b in full moved; do
		code=0
		out=$(in_vm sudo "/tmp/vm-test/$b/front.sh" "/tmp/vm-test/$b/bugnet-certs" 2>&1) || code=$?
		[[ $code -ne 0 ]] || die "front.sh took the $b bundle"
		say "front.sh refused the $b bundle: $out"
	done
	in_vm sudo bash -c '! command -v haproxy >/dev/null' ||
		die "a refused bundle still installed haproxy"

	say "running front.sh"
	in_vm sudo /tmp/vm-test/front/front.sh /tmp/vm-test/front/bugnet-certs
	first=$(digest)
	say "running front.sh again"
	in_vm sudo /tmp/vm-test/front/front.sh /tmp/vm-test/front/bugnet-certs
	second=$(digest)
	[[ $first == "$second" ]] || die "the second run changed the state:
$(diff <(echo "$first") <(echo "$second") || true)"
	say "the second run changed nothing"

	in_vm sudo bash /tmp/vm-test/front-checks.sh /tmp/vm-test/front /tmp/vm-test/tunnel \
		"$(in_vm id -un)"

	say "rebooting $vm"
	limactl stop "$vm" >/dev/null
	limactl start --tty=false "$vm" >/dev/null
	in_vm sudo bash /tmp/vm-test/front-checks.sh /tmp/vm-test/front /tmp/vm-test/tunnel \
		"$(in_vm id -un)" loopback
}

cleanup() {
	if [[ -n ${vm:-} && ${KEEP_VM:-} != 1 ]] && limactl list -q 2>/dev/null | grep -qx "$vm"; then
		limactl delete --force "$vm" >/dev/null 2>&1 || true
	fi
	rm -rf "${work:-}"
}

main() {
	[[ $# -eq 1 && $1 == front ]] || die "usage: vm-test.sh front"
	command -v limactl >/dev/null || die "limactl is missing; brew install lima"
	command -v go >/dev/null || die "go is missing"
	vm=bugnet-vm-test-$1
	if limactl list -q 2>/dev/null | grep -qx "$vm"; then
		die "a VM named $vm exists; limactl delete --force $vm"
	fi
	work=$(mktemp -d)
	trap cleanup EXIT
	"$1"
	say "$1 passed"
}

main "$@"
