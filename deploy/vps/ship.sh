#!/usr/bin/env bash
# Ships a production bundle from the Mac to the VPS over the admin SSH, then runs deploy.sh there
# with sudo (data-plane plan Task 39). It cross-builds linux/amd64 binaries from the checkouts
# beside this repository (bugnet-router, bugnet-edges, bugnet-run, with bugnet-proto through their
# go.mod replace lines), renders the HAProxy and bugnet-certs files with `bugnet-dev deploy maps`,
# and copies them with the units and settings to ~/bugnet-deploy/<version>/ on the VPS. It never
# makes, reads or prints a secret: the env files are made on the VPS (README.md).
#
#   deploy/vps/ship.sh [flags] [<user@host>]
#     --port N        admin SSH port, default site.toml's admin_ssh_port
#     --site FILE     default deploy/vps/site.toml; the host defaults to its ipv4
#     --version V     default <UTC date>-<bugnet-router commit>
#     --keys          run `deploy.sh keys` instead of a deploy, once before the first deploy
#     --mtls DIR      copy `bugnet-dev mtls init --out DIR` and run `deploy.sh mtls` instead
#     --copy-only     copy the bundle and print the command, run nothing
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
router=$(cd "$here/../.." && pwd)
root=$(dirname "$router")
edges=$root/bugnet-edges
run=$root/bugnet-run

die() {
	printf 'ship.sh: %s\n' "$*" >&2
	exit 1
}

say() {
	printf 'ship.sh: %s\n' "$*"
}

# site_value prints a plain top-level value of site.toml
site_value() {
	sed -n "s/^$1[[:space:]]*=[[:space:]]*\"\{0,1\}\([^\"#[:space:]]*\)\"\{0,1\}.*/\1/p" "$site" |
		head -n 1
}

port="" site=$here/site.toml version="" mode=deploy mtls="" host=""
while (($# > 0)); do
	case $1 in
	--port) port=${2:?--port needs a value} && shift ;;
	--site) site=${2:?--site needs a value} && shift ;;
	--version) version=${2:?--version needs a value} && shift ;;
	--keys) mode=keys ;;
	--mtls) mode=mtls mtls=${2:?--mtls needs a directory} && shift ;;
	--copy-only) mode=copy ;;
	-*) die "unknown flag $1" ;;
	*) host=$1 ;;
	esac
	shift
done

[[ -f $site ]] || die "$site is missing"
for d in "$edges" "$run" "$root/bugnet-proto"; do
	[[ -d $d ]] || die "$d is missing; ship.sh builds from the checkouts beside bugnet-router"
done
[[ -n $port ]] || port=$(site_value admin_ssh_port)
[[ -n $host ]] || host=$(site_value ipv4)
[[ -n $host ]] || die "give <user@host>, or write the VPS's address into site.toml's ipv4"
[[ $port =~ ^[0-9]+$ ]] || die "no admin SSH port; give --port"
[[ -z $mtls || -f $mtls/payments.env.add ]] || die "$mtls is not a mtls init --out directory"
[[ -n $version ]] || version=$(date -u +%Y%m%d-%H%M)-$(git -C "$router" rev-parse --short HEAD)
[[ $version =~ ^[A-Za-z0-9][A-Za-z0-9.-]{0,63}$ ]] || die "a version is letters, digits, . and -"

bundle=$(mktemp -d)
trap 'rm -rf "$bundle"' EXIT
mkdir -p "$bundle/bin" "$bundle/maps" "$bundle/config"

for repo in "$router" "$edges" "$run" "$root/bugnet-proto"; do
	dirty=""
	[[ -z $(git -C "$repo" status --porcelain) ]] || dirty=" (uncommitted changes)"
	printf '%s %s %s%s\n' "$(basename "$repo")" "$(git -C "$repo" branch --show-current)" \
		"$(git -C "$repo" rev-parse --short HEAD)" "$dirty"
done | tee "$bundle/COMMITS"

say "building linux/amd64 binaries"
export GOWORK=off CGO_ENABLED=0
(cd "$router" && GOOS=linux GOARCH=amd64 go build -trimpath -o "$bundle/bin/" \
	./cmd/router-api ./cmd/router-worker ./cmd/bugnet-dev)
(cd "$edges" && GOOS=linux GOARCH=amd64 go build -trimpath -o "$bundle/bin/" \
	./cmd/bugnet-edge ./cmd/bugnet-egress ./cmd/bugnet-ssh ./cmd/bugnet-certs)
(cd "$run" && make build >/dev/null)
cp "$run/dist/bugnet-run-linux-amd64" "$bundle/bin/"

say "rendering the HAProxy and bugnet-certs files from names.toml and $site"
(cd "$router" && go run ./cmd/bugnet-dev deploy maps --names "$here/names.toml" \
	--site "$site" --out "$bundle/maps" >/dev/null)

cp "$here/deploy.sh" "$here/check.sh" "$here/bugnet-ops" "$bundle/"
cp -R "$here/systemd" "$here/postgres" "$here/valkey" "$bundle/"
cp "$here"/config/*.toml "$bundle/config/"
cp "$router/config/packages.json" "$bundle/config/packages.json"
if [[ -n $mtls ]]; then
	cp -Rp "$mtls" "$bundle/mtls"
fi

dir="bugnet-deploy/$version"
say "copying the bundle to $host:$dir over port $port"
ssh -p "$port" "$host" "mkdir -p bugnet-deploy && rm -rf '$dir'"
scp -q -p -r -P "$port" "$bundle" "$host:$dir"
if [[ -n $mtls ]]; then
	say "the bundle's copy of both service keys is deleted on the VPS by deploy.sh mtls;" \
		"delete the Mac copies in $mtls once it passed (README)"
fi

case $mode in
keys) cmd="sudo ./deploy.sh keys ./bin" ;;
mtls) cmd="sudo ./deploy.sh mtls ./mtls" ;;
*) cmd="sudo ./deploy.sh $version ./bin" ;;
esac
if [[ $mode == copy ]]; then
	say "copied; on the VPS run: cd ~/$dir && $cmd"
	exit 0
fi
say "running on the VPS: cd ~/$dir && $cmd"
ssh -t -p "$port" "$host" "cd ~/$dir && $cmd"
