#!/usr/bin/env bash
# The one setup script of the Bugnet base image, run as root on every provider
#   setup-image.sh build  --target oci|boxd|boat --channel dev|prod --inputs <sha256>
#   setup-image.sh finish --target e2b|boxd
#   setup-image.sh probe
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "$HERE/pins.conf"

LIB=/usr/local/lib/bugnet
STATE=$LIB/build.env
MARKER=/opt/bugnet/.build-marker
APT_BACKUP=/var/backups/bugnet-apt
TMPWORK=""

die() { echo "setup-image: $*" >&2; exit 1; }

cleanup() { if [ -n "$TMPWORK" ]; then rm -rf "$TMPWORK"; fi; }
trap cleanup EXIT

# ver prints the major and minor number of a command's version output, or none
ver() {
    command -v "$1" >/dev/null 2>&1 || { echo none; return; }
    local out
    out="$("$@" 2>&1 || true)"
    out="$(printf '%s\n' "$out" | grep -oE '[0-9]+\.[0-9]+' | head -n 1 || true)"
    echo "${out:-none}"
}

tools_json() {
    jq -n \
        --arg git "$(ver git --version)" --arg tar "$(ver tar --version)" \
        --arg python3 "$(ver python3 --version)" --arg node "$(ver node --version)" \
        --arg docker "$(ver docker --version)" --arg pnpm "$(ver pnpm --version)" \
        --arg uv "$(ver uv --version)" --arg hf "$(ver hf version)" \
        --arg gcc "$(ver gcc --version)" --arg make "$(ver make --version)" \
        --arg sshd "$(ver ssh -V)" --arg nft "$(ver nft --version)" \
        --arg curl "$(ver curl --version)" \
        '{git:$git,tar:$tar,python3:$python3,node:$node,docker:$docker,pnpm:$pnpm,uv:$uv,
          hf:$hf,gcc:$gcc,make:$make,sshd:$sshd,nft:$nft,curl:$curl}'
}

cmd_probe() { tools_json; }

pin() { # pin NAME prints the pinned value of a pins.conf key, empty when unset
    local v="${!1:-}"
    printf '%s' "$v"
}

fetch_checked() { # fetch_checked URL SHA256 FILE
    curl -fsSL --retry 3 -o "$3" "$1"
    echo "$2  $3" | sha256sum -c - >/dev/null || die "checksum mismatch for $1"
}

step1_refuse() {
    [ "$(id -u)" = 0 ] || die "must run as root"
    # shellcheck disable=SC1091
    . /etc/os-release
    [ "${ID:-}" = ubuntu ] || die "not ubuntu"
    case "${VERSION_ID:-}" in
        24.04) OS_NAME=ubuntu-24.04 ;;
        22.04) OS_NAME=ubuntu-22.04 ;;
        *) die "unsupported ubuntu ${VERSION_ID:-}" ;;
    esac
    mkdir -p /opt/bugnet "$LIB"
    touch "$MARKER"
}

step2_apt() {
    mkdir -p "$APT_BACKUP"
    cp -a /etc/apt/sources.list "$APT_BACKUP/" 2>/dev/null || true
    cp -a /etc/apt/sources.list.d "$APT_BACKUP/" 2>/dev/null || true
    local codename base
    # shellcheck disable=SC1091
    codename="$(. /etc/os-release && echo "$VERSION_CODENAME")"
    base="https://snapshot.ubuntu.com/ubuntu/${APT_SNAPSHOT}"
    rm -f /etc/apt/sources.list.d/ubuntu.sources
    cat >/etc/apt/sources.list <<EOF
deb [check-valid-until=no] $base $codename main restricted universe multiverse
deb [check-valid-until=no] $base $codename-updates main restricted universe multiverse
deb [check-valid-until=no] $base $codename-security main restricted universe multiverse
EOF
    printf '#!/bin/sh\nexit 101\n' >/usr/sbin/policy-rc.d
    chmod 0755 /usr/sbin/policy-rc.d
}

step3_packages() {
    export DEBIAN_FRONTEND=noninteractive
    # Ubuntu's base images carry no CA bundle, so apt verifies with the build host's until one lands
    if [ ! -s /etc/ssl/certs/ca-certificates.crt ] && [ -s "$HERE/apt-ca.pem" ]; then
        apt-get -o "Acquire::https::CAInfo=$HERE/apt-ca.pem" update --error-on=any
        apt-get -o "Acquire::https::CAInfo=$HERE/apt-ca.pem" install -y --no-install-recommends \
            ca-certificates
    fi
    apt-get update --error-on=any
    # APT_PACKAGES is a word list on purpose
    # shellcheck disable=SC2086
    apt-get install -y --no-install-recommends $APT_PACKAGES
}

step4_node() {
    TMPWORK="$(mktemp -d /opt/bugnet/work.XXXXXX)"
    local tgz="$TMPWORK/node.tar.xz"
    fetch_checked "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
        "$NODE_SHA256_AMD64" "$tgz"
    rm -rf /opt/bugnet/node
    mkdir -p /opt/bugnet/node
    tar -xJf "$tgz" -C /opt/bugnet/node --strip-components=1
    for b in node npm npx; do ln -sf "/opt/bugnet/node/bin/$b" "/usr/local/bin/$b"; done
    PATH="/opt/bugnet/node/bin:$PATH" npm install -g --ignore-scripts "pnpm@${PNPM_VERSION}"
    for b in pnpm pnpx; do ln -sf "/opt/bugnet/node/bin/$b" "/usr/local/bin/$b"; done
}

step5_uv() {
    local tgz="$TMPWORK/uv.tar.gz"
    fetch_checked \
        "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-x86_64-unknown-linux-gnu.tar.gz" \
        "$UV_SHA256_AMD64" "$tgz"
    tar -xzf "$tgz" -C "$TMPWORK"
    install -m 0755 "$TMPWORK"/uv-x86_64-unknown-linux-gnu/uv /usr/local/bin/uv
    install -m 0755 "$TMPWORK"/uv-x86_64-unknown-linux-gnu/uvx /usr/local/bin/uvx
}

step6_python22() {
    [ "$OS_NAME" = ubuntu-22.04 ] || return 0
    export UV_PYTHON_INSTALL_DIR=/opt/bugnet/python
    uv python install 3.12
    local py
    py="$(uv python find 3.12)"
    ln -sf "$py" /usr/local/bin/python3
    ln -sf "$py" /usr/local/bin/python3.12
}

step7_hf() {
    UV_TOOL_DIR=/opt/bugnet/uv-tools UV_TOOL_BIN_DIR=/usr/local/bin \
        UV_EXCLUDE_NEWER="$UV_EXCLUDE_NEWER" \
        uv tool install --python python3 "huggingface_hub[cli]==${HF_HUB_VERSION}"
}

step8_ca() {
    local f n key want got found=0
    for f in "$HERE"/bugnet-egress-v*.crt; do
        [ -e "$f" ] || continue
        found=1
        n="${f##*-v}"
        n="${n%.crt}"
        key="CA_SHA256_$(echo "$CHANNEL" | tr '[:lower:]' '[:upper:]')_V${n}"
        want="$(pin "$key")"
        [ -n "$want" ] || die "pins.conf has no $key"
        got="$(sha256sum "$f" | cut -d' ' -f1)"
        [ "$got" = "$want" ] || die "the CA certificate v${n} does not match $key"
        install -m 0644 "$f" "/usr/local/share/ca-certificates/bugnet-egress-v${n}.crt"
    done
    [ "$found" = 1 ] || die "the payload holds no CA certificate"
    update-ca-certificates
}

step9_sshd() {
    local target="$1"
    if [ "$target" != boat ]; then
        mkdir -p /etc/systemd/system
        if [ "$target" = oci ]; then
            # E2B's provisioning enables ssh and fails on a masked unit, so finish masks it after
            rm -f /etc/systemd/system/ssh.service /etc/systemd/system/sshd.service \
                /etc/systemd/system/ssh.socket \
                /etc/systemd/system/multi-user.target.wants/ssh.service \
                /etc/systemd/system/sockets.target.wants/ssh.socket
        else
            ln -sf /dev/null /etc/systemd/system/ssh.service
            ln -sf /dev/null /etc/systemd/system/ssh.socket
        fi
        if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
            systemctl stop ssh.service ssh.socket 2>/dev/null || true
        fi
        rm -f /etc/ssh/ssh_host_*
    fi
}

step10_install() {
    mkdir -p "$LIB"
    local arch
    arch="$(uname -m)"
    [ "$arch" = x86_64 ] || die "the payload runner is for amd64"
    install -m 0755 "$HERE/bugnet-run-linux-amd64" "$LIB/bugnet-run"
    install -m 0755 "$HERE/setup-image.sh" "$LIB/setup-image.sh"
    install -m 0644 "$HERE/pins.conf" "$LIB/pins.conf"
    "$LIB/bugnet-run" ping >/dev/null
    mkdir -p /etc/bugnet
    printf 'LANG=C.UTF-8\nLC_ALL=C.UTF-8\nHF_XET_CHUNK_CACHE_SIZE_BYTES=0\n' >/etc/bugnet/image.env
    chmod 0644 /etc/bugnet/image.env
    printf 'LANG=C.UTF-8\n' >/etc/default/locale
    locale -a | grep -qx 'C.utf8' || die "locale -a lists no C.utf8"
}

step11_clean() {
    if [ -d "$APT_BACKUP" ]; then
        [ -f "$APT_BACKUP/sources.list" ] && cp -a "$APT_BACKUP/sources.list" /etc/apt/sources.list
        if [ -d "$APT_BACKUP/sources.list.d" ]; then
            rm -rf /etc/apt/sources.list.d
            cp -a "$APT_BACKUP/sources.list.d" /etc/apt/sources.list.d
        fi
        rm -rf "$APT_BACKUP"
    fi
    rm -f /usr/sbin/policy-rc.d
    apt-get clean
    rm -rf /var/lib/apt/lists/* /root/.cache /root/.npm /root/.bash_history
    find /tmp /var/tmp -mindepth 1 -delete 2>/dev/null || true
    for f in /var/log/apt/* /var/log/dpkg.log; do
        [ -f "$f" ] && : >"$f"
    done
    return 0
}

step12_check() {
    local stray
    stray="$(find /root /tmp /var/tmp -type f -newer "$MARKER" 2>/dev/null | head -n 5 || true)"
    if [ -d /home ]; then
        stray="$stray$(find /home -type f -user root -newer "$MARKER" 2>/dev/null | head -n 5 || true)"
    fi
    [ -z "$stray" ] || die "the build left files behind: $stray"
    rm -f "$MARKER"
}

step13_manifest() {
    local target="$1" runner cas used disk
    runner="$(sha256sum "$LIB/bugnet-run" | cut -d' ' -f1)"
    cas="$(for f in /usr/local/share/ca-certificates/bugnet-egress-v*.crt; do
        [ -e "$f" ] || continue
        n="${f##*-v}"
        n="${n%.crt}"
        jq -n --argjson v "$n" --arg s "$(sha256sum "$f" | cut -d' ' -f1)" '{version:$v,sha256:$s}'
    done | jq -s '.')"
    used="$(df -B1 --output=used / | tail -n 1 | tr -d ' ')"
    disk=$((used - ${DISK_BASE:-0}))
    [ "$disk" -ge 0 ] || disk=0
    # shellcheck disable=SC1091
    . /etc/os-release
    mkdir -p /etc/bugnet
    jq -n --arg version "$(tr -d '[:space:]' <"$LIB/VERSION")" --arg channel "$CHANNEL" \
        --arg target "$target" --arg os "ubuntu-${VERSION_ID}" --arg inputs "$INPUTS" \
        --arg built "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg runner "$runner" \
        --argjson cas "$cas" --argjson tools "$(tools_json)" --argjson disk "$disk" \
        '{v:1,version:$version,channel:$channel,target:$target,os:$os,inputs_sha256:$inputs,
          built_at:$built,runner_sha256:{amd64:$runner},ca:$cas,tools:$tools,disk_bytes:$disk}' \
        >/etc/bugnet/image.json
    chmod 0644 /etc/bugnet/image.json
}

parse_args() {
    TARGET="" CHANNEL="" INPUTS=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --target) TARGET="$2"; shift 2 ;;
            --channel) CHANNEL="$2"; shift 2 ;;
            --inputs) INPUTS="$2"; shift 2 ;;
            *) die "unknown argument $1" ;;
        esac
    done
}

cmd_build() {
    parse_args "$@"
    case "$TARGET" in oci | boxd | boat) ;; *) die "--target must be oci, boxd or boat" ;; esac
    case "$CHANNEL" in dev | prod) ;; *) die "--channel must be dev or prod" ;; esac
    [[ "$INPUTS" =~ ^[0-9a-f]{64}$ ]] || die "--inputs must be a SHA-256"
    step1_refuse
    install -d "$LIB"
    install -m 0644 "$HERE/VERSION" "$LIB/VERSION"
    {
        echo "CHANNEL=$CHANNEL"
        echo "INPUTS=$INPUTS"
        echo "DISK_BASE=$(df -B1 --output=used / | tail -n 1 | tr -d ' ')"
    } >"$STATE"
    step2_apt
    step3_packages
    step4_node
    step5_uv
    step6_python22
    step7_hf
    step8_ca
    step9_sshd "$TARGET"
    step10_install
    step11_clean
    step12_check
    step13_manifest "$TARGET"
}

cmd_finish() {
    parse_args "$@"
    case "$TARGET" in e2b | boxd) ;; *) die "--target must be e2b or boxd" ;; esac
    [ "$(id -u)" = 0 ] || die "must run as root"
    [ -f "$STATE" ] || die "no image was built here"
    # shellcheck disable=SC1090
    . "$STATE"
    step9_sshd "$TARGET"
    step11_clean
    step13_manifest "$TARGET"
}

main() {
    local sub="${1:-}"
    [ $# -gt 0 ] && shift
    case "$sub" in
        build) cmd_build "$@" ;;
        finish) cmd_finish "$@" ;;
        probe) cmd_probe ;;
        *) die "usage: setup-image.sh build|finish|probe" ;;
    esac
}

main "$@"
