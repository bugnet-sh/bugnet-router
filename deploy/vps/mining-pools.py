#!/usr/bin/env python3
"""Writes config/mining-pools.txt, the addresses harden blocks, from config/mining-pool-hosts.txt.

Resolves every host to its IPv4 and IPv6 addresses and drops any address inside Cloudflare's,
CloudFront's or Fastly's published ranges, since blocking a shared CDN address would cut sandboxes
off from unrelated sites. It refuses to write a list when any CDN range list cannot be read. Run it
on the Mac about monthly, commit the result, and ship.

    deploy/vps/mining-pools.py [--hosts FILE] [--out FILE]
"""

import argparse
import ipaddress
import json
import os
import socket
import ssl
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
MAX_ENTRIES = 10_000
TIMEOUT = 30
# The python.org build on macOS ships without trust roots, so the system bundle stands in
CAFILE = "/etc/ssl/cert.pem" if os.path.exists("/etc/ssl/cert.pem") else None


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "bugnet-mining-pools/1"})
    ctx = ssl.create_default_context(cafile=CAFILE)
    with urllib.request.urlopen(req, timeout=TIMEOUT, context=ctx) as resp:
        return resp.read().decode()


def cdn_ranges():
    nets = []
    for url in ("https://www.cloudflare.com/ips-v4", "https://www.cloudflare.com/ips-v6"):
        nets += [line.strip() for line in fetch(url).splitlines() if line.strip()]
    aws = json.loads(fetch("https://ip-ranges.amazonaws.com/ip-ranges.json"))
    nets += [p["ip_prefix"] for p in aws["prefixes"] if p["service"] == "CLOUDFRONT"]
    nets += [p["ipv6_prefix"] for p in aws["ipv6_prefixes"] if p["service"] == "CLOUDFRONT"]
    fastly = json.loads(fetch("https://api.fastly.com/public-ip-list"))
    nets += fastly["addresses"] + fastly["ipv6_addresses"]
    if not nets:
        raise RuntimeError("the CDN range lists came back empty")
    return [ipaddress.ip_network(n, strict=False) for n in nets]


def hosts(path):
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            yield line


def resolve(host):
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return set()
    return {ipaddress.ip_address(info[4][0]) for info in infos}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--hosts", type=Path, default=HERE / "config" / "mining-pool-hosts.txt")
    ap.add_argument("--out", type=Path, default=HERE / "config" / "mining-pools.txt")
    args = ap.parse_args()

    try:
        cdns = cdn_ranges()
    except Exception as err:
        sys.exit(f"mining-pools: cannot read the CDN ranges, so no list is written: {err}")

    kept, dropped, unresolved, names = set(), {}, [], 0
    for host in hosts(args.hosts):
        names += 1
        addrs = resolve(host)
        if not addrs:
            unresolved.append(host)
        for addr in addrs:
            if not addr.is_global:
                continue
            cdn = next((n for n in cdns if addr.version == n.version and addr in n), None)
            if cdn is not None:
                dropped[str(addr)] = f"{host} in {cdn}"
            else:
                kept.add(addr)

    if len(kept) > MAX_ENTRIES:
        sys.exit(f"mining-pools: {len(kept)} addresses, more than the router's {MAX_ENTRIES}")
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = [
        f"# Mining pool addresses for harden, written {stamp} by deploy/vps/mining-pools.py",
        f"# {names} hosts, {len(kept)} addresses, {len(dropped)} CDN addresses left out",
    ]
    for addr in sorted(kept, key=lambda a: (a.version, a)):
        lines.append(f"{addr}/{addr.max_prefixlen}")
    args.out.write_text("\n".join(lines) + "\n")

    print(f"wrote {args.out}: {len(kept)} addresses from {names} hosts")
    for addr, why in sorted(dropped.items()):
        print(f"left out {addr}: {why}")
    for host in unresolved:
        print(f"no address: {host}")


if __name__ == "__main__":
    main()
