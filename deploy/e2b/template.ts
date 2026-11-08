import { Template } from "e2b";

// Only ssh-up's sshd behind the gate runs, so the image's own never starts and keeps no keys
const noStockSSHD = [
    "if command -v systemctl >/dev/null 2>&1; then",
    "systemctl disable ssh.service ssh.socket >/dev/null 2>&1 || true;",
    "systemctl mask ssh.service ssh.socket;",
    "fi;",
    "rm -f /etc/ssh/ssh_host_*",
].join(" ");

// The Bugnet CA a template trusts, its certificate in PEM and the version the router names
export interface CA {
    pem: string;
    version: number;
}

// A boot's setup finds the file by this name and the SHA-256 of its exact bytes (DP 6.9)
export function caFile(version: number): string {
    return `/usr/local/share/ca-certificates/bugnet-egress-v${version}.crt`;
}

// installCA writes the certificate byte for byte, then lets the image's own tool trust it
export function installCA(ca: CA): string {
    const file = caFile(ca.version);
    const body = Buffer.from(ca.pem).toString("base64");
    return [
        "set -e;",
        "if ! command -v update-ca-certificates >/dev/null 2>&1; then",
        "apt-get update -qq && apt-get install -y -qq ca-certificates; fi;",
        "mkdir -p /usr/local/share/ca-certificates;",
        `echo ${body} | base64 -d > ${file};`,
        `chmod 644 ${file};`,
        "update-ca-certificates",
    ].join(" ");
}

// Bugnet runs every command as root, so root is the default user and its home the workdir
export function template(ca: CA) {
    return Template()
        .fromBaseImage()
        .setUser("root")
        .runCmd(noStockSSHD)
        .runCmd(installCA(ca))
        .setWorkdir("/root");
}
