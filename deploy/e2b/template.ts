import { Template } from "e2b";

// Only ssh-up's sshd behind the gate runs, so the image's own never starts and keeps no keys
const noStockSSHD = [
    "if command -v systemctl >/dev/null 2>&1; then",
    "systemctl disable ssh.service ssh.socket >/dev/null 2>&1 || true;",
    "systemctl mask ssh.service ssh.socket;",
    "fi;",
    "rm -f /etc/ssh/ssh_host_*",
].join(" ");

// Bugnet runs every command as root, so root is the default user and its home the workdir
export const template = Template()
    .fromBaseImage()
    .setUser("root")
    .runCmd(noStockSSHD)
    .setWorkdir("/root");
