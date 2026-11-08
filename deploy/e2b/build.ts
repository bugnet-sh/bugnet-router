import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { Template, defaultBuildLogger } from "e2b";

import { template } from "./template";

// templates.json lists each template with the CA it trusts, production or dev
interface Settings {
    ca: Record<string, { cert_file: string; version: number }>;
    templates: { name: string; cpu_count: number; memory_mb: number; ca: string }[];
}

const settings: Settings = JSON.parse(
    readFileSync(new URL("./templates.json", import.meta.url), "utf8"),
);

// The CA certificate is public, and a file holding a key is refused so a key never reaches an image
function readCA(which: string) {
    const entry = settings.ca[which];
    if (!entry || entry.cert_file === "") {
        throw new Error(`set ca.${which}.cert_file in templates.json to the ${which} CA`);
    }
    const path = entry.cert_file.replace(/^~\//, `${homedir()}/`);
    const pem = readFileSync(path, "utf8");
    if (!pem.includes("BEGIN CERTIFICATE") || pem.includes("PRIVATE KEY")) {
        throw new Error(`${path} must hold PEM certificates and no key`);
    }
    return { pem, version: entry.version };
}

// pnpm run build makes the production templates, pnpm run build:dev the dev ones
const which = process.argv[2] ?? "production";
if (!["production", "dev"].includes(which)) {
    throw new Error("usage: tsx build.ts [production|dev]");
}
const ca = readCA(which);

// Each router size is its own template, since a build fixes the vCPUs and memory
for (const size of settings.templates.filter((t) => t.ca === which)) {
    const info = await Template.build(template(ca), size.name, {
        cpuCount: size.cpu_count,
        memoryMB: size.memory_mb,
        onBuildLogs: defaultBuildLogger(),
    });
    console.log(`${size.name} is template ${info.templateId}`);
}
