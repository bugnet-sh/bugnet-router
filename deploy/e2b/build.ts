import { readFileSync, writeFileSync } from "node:fs";
import { Template, defaultBuildLogger } from "e2b";

import { templateFor } from "./template";

// templates.json lists each size, the template name carries the version and the channel
interface Settings {
    templates: { size: string; cpu_count: number; memory_mb: number }[];
}

const settings: Settings = JSON.parse(
    readFileSync(new URL("./templates.json", import.meta.url), "utf8"),
);

// pnpm run build makes the production templates, pnpm run build:dev the dev ones
const channel = process.argv[2] ?? "prod";
if (!["prod", "dev"].includes(channel)) {
    throw new Error("usage: tsx build.ts [prod|dev]");
}
const imageRef = process.env.IMAGE_REF ?? "";
if (!/^[^@\s]+@sha256:[0-9a-f]{64}$/.test(imageRef)) {
    throw new Error("IMAGE_REF must be <registry>@sha256:<digest>");
}
const version = process.env.IMAGE_VERSION ?? "";
if (!/^\d{4}\.\d{2}\.\d+$/.test(version)) {
    throw new Error("IMAGE_VERSION must look like 2026.11.1");
}
if ((process.env.IMAGE_CHANNEL ?? channel) !== channel) {
    throw new Error("IMAGE_CHANNEL differs from the channel argument");
}

// The version with dashes, as E2B names allow
const v = version.replaceAll(".", "-");
const prefix = channel === "dev" ? "bugnet-dev" : "bugnet";

// Each router size is its own template, since a build fixes the vCPUs and memory
const built: { name: string; size: string; id: string }[] = [];
for (const size of settings.templates) {
    const name = `${prefix}-${v}-${size.size}`;
    const info = await Template.build(templateFor(imageRef), name, {
        cpuCount: size.cpu_count,
        memoryMB: size.memory_mb,
        onBuildLogs: defaultBuildLogger(),
    });
    console.log(`${name} is template ${info.templateId}`);
    built.push({ name, size: size.size, id: info.templateId });
}

// The E2B part of the build record, which scripts/image.sh merges into builds/<channel>-<v>.json
const out = process.env.E2B_RECORD;
if (out) {
    writeFileSync(out, JSON.stringify({ version, channel, templates: built }, null, 4) + "\n");
}
