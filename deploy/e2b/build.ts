import { Template, defaultBuildLogger } from "e2b";

import { template } from "./template";

// Each router size is its own template, since a build fixes the vCPUs and memory
const sizes = [
    { name: "bugnet-2x2", cpuCount: 2, memoryMB: 2048 },
    { name: "bugnet-4x8", cpuCount: 4, memoryMB: 8192 },
];

for (const size of sizes) {
    const info = await Template.build(template, size.name, {
        cpuCount: size.cpuCount,
        memoryMB: size.memoryMB,
        onBuildLogs: defaultBuildLogger(),
    });
    console.log(`${size.name} is template ${info.templateId}`);
}
