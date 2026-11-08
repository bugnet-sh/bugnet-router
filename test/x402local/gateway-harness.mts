// Runs the real MCP gateway for the flow subtest of make x402-local through its own startGateway
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";

interface Input {
    gatewayDir: string;
    config: string;
    sellerHost: string;
    sellerAddr: string;
    sellerCA: string;
}

const input = JSON.parse(readFileSync(process.argv[2]!, "utf8")) as Input;
const pkgDir = join(input.gatewayDir, "packages", "gateway");
const src = (p: string) => import(join(pkgDir, "src", p));
const undici = await import(createRequire(join(pkgDir, "package.json")).resolve("undici"));

const { ConfigError, loadConfig } = await src("config.ts");
const { jsonLog } = await src("log.ts");
const { startGateway } = await src("start.ts");
const { createSafeFetcher } = await src("assess/fetch.ts");

// Probes reach the fake seller alone, dialled as egress' upstream_addrs do
const [sellerIP, sellerPort] = input.sellerAddr.split(":") as [string, string];
const tls = undici.buildConnector({ ca: readFileSync(input.sellerCA, "utf8") });
const agent = new undici.Agent({
    connect: (opts: { hostname: string }, done: (err: Error | null, s: unknown) => void) => {
        if (opts.hostname !== input.sellerHost) {
            done(new Error("x402 local: the probe reaches the fake seller only"), null);
            return;
        }
        tls(
            {
                ...opts,
                hostname: sellerIP,
                host: sellerIP,
                port: sellerPort,
                servername: input.sellerHost,
            },
            done,
        );
    },
});
const rawFetch = async (url: string, init: Record<string, unknown>) =>
    (await undici.fetch(url, { ...init, dispatcher: agent })) as unknown as Response;

const log = jsonLog("gateway");
try {
    const config = loadConfig(input.config);
    const x402Fetch = createSafeFetcher({
        timeoutMs: Math.max(config.x402.search_timeout, config.x402.probe_timeout),
        rawFetch,
    });
    const gateway = await startGateway(config, log, { x402Fetch });
    const stop = () => void gateway.stop().then(() => process.exit(0));
    process.on("SIGTERM", stop);
    process.on("SIGINT", stop);
} catch (e) {
    log.error(e instanceof ConfigError ? e.message : `start failed: ${(e as Error).message}`);
    process.exit(1);
}
