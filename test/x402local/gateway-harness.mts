// Runs the real MCP gateway for the flow subtest of make x402-local, as src/main.ts starts it
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
const { paymentsFor } = await src("payments/index.ts");
const { createRouterClient } = await src("router.ts");
const { startHttp } = await src("server.ts");
const { createAssessor } = await src("assess/index.ts");
const { createSafeFetcher } = await src("assess/fetch.ts");
const { watchHostsSeen } = await src("x402/index.ts");
const { refreshPayai } = await src("x402/search.ts");
const { pollTerms } = await src("x402/terms.ts");

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
    const deps = {
        config,
        router: createRouterClient(config, []),
        payments: paymentsFor(config),
        assessor: createAssessor(config),
        log,
        now: Date.now,
        x402Fetch: createSafeFetcher({
            timeoutMs: Math.max(config.x402.search_timeout, config.x402.probe_timeout),
            rawFetch,
        }),
    };
    const stops = [pollTerms(deps), watchHostsSeen(config.x402, log), refreshPayai(deps)];
    const http = await startHttp(deps);
    log.info("listening", {
        addr: config.listen_addr,
        payments: config.payments.mode,
        router: config.router.url,
    });
    const stop = () => {
        for (const s of stops) s();
        http.close(() => process.exit(0));
    };
    process.on("SIGTERM", stop);
    process.on("SIGINT", stop);
} catch (e) {
    log.error(e instanceof ConfigError ? e.message : `start failed: ${(e as Error).message}`);
    process.exit(1);
}
