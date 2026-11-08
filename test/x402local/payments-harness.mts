// Runs the real Payments service on a fake chain for make x402-local, steered over HTTP
//
// The Go test writes the input file named by argv[2], starts this file with tsx from a directory
// whose node_modules links to bugnet-payments, and reads one JSON line with the control URL.
import { readFileSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { createServer as netServer } from "node:net";
import { createRequire } from "node:module";
import { join } from "node:path";

interface Input {
    paymentsDir: string;
    workDir: string;
    routerURL: string;
    routerToken: string;
    eventPublicKey: string;
    orderSeed: string;
    claimKey: string;
    paymentsToken: string;
    pki: { ca: string; serverCert: string; serverKeyB64: string };
}

const input = JSON.parse(readFileSync(process.argv[2]!, "utf8")) as Input;
const src = (p: string) => import(join(input.paymentsDir, "src", p));
const pkg = (p: string) => import(createRequire(join(input.paymentsDir, "package.json")).resolve(p));

const kit = await pkg("@solana/kit");
const { stringify, parse } = await pkg("smol-toml");
const { x402Client, x402HTTPClient } = await import(
    createRequire(join(input.paymentsDir, "package.json")).resolve("@x402/core/client")
);
const { startFakeChain } = await src("testing/fake-chain.ts");
const { testSigner } = await src("testing/keys.ts");
const { testDb } = await src("testing/db.ts");
const { parseSettings } = await src("config.ts");
const { startService } = await src("main.ts");
const { usdcAta } = await src("solana/accounts.ts");
const { orderPublicKeyBase64 } = await src("orders/token.ts");
const { serviceIdFor } = await src("x402/ids.ts");
const { jsonLog } = await src("log.ts");
const { signedPayment } = await src("testing/payment.ts");

function keypairSecret(seed: number, signer: { address: string }): string {
    const bytes = new Uint8Array(64);
    bytes.fill(seed, 0, 32);
    bytes.set(kit.getAddressEncoder().encode(signer.address), 32);
    return kit.getBase58Decoder().decode(bytes);
}

const SEEDS = { payer: 3, feePayer: 5, refund: 6, recipient: 1, allowance: 11 };
const signers: Record<string, { address: string; keyPair: unknown }> = {};
for (const [name, n] of Object.entries(SEEDS)) signers[name] = await testSigner(n);

const chain = await startFakeChain({ autoFinalize: true, autoAdvanceMs: 400 });
const own = [
    ["payer", 20_000_000n],
    ["recipient", 0n],
    ["refund", 0n],
    ["allowance", 0n],
] as const;
for (const [name, amount] of own) {
    const owner = signers[name]!.address;
    chain.setTokenAccount({ address: await usdcAta(owner), owner, amount });
}

const db = await testDb();
const dev = parse(readFileSync(join(input.paymentsDir, "config/dev.toml"), "utf8")) as Record<
    string,
    Record<string, unknown>
>;
// The public URL names the public port, so that port is picked before the service starts
const publicPort = await new Promise<number>((done, fail) => {
    const probe = netServer();
    probe.once("error", fail);
    probe.listen(0, "127.0.0.1", () => {
        const { port } = probe.address() as { port: number };
        probe.close(() => done(port));
    });
});
const tables = {
    ...dev,
    http: {
        ...dev.http,
        internal_listen: "127.0.0.1:0",
        public_listen: `127.0.0.1:${publicPort}`,
        public_url: `http://127.0.0.1:${publicPort}`,
    },
    database: { url: db.url },
    solana: { ...dev.solana, recipient: signers.recipient!.address },
    router: {
        ...dev.router,
        url: input.routerURL,
        event_public_key: input.eventPublicKey,
        allowance_claim: true,
    },
    x402: {
        ...dev.x402,
        enabled: true,
        allowance_wallet: signers.allowance!.address,
        listen: "127.0.0.1:0",
        tls_cert_file: input.pki.serverCert,
        client_ca_file: input.pki.ca,
        indexed_services_file: join(input.workDir, "x402-index.json"),
        min_allowance_micros: 50_000,
        release_every: "1s",
        events_every: "1s",
        fast_pass_every: "1s",
        mirror_every: "1s",
        mirror_idle_every: "1s",
    },
};
writeFileSync(join(input.workDir, "x402-index.json"), '{"v":1,"services":[]}');
const settings = parseSettings(stringify(tables), "x402-local");
const secrets = {
    paymentsToken: input.paymentsToken,
    routerToken: input.routerToken,
    orderSeed: Buffer.from(input.orderSeed, "base64"),
    claimKey: Buffer.from(input.claimKey, "base64"),
    feePayerKey: keypairSecret(SEEDS.feePayer, signers.feePayer!),
    refundKey: keypairSecret(SEEDS.refund, signers.refund!),
    rpcUrl: "",
    allowanceKey: keypairSecret(SEEDS.allowance, signers.allowance!),
    x402TlsKey: Buffer.from(input.pki.serverKeyB64, "base64").toString("utf8"),
};
// The fake chain has no getBalance and no simulation, so those two answer for the fee payer's
// float and for the order facilitator's check before it broadcasts
const withBalance = new Proxy(chain.rpc, {
    get(target, prop, receiver) {
        if (prop === "getBalance") {
            return () => ({ send: async () => ({ value: 1_000_000_000n }) });
        }
        if (prop === "simulateTransaction") {
            return () => ({
                send: async () => ({
                    context: { slot: chain.slot() },
                    value: { err: null, logs: [], unitsConsumed: 5_000n },
                }),
            });
        }
        return Reflect.get(target, prop, receiver);
    },
});
const svc = await startService({
    settings,
    secrets,
    rpc: withBalance,
    log: jsonLog("payments", { secrets: [] }),
});

// The fake chain answers jsonParsed reads only, so the payment is built from the challenge's hints
const pay = new x402HTTPClient(new x402Client());
const internal = (path: string, body: unknown) =>
    fetch(`http://127.0.0.1:${svc.internalPort}${path}`, {
        method: "POST",
        headers: {
            authorization: `Bearer ${input.paymentsToken}`,
            "content-type": "application/json",
        },
        body: JSON.stringify(body),
    });

interface OrderBody {
    packageId: string;
    packageMicros: number;
    allowanceMicros: number;
    feeMicros: number;
    method: string;
    url: string;
    maxPriceMicros: number;
    payTo: string;
    accepted: unknown;
}

// A start order with one pinned service, paid from the test payer, then redeemed for its token
async function order(o: OrderBody): Promise<unknown> {
    const u = new URL(o.url);
    const prefix = u.pathname.slice(0, u.pathname.lastIndexOf("/") + 1);
    const created = (await (
        await internal("/v1/orders", {
            kind: "start",
            packageId: o.packageId,
            amountMicros: o.packageMicros + o.allowanceMicros + o.feeMicros,
            allowanceMicros: o.allowanceMicros,
            feeMicros: o.feeMicros,
            description: "Bugnet x402 local run",
            x402: {
                services: [
                    {
                        serviceId: serviceIdFor(o.method, u.host, prefix),
                        method: o.method,
                        host: u.host,
                        prefix,
                        url: o.url,
                        maxPriceMicros: o.maxPriceMicros,
                        payTo: o.payTo,
                        listed: "unlisted",
                        pinnedFrom: "probe",
                        probe: { at: new Date().toISOString(), status: 402, accepted: o.accepted },
                    },
                ],
            },
        })
    ).json()) as { status: string; orderId: string; claim: string; payUrl: string };
    if (created.status !== "payment_required") throw new Error(`order: ${JSON.stringify(created)}`);
    const res = await fetch(created.payUrl, { headers: { accept: "application/json" } });
    const required = pay.getPaymentRequiredResponse((n: string) => res.headers.get(n), await res.json());
    const payload = await signedPayment(required.accepts[0], signers.payer);
    const paid = await fetch(created.payUrl, { headers: pay.encodePaymentSignatureHeader(payload) });
    if (paid.status !== 200) throw new Error(`pay: ${paid.status} ${await paid.text()}`);
    const redeemed = (await (
        await internal("/v1/orders", { orderId: created.orderId, claim: created.claim })
    ).json()) as { status: string; orderToken?: string };
    if (redeemed.status !== "paid") throw new Error(`redeem: ${JSON.stringify(redeemed)}`);
    return { orderId: created.orderId, orderToken: redeemed.orderToken };
}

async function ledger(): Promise<unknown> {
    const rows = await svc.pool.query(
        `select ref, state, amount_micros::text as amount_micros, tx_signature, host, path
         from x402_payments order by created_at`,
    );
    const acct = await svc.pool.query(
        `select id, funded_micros::text, spent_micros::text, held_micros::text, state
         from x402_accounts`,
    );
    const tail = await svc.pool.query("select * from x402_tail");
    const incidents = await svc.pool
        .query("select kind, detail from x402_incidents order by id")
        .catch(() => ({ rows: [] }));
    return {
        tail: tail.rows,
        incidents: incidents.rows,
        payments: rows.rows,
        accounts: acct.rows,
        allowanceUsdc: String(chain.balance(await usdcAta(signers.allowance!.address))),
        submitted: chain.submitted().length,
    };
}

const server = createServer(async (req, res) => {
    try {
        const url = new URL(req.url ?? "/", "http://x");
        const raw: Buffer[] = [];
        for await (const c of req) raw.push(c as Buffer);
        const body = raw.length > 0 ? JSON.parse(Buffer.concat(raw).toString("utf8")) : {};
        let out: unknown;
        if (url.pathname === "/account") {
            const ata = await usdcAta(body.owner);
            chain.setTokenAccount({ address: ata, owner: body.owner, amount: 0n });
            out = { ata };
        } else if (url.pathname === "/order") {
            out = await order(body);
        } else if (url.pathname === "/ledger") {
            out = await ledger();
        } else if (url.pathname === "/balance") {
            out = { micros: String(chain.balance(await usdcAta(body.owner))) };
        } else if (url.pathname === "/stop") {
            out = { ok: true };
            res.end(JSON.stringify(out));
            await svc.stop();
            await chain.close();
            await db.close();
            process.exit(0);
        } else {
            res.statusCode = 404;
            out = { error: "not found" };
        }
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify(out));
    } catch (err) {
        res.statusCode = 500;
        res.end(JSON.stringify({ error: String(err instanceof Error ? err.stack : err) }));
    }
});
server.listen(0, "127.0.0.1", () => {
    const { port } = server.address() as { port: number };
    process.stdout.write(
        `${JSON.stringify({
            control: `http://127.0.0.1:${port}`,
            signer: svc.x402Port,
            orderPublicKey: orderPublicKeyBase64(Buffer.from(input.orderSeed, "base64")),
            chain: chain.url,
        })}\n`,
    );
});
for (const sig of ["SIGTERM", "SIGINT"] as const) {
    process.on(sig, () => {
        void svc.stop().finally(() => process.exit(0));
    });
}
