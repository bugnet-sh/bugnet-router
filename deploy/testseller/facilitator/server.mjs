// The test seller's x402 facilitator, @x402/svm's exact scheme behind /verify and /settle
import { createServer } from "node:http";
import { x402Facilitator } from "@x402/core/facilitator";
import { registerExactSvmScheme } from "@x402/svm/exact/facilitator";
import { toFacilitatorSvmSigner } from "@x402/svm";
import { createKeyPairSignerFromBytes, getBase58Encoder } from "@solana/kit";

const MAINNET = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp";
const MAX_BODY = 1 << 20;

// keyBytes reads a Solana key as base58 or as a JSON array of 64 numbers
function keyBytes(value) {
    const text = value.trim();
    if (text.startsWith("[")) return Uint8Array.from(JSON.parse(text));
    return Uint8Array.from(getBase58Encoder().encode(text));
}

// fail ends the process with a reason that never holds the key
function fail(reason) {
    console.error(`facilitator: ${reason}`);
    process.exit(1);
}

const keyValue = process.env.TESTSELLER_FEE_PAYER_KEY;
const rpcUrl = process.env.SOLANA_RPC_URL;
const listen = process.env.FACILITATOR_ADDR ?? "127.0.0.1:8402";
if (!keyValue) fail("TESTSELLER_FEE_PAYER_KEY is not set");
if (!rpcUrl) fail("SOLANA_RPC_URL is not set");
const sep = listen.lastIndexOf(":");
const host = listen.slice(0, sep);
const port = Number(listen.slice(sep + 1));
if (!["127.0.0.1", "::1", "localhost"].includes(host)) fail("FACILITATOR_ADDR must be loopback");

let keypair;
try {
    keypair = await createKeyPairSignerFromBytes(keyBytes(keyValue));
} catch {
    fail("TESTSELLER_FEE_PAYER_KEY is not a Solana key");
}

const facilitator = new x402Facilitator();
registerExactSvmScheme(facilitator, {
    signer: toFacilitatorSvmSigner(keypair, { defaultRpcUrl: rpcUrl }),
    networks: MAINNET,
});

// readJSON reads a request body of at most MAX_BODY bytes
async function readJSON(req) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
        size += chunk.length;
        if (size > MAX_BODY) throw new Error("body too large");
        chunks.push(chunk);
    }
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

// send answers one JSON body
function send(res, status, body) {
    const text = JSON.stringify(body, (_, v) => (typeof v === "bigint" ? v.toString() : v));
    res.writeHead(status, { "content-type": "application/json" });
    res.end(text);
}

const server = createServer(async (req, res) => {
    if (req.method === "GET" && req.url === "/health") return send(res, 200, { ok: true });
    const action = req.url === "/verify" ? "verify" : req.url === "/settle" ? "settle" : "";
    if (req.method !== "POST" || !action) return send(res, 404, { error: "not found" });
    let body;
    try {
        body = await readJSON(req);
    } catch {
        return send(res, 400, { error: "bad body" });
    }
    try {
        const answer = await facilitator[action](body.paymentPayload, body.paymentRequirements);
        return send(res, 200, answer);
    } catch (err) {
        console.error(`facilitator: ${action} failed: ${err?.message ?? "unknown"}`);
        const reason = typeof err?.errorReason === "string" ? err.errorReason : "unexpected_error";
        return send(res, 200, action === "verify"
            ? { isValid: false, invalidReason: reason }
            : { success: false, errorReason: reason, transaction: "", network: MAINNET });
    }
});

server.listen(port, host, () => {
    const a = server.address();
    console.log(`facilitator listening ${a.address}:${a.port}`);
});
for (const sig of ["SIGTERM", "SIGINT"]) process.on(sig, () => server.close(() => process.exit(0)));
