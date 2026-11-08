# Mainnet test seller

The x402 test seller (XS 6.17) runs on the founder's Mac during a test run. Tailscale Funnel
publishes it through two nodes, `x402-seller-a` and `x402-seller-b`, and TLS ends in the seller.

- `bugnet-edges/testseller` is the front end, its own module so `tailscale.com` stays out of
  every production build. It serves `internal/fakeseller` on both nodes.
- `facilitator/` is the Node service that verifies and settles for the seller, on loopback only.
- `seller.toml` holds the settings, with every key at its default.
- `bugnet-dev x402 seller-check` is the gate before each run.

## Build and run

```
cd bugnet-edges/testseller && GOWORK=off go build -o ../bin/x402-test-seller .
cd bugnet-router/deploy/testseller/facilitator && pnpm install
TESTSELLER_TEST_TOKEN=$(openssl rand -hex 16) caffeinate -i \
    x402-test-seller -config seller.toml
```

It prints `https://<name>` for each node once it serves. `-only a` or `-only b` runs one node.
The check and the harness take the names from that output.

The front end refuses to start when `tailscale_env_file` or `env_file` is missing, is not mode
0600, or lacks its key. It never prints either file's values. A state directory is made 0700 when
missing, refused when group or others can read it, and never emptied. When a node's name is not
exactly its `name`, such as `x402-seller-a-1`, it stops and prints both.

## The check

```
bugnet-dev x402 seller-check --names <name-a>,<name-b>
```

It resolves through public resolvers, fails on any answer in 100.64.0.0/10, pays nothing, and
prints one line per check. It exits 1 on the first failure, naming it. It takes about 5 minutes.

1. Each name's certificate verifies for that name, waiting up to 90 s for a first handshake.
2. `GET /hold/150/` answers 200 after at least 150 s, with no byte before.
3. `GET /sse/` gives 36 events, each at most 7 s after the one before.
4. A 1 MiB `POST /cent/x` at 64 KiB a second gets its 402 before the last byte is sent.
5. 32 parallel `GET /free/` on their own connections, then 300 more within a minute, and the
   second name, all answer 200.

Run it from the VPS over `check_ssh` with a Linux build of `bugnet-dev`, then from the Mac.

## The facilitator

`facilitator/server.mjs` registers `@x402/svm`'s exact facilitator scheme at an exact version
with its lockfile. `GET /health`, `POST /verify` and `POST /settle` take
`{x402Version, paymentPayload, paymentRequirements}`. Its environment holds only
`TESTSELLER_FEE_PAYER_KEY`, `FACILITATOR_ADDR`, `SOLANA_RPC_URL`, `PATH` and `HOME`.
`facilitator_test.go` settles Task X34's two goldens against a fake JSON-RPC and is skipped
without `node` or `pnpm install`.
