// Command bugnet-dev drives a Sandbox Router from the terminal during development.
//
// keys, pubkey, token, deploy and mtls work offline, and owned calls the provider with its key from
// the environment. Every other command calls the API at api.listen_addr of the settings file over
// unencrypted HTTP/2 with BUGNET_INTERNAL_TOKEN as the bearer. create and extend sign their own
// order token when BUGNET_ORDER_PRIVATE_KEY is set, priced from the settings file's
// packages_file. Only commands that need a setting read the file. exec and watch write a job's
// output to stdout and stderr as it arrives and exit with the job's code. transfer prints a
// transfer URL and its curl command, preview a preview link, and ssh-key the SSH hand-off of a
// public key, refusing a private key file before anything is sent. revoke-preview, wallet and
// session hold call OperatorService with BUGNET_OPERATOR_TOKEN instead, and session rotate moves a
// placement's session link on. bench start and bench report call it too, bench router times
// orders through the API, and simulate replays the start samples it reads from
// BUGNET_DATABASE_URL. deploy maps renders the VPS's HAProxy and bugnet-certs files from
// deploy/vps. mtls init makes the internal CA and the egress to Payments certificate pairs, renews
// them for their existing keys, or makes a development set.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/solana"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
)

const usage = `usage: bugnet-dev [--config FILE] <command> [flags]

  keys [--production]                    print fresh secrets as KEY=value lines, --production
                                         without BUGNET_ORDER_PRIVATE_KEY, which Payments holds
  pubkey [--priv K]                      print the order token public key of a private key
  pubkey --event                         print the public half of BUGNET_EVENT_SIGNING_KEY
  token --package P [--kind start|extend] [--handle H] [--order O] [--amount N] [--ttl D]
        [--priv K] [--payer A] [--fee-payer A] [--tx S] [--allowance-micros N]
        [--no-issued-at]                 print a signed order token, with issued_at set to now
                                         unless --no-issued-at
  create --package P [--order O] [--token T] [--payer A] [--fee-payer A] [--tx S]
         [--allowance-micros N] [--estimate-id ID] [--quote Q] [--no-issued-at]
                                         create a placement and wait until it is ready, the
                                         token signed with issued_at unless --no-issued-at
  reserve --quote Q --package P [--ssh] [--preview] [--secrets] [--client-ip A]
                                         hold a prepared sandbox for the quote and print what
                                         Reserve answered, then create --quote Q binds it
  fetch-source --handle H                put the placement's GitHub source in its sandbox and
                                         print the bootstrap state
  get --handle H                         print a placement
  extend --handle H --package P [--order O] [--token T]
                                         extend a ready placement
  check-extend --handle H --package P    print a placement and an extension's deadline and price
  extensions --handle H                  list the packages that can extend a placement
  stop --handle H                        stop a placement
  exec --handle H [--cwd D] [--env K=V]... -- ARGV
                                         run a command and stream its output
  watch --handle H --job J [--cursor C]  stream a job's output after a cursor
  transfer --handle H (--upload FILE | --download PATH) [--path P] [--sha256 HEX]
                                         print a transfer URL and the curl command that uses it,
                                         an upload landing at --path or ~/<file name>
  preview --handle H --port N            print the preview link of one port of a placement
  secret --handle H --name N --host HOST... [--header NAME]... [--fetch-only]
                                         seal a value read from stdin to the router's newest
                                         sealing key and put it as a secret of the placement
  x402 check [--handle H] [--service M,HOST,PREFIX,PRICE,PAY_TO[,PAID_PATH]]...
        [--disable ID]...                check an x402 change without making it, a service
                                         indexed when it has no paid path, with no handle only
                                         the checks that need no placement
  x402 enable --handle H [--service ...]... [--disable ID]... [--wait] [--request-id R]
                                         make an x402 change and print every service
  x402 list --handle H                   list a placement's x402 services and refusals
  x402 seller-check --names A,B [--resolvers HOST:PORT,...]
                                         check the mainnet test seller's two public names
                                         over public DNS, paying nothing, in about 5 minutes
  x402 allowed-hosts --index FILE --config FILE
                                         print the router's allowed_shared_hosts line: the
                                         index's hosts under the settings file's
                                         shared_hosting_suffixes, sorted, each once
  revoke-preview ID --reason R [--actor A]
                                         revoke a preview for good, the actor from USER when
                                         --actor is not given
  ssh-key --handle H --public-key FILE [--identity-file P] [--known-hosts-file P]
                                         register a public key on a placement and print the
                                         ssh command, known_hosts lines and an ssh_config stanza
  ssh-key --handle H --remove FINGERPRINT
                                         remove a key from a placement
  wallet block PAYER --reason R [--until T] [--stop-live] [--actor A]
                                         block a payer's new orders, until a time or a
                                         duration from now, or until lifted
  wallet unblock PAYER [--note N] [--actor A]
                                         lift a payer's block
  wallet show PAYER                      print a wallet, its live placements and its events
  wallet list [--blocked] [--limit N] [--page TOKEN]
                                         list wallets in payer order
  abuse find TARGET                      print the placement a report names, TARGET being a
                                         placement id, a preview URL or id, a transfer URL or
                                         bnx_ token, or PROVIDER:SANDBOX_ID
  abuse end TARGET --note N [--actor A]  end the placement for abuse, live or already ended,
                                         block every payer and print the payers blocked
  placement end TARGET --note N [--refund] [--actor A]
                                         end a live placement with no wallet action
  signals [--since T] [--provider P] [--class C] [--held] [--limit N]
                                         list provider signals newest first, --since an RFC 3339
                                         time or a duration back from now, --held for the
                                         signals whose wallet actions the guard held
  providers                              print each provider's health, account state, slots and
                                         limit source
  estimate --replay ID                   fetch a stored estimate and run the rules locally on its
                                         inputs and catalog, comparing the floor and the
                                         recommendation byte for byte
  bench start --provider P [--size S] [--runs 30] [--concurrency 3] [--yes]
        [--allow-paid-starts] [--actor A]
                                         start a probe benchmark in the worker and print its id
                                         and estimated cost, --yes accepting an estimate over
                                         bench.confirm_above_micros
  bench report [--bench-id ID | --since 24h] [--kind bench,canary,placement]
        [--provider P] [--json]
                                         print the start timings of a benchmark or a window,
                                         one table per provider, size and kind
  bench router --package P [--runs 20] [--concurrency 2] [--path plain|reserved]
        [--pay-delay 8s] [--json]
                                         time orders from payment to the first command against a
                                         development router, with dev order tokens, one table per
                                         start path, reserved calling Reserve and waiting the
                                         pay delay before each order
  simulate [--since 7d] [--budget-seconds 30[,N...]] [--providers A,B] [--orders 1000]
        [--json]
                                         replay the recorded start samples through the saga's
                                         failover rules, the samples read from the database at
                                         BUGNET_DATABASE_URL
  session hold --placement ID            print a held session record, as OperatorService
                                         serves it to operators
  session rotate --handle H              end a placement's session link and print its new one
  hooks register|list|rotate PROVIDER    register this deployment's webhook endpoint at e2b or
                                         boat, list the endpoints, or rotate the secret and
                                         print it once, the path secret from
                                         BUGNET_HOOK_PATH_SECRET and Boat's admin key from
                                         BOAT_ADMIN_API_KEY, each asked for with echo off
  hooks delete PROVIDER ID               remove an endpoint
  pools                                  list every warm pool with its members, target, demand
                                         and pause, as OperatorService serves it
  pools drain --provider P --size S [--pause D] [--actor A]
                                         retire the warm members of one pool, and with --pause
                                         keep its refill off that long
  owned PROVIDER                         count the provider account's sandboxes under Bugnet's
                                         labels and list their ids, its key read from the
                                         environment
  deploy maps --names FILE --site FILE --out DIR [--front]
                                         write the VPS's HAProxy maps and config, bugnet-certs'
                                         settings and deploy.env, --front for the dev front
  mtls init --out DIR [--state DIR]      make the internal CA, Payments' server pair and
                                         egress's client pair, laid out under DIR as on the
                                         VPS, the CA key kept in --state, default
                                         ~/.local/state/bugnet-dev/mtls
  mtls init --renew --from CLIENT_PEM,SERVER_PEM --out DIR [--state DIR]
                                         renew both certificates for their existing keys
  mtls init --dev --out DIR              a development CA and both pairs flat in DIR, the CA
                                         key not kept

Settings
  --config FILE             router settings file, default config/dev.toml, read only by commands
                            that call the API or price a token without --amount
  api.listen_addr           the API is at http://ADDR, with 127.0.0.1 for an empty or
                            unspecified host
  packages_file             catalog that prices a token when --amount is not given

Environment
  BUGNET_INTERNAL_TOKEN     bearer token for the API
  BUGNET_OPERATOR_TOKEN     bearer token for revoke-preview, wallet, abuse, placement end, signals,
                            providers, pools, estimate, bench start and report, and session hold,
                            which call OperatorService
  BUGNET_DATABASE_URL       Postgres URL, read only by simulate
  BUGNET_ORDER_PRIVATE_KEY  signs order tokens when --priv or --token is not given
`

const (
	defaultConfig = "config/dev.toml"
	defaultTTL    = 10 * time.Minute
	pollWaitMs    = 25000
	// execWaitMs keeps Exec short so the output streams through WatchJob as it arrives
	execWaitMs  = 1
	secretBytes = 32
)

var errUsage = errors.New("bad usage")

// exitCode ends bugnet-dev with a job's exit code
type exitCode int

func (c exitCode) Error() string { return "exit code " + strconv.Itoa(int(c)) }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv)
	stop()
	os.Exit(code)
}

type cli struct {
	ctx        context.Context
	stdout     io.Writer
	stderr     io.Writer
	getenv     func(string) string
	configPath string
	settings   *config.Config // read on first use
	// seller replaces the seller check's resolver and dialer in tests
	seller *sellerCheck
	// ask replaces the terminal prompt with echo off in tests
	ask func(label string) (string, error)
	// hookBase replaces each provider's webhook API address in tests
	hookBase map[string]string
}

// run executes one command and returns the process exit code, 2 for bad usage
func run(
	ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string,
) int {
	c := &cli{ctx: ctx, stdout: stdout, stderr: stderr, getenv: getenv}
	commands := map[string]func([]string) error{
		"keys": c.keys, "pubkey": c.pubkey, "token": c.token, "create": c.create, "get": c.get,
		"reserve": c.reserve,
		"extend":  c.extend, "check-extend": c.checkExtend, "extensions": c.extensions,
		"stop": c.stop, "exec": c.exec, "watch": c.watch, "deploy": c.deploy,
		"transfer": c.transfer, "owned": c.owned, "preview": c.preview,
		"revoke-preview": c.revokePreview, "secret": c.secret, "ssh-key": c.sshKey,
		"wallet": c.wallet, "x402": c.x402, "mtls": c.mtls, "estimate": c.estimate,
		"session":      c.session,
		"fetch-source": c.fetchSource, "hooks": c.hooks, "bench": c.benchCmd,
		"simulate": c.simulate, "abuse": c.abuse, "placement": c.placementCmd,
		"signals": c.signals, "providers": c.providers, "pools": c.pools,
	}
	global := newFlags("bugnet-dev")
	global.StringVar(&c.configPath, "config", defaultConfig, "settings file")
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = io.WriteString(stdout, usage)
			return 0
		}
		_, _ = fmt.Fprintf(stderr, "bugnet-dev: %v\n\n%s", err, usage)
		return 2
	}
	args = global.Args()
	switch {
	case len(args) == 0:
		_, _ = io.WriteString(stderr, usage)
		return 2
	case args[0] == "help":
		_, _ = io.WriteString(stdout, usage)
		return 0
	}
	command, ok := commands[args[0]]
	if !ok {
		_, _ = fmt.Fprintf(stderr, "bugnet-dev: unknown command %q\n\n%s", args[0], usage)
		return 2
	}
	err := command(args[1:])
	var code exitCode
	switch {
	case err == nil:
		return 0
	case errors.As(err, &code):
		return int(code)
	case errors.Is(err, flag.ErrHelp):
		_, _ = io.WriteString(stdout, usage)
		return 0
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "bugnet-dev %s: %v\n\n%s", args[0], err, usage)
		return 2
	}
	_, _ = fmt.Fprintf(stderr, "bugnet-dev %s: %v\n", args[0], err)
	return 1
}

/* Arguments */

func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

// parse refuses positional arguments unless the command takes them
func parse(fs *flag.FlagSet, args []string, positional bool) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	if !positional && fs.NArg() > 0 {
		return fmt.Errorf("%w: unexpected argument %q", errUsage, fs.Arg(0))
	}
	return nil
}

func required(name, value string) error {
	if value == "" {
		return fmt.Errorf("%w: --%s is required", errUsage, name)
	}
	return nil
}

func isSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) { set = set || f.Name == name })
	return set
}

// envFlag collects repeated --env K=V values
type envFlag map[string]string

func (e envFlag) String() string { return "" }

func (e envFlag) Set(kv string) error {
	k, v, ok := strings.Cut(kv, "=")
	if !ok || k == "" {
		return fmt.Errorf("%q is not K=V", kv)
	}
	e[k] = v
	return nil
}

/* Keys and tokens */

// keys prints only secrets, the order token public key comes from pubkey. In production Payments
// holds the order key, so --production leaves it out (DP 15.5)
func (c *cli) keys(args []string) error {
	fs := newFlags("keys")
	production := fs.Bool("production", false, "leave out BUGNET_ORDER_PRIVATE_KEY")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	_, orderPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	_, eventPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	b64 := base64.StdEncoding.EncodeToString
	lines := [][2]string{
		{"BUGNET_INTERNAL_TOKEN", hex.EncodeToString(rand32())},
		{"BUGNET_ORDER_PRIVATE_KEY", b64(orderPriv.Seed())},
		{"BUGNET_HANDLE_KEY", b64(rand32())},
		{"BUGNET_EVENT_SIGNING_KEY", b64(eventPriv.Seed())},
		{"BUGNET_DATA_PLANE_KEYS", dpkeys.NewEnv()},
	}
	if *production {
		lines = slices.Delete(lines, 1, 2)
	}
	for _, name := range []string{
		"BUGNET_EDGE_TOKEN", "BUGNET_SECRETS_TOKEN", "BUGNET_SSH_TOKEN", "BUGNET_EGRESS_TOKEN",
		"BUGNET_EGRESS_FETCH_TOKEN", "BUGNET_OPERATOR_TOKEN", "BUGNET_PAYMENTS_ROUTER_TOKEN",
		"BUGNET_SESSIONS_TOKEN",
	} {
		lines = append(lines, [2]string{name, hex.EncodeToString(rand32())})
	}
	// The edge compares this part of the hooks path, so it is base64url without padding
	lines = append(lines,
		[2]string{"BUGNET_HOOK_PATH_SECRET", base64.RawURLEncoding.EncodeToString(rand32())})
	return c.print(lines)
}

// rand32 returns secretBytes random bytes, and crypto/rand never fails to give them
func rand32() []byte {
	b := make([]byte, secretBytes)
	_, _ = rand.Read(b)
	return b
}

// pubkey prints the key order_token_public_key needs for the order private key, or with --event
// the key Payments verifies the router's events with, read from the environment only
func (c *cli) pubkey(args []string) error {
	fs := newFlags("pubkey")
	priv := fs.String("priv", "", "base64 Ed25519 private key")
	event := fs.Bool("event", false, "the public half of BUGNET_EVENT_SIGNING_KEY")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	var key ed25519.PrivateKey
	var err error
	switch {
	case *event && *priv != "":
		return fmt.Errorf("%w: --event reads the key from the environment, not --priv", errUsage)
	case *event:
		raw := c.getenv("BUGNET_EVENT_SIGNING_KEY")
		if raw == "" {
			return fmt.Errorf("%w: BUGNET_EVENT_SIGNING_KEY is not set", errUsage)
		}
		seed, derr := base64.StdEncoding.DecodeString(raw)
		if derr != nil || len(seed) != ed25519.SeedSize {
			return errors.New("BUGNET_EVENT_SIGNING_KEY must be base64 of a 32 byte seed")
		}
		key = ed25519.NewKeyFromSeed(seed)
	default:
		if key, err = c.privateKey(*priv); err != nil {
			return err
		}
	}
	pub := key.Public().(ed25519.PublicKey)
	_, err = fmt.Fprintln(c.stdout, base64.StdEncoding.EncodeToString(pub))
	return err
}

// order is what an order token claims before it is signed
type order struct {
	kind   ordertoken.Kind
	pkg    string
	id     string // random when empty
	handle string // extend tokens only
	amount int64
	priced bool // amount was given, so the catalog is not read
	ttl    time.Duration
	priv   string // BUGNET_ORDER_PRIVATE_KEY when empty
	// payer, feePayer, tx and allowance are the payment claims, each left out when empty or 0
	payer     string
	feePayer  string
	tx        string
	allowance int64
	// noIssuedAt leaves the issued_at claim out, which Payments adds after settlement
	noIssuedAt bool
}

// paymentFlags adds --payer, --fee-payer, --tx and --allowance-micros to fs
func paymentFlags(fs *flag.FlagSet, o *order) {
	fs.StringVar(&o.payer, "payer", "", "base58 Solana address that paid")
	fs.StringVar(&o.feePayer, "fee-payer", "", "base58 address of the facilitator's fee payer")
	fs.StringVar(&o.tx, "tx", "", "base58 signature of the payment's transaction")
	fs.Int64Var(&o.allowance, "allowance-micros", 0, "x402 allowance inside the amount, start only")
	fs.BoolVar(&o.noIssuedAt, "no-issued-at", false, "leave the issued_at claim out")
}

func (c *cli) token(args []string) error {
	fs := newFlags("token")
	var o order
	kind := fs.String("kind", string(ordertoken.KindStart), "start or extend")
	fs.StringVar(&o.pkg, "package", "", "package id")
	fs.StringVar(&o.handle, "handle", "", "handle the extend token is for")
	fs.StringVar(&o.id, "order", "", "order id")
	fs.Int64Var(&o.amount, "amount", 0, "paid micro-USD")
	fs.DurationVar(&o.ttl, "ttl", defaultTTL, "token lifetime")
	fs.StringVar(&o.priv, "priv", "", "base64 Ed25519 private key")
	paymentFlags(fs, &o)
	if err := parse(fs, args, false); err != nil {
		return err
	}
	o.kind, o.priced = ordertoken.Kind(*kind), isSet(fs, "amount")
	token, err := c.sign(o)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(c.stdout, token)
	return err
}

func (c *cli) sign(o order) (string, error) {
	if o.kind != ordertoken.KindStart && o.kind != ordertoken.KindExtend {
		return "", fmt.Errorf("%w: --kind must be start or extend", errUsage)
	}
	if err := required("package", o.pkg); err != nil {
		return "", err
	}
	if o.ttl <= 0 {
		return "", fmt.Errorf("%w: --ttl must be positive", errUsage)
	}
	if err := o.checkPayment(); err != nil {
		return "", err
	}
	claims := ordertoken.Claims{
		OrderID: o.id, Kind: o.kind, PackageID: o.pkg, AmountMicros: o.amount,
		ExpiresAt: time.Now().UTC().Add(o.ttl), Payer: o.payer, FeePayer: o.feePayer,
		Tx: o.tx, AllowanceMicros: o.allowance,
	}
	if claims.OrderID == "" {
		claims.OrderID = "dev-" + strings.ToLower(rand.Text())
	}
	if !o.noIssuedAt {
		issued := time.Now().UTC()
		claims.IssuedAt = &issued
	}
	switch {
	case o.kind == ordertoken.KindExtend:
		if err := required("handle", o.handle); err != nil {
			return "", err
		}
		sum, err := handle.Hash(o.handle)
		if err != nil {
			return "", err
		}
		claims.HandleSHA256 = hex.EncodeToString(sum)
	case o.handle != "":
		return "", fmt.Errorf("%w: --handle is only for extend tokens", errUsage)
	}
	if !o.priced {
		price, err := c.price(o.pkg)
		if err != nil {
			return "", err
		}
		// The allowance rides inside the amount, so the package keeps its full price
		claims.AmountMicros = price + o.allowance
	}
	key, err := c.privateKey(o.priv)
	if err != nil {
		return "", err
	}
	return ordertoken.Sign(key, claims)
}

// checkPayment refuses payment claims the router would refuse
func (o order) checkPayment() error {
	for _, f := range [][2]string{{"payer", o.payer}, {"fee-payer", o.feePayer}} {
		if _, err := wallet.ParseSolana(f[1]); f[1] != "" && err != nil {
			return fmt.Errorf("%w: --%s must be a canonical base58 Solana address", errUsage, f[0])
		}
	}
	if _, ok := solana.Signature(o.tx); o.tx != "" && !ok {
		return fmt.Errorf("%w: --tx must be base58 of a 64-byte signature", errUsage)
	}
	switch {
	case o.allowance < 0:
		return fmt.Errorf("%w: --allowance-micros must not be negative", errUsage)
	case o.allowance > 0 && o.kind != ordertoken.KindStart:
		return fmt.Errorf("%w: --allowance-micros is only for start tokens", errUsage)
	case o.allowance > 0 && o.priced && o.allowance > o.amount:
		return fmt.Errorf("%w: --allowance-micros must not be over --amount", errUsage)
	}
	return nil
}

func (c *cli) price(pkg string) (int64, error) {
	cfg, err := c.config()
	if err != nil {
		return 0, err
	}
	path := cfg.PackagesFile
	if path == "" {
		return 0, fmt.Errorf("%w: --amount is required when %s sets no packages_file",
			errUsage, c.configPath)
	}
	cat, err := catalog.Load(path)
	if err != nil {
		return 0, err
	}
	p, ok := cat.Get(pkg)
	if !ok {
		return 0, fmt.Errorf("package %q is not in %s", pkg, path)
	}
	return p.PriceMicros, nil
}

// privateKey takes a 32-byte seed, as bugnet-dev keys prints, or a full 64-byte key
func (c *cli) privateKey(given string) (ed25519.PrivateKey, error) {
	raw := given
	if raw == "" {
		raw = c.getenv("BUGNET_ORDER_PRIVATE_KEY")
	}
	if raw == "" {
		return nil, fmt.Errorf("%w: --priv or BUGNET_ORDER_PRIVATE_KEY is required", errUsage)
	}
	b, err := base64.StdEncoding.DecodeString(raw)
	switch {
	case err == nil && len(b) == ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(b), nil
	case err == nil && len(b) == ed25519.PrivateKeySize:
		return ed25519.PrivateKey(b), nil
	}
	return nil, errors.New("the order private key must be base64 of a 32 or 64 byte key")
}

// orderToken returns given, or signs a token for o when none was given
func (c *cli) orderToken(o order, given string) (string, error) {
	if given != "" {
		return given, nil
	}
	if c.getenv("BUGNET_ORDER_PRIVATE_KEY") == "" {
		return "", fmt.Errorf("%w: --token is required when BUGNET_ORDER_PRIVATE_KEY is not set",
			errUsage)
	}
	return c.sign(o)
}

/* Placements */

func (c *cli) create(args []string) error {
	fs := newFlags("create")
	o := order{kind: ordertoken.KindStart, ttl: defaultTTL}
	fs.StringVar(&o.pkg, "package", "", "package id")
	fs.StringVar(&o.id, "order", "", "order id")
	given := fs.String("token", "", "order token")
	estimate := fs.String("estimate-id", "", "estimate id from Estimate")
	quote := fs.String("quote", "", "quote id Reserve was called with")
	paymentFlags(fs, &o)
	if err := parse(fs, args, false); err != nil {
		return err
	}
	token, err := c.orderToken(o, *given)
	if err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.CreatePlacement(c.ctx, connect.NewRequest(&routerv1.CreatePlacementRequest{
		OrderToken: token, EstimateId: *estimate, QuoteId: *quote,
	}))
	if err != nil {
		return err
	}
	h, p := res.Msg.GetHandle(), res.Msg.GetPlacement()
	// The handle comes first so it is not lost when waiting is interrupted
	if err := c.print([][2]string{{"handle", h}}); err != nil {
		return err
	}
	for p.GetState() == routerv1.PlacementState_PLACEMENT_STATE_PLACING {
		r, err := api.GetPlacement(c.ctx, connect.NewRequest(&routerv1.GetPlacementRequest{
			Handle: h, WaitMs: pollWaitMs, KnownState: p.GetState(),
		}))
		if err != nil {
			return fmt.Errorf("wait for the placement: %w", err)
		}
		p = r.Msg.GetPlacement()
	}
	if err := c.printPlacement(p); err != nil {
		return err
	}
	if p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY {
		return fmt.Errorf("placement is %s: %s", stateName(p.GetState()), p.GetFailureReason())
	}
	return nil
}

// handleFlag parses a command whose only flag is --handle
func handleFlag(name string, args []string) (string, error) {
	fs := newFlags(name)
	h := fs.String("handle", "", "sandbox handle")
	if err := parse(fs, args, false); err != nil {
		return "", err
	}
	return *h, required("handle", *h)
}

func (c *cli) get(args []string) error {
	h, err := handleFlag("get", args)
	if err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.GetPlacement(c.ctx, connect.NewRequest(&routerv1.GetPlacementRequest{
		Handle: h,
	}))
	if err != nil {
		return err
	}
	return c.printPlacement(res.Msg.GetPlacement())
}

func (c *cli) extend(args []string) error {
	fs := newFlags("extend")
	o := order{kind: ordertoken.KindExtend, ttl: defaultTTL}
	fs.StringVar(&o.handle, "handle", "", "sandbox handle")
	fs.StringVar(&o.pkg, "package", "", "extension package id")
	fs.StringVar(&o.id, "order", "", "order id")
	given := fs.String("token", "", "order token")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("handle", o.handle); err != nil {
		return err
	}
	token, err := c.orderToken(o, *given)
	if err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.ExtendPlacement(c.ctx, connect.NewRequest(&routerv1.ExtendPlacementRequest{
		Handle: o.handle, OrderToken: token,
	}))
	if err != nil {
		return err
	}
	return c.printPlacement(res.Msg.GetPlacement())
}

// checkExtend prints the placement, then the deadline and the price an extension would give it
func (c *cli) checkExtend(args []string) error {
	fs := newFlags("check-extend")
	h := fs.String("handle", "", "sandbox handle")
	pkg := fs.String("package", "", "extension package id")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("handle", *h); err != nil {
		return err
	}
	if err := required("package", *pkg); err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.CheckExtendPlacement(c.ctx, connect.NewRequest(
		&routerv1.CheckExtendPlacementRequest{Handle: *h, PackageId: *pkg}))
	if err != nil {
		return err
	}
	if err := c.printPlacement(res.Msg.GetPlacement()); err != nil {
		return err
	}
	return c.print([][2]string{
		{"deadline_after", stamp(res.Msg.GetDeadline())},
		{"price_micros", strconv.FormatInt(res.Msg.GetPackage().GetPriceMicros(), 10)},
	})
}

// extensions prints a line for each package that can extend a placement and whether it would now
func (c *cli) extensions(args []string) error {
	h, err := handleFlag("extensions", args)
	if err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.ListExtensions(c.ctx, connect.NewRequest(&routerv1.ListExtensionsRequest{
		Handle: h,
	}))
	if err != nil {
		return err
	}
	var b strings.Builder
	for _, o := range res.Msg.GetOptions() {
		fmt.Fprintf(&b, "%s minutes=%d price_micros=%d available=%t reason=%s\n",
			o.GetPackageId(), o.GetMinutes(), o.GetPriceMicros(), o.GetAvailable(),
			o.GetUnavailableReason())
	}
	_, err = io.WriteString(c.stdout, b.String())
	return err
}

func (c *cli) stop(args []string) error {
	h, err := handleFlag("stop", args)
	if err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.StopPlacement(c.ctx, connect.NewRequest(&routerv1.StopPlacementRequest{
		Handle: h,
	}))
	if err != nil {
		return err
	}
	return c.printPlacement(res.Msg.GetPlacement())
}

// fetchSource runs FetchSource and prints the bootstrap it answers
func (c *cli) fetchSource(args []string) error {
	h, err := handleFlag("fetch-source", args)
	if err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.FetchSource(c.ctx, connect.NewRequest(&routerv1.FetchSourceRequest{
		Handle: h,
	}))
	if err != nil {
		return err
	}
	b := res.Msg.GetBootstrap()
	return c.print([][2]string{
		{"state", bootstrapName(b.GetState())},
		{"path", b.GetPath()}, {"revision", b.GetRevision()}, {"job_id", b.GetJobId()},
		{"failure_reason", b.GetFailureReason()},
	})
}

/* Jobs */

func (c *cli) exec(args []string) error {
	fs := newFlags("exec")
	h := fs.String("handle", "", "sandbox handle")
	cwd := fs.String("cwd", "", "working directory")
	env := envFlag{}
	fs.Var(env, "env", "K=V added to the environment, repeatable")
	if err := parse(fs, args, true); err != nil {
		return err
	}
	if err := required("handle", *h); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return fmt.Errorf("%w: a command is required after --", errUsage)
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.Exec(c.ctx, connect.NewRequest(&routerv1.ExecRequest{
		Handle: *h, Argv: fs.Args(), Cwd: *cwd, Env: env, WaitMs: execWaitMs,
	}))
	if err != nil {
		return err
	}
	m := res.Msg
	if m.GetTruncated() {
		// The tails miss output, so the whole stream is read from its start
		return c.follow(api, *h, m.GetJobId(), "")
	}
	if _, err := c.stdout.Write(m.GetStdoutTail()); err != nil {
		return err
	}
	if _, err := c.stderr.Write(m.GetStderrTail()); err != nil {
		return err
	}
	switch m.GetState() {
	case routerv1.JobState_JOB_STATE_RUNNING:
		return c.follow(api, *h, m.GetJobId(), m.GetCursor())
	case routerv1.JobState_JOB_STATE_EXITED, routerv1.JobState_JOB_STATE_CANCELLED:
		return exitCode(m.GetExitCode())
	}
	return fmt.Errorf("job %s is %s", m.GetJobId(), m.GetState())
}

func (c *cli) watch(args []string) error {
	fs := newFlags("watch")
	h := fs.String("handle", "", "sandbox handle")
	job := fs.String("job", "", "job id")
	cursor := fs.String("cursor", "", "stream cursor, the start when empty")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("handle", *h); err != nil {
		return err
	}
	if err := required("job", *job); err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	return c.follow(api, *h, *job, *cursor)
}

// follow writes a job's output after cursor until its exit frame and returns its exit code
func (c *cli) follow(
	api routerv1connect.SandboxRouterServiceClient, h, job, cursor string,
) error {
	stream, err := api.WatchJob(c.ctx, connect.NewRequest(&routerv1.WatchJobRequest{
		Handle: h, JobId: job, Cursor: cursor,
	}))
	if err != nil {
		return c.resumable(err, h, job, cursor)
	}
	defer func() { _ = stream.Close() }()
	for stream.Receive() {
		m := stream.Msg()
		switch f := m.GetFrame().(type) {
		case *routerv1.WatchJobResponse_Output:
			w := c.stdout
			if f.Output.GetStream() == routerv1.OutputStream_OUTPUT_STREAM_STDERR {
				w = c.stderr
			}
			if _, err := w.Write(f.Output.GetData()); err != nil {
				return err
			}
		case *routerv1.WatchJobResponse_Skip:
			_, err := fmt.Fprintf(c.stderr, "bugnet-dev: output skipped to stdout byte %d and "+
				"stderr byte %d, the whole output is in /tmp/bugnet/jobs/%s/out and err\n",
				f.Skip.GetStdoutOffset(), f.Skip.GetStderrOffset(), job)
			if err != nil {
				return err
			}
		case *routerv1.WatchJobResponse_Exit:
			return exitCode(f.Exit.GetExitCode())
		}
		cursor = m.GetCursor()
	}
	err = stream.Err()
	if err == nil {
		err = errors.New("the stream ended before the job exited")
	}
	return c.resumable(err, h, job, cursor)
}

// resumable adds the watch command that continues the stream, unless the job itself ended
func (c *cli) resumable(err error, h, job, cursor string) error {
	if connect.CodeOf(err) == connect.CodeAborted {
		return err
	}
	resume := "bugnet-dev "
	if c.configPath != defaultConfig {
		resume += "--config " + shellWord(c.configPath) + " "
	}
	resume += "watch --handle " + h + " --job " + job
	if cursor != "" {
		resume += " --cursor " + cursor
	}
	return fmt.Errorf("%w\nresume with: %s", err, resume)
}

// plainWord is a word a POSIX shell reads as it is
var plainWord = regexp.MustCompile(`^[A-Za-z0-9_./:@%+=,-]+$`)

// shellWord quotes s for a POSIX shell when it holds anything but plain characters
func shellWord(s string) string {
	if plainWord.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

/* Settings */

// config reads the settings file once, for the commands that need a setting
func (c *cli) config() (config.Config, error) {
	if c.settings == nil {
		cfg, err := config.LoadFile(c.configPath)
		if err != nil {
			return config.Config{}, err
		}
		c.settings = &cfg
	}
	return *c.settings, nil
}

// apiURL is where a router-api listening on addr answers, on loopback for an unspecified host
func apiURL(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("api.listen_addr %q is not host:port: %w", addr, err)
	}
	if ip := net.ParseIP(host); host == "" || ip != nil && ip.IsUnspecified() {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port), nil
}

/* API */

// bearer sends every request with one service's token
type bearer struct {
	token string
	next  http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(r)
}

func (c *cli) client() (routerv1connect.SandboxRouterServiceClient, error) {
	hc, url, err := c.http("BUGNET_INTERNAL_TOKEN")
	if err != nil {
		return nil, err
	}
	return routerv1connect.NewSandboxRouterServiceClient(hc, url), nil
}

// operator is a client of OperatorService, which takes its own token
func (c *cli) operator() (routerv1connect.OperatorServiceClient, error) {
	hc, url, err := c.http("BUGNET_OPERATOR_TOKEN")
	if err != nil {
		return nil, err
	}
	return routerv1connect.NewOperatorServiceClient(hc, url), nil
}

// http is an h2c client sending the token tokenEnv holds, and the API's URL
func (c *cli) http(tokenEnv string) (*http.Client, string, error) {
	token := c.getenv(tokenEnv)
	if token == "" {
		return nil, "", errors.New(tokenEnv + " is not set")
	}
	cfg, err := c.config()
	if err != nil {
		return nil, "", err
	}
	url, err := apiURL(cfg.ListenAddr)
	if err != nil {
		return nil, "", err
	}
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	rt := bearer{token: token, next: &http.Transport{Protocols: &protocols}}
	return &http.Client{Transport: rt}, url, nil
}

/* Output */

func (c *cli) print(kv [][2]string) error {
	var b strings.Builder
	for _, p := range kv {
		b.WriteString(p[0] + "=" + p[1] + "\n")
	}
	_, err := io.WriteString(c.stdout, b.String())
	return err
}

func (c *cli) printPlacement(p *routerv1.Placement) error {
	kv := [][2]string{
		{"placement_id", p.GetPlacementId()},
		{"state", stateName(p.GetState())},
		{"package_id", p.GetPackageId()},
		{"created_at", stamp(p.GetCreatedAt())},
	}
	if p.GetReadyAt() != nil {
		kv = append(kv, [2]string{"ready_at", stamp(p.GetReadyAt())})
	}
	if p.GetDeadline() != nil {
		kv = append(kv, [2]string{"deadline", stamp(p.GetDeadline())})
	}
	if p.GetSshAlias() != "" {
		kv = append(kv, [2]string{"ssh_alias", p.GetSshAlias()})
	}
	if p.GetFailureReason() != "" {
		kv = append(kv, [2]string{"failure_reason", p.GetFailureReason()},
			[2]string{"refundable", strconv.FormatBool(p.GetRefundable())})
	}
	if p.GetPaidMicros() != 0 {
		kv = append(kv, [2]string{"paid_micros", strconv.FormatInt(p.GetPaidMicros(), 10)})
	}
	if p.GetSessionUrl() != "" {
		kv = append(kv, [2]string{"session_url", p.GetSessionUrl()})
	}
	if b := p.GetBootstrap(); b != nil {
		kv = append(kv, [2]string{"bootstrap_state", bootstrapName(b.GetState())},
			[2]string{"bootstrap_reason", b.GetFailureReason()})
	}
	return c.print(kv)
}

func bootstrapName(s routerv1.BootstrapState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "BOOTSTRAP_STATE_"))
}

func stateName(s routerv1.PlacementState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "PLACEMENT_STATE_"))
}

func stamp(ts *timestamppb.Timestamp) string {
	return ts.AsTime().UTC().Format(time.RFC3339Nano)
}
