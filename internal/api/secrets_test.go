package api_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/hostrules"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	secretsHost = "secrets.bugnet.sh"
	egressHost  = "egress.bugnet.sh"
)

// boxKeys are two X25519 public keys of the proxy, newest first
var boxKeys = []config.BoxKey{{ID: "k2", Key: randomKey()}, {ID: "k1", Key: randomKey()}}

func randomKey() []byte {
	k := make([]byte, 32)
	_, _ = rand.Read(k)
	return k
}

// secretsOf is the API server's [secrets] settings, the waits short so tests stay quick
func secretsOf(opts options) config.Secrets {
	c := config.Secrets{
		EgressIPs: []netip.Addr{netip.MustParseAddr("203.0.113.7")}, BoxPublicKeys: boxKeys,
		LinkTTL: 10 * time.Minute, PutGrantTTL: 10 * time.Minute, CustomHosts: true,
		MaxPerPlacement: 20, LinksPerHour: 10, PutsPerHour: 30, ApplyWait: 20 * time.Millisecond,
	}
	if opts.secretSettings != nil {
		opts.secretSettings(&c)
	}
	return c
}

// hostRules are the router's host rules in their x402 form, from x
func hostRules(x config.X402) hostrules.Rules {
	return hostrules.New(x, config.Names{
		PreviewDomain: previewDomain, TransferHost: transferHost, SecretsHost: secretsHost,
		EgressHost: egressHost,
	}, "")
}

func (e *env) secretsRoute(t *testing.T) routerv1connect.SecretsRouteServiceClient {
	return routerv1connect.NewSecretsRouteServiceClient(e.httpClient(t, secretsToken), e.url)
}

func hf() *routerv1.SecretSpec {
	return &routerv1.SecretSpec{Name: "HF_TOKEN", Hosts: []string{"huggingface.co"}}
}

func sealed() *routerv1.SealedValue {
	return &routerv1.SealedValue{KeyId: "k2", Box: []byte("sealed box bytes")}
}

func (e *env) putSecret(
	t *testing.T, h string, spec *routerv1.SecretSpec, key string,
) (*routerv1.PutSecretResponse, error) {
	t.Helper()
	res, err := e.client.PutSecret(t.Context(), connect.NewRequest(&routerv1.PutSecretRequest{
		Handle: h, Spec: spec, Value: sealed(), IdempotencyKey: key,
	}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (e *env) mustPutSecret(
	t *testing.T, h string, spec *routerv1.SecretSpec,
) *routerv1.PutSecretResponse {
	t.Helper()
	r, err := e.putSecret(t, h, spec, "")
	if err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	return r
}

func (e *env) createLink(
	t *testing.T, h, requestID, name string, hosts ...string,
) (*routerv1.CreateSecretLinkResponse, error) {
	t.Helper()
	res, err := e.client.CreateSecretLink(t.Context(), connect.NewRequest(
		&routerv1.CreateSecretLinkRequest{
			Handle: h, Name: name, Hosts: hosts, RequestId: requestID,
		}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

// linkToken is the bnsl_ token of a link URL
func linkToken(t *testing.T, url string) string {
	t.Helper()
	tok, ok := strings.CutPrefix(url, "https://"+secretsHost+"/l/")
	if !ok || !strings.HasPrefix(tok, "bnsl_") {
		t.Fatalf("link URL %q", url)
	}
	return tok
}

func (e *env) mustLink(t *testing.T, h, requestID string) string {
	t.Helper()
	r, err := e.createLink(t, h, requestID, "HF_TOKEN")
	if err != nil {
		t.Fatalf("CreateSecretLink: %v", err)
	}
	return linkToken(t, r.GetUrl())
}

func (e *env) submit(
	t *testing.T, token, submission string, spec *routerv1.SecretSpec,
) (*routerv1.SubmitSecretLinkResponse, error) {
	t.Helper()
	res, err := e.secretsRoute(t).SubmitSecretLink(t.Context(), connect.NewRequest(
		&routerv1.SubmitSecretLinkRequest{
			LinkToken: token, Spec: spec, Value: sealed(), SubmissionId: submission,
		}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (e *env) getLink(t *testing.T, token string) (*routerv1.GetSecretLinkResponse, error) {
	t.Helper()
	res, err := e.secretsRoute(t).GetSecretLink(t.Context(), connect.NewRequest(
		&routerv1.GetSecretLinkRequest{LinkToken: token}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (e *env) putBridge(
	t *testing.T, grant string, spec *routerv1.SecretSpec,
) (*routerv1.PutBridgeSecretResponse, error) {
	t.Helper()
	res, err := e.secretsRoute(t).PutBridgeSecret(t.Context(), connect.NewRequest(
		&routerv1.PutBridgeSecretRequest{PutGrant: grant, Spec: spec, Value: sealed()}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (e *env) grant(t *testing.T, h string) string {
	t.Helper()
	res, err := e.client.CreateSecretPutGrant(t.Context(), connect.NewRequest(
		&routerv1.CreateSecretPutGrantRequest{Handle: h}))
	if err != nil {
		t.Fatalf("CreateSecretPutGrant: %v", err)
	}
	return res.Msg.GetPutGrant()
}

// secretCalls maps the built secret RPCs that take a handle to a call of each
func (e *env) secretCalls(t *testing.T) map[string]func(h string) error {
	ctx := t.Context()
	return map[string]func(h string) error{
		"CreateSecretLink": func(h string) error {
			_, err := e.createLink(t, h, "req-1", "HF_TOKEN")
			return err
		},
		"CreateSecretPutGrant": func(h string) error {
			_, err := e.client.CreateSecretPutGrant(ctx, connect.NewRequest(
				&routerv1.CreateSecretPutGrantRequest{Handle: h}))
			return err
		},
		"GetSecretSealingKey": func(h string) error {
			_, err := e.client.GetSecretSealingKey(ctx, connect.NewRequest(
				&routerv1.GetSecretSealingKeyRequest{Handle: h}))
			return err
		},
		"PutSecret": func(h string) error {
			_, err := e.putSecret(t, h, hf(), "")
			return err
		},
		"ListSecrets": func(h string) error {
			_, err := e.client.ListSecrets(ctx, connect.NewRequest(
				&routerv1.ListSecretsRequest{Handle: h}))
			return err
		},
		"DeleteSecret": func(h string) error {
			_, err := e.client.DeleteSecret(ctx, connect.NewRequest(
				&routerv1.DeleteSecretRequest{Handle: h, Name: "HF_TOKEN"}))
			return err
		},
	}
}

// applyJobs counts the apply_secrets jobs inserted for the placement
func (e *env) applyJobs(t *testing.T, id string) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `select count(*) from river_job
		where kind = 'apply_secrets' and queue = 'dataplane' and args->>'placement_id' = $1`,
		id).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *env) secretsVersion(t *testing.T, p *routerv1.Placement) int64 {
	t.Helper()
	return e.placementOf(t, p).SecretsVersion
}

func (e *env) changedEvents(t *testing.T, id string) int {
	t.Helper()
	n := 0
	for _, ev := range e.edgeEvents(t) {
		if ev.GetSecretsChanged().GetPlacementId() == id {
			n++
		}
	}
	return n
}

// markActive does what apply_secrets step 5 does to the placement's applying secrets
func (e *env) markActive(t *testing.T, p *routerv1.Placement) {
	t.Helper()
	id := uuid.MustParse(p.GetPlacementId())
	all, err := e.store.Secrets(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.MarkSecrets(t.Context(), id, all, store.SecretActive, ""); err != nil {
		t.Fatal(err)
	}
}

const (
	applying = routerv1.SecretState_SECRET_STATE_APPLYING
	active   = routerv1.SecretState_SECRET_STATE_ACTIVE
)

func TestEveryWriteNeedsTheCapability(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	id := uuid.MustParse(p.GetPlacementId())
	sum := sha256.Sum256([]byte("bnsl_handmade"))
	if _, err := e.store.InsertSecretLink(t.Context(), store.SecretLink{
		ID: uuid.New(), PlacementID: id, TokenSHA256: sum[:], PairingCode: "ABCDEF",
	}, time.Minute); err != nil {
		t.Fatal(err)
	}
	grant := e.dpKeys.PutGrant(id, time.Now().Add(time.Minute))
	writes := map[string]func() error{
		"CreateSecretLink": func() error { _, err := e.createLink(t, h, "r", ""); return err },
		"CreateSecretPutGrant": func() error {
			_, err := e.client.CreateSecretPutGrant(t.Context(), connect.NewRequest(
				&routerv1.CreateSecretPutGrantRequest{Handle: h}))
			return err
		},
		"PutSecret":       func() error { _, err := e.putSecret(t, h, hf(), ""); return err },
		"PutBridgeSecret": func() error { _, err := e.putBridge(t, grant, hf()); return err },
		"SubmitSecretLink": func() error {
			_, err := e.submit(t, "bnsl_handmade", "s1", hf())
			return err
		},
		"DeleteSecret": func() error { return e.secretCalls(t)["DeleteSecret"](h) },
	}
	for name, call := range writes {
		t.Run(name, func(t *testing.T) {
			mustCode(t, call(), connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")
		})
	}
	eq(t, e.applyJobs(t, p.GetPlacementId()), 0)
}

func TestPutSecretWaitsThenAnswersApplying(t *testing.T) {
	e := newEnv(t, options{secrets: true,
		secretSettings: func(c *config.Secrets) { c.ApplyWait = 300 * time.Millisecond }})
	h, p := e.readyPlacement(t)
	start := time.Now()
	r := e.mustPutSecret(t, h, hf())
	if time.Since(start) < 250*time.Millisecond {
		t.Fatal("PutSecret answered before secrets.apply_wait")
	}
	eq(t, r.GetSecret().GetState(), applying)
	eq(t, r.GetReplaced(), false)
	if !proto.Equal(r.GetSecret().GetSpec(), &routerv1.SecretSpec{
		Name: "HF_TOKEN", Hosts: []string{"huggingface.co"}, HeaderNames: []string{},
	}) {
		t.Fatalf("spec %v", r.GetSecret().GetSpec())
	}
	eq(t, e.applyJobs(t, p.GetPlacementId()), 1)
	again := e.mustPutSecret(t, h, hf())
	eq(t, again.GetReplaced(), true)
	eq(t, e.applyJobs(t, p.GetPlacementId()), 2)
	row, err := e.store.SecretByKey(t.Context(), uuid.MustParse(p.GetPlacementId()), "")
	if err == nil {
		t.Fatalf("a put without a key stored key %q", row.IdempotencyKey)
	}
}

func TestSameIdempotencyKeyIsAnsweredAgain(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	first, err := e.putSecret(t, h, hf(), "key-1")
	if err != nil {
		t.Fatal(err)
	}
	e.markActive(t, p)
	again, err := e.putSecret(t, h, hf(), "key-1")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, again.GetSecret().GetState(), active)
	eq(t, again.GetSecret().GetUpdatedAt().AsTime(), first.GetSecret().GetUpdatedAt().AsTime())
	eq(t, e.applyJobs(t, p.GetPlacementId()), 1)
	// Another value under the same key is a new put
	spec := hf()
	spec.Hosts = []string{"huggingface.co", "api.example.com"}
	if _, err := e.putSecret(t, h, spec, "key-1"); err != nil {
		t.Fatal(err)
	}
	eq(t, e.applyJobs(t, p.GetPlacementId()), 2)
}

func TestFetchOnlyIsActiveAtOnce(t *testing.T) {
	e := newEnv(t, options{secrets: true,
		secretSettings: func(c *config.Secrets) { c.ApplyWait = 5 * time.Second }})
	h, p := e.readyPlacement(t)
	spec := &routerv1.SecretSpec{
		Name: "GITHUB_TOKEN", Hosts: []string{"api.github.com"}, FetchOnly: true,
	}
	start := time.Now()
	r := e.mustPutSecret(t, h, spec)
	if time.Since(start) > 2*time.Second {
		t.Fatal("a fetch-only put waited for an apply")
	}
	eq(t, r.GetSecret().GetState(), active)
	eq(t, e.applyJobs(t, p.GetPlacementId()), 0)
	eq(t, e.secretsVersion(t, p), int64(1))
	eq(t, e.changedEvents(t, p.GetPlacementId()), 1)
	// Replacing a secret the sandbox holds with a fetch-only one takes its placeholder away
	sum := sha256.Sum256([]byte("HF_TOKEN"))
	if _, _, err := e.store.PutSecret(t.Context(), store.Secret{
		ID: uuid.New(), PlacementID: uuid.MustParse(p.GetPlacementId()), Name: "HF_TOKEN",
		Hosts: []string{"huggingface.co"}, PlaceholderSHA256: sum[:], Sealed: []byte("box"),
		KeyID: "k2", Source: store.SourceGateway, State: store.SecretActive,
	}); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	e.mustPutSecret(t, h, &routerv1.SecretSpec{Name: "HF_TOKEN", Hosts: []string{"huggingface.co"},
		FetchOnly: true})
	if time.Since(start) > 2*time.Second {
		t.Fatal("a fetch-only replacement waited for its apply")
	}
	eq(t, e.applyJobs(t, p.GetPlacementId()), 1)
	eq(t, e.secretsVersion(t, p), int64(2))
}

func TestListSecretsNeverValues(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	e.mustPutSecret(t, h, hf())
	e.mustPutSecret(t, h, &routerv1.SecretSpec{
		Name: "ANTHROPIC_API_KEY", Hosts: []string{"api.anthropic.com"},
		HeaderNames: []string{"x-api-key"},
	})
	res, err := e.client.ListSecrets(t.Context(), connect.NewRequest(
		&routerv1.ListSecretsRequest{Handle: h}))
	if err != nil {
		t.Fatal(err)
	}
	got := res.Msg.GetSecrets()
	eq(t, len(got), 2)
	eq(t, got[0].GetSpec().GetName(), "ANTHROPIC_API_KEY")
	eq(t, got[0].GetSpec().GetHeaderNames(), []string{"x-api-key"})
	eq(t, got[1].GetState(), applying)
	raw, err := proto.Marshal(res.Msg)
	if err != nil {
		t.Fatal(err)
	}
	ph, err := e.dpKeys.Placeholder(e.placementOf(t, p).KeyID,
		uuid.MustParse(p.GetPlacementId()), "HF_TOKEN")
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range [][]byte{sealed().GetBox(), []byte(ph), []byte("bnsec_")} {
		if bytes.Contains(raw, leak) {
			t.Fatalf("ListSecrets answered %q", leak)
		}
	}
}

func TestDeleteSecretBumpsVersion(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	e.mustPutSecret(t, h, hf())
	del := e.secretCalls(t)["DeleteSecret"]
	if err := del(h); err != nil {
		t.Fatal(err)
	}
	eq(t, e.secretsVersion(t, p), int64(1))
	eq(t, e.applyJobs(t, p.GetPlacementId()), 2)
	eq(t, e.changedEvents(t, p.GetPlacementId()), 1)
	all, err := e.store.Secrets(t.Context(), uuid.MustParse(p.GetPlacementId()))
	if err != nil || len(all) != 0 {
		t.Fatalf("left %v %v", all, err)
	}
	// Deleting a name the placement lacks changes nothing
	if err := del(h); err != nil {
		t.Fatal(err)
	}
	eq(t, e.secretsVersion(t, p), int64(1))
	eq(t, e.applyJobs(t, p.GetPlacementId()), 2)
}

func TestSecretRefusalsReachTheCaller(t *testing.T) {
	e := newEnv(t, options{secrets: true, opsBurst: 100})
	h, _ := e.readyPlacement(t)
	for _, c := range []struct {
		spec   *routerv1.SecretSpec
		value  *routerv1.SealedValue
		code   connect.Code
		reason string
	}{
		{&routerv1.SecretSpec{Name: "PATH", Hosts: []string{"huggingface.co"}}, sealed(),
			connect.CodeInvalidArgument, "NAME_RESERVED"},
		{&routerv1.SecretSpec{Name: "X", Hosts: []string{"preview.bugnet.sh"}}, sealed(),
			connect.CodeInvalidArgument, "HOST_REFUSED"},
		{&routerv1.SecretSpec{Name: "X", Hosts: []string{"HuggingFace.co"}}, sealed(),
			connect.CodeInvalidArgument, "HOST_REFUSED"},
		{&routerv1.SecretSpec{Name: "X", Hosts: []string{"a.example"},
			HeaderNames: []string{"cookie"}}, sealed(), connect.CodeInvalidArgument,
			"HEADER_REFUSED"},
		// a key id not in secrets.box_public_keys -> KEY_UNKNOWN
		{hf(), &routerv1.SealedValue{KeyId: "k3", Box: []byte("x")},
			connect.CodeInvalidArgument, "KEY_UNKNOWN"},
		{hf(), &routerv1.SealedValue{KeyId: "k1", Box: make([]byte, 70<<10+1)},
			connect.CodeInvalidArgument, "SEALED_TOO_LARGE"},
	} {
		_, err := e.client.PutSecret(t.Context(), connect.NewRequest(&routerv1.PutSecretRequest{
			Handle: h, Spec: c.spec, Value: c.value,
		}))
		mustCode(t, err, c.code, c.reason)
		quotesNone(t, err, c.spec.GetHosts()...)
	}
	for i := range 20 {
		e.mustPutSecret(t, h, &routerv1.SecretSpec{
			Name: "S" + strconv.Itoa(i), Hosts: []string{"huggingface.co"},
		})
	}
	_, err := e.putSecret(t, h, &routerv1.SecretSpec{
		Name: "S20", Hosts: []string{"huggingface.co"},
	}, "")
	mustCode(t, err, connect.CodeResourceExhausted, "TOO_MANY_SECRETS")
}

func TestCustomHostsSwitchedOff(t *testing.T) {
	e := newEnv(t, options{secrets: true,
		secretSettings: func(c *config.Secrets) { c.CustomHosts = false }})
	h, _ := e.readyPlacement(t)
	_, err := e.putSecret(t, h, &routerv1.SecretSpec{
		Name: "MY_KEY", Hosts: []string{"api.example.com"},
	}, "")
	mustCode(t, err, connect.CodeFailedPrecondition, "HOST_NOT_ALLOWED")
	e.mustPutSecret(t, h, hf())
}

// with egress.allow on the package, a secret host outside it -> HOST_NOT_ALLOWED (X12)
func TestRestrictedPackageHostMustBeAllowed(t *testing.T) {
	allow := `, "egress": {"allow": ["api.openai.com"]}}`
	restricted := strings.Replace(catalogJSON, `"price_micros": 50000}`,
		`"price_micros": 50000`+allow, 1)
	restricted = strings.Replace(restricted, `"extension_of": "small-10m"}`,
		`"extension_of": "small-10m"`+allow, 1)
	e := newEnv(t, options{secrets: true, apiCatalog: restricted})
	h, _ := e.readyPlacement(t)
	_, err := e.putSecret(t, h, hf(), "")
	mustCode(t, err, connect.CodeFailedPrecondition, "HOST_NOT_ALLOWED")
	e.mustPutSecret(t, h, &routerv1.SecretSpec{
		Name: "OPENAI_API_KEY", Hosts: []string{"api.openai.com"},
	})
}

// CreateSecretLink, CreateSecretPutGrant, PutSecret and PutBridgeSecret take one ops token each
func TestSecretWritesSpendOneOperation(t *testing.T) {
	e := newEnv(t, options{secrets: true, opsPerSecond: 1, opsBurst: 20})
	h, p := e.readyPlacement(t)
	key := e.prefix + "ops:" + p.GetPlacementId()
	tokens := func() float64 {
		t.Helper()
		v, err := e.vk.Do(t.Context(), e.vk.B().Hget().Key(key).Field("tokens").Build()).
			AsFloat64()
		if err != nil {
			return 20e6
		}
		return v
	}
	grant := e.grant(t, h)
	for name, call := range map[string]func() error{
		"CreateSecretLink":     func() error { _, err := e.createLink(t, h, "r1", ""); return err },
		"CreateSecretPutGrant": func() error { e.grant(t, h); return nil },
		"PutSecret":            func() error { _, err := e.putSecret(t, h, hf(), ""); return err },
		"PutBridgeSecret":      func() error { _, err := e.putBridge(t, grant, hf()); return err },
	} {
		before := tokens()
		if err := call(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		spent := before - tokens()
		if spent < 0.5e6 || spent > 1e6 {
			t.Errorf("%s spent %.0f micro-tokens, want one token", name, spent)
		}
	}
	// Reads and deletes spend nothing
	before := tokens()
	for _, name := range []string{"GetSecretSealingKey", "ListSecrets", "DeleteSecret"} {
		if err := e.secretCalls(t)[name](h); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if before-tokens() > 0 {
		t.Errorf("reads spent %.0f micro-tokens", before-tokens())
	}
}

func TestLinkBucketTenAnHour(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, _ := e.readyPlacement(t)
	for i := range 10 {
		e.mustLink(t, h, "req-"+strconv.Itoa(i))
	}
	// A repeated request id answers its link without spending
	e.mustLink(t, h, "req-3")
	_, err := e.createLink(t, h, "req-10", "")
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
}

func TestPutBucketThirtyAnHourForBothPaths(t *testing.T) {
	e := newEnv(t, options{secrets: true, opsBurst: 100,
		secretSettings: func(c *config.Secrets) { c.PutsPerHour = 4 }})
	h, _ := e.readyPlacement(t)
	grant := e.grant(t, h)
	e.mustPutSecret(t, h, hf())
	e.mustPutSecret(t, h, hf())
	for range 2 {
		if _, err := e.putBridge(t, grant, hf()); err != nil {
			t.Fatal(err)
		}
	}
	_, err := e.putBridge(t, grant, hf())
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
	_, err = e.putSecret(t, h, hf(), "")
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
}

func TestCreateSecretLinkAndGetIt(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	r, err := e.createLink(t, h, "req-1", "HF_TOKEN", "evil.example.com", "huggingface.co")
	if err != nil {
		t.Fatal(err)
	}
	same, err := e.createLink(t, h, "req-1", "HF_TOKEN")
	if err != nil || !proto.Equal(same, r) {
		t.Fatalf("a repeated request id answered %v %v, want %v", same, err, r)
	}
	tok := linkToken(t, r.GetUrl())
	if len(r.GetPairingCode()) != 6 {
		t.Fatalf("pairing code %q", r.GetPairingCode())
	}
	if d := time.Until(r.GetExpiresAt().AsTime()); d < 9*time.Minute || d > 10*time.Minute {
		t.Fatalf("the link lives %v", d)
	}
	got, err := e.getLink(t, tok)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.GetPairingCode(), r.GetPairingCode())
	// The agent's custom host is dropped, the preset host kept
	if !proto.Equal(got.GetSuggested(), &routerv1.SecretSpec{
		Name: "HF_TOKEN", Hosts: []string{"huggingface.co"}, HeaderNames: []string{},
	}) {
		t.Fatalf("suggested %v", got.GetSuggested())
	}
	keys := got.GetSealingKeys()
	eq(t, len(keys), 2)
	eq(t, keys[0].GetKeyId(), "k2")
	eq(t, keys[0].GetPublicKey(), boxKeys[0].Key)
	eq(t, keys[1].GetBinding(), dpkeys.Binding(uuid.MustParse(p.GetPlacementId())))
	eq(t, got.GetSandboxCreatedAt().AsTime(), p.GetCreatedAt().AsTime())
	// Reading never consumes
	if _, err := e.getLink(t, tok); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "bnsl_unknown", strings.ToUpper(tok)} {
		_, err := e.getLink(t, bad)
		mustCode(t, err, connect.CodeNotFound, "LINK_UNKNOWN")
	}
	_, err = e.createLink(t, h, strings.Repeat("x", 129), "")
	wantCode(t, err, connect.CodeInvalidArgument)
	_, err = e.createLink(t, h, "req-2", "LD_PRELOAD")
	mustCode(t, err, connect.CodeInvalidArgument, "NAME_RESERVED")
}

func TestLinkConsumedOnce(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	tok := e.mustLink(t, h, "req-1")
	r, err := e.submit(t, tok, "sub-1", hf())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r.GetSecret().GetState(), applying)
	_, err = e.submit(t, tok, "sub-2", hf())
	mustCode(t, err, connect.CodeNotFound, "LINK_UNKNOWN")
	_, err = e.getLink(t, tok)
	mustCode(t, err, connect.CodeNotFound, "LINK_UNKNOWN")
	// G1, the request id of a used link answers TOKEN_USED, never the dead link
	_, err = e.createLink(t, h, "req-1", "")
	mustCode(t, err, connect.CodeFailedPrecondition, "TOKEN_USED")
	eq(t, e.applyJobs(t, p.GetPlacementId()), 1)
	row, err := e.store.SecretByKey(t.Context(), uuid.MustParse(p.GetPlacementId()),
		"link:"+mustLinkRow(t, e, tok).ID.String())
	if err != nil || row.Source != store.SourceLink {
		t.Fatalf("link secret %+v %v", row, err)
	}
}

func mustLinkRow(t *testing.T, e *env, tok string) store.SecretLink {
	t.Helper()
	sum := sha256.Sum256([]byte(tok))
	l, err := e.store.SecretLinkByToken(t.Context(), sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestExpiredLinkRequestIDIsTokenExpired(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, _ := e.readyPlacement(t)
	tok := e.mustLink(t, h, "req-1")
	sum := sha256.Sum256([]byte(tok))
	if _, err := e.pool.Exec(t.Context(), `update secret_links
		set created_at = now() - interval '1 hour', expires_at = now() - interval '1 second'
		where token_sha256 = $1`, sum[:]); err != nil {
		t.Fatal(err)
	}
	_, err := e.createLink(t, h, "req-1", "")
	mustCode(t, err, connect.CodeFailedPrecondition, "TOKEN_EXPIRED")
	_, err = e.submit(t, tok, "sub-1", hf())
	mustCode(t, err, connect.CodeNotFound, "LINK_UNKNOWN")
}

func TestSameSubmissionAnsweredTwice(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	tok := e.mustLink(t, h, "req-1")
	first, err := e.submit(t, tok, "sub-1", hf())
	if err != nil {
		t.Fatal(err)
	}
	e.markActive(t, p)
	again, err := e.submit(t, tok, "sub-1", hf())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, again.GetSecret().GetState(), active)
	eq(t, again.GetSecret().GetUpdatedAt().AsTime(), first.GetSecret().GetUpdatedAt().AsTime())
	eq(t, e.applyJobs(t, p.GetPlacementId()), 1)
}

func TestFiveFailuresLockALink(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	tok := e.mustLink(t, h, "req-1")
	bad := &routerv1.SecretSpec{Name: "PATH", Hosts: []string{"huggingface.co"}}
	for range 5 {
		_, err := e.submit(t, tok, "sub", bad)
		mustCode(t, err, connect.CodeInvalidArgument, "NAME_RESERVED")
	}
	_, err := e.submit(t, tok, "sub", hf())
	mustCode(t, err, connect.CodeFailedPrecondition, "LINK_LOCKED")
	_, err = e.getLink(t, tok)
	mustCode(t, err, connect.CodeFailedPrecondition, "LINK_LOCKED")
	// G1, the request id of a locked link never answers the dead link
	_, err = e.createLink(t, h, "req-1", "HF_TOKEN")
	mustCode(t, err, connect.CodeFailedPrecondition, "LINK_LOCKED")
	eq(t, e.applyJobs(t, p.GetPlacementId()), 0)
}

// lockRow holds the placement's row lock until the returned func commits it
func (e *env) lockRow(t *testing.T, p *routerv1.Placement) func() {
	t.Helper()
	tx, err := e.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	id := uuid.MustParse(p.GetPlacementId())
	if _, err := e.store.Tx(tx).PlacementForUpdate(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestALinkLockedDuringASubmitRefusesIt(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	tok := e.mustLink(t, h, "req-1")
	release := e.lockRow(t, p)
	done := make(chan error, 1)
	go func() {
		_, err := e.submit(t, tok, "sub-1", hf())
		done <- err
	}()
	// The submit read the link as live and now waits on the row lock
	time.Sleep(300 * time.Millisecond)
	sum := sha256.Sum256([]byte(tok))
	if _, err := e.pool.Exec(t.Context(), `update secret_links set failed_attempts = 5
		where token_sha256 = $1`, sum[:]); err != nil {
		t.Fatal(err)
	}
	release()
	mustCode(t, <-done, connect.CodeFailedPrecondition, "LINK_LOCKED")
	eq(t, e.applyJobs(t, p.GetPlacementId()), 0)
}

func TestARefusedPutSpendsNothing(t *testing.T) {
	e := newEnv(t, options{secrets: true, opsBurst: 100, secretSettings: func(c *config.Secrets) {
		c.MaxPerPlacement, c.PutsPerHour = 2, 3
	}})
	h, _ := e.readyPlacement(t)
	spec := func(name string) *routerv1.SecretSpec {
		return &routerv1.SecretSpec{Name: name, Hosts: []string{"huggingface.co"}}
	}
	e.mustPutSecret(t, h, spec("S0"))
	e.mustPutSecret(t, h, spec("S1"))
	for range 3 {
		_, err := e.putSecret(t, h, spec("S2"), "")
		mustCode(t, err, connect.CodeResourceExhausted, "TOO_MANY_SECRETS")
	}
	e.mustPutSecret(t, h, spec("S0"))
	_, err := e.putSecret(t, h, spec("S1"), "")
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
}

func TestSameRequestIDAtOnceSpendsOneLink(t *testing.T) {
	e := newEnv(t, options{secrets: true, opsBurst: 100,
		secretSettings: func(c *config.Secrets) { c.LinksPerHour = 2 }})
	h, p := e.readyPlacement(t)
	release := e.lockRow(t, p)
	urls := make(chan string, 2)
	for range 2 {
		go func() {
			r, err := e.createLink(t, h, "req-1", "HF_TOKEN")
			if err != nil {
				t.Error(err)
			}
			urls <- r.GetUrl()
		}()
	}
	// Both calls found no link and now wait on the row lock
	time.Sleep(300 * time.Millisecond)
	release()
	eq(t, <-urls, <-urls)
	e.mustLink(t, h, "req-2")
	_, err := e.createLink(t, h, "req-3", "")
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
}

func TestPutGrantOnlyForItsPlacement(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	id := uuid.MustParse(p.GetPlacementId())
	grant := e.grant(t, h)
	res, err := e.secretsRoute(t).GetBridgeSealingKey(t.Context(), connect.NewRequest(
		&routerv1.GetBridgeSealingKeyRequest{PutGrant: grant}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.Msg.GetSealingKeys()[0].GetBinding(), dpkeys.Binding(id))
	r, err := e.putBridge(t, grant, hf())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r.GetSecret().GetSpec().GetName(), "HF_TOKEN")

	_, q := e.readyPlacement(t)
	// The grant's first 16 bytes are its placement, so another placement's id breaks the MAC
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(grant, "bnsp_"))
	if err != nil {
		t.Fatal(err)
	}
	qid := uuid.MustParse(q.GetPlacementId())
	copy(raw, qid[:])
	swapped := "bnsp_" + base64.RawURLEncoding.EncodeToString(raw)
	forged := []byte(grant)
	forged[len(forged)-2] ^= 1
	expired := e.dpKeys.PutGrant(id, time.Now().Add(-time.Second))
	for name, g := range map[string]string{
		"expired": expired, "forged": string(forged), "another placement": swapped,
		"empty": "", "garbage": "bnsp_x",
	} {
		_, err := e.putBridge(t, g, hf())
		mustCode(t, err, connect.CodePermissionDenied, "PUT_GRANT_INVALID")
		if g != "" {
			quotesNone(t, err, g)
		}
		_, err = e.secretsRoute(t).GetBridgeSealingKey(t.Context(), connect.NewRequest(
			&routerv1.GetBridgeSealingKeyRequest{PutGrant: g}))
		if name != "empty" {
			mustCode(t, err, connect.CodePermissionDenied, "PUT_GRANT_INVALID")
		}
	}
	e.stop(t, h)
	_, err = e.putBridge(t, grant, hf())
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
}

// each of the five writes waits on a held row lock, and two puts at once both commit their jobs
func TestSecretWritesTakeTheRowLock(t *testing.T) {
	e := newEnv(t, options{secrets: true, opsBurst: 100})
	h, p := e.readyPlacement(t)
	id := uuid.MustParse(p.GetPlacementId())
	e.mustPutSecret(t, h, &routerv1.SecretSpec{Name: "DOOMED", Hosts: []string{"a.example"}})
	tok := e.mustLink(t, h, "req-1")
	grant := e.grant(t, h)
	writes := map[string]func() error{
		"CreateSecretLink": func() error { _, err := e.createLink(t, h, "req-2", ""); return err },
		"SubmitSecretLink": func() error { _, err := e.submit(t, tok, "s", hf()); return err },
		"PutSecret":        func() error { _, err := e.putSecret(t, h, hf(), ""); return err },
		"PutBridgeSecret":  func() error { _, err := e.putBridge(t, grant, hf()); return err },
		"DeleteSecret": func() error {
			_, err := e.client.DeleteSecret(t.Context(), connect.NewRequest(
				&routerv1.DeleteSecretRequest{Handle: h, Name: "DOOMED"}))
			return err
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			tx, err := e.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if _, err := e.store.Tx(tx).PlacementForUpdate(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- write() }()
			select {
			case err := <-done:
				t.Fatalf("%s finished while the row was locked: %v", name, err)
			case <-time.After(300 * time.Millisecond):
			}
			if err := tx.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		})
	}
	before := e.applyJobs(t, p.GetPlacementId())
	var wg sync.WaitGroup
	for _, name := range []string{"A", "B"} {
		wg.Go(func() {
			_, err := e.putSecret(t, h, &routerv1.SecretSpec{
				Name: name, Hosts: []string{"huggingface.co"},
			}, "")
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	eq(t, e.applyJobs(t, p.GetPlacementId()), before+2)
	all, err := e.store.Secrets(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, s := range all {
		names = append(names, s.Name)
	}
	eq(t, names, []string{"A", "B", "HF_TOKEN"})
}

// SubmitSecretLink's transaction has committed while it still waits for ACTIVE
func TestSecretWriteCommitsBeforeTheWait(t *testing.T) {
	e := newEnv(t, options{secrets: true,
		secretSettings: func(c *config.Secrets) { c.ApplyWait = 10 * time.Second }})
	h, p := e.readyPlacement(t)
	id := uuid.MustParse(p.GetPlacementId())
	tok := e.mustLink(t, h, "req-1")
	done := make(chan *routerv1.SubmitSecretLinkResponse, 1)
	go func() {
		r, err := e.submit(t, tok, "sub-1", hf())
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		all, err := e.store.Secrets(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if len(all) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the submit's row never committed while it waited")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if e.applyJobs(t, p.GetPlacementId()) != 1 {
		t.Fatal("the job did not commit with the row")
	}
	select {
	case <-done:
		t.Fatal("the submit answered before the apply")
	default:
	}
	start := time.Now()
	e.markActive(t, p)
	r := <-done
	eq(t, r.GetSecret().GetState(), active)
	if time.Since(start) > 2*time.Second {
		t.Fatal("the submit kept waiting after the apply")
	}
}

// a key id not in secrets.box_public_keys -> KEY_UNKNOWN, on both put paths and a link
func TestSealedWithUnknownKeyRefused(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	h, p := e.readyPlacement(t)
	stale := &routerv1.SealedValue{KeyId: "k0", Box: []byte("sealed to a rotated key")}
	_, err := e.client.PutSecret(t.Context(), connect.NewRequest(&routerv1.PutSecretRequest{
		Handle: h, Spec: hf(), Value: stale,
	}))
	mustCode(t, err, connect.CodeInvalidArgument, "KEY_UNKNOWN")
	_, err = e.secretsRoute(t).PutBridgeSecret(t.Context(), connect.NewRequest(
		&routerv1.PutBridgeSecretRequest{PutGrant: e.grant(t, h), Spec: hf(), Value: stale}))
	mustCode(t, err, connect.CodeInvalidArgument, "KEY_UNKNOWN")
	_, err = e.secretsRoute(t).SubmitSecretLink(t.Context(), connect.NewRequest(
		&routerv1.SubmitSecretLinkRequest{
			LinkToken: e.mustLink(t, h, "req-1"), Spec: hf(), Value: stale, SubmissionId: "s",
		}))
	mustCode(t, err, connect.CodeInvalidArgument, "KEY_UNKNOWN")
	eq(t, e.applyJobs(t, p.GetPlacementId()), 0)
}
