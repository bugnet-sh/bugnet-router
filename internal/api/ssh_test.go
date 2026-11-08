package api_test

import (
	"context"
	"crypto/elliptic"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys/sshkeystest"
)

const sshHost = "ssh.bugnet.sh"

// gatewayHostKey is the gateway's host key line in ssh.host_keys
const gatewayHostKey = "ssh-ed25519 " +
	"AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl gateway"

// sshdHostKey is the host key the fake ssh-up reports for every sandbox
const sshdHostKey = "ssh-ed25519 " +
	"AAAAC3NzaC1lZDI1NTE5AAAAIHB4gOKiJXy6m2mLz4CVrxw6NWSxHxBIkmOBbzWNCkQ8"

// sshSettings fills the zero fields of s with the [ssh] defaults and one gateway host key
func sshSettings(s config.SSH) config.SSH {
	if s.HostKeys == nil {
		s.HostKeys = []string{gatewayHostKey}
	}
	if s.PrepareTimeout == 0 {
		s.PrepareTimeout = 5 * time.Second
	}
	return s
}

// sshUpFake answers ssh-up itself on its spec's first port and records each spec it got
type sshUpFake struct {
	*local.Provider
	mu    *sync.Mutex
	specs *[]sshUpSpec
	// user replaces the exec user when set
	user string
}

// sshUpSpec is the part of an ssh-up spec the tests read
type sshUpSpec struct {
	User      string   `json:"user"`
	Ports     []int    `json:"ports"`
	Authorize []string `json:"authorize"`
}

func newSSHUpFake(l *local.Provider) sshUpFake {
	return sshUpFake{Provider: l, mu: &sync.Mutex{}, specs: &[]sshUpSpec{}}
}

func (f sshUpFake) Capabilities() provider.Capabilities {
	c := f.Provider.Capabilities()
	if f.user != "" {
		c.ExecUser = f.user
	}
	return c
}

func (f sshUpFake) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	if !slices.Contains(spec.Argv, "ssh-up") {
		return f.Provider.Exec(ctx, ref, spec)
	}
	raw, _, err := f.ReadFile(ctx, ref, "/tmp/bugnet/ssh-spec.json", 0, 1<<20)
	var got sshUpSpec
	if err != nil || json.Unmarshal(raw, &got) != nil || len(got.Ports) == 0 {
		got.Ports = []int{0}
	}
	f.mu.Lock()
	*f.specs = append(*f.specs, got)
	f.mu.Unlock()
	// The sandbox's root may print any user, which the router never takes
	out := `{"port":` + strconv.Itoa(got.Ports[0]) + `,"host_key":"` + sshdHostKey +
		` root@x","user":"evil"}`
	return f.Provider.Exec(ctx, ref, provider.ExecSpec{
		Argv: []string{"sh", "-c", `printf '%s\n' "$1"`, "sh", out},
	})
}

func (f sshUpFake) runs() []sshUpSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(*f.specs)
}

// sshEnv is an env whose provider answers ssh-up through fake
func sshEnv(t *testing.T, opts options) (*env, sshUpFake) {
	t.Helper()
	var fake sshUpFake
	opts.withSSH = true
	opts.wrap = func(l *local.Provider, _ string) provider.Provider {
		fake = newSSHUpFake(l)
		if opts.providerName == "boxd" {
			fake.user = "boxd"
		}
		return fake
	}
	return newEnv(t, opts), fake
}

// readyWithSSH places an order that needs SSH and waits until it is ready
func (e *env) readyWithSSH(t *testing.T) (string, *routerv1.Placement) {
	t.Helper()
	resp, err := e.createFor(t, e.startToken(t, smallPkg, smallPrice),
		&routerv1.Requirements{Ssh: true})
	if err != nil {
		t.Fatalf("create placement: %v", err)
	}
	h := resp.Msg.GetHandle()
	return h, e.waitReady(t, h)
}

func (e *env) addKey(t *testing.T, h, key string) (*routerv1.AddSshKeyResponse, error) {
	t.Helper()
	resp, err := e.client.AddSshKey(t.Context(), connect.NewRequest(
		&routerv1.AddSshKeyRequest{Handle: h, PublicKey: key}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (e *env) mustAddKey(t *testing.T, h, key string) *routerv1.AddSshKeyResponse {
	t.Helper()
	a, err := e.addKey(t, h, key)
	if err != nil {
		t.Fatalf("AddSshKey: %v", err)
	}
	return a
}

func (e *env) authorize(
	t *testing.T, alias, key string,
) (*routerv1.AuthorizeSshKeyResponse, error) {
	t.Helper()
	resp, err := e.ssh(t).AuthorizeSshKey(t.Context(), connect.NewRequest(
		&routerv1.AuthorizeSshKeyRequest{
			Alias: alias, PublicKey: sshkeystest.Wire(t, key), ClientIp: "203.0.113.9",
		}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// grantFor adds key to a placement and answers its grant
func (e *env) grantFor(t *testing.T, h, key string) string {
	t.Helper()
	a := e.mustAddKey(t, h, key)
	g, err := e.authorize(t, a.GetAlias(), key)
	if err != nil {
		t.Fatalf("AuthorizeSshKey: %v", err)
	}
	return g.GetGrantId()
}

func (e *env) acquire(t *testing.T, grant string) (*routerv1.AcquireSshConnectionResponse, error) {
	t.Helper()
	resp, err := e.ssh(t).AcquireSshConnection(t.Context(), connect.NewRequest(
		&routerv1.AcquireSshConnectionRequest{GrantId: grant, GatewayId: "gw-1"}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (e *env) openUpstream(
	t *testing.T, grant string, gatewayKey []byte,
) (*routerv1.OpenSshUpstreamResponse, error) {
	t.Helper()
	resp, err := e.ssh(t).OpenSshUpstream(t.Context(), connect.NewRequest(
		&routerv1.OpenSshUpstreamRequest{GrantId: grant, GatewayPublicKey: gatewayKey}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (e *env) checkGrants(t *testing.T, grants ...string) map[string]time.Time {
	t.Helper()
	resp, err := e.ssh(t).CheckSshGrants(t.Context(), connect.NewRequest(
		&routerv1.CheckSshGrantsRequest{GrantIds: grants}))
	if err != nil {
		t.Fatalf("CheckSshGrants: %v", err)
	}
	out := map[string]time.Time{}
	for id, ts := range resp.Msg.GetDeadlines() {
		out[id] = ts.AsTime()
	}
	return out
}

func gatewayKey(t *testing.T) []byte {
	return sshkeystest.Wire(t, sshkeystest.ED25519(t))
}

func (e *env) connsCount(t *testing.T, placementID string) int64 {
	t.Helper()
	n, err := e.vk.Do(t.Context(), e.vk.B().Zcard().Key(e.prefix+"ssh:conns:"+placementID).
		Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

/* AddSshKey */

func TestAddSshKeyKinds(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, _ := e.readyPlacement(t)
	other, _ := e.readyPlacement(t)
	kinds := []string{
		sshkeystest.ED25519(t), sshkeystest.ECDSA(t, elliptic.P256()),
		sshkeystest.ECDSA(t, elliptic.P384()), sshkeystest.ECDSA(t, elliptic.P521()),
		sshkeystest.SK(t, false), sshkeystest.SK(t, true), sshkeystest.RSA(t, 3072),
	}
	for i, k := range kinds {
		// Four keys fit one placement, so the rest go to a second
		on := h
		if i >= 4 {
			on = other
		}
		if _, err := e.addKey(t, on, k+" me@laptop"); err != nil {
			t.Fatalf("kind %d: %v", i, err)
		}
	}
	for _, k := range []string{sshkeystest.RSA(t, 2048), sshkeystest.DSA(),
		sshkeystest.Cert(t), `command="x" ` + sshkeystest.ED25519(t), ""} {
		_, err := e.addKey(t, other, k)
		mustCode(t, err, connect.CodeInvalidArgument, "KEY_INVALID")
	}
}

func TestAddSshKeyCap(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, _ := e.readyPlacement(t)
	keys := []string{sshkeystest.ED25519(t), sshkeystest.ED25519(t), sshkeystest.ED25519(t),
		sshkeystest.ED25519(t)}
	for _, k := range keys {
		e.mustAddKey(t, h, k)
	}
	// The same key again is idempotent, even at the cap
	e.mustAddKey(t, h, keys[0])
	_, err := e.addKey(t, h, sshkeystest.ED25519(t))
	mustCode(t, err, connect.CodeResourceExhausted, "TOO_MANY_KEYS")
}

func TestAddSshKeyAnswer(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, msg := e.readyPlacement(t)
	key := sshkeystest.ED25519(t)
	a := e.mustAddKey(t, h, key)
	eq(t, regexp.MustCompile(`^bn-[a-z2-7]{10}$`).MatchString(a.GetAlias()), true)
	eq(t, a.GetAlias(), msg.GetSshAlias())
	got := e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED, 0)
	eq(t, got.GetSshAlias(), a.GetAlias())
	eq(t, a.GetCommand(), "ssh -i ~/.ssh/bugnet_"+a.GetAlias()+" -o IdentitiesOnly=yes"+
		" -o BatchMode=yes -o StrictHostKeyChecking=yes"+
		" -o UserKnownHostsFile=~/.ssh/bugnet_known_hosts "+a.GetAlias()+"@ssh.bugnet.sh")
	eq(t, a.GetKnownHosts(), []string{"ssh.bugnet.sh " +
		strings.TrimSuffix(gatewayHostKey, " gateway")})
	eq(t, a.GetHost(), sshHost)
	eq(t, a.GetPort(), uint32(22))
	eq(t, a.GetFingerprint(), sshkeystest.Fingerprint(t, key))
	eq(t, a.GetExpiresAt().AsTime().Equal(msg.GetDeadline().AsTime()), true)
	eq(t, strings.Contains(a.GetConfig(), "Host bugnet-"+a.GetAlias()+"\n"), true)
	eq(t, strings.Contains(a.GetConfig(), "ControlMaster auto\n"), true)
}

func TestIdentityFileRule(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, _ := e.readyPlacement(t)
	for _, req := range []*routerv1.AddSshKeyRequest{
		{IdentityFile: "~/.ssh/x;rm"}, {KnownHostsFile: "$(reboot)"}, {IdentityFile: "a b"},
	} {
		req.Handle, req.PublicKey = h, sshkeystest.ED25519(t)
		_, err := e.client.AddSshKey(t.Context(), connect.NewRequest(req))
		mustCode(t, err, connect.CodeInvalidArgument, "PATH_REFUSED")
		quotesNone(t, err, req.GetIdentityFile()+req.GetKnownHostsFile())
	}
	resp, err := e.client.AddSshKey(t.Context(), connect.NewRequest(&routerv1.AddSshKeyRequest{
		Handle: h, PublicKey: sshkeystest.ED25519(t), IdentityFile: "$TMPDIR/bugnet/k",
		KnownHostsFile: "$TMPDIR/bugnet/known_hosts",
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, strings.Contains(resp.Msg.GetCommand(), "-i $TMPDIR/bugnet/k "), true)
	eq(t, strings.Contains(resp.Msg.GetConfig(), "IdentityFile ${TMPDIR}/bugnet/k\n"), true)
}

func TestAddSshKeyNeedsAReadyPlacementWithSSH(t *testing.T) {
	e := newEnv(t, options{})
	h, msg := e.readyPlacement(t)
	eq(t, msg.GetSshAlias(), "")
	_, err := e.addKey(t, h, sshkeystest.ED25519(t))
	mustCode(t, err, connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")

	e = newEnv(t, options{withSSH: true})
	h, _ = e.readyPlacement(t)
	e.stop(t, h)
	_, err = e.addKey(t, h, sshkeystest.ED25519(t))
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
}

func TestAliasOnlyWithSSH(t *testing.T) {
	e := newEnv(t, options{})
	_, msg := e.readyPlacement(t)
	eq(t, msg.GetSshAlias(), "")
	e = newEnv(t, options{withSSH: true})
	_, msg = e.readyPlacement(t)
	eq(t, regexp.MustCompile(`^bn-[a-z2-7]{10}$`).MatchString(msg.GetSshAlias()), true)
}

/* Grants and leases */

func TestAuthorizeTakesNoLease(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, msg := e.readyPlacement(t)
	key := sshkeystest.ED25519(t)
	a := e.mustAddKey(t, h, key)
	for range 3 {
		g, err := e.authorize(t, a.GetAlias(), key)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, g.GetPlacementId(), msg.GetPlacementId())
		eq(t, g.GetMaxSessions(), uint32(10))
		eq(t, g.GetMaxForwards(), uint32(32))
		eq(t, strings.HasPrefix(g.GetGrantId(), "g1."), true)
		eq(t, g.GetDeadline().AsTime().Equal(msg.GetDeadline().AsTime()), true)
	}
	eq(t, e.connsCount(t, msg.GetPlacementId()), int64(0))
}

func TestAuthorizeDenialsLookAlike(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, _ := e.readyPlacement(t)
	ended, _ := e.readyPlacement(t)
	key, stranger := sshkeystest.ED25519(t), sshkeystest.ED25519(t)
	a := e.mustAddKey(t, h, key)
	endedAlias := e.mustAddKey(t, ended, key).GetAlias()
	e.stop(t, ended)
	for name, c := range map[string]struct{ alias, key string }{
		"unknown alias":   {"bn-aaaaaaaaaa", key},
		"malformed alias": {"root", key},
		"wrong key":       {a.GetAlias(), stranger},
		"ended placement": {endedAlias, key},
		"certificate":     {a.GetAlias(), sshkeystest.Cert(t)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := e.authorize(t, c.alias, c.key)
			mustCode(t, err, connect.CodeNotFound, "GRANT_UNKNOWN")
			var ce *connect.Error
			if errors.As(err, &ce) {
				eq(t, ce.Message(), "api: no SSH grant stands for that alias and key")
			}
		})
	}
}

func TestNinthConnectionRefused(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, msg := e.readyPlacement(t)
	grant := e.grantFor(t, h, sshkeystest.ED25519(t))
	var leases []string
	for range 8 {
		l, err := e.acquire(t, grant)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, l.GetBudget().GetBytesRemaining() > 0, true)
		leases = append(leases, l.GetLeaseId())
	}
	_, err := e.acquire(t, grant)
	mustCode(t, err, connect.CodeResourceExhausted, "TOO_MANY_CONNECTIONS")
	_, err = e.ssh(t).ReleaseSshConnection(t.Context(), connect.NewRequest(
		&routerv1.ReleaseSshConnectionRequest{GrantId: grant, LeaseId: leases[0]}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.acquire(t, grant); err != nil {
		t.Fatalf("acquire after a release: %v", err)
	}
	eq(t, e.connsCount(t, msg.GetPlacementId()), int64(8))
	_, err = e.ssh(t).RenewSshConnection(t.Context(), connect.NewRequest(
		&routerv1.RenewSshConnectionRequest{GrantId: grant, LeaseId: leases[1]}))
	if err != nil {
		t.Fatalf("renew a live lease at the cap: %v", err)
	}
	_, err = e.ssh(t).RenewSshConnection(t.Context(), connect.NewRequest(
		&routerv1.RenewSshConnectionRequest{GrantId: grant, LeaseId: "not-a-lease"}))
	mustCode(t, err, connect.CodeInvalidArgument, "")
	_, err = e.acquire(t, "g1.forged")
	mustCode(t, err, connect.CodePermissionDenied, "GRANT_INVALID")
}

func TestAcquireRefusesSpentBudget(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, msg := e.readyPlacement(t)
	grant := e.grantFor(t, h, sshkeystest.ED25519(t))
	err := e.vk.Do(t.Context(), e.vk.B().Set().Key(e.prefix+"edge:spent:"+
		msg.GetPlacementId()).Value("1").Build()).Error()
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.acquire(t, grant)
	mustCode(t, err, connect.CodeResourceExhausted, "BUDGET_SPENT")
}

func TestLeaseLapsesAfter60s(t *testing.T) {
	const lease = 400 * time.Millisecond
	e := newEnv(t, options{withSSH: true, ssh: config.SSH{
		ConnectionLease: lease, MaxConnectionsPerPlacement: 1,
	}})
	h, _ := e.readyPlacement(t)
	grant := e.grantFor(t, h, sshkeystest.ED25519(t))
	first, err := e.acquire(t, grant)
	if err != nil {
		t.Fatal(err)
	}
	if got := time.Until(first.GetLeaseExpiresAt().AsTime()); got > lease || got < 0 {
		t.Fatalf("the lease ends in %v, want within %v", got, lease)
	}
	_, err = e.acquire(t, grant)
	mustCode(t, err, connect.CodeResourceExhausted, "TOO_MANY_CONNECTIONS")
	time.Sleep(lease + 100*time.Millisecond)
	second, err := e.acquire(t, grant)
	if err != nil {
		t.Fatalf("acquire once the first lease lapsed: %v", err)
	}
	// A renew of the lapsed lease finds no room, since the second holds the one place
	_, err = e.ssh(t).RenewSshConnection(t.Context(), connect.NewRequest(
		&routerv1.RenewSshConnectionRequest{GrantId: grant, LeaseId: first.GetLeaseId()}))
	mustCode(t, err, connect.CodeResourceExhausted, "TOO_MANY_CONNECTIONS")
	_, err = e.ssh(t).RenewSshConnection(t.Context(), connect.NewRequest(
		&routerv1.RenewSshConnectionRequest{GrantId: grant, LeaseId: second.GetLeaseId()}))
	if err != nil {
		t.Fatalf("renew the live lease: %v", err)
	}
}

// AddSshKey takes one ops token, the gateway's RPCs take none (DP 11.3)
func TestSshRPCsAndTheOperationBucket(t *testing.T) {
	e, _ := sshEnv(t, options{opsPerSecond: 1, opsBurst: 1})
	h, msg := e.readyWithSSH(t)
	e.waitFull(t, msg.GetPlacementId())
	key := sshkeystest.ED25519(t)
	a := e.mustAddKey(t, h, key)
	_, err := e.addKey(t, h, sshkeystest.ED25519(t))
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
	for range 3 {
		g, err := e.authorize(t, a.GetAlias(), key)
		if err != nil {
			t.Fatal(err)
		}
		l, err := e.acquire(t, g.GetGrantId())
		if err != nil {
			t.Fatal(err)
		}
		_, err = e.ssh(t).RenewSshConnection(t.Context(), connect.NewRequest(
			&routerv1.RenewSshConnectionRequest{GrantId: g.GetGrantId(), LeaseId: l.GetLeaseId()}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.openUpstream(t, g.GetGrantId(), gatewayKey(t)); err != nil {
			t.Fatal(err)
		}
		eq(t, len(e.checkGrants(t, g.GetGrantId())), 1)
		release := &routerv1.ReleaseSshConnectionRequest{
			GrantId: g.GetGrantId(), LeaseId: l.GetLeaseId(),
		}
		_, err = e.ssh(t).ReleaseSshConnection(t.Context(), connect.NewRequest(release))
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = e.client.RemoveSshKey(t.Context(), connect.NewRequest(&routerv1.RemoveSshKeyRequest{
		Handle: h, Fingerprint: a.GetFingerprint(),
	}))
	if err != nil {
		t.Fatalf("RemoveSshKey with the bucket spent: %v", err)
	}
}

/* Upstream */

func TestOpenSshUpstreamWaitsForPrepare(t *testing.T) {
	const wait = 500 * time.Millisecond
	e, fake := sshEnv(t, options{ssh: config.SSH{PrepareTimeout: wait}})
	h, msg := e.readyPlacement(t)
	grant := e.grantFor(t, h, sshkeystest.ED25519(t))
	start := time.Now()
	_, err := e.openUpstream(t, grant, gatewayKey(t))
	mustCode(t, err, connect.CodeUnavailable, "SSH_PREPARING")
	if took := time.Since(start); took < wait {
		t.Fatalf("the upstream gave up after %v, want %v", took, wait)
	}
	eq(t, len(fake.runs()), 0)
	var jobs int
	err = e.pool.QueryRow(t.Context(), `select count(*) from river_job
		where kind = 'prepare_ssh' and args->>'placement_id' = $1`,
		msg.GetPlacementId()).Scan(&jobs)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, jobs, 1)

	// The job finishing while the upstream waits lets it through
	go func() {
		time.Sleep(wait / 5)
		_ = e.saga.PrepareSSH(context.Background(), uuid.MustParse(msg.GetPlacementId()))
	}()
	up, err := e.openUpstream(t, grant, gatewayKey(t))
	if err != nil {
		t.Fatalf("the upstream once prepared: %v", err)
	}
	eq(t, up.GetHostKeys(), []string{sshdHostKey})
	eq(t, len(fake.runs()), 2)
}

func TestOpenSshUpstreamAuthorizesGatewayKeyOnce(t *testing.T) {
	e, fake := sshEnv(t, options{})
	h, _ := e.readyWithSSH(t)
	eq(t, len(fake.runs()), 1)
	eq(t, fake.runs()[0].Authorize, []string{})
	grant := e.grantFor(t, h, sshkeystest.ED25519(t))
	gk := gatewayKey(t)
	for range 2 {
		if _, err := e.openUpstream(t, grant, gk); err != nil {
			t.Fatal(err)
		}
	}
	runs := fake.runs()
	eq(t, len(runs), 2)
	line := regexp.MustCompile(`^restrict,pty,port-forwarding,permitopen="127\.0\.0\.1:\*",` +
		`permitopen="\[::1\]:\*" ssh-ed25519 \S+ bn-gw$`)
	if len(runs[1].Authorize) != 1 || !line.MatchString(runs[1].Authorize[0]) {
		t.Fatalf("ssh-up authorised %q", runs[1].Authorize)
	}
	if _, err := e.openUpstream(t, grant, gatewayKey(t)); err != nil {
		t.Fatal(err)
	}
	eq(t, len(fake.runs()), 3)
}

func TestOpenSshUpstreamUserIsExecUser(t *testing.T) {
	e, _ := sshEnv(t, options{providerName: "boxd"})
	h, msg := e.readyWithSSH(t)
	grant := e.grantFor(t, h, sshkeystest.ED25519(t))
	up, err := e.openUpstream(t, grant, gatewayKey(t))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, up.GetUser(), "boxd")
	eq(t, up.GetHostKeys(), []string{sshdHostKey})
	eq(t, up.GetDeadline().AsTime().Equal(msg.GetDeadline().AsTime()), true)
	ws := up.GetWebsocket()
	if ws == nil || !regexp.MustCompile(`^ws://127\.0\.0\.1:\d+/__bugnet/ssh$`).
		MatchString(ws.GetUrl()) {
		t.Fatalf("upstream %+v, want the gate's SSH path", up)
	}
	var gate string
	for _, hv := range ws.GetHeaders() {
		if hv.GetName() == "Bugnet-Gate" {
			gate = hv.GetValue()
		}
	}
	want, err := e.dpKeys.GateKey(e.dpKeys.Current().ID, uuid.MustParse(msg.GetPlacementId()))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, gate, want)
}

func TestOpenSshUpstreamRefusesNonEd25519Key(t *testing.T) {
	e, _ := sshEnv(t, options{})
	h, _ := e.readyWithSSH(t)
	grant := e.grantFor(t, h, sshkeystest.ED25519(t))
	for _, k := range [][]byte{
		sshkeystest.Wire(t, sshkeystest.ECDSA(t, elliptic.P256())),
		sshkeystest.Wire(t, sshkeystest.SK(t, false)), []byte("garbage"), nil,
	} {
		_, err := e.openUpstream(t, grant, k)
		mustCode(t, err, connect.CodeInvalidArgument, "KEY_INVALID")
	}
}

func TestOpenSshUpstreamOnTheLocalRunner(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, _ := e.readyWithSSH(t)
	grant := e.grantFor(t, h, sshkeystest.ED25519(t))
	up, err := e.openUpstream(t, grant, gatewayKey(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(up.GetHostKeys()) != 1 || !strings.HasPrefix(up.GetHostKeys()[0], "ssh-ed25519 ") {
		t.Fatalf("host keys %q, want the sandbox's ed25519 key", up.GetHostKeys())
	}
	if up.GetWebsocket() == nil {
		t.Fatalf("upstream %+v, want the gate's SSH path", up)
	}
}

/* Removal and checks */

func TestRemoveSshKeyPublishesEvent(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, msg := e.readyPlacement(t)
	key, kept := sshkeystest.ED25519(t), sshkeystest.ED25519(t)
	grant := e.grantFor(t, h, key)
	keptGrant := e.grantFor(t, h, kept)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream, err := e.ssh(t).WatchSshEvents(ctx, connect.NewRequest(
		&routerv1.WatchSshEventsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	fp := sshkeystest.Fingerprint(t, key)
	_, err = e.client.RemoveSshKey(t.Context(), connect.NewRequest(&routerv1.RemoveSshKeyRequest{
		Handle: h, Fingerprint: fp,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !stream.Receive() {
		t.Fatalf("no event: %v", stream.Err())
	}
	got := stream.Msg().GetEvent().GetSshKeyRemoved()
	eq(t, got.GetPlacementId(), msg.GetPlacementId())
	eq(t, got.GetFingerprint(), fp)
	left := e.checkGrants(t, grant, keptGrant, "g1.forged", "")
	eq(t, len(left), 1)
	eq(t, left[keptGrant].Equal(msg.GetDeadline().AsTime()), true)
	_, err = e.acquire(t, grant)
	mustCode(t, err, connect.CodeNotFound, "GRANT_UNKNOWN")
	_, err = e.client.RemoveSshKey(t.Context(), connect.NewRequest(&routerv1.RemoveSshKeyRequest{
		Handle: h, Fingerprint: "not a fingerprint",
	}))
	mustCode(t, err, connect.CodeInvalidArgument, "")
}

func TestCheckSshGrantsDropsEndedPlacements(t *testing.T) {
	e := newEnv(t, options{withSSH: true})
	h, _ := e.readyPlacement(t)
	ended, _ := e.readyPlacement(t)
	live := e.grantFor(t, h, sshkeystest.ED25519(t))
	gone := e.grantFor(t, ended, sshkeystest.ED25519(t))
	e.stop(t, ended)
	got := e.checkGrants(t, live, gone)
	_, ok := got[live]
	eq(t, ok, true)
	_, ok = got[gone]
	eq(t, ok, false)
	_, err := e.openUpstream(t, gone, gatewayKey(t))
	mustCode(t, err, connect.CodeNotFound, "GRANT_UNKNOWN")
}

/* Boat's own sshd */

// nativeBoat is a local provider named boat with an SSH server of its own
type nativeBoat struct {
	*local.Provider
	calls *atomic.Int32
}

func (n nativeBoat) AuthorizeNativeSSH(
	_ context.Context, _ provider.Ref, gatewayKey string,
) (provider.NativeSSH, error) {
	n.calls.Add(1)
	if !strings.HasSuffix(gatewayKey, " bn-gw") {
		return provider.NativeSSH{}, errors.New("not the gateway's line")
	}
	return provider.NativeSSH{Address: "203.0.113.10:22", User: "user", HostKey: sshdHostKey},
		nil
}

func TestBoatNativeSSHCachedPerKey(t *testing.T) {
	calls := &atomic.Int32{}
	e := newEnv(t, options{providerName: "boat", nativeSSH: true,
		wrap: func(l *local.Provider, _ string) provider.Provider {
			return nativeBoat{Provider: l, calls: calls}
		}})
	h, _ := e.readyWithSSH(t)
	grant := e.grantFor(t, h, sshkeystest.ED25519(t))
	gk := gatewayKey(t)
	for range 2 {
		up, err := e.openUpstream(t, grant, gk)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, up.GetTcp().GetAddress(), "203.0.113.10:22")
		eq(t, up.GetUser(), "user")
		eq(t, up.GetHostKeys(), []string{sshdHostKey})
	}
	eq(t, calls.Load(), int32(1))
	if _, err := e.openUpstream(t, grant, gatewayKey(t)); err != nil {
		t.Fatal(err)
	}
	eq(t, calls.Load(), int32(2))
}

// A router without a gateway host, host keys or root keys refuses before it spends a token
func TestAddSshKeyWithoutAGateway(t *testing.T) {
	for name, opts := range map[string]options{
		"no host keys": {ssh: config.SSH{HostKeys: []string{}}},
		"no ssh host":  {noSSHHost: true},
		"no root keys": {noKeys: true},
	} {
		t.Run(name, func(t *testing.T) {
			opts.opsPerSecond, opts.opsBurst = 1, 1
			e, _ := sshEnv(t, opts)
			h, msg := e.readyWithSSH(t)
			for range 3 {
				_, err := e.addKey(t, h, sshkeystest.ED25519(t))
				mustCode(t, err, connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")
			}
			var n int
			err := e.pool.QueryRow(t.Context(), `select count(*) from ssh_keys
				where placement_id = $1`, msg.GetPlacementId()).Scan(&n)
			eq(t, err, nil)
			eq(t, n, 0)
		})
	}
}

func TestAuthorizeSshKeyWithoutRootKeys(t *testing.T) {
	e, _ := sshEnv(t, options{noKeys: true})
	_, err := e.authorize(t, "bn-aaaaaaaaaa", sshkeystest.ED25519(t))
	mustCode(t, err, connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")
}
