package saga_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys/sshkeystest"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// testHostKey is an ed25519 public key line as ssh-up prints it
var testHostKey = func() string {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	blob := append([]byte{0, 0, 0, 11}, "ssh-ed25519"...)
	blob = append(append(blob, 0, 0, 0, 32), pub...)
	return "ssh-ed25519 " + base64.StdEncoding.EncodeToString(blob)
}()

// sshUpOK is ssh-up's answer on the spec's port, naming a user the router must ignore
var sshUpOK = answer{
	`{"port":$PORT,"host_key":"` + testHostKey + ` root@sandbox","user":"evil"}`, 0,
}

// withSSH lists the named providers for SSH and previews in the saga's registry and engine
func withSSH(names ...string) func(*saga.Deps) {
	return func(d *saga.Deps) {
		reg, err := provider.NewRegistry(d.Registry.Ordered(),
			provider.Features{SSH: names, Preview: names})
		if err != nil {
			panic(err)
		}
		d.Registry = reg
		d.Engine = placement.New(reg, d.Tracker, nil)
		d.SSH.Ports = []int{22}
	}
}

// placeWith places an order with requirements and waits for placing to finish
func (e *env) placeWith(
	t *testing.T, order string, req placement.Requirements,
) (store.Placement, string) {
	t.Helper()
	p, h, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.startToken(t, order), Requirements: req,
	})
	if err != nil {
		t.Fatalf("create %s: %v", order, err)
	}
	e.saga.Wait()
	p = e.reload(t, p.ID)
	if p.State != store.PlacementReady {
		t.Fatalf("placement %s is %s (%s), want ready", order, p.State, p.FailureReason)
	}
	return p, h
}

// sshSpec reads the ssh-up spec the saga left in a scripted sandbox
func sshSpec(t *testing.T, root string, ref provider.Ref) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(sandboxFS(root, ref), "tmp/bugnet/ssh-spec.json"))
	if err != nil {
		t.Fatal(err)
	}
	var spec map[string]any
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	return spec
}

var aliasShape = regexp.MustCompile(`^bn-[a-z2-7]{10}$`)

func TestBootRunsSSHUpWhenTheOrderNeedsSSH(t *testing.T) {
	root := t.TempDir()
	a := newScripted(newLocal(t, local.Options{Name: "a", Root: root}),
		map[string]answer{"ssh-up": sshUpOK})
	e := newEnvWith(t, func(d *saga.Deps) {
		withSSH("a")(d)
		d.SSH.LocalSSHDPath = "/opt/fake/sshd"
	}, a)
	p, _ := e.placeWith(t, "order-1", placement.Requirements{SSH: true})
	eq(t, a.calls(), []string{"setup sample=false", "ssh-up", "setup sample=true"})
	host, err := e.store.SSHHost(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	user := local.New(local.Options{Root: root}).Capabilities().ExecUser
	eq(t, host.SSHUser, cmpOr(user, "root"))
	eq(t, host.HostKey, testHostKey)
	port, err := strconv.Atoi(p.ProviderRef.Meta["sshd_port"])
	if err != nil {
		t.Fatal(err)
	}
	eq(t, host.SSHDPort, port)
	eq(t, host.Transport, store.SSHTransportGate)
	eq(t, aliasShape.MatchString(p.SSHAlias), true)
	spec := sshSpec(t, root, *p.ProviderRef)
	eq(t, spec["sshd_path"], any("/opt/fake/sshd"))
	eq(t, spec["install"], any(false))
	eq(t, spec["ports"], any([]any{float64(port)}))
	eq(t, spec["authorize"], any([]any{}))
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func TestBootSSHUpFailureFailsOver(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}),
		map[string]answer{"ssh-up": {`{"error":"install_failed"}`, 1}})
	b := newScripted(newLocal(t, local.Options{Name: "b"}),
		map[string]answer{"ssh-up": sshUpOK})
	e := newEnvWith(t, withSSH("a", "b"), a, b)
	p, _ := e.placeWith(t, "order-1", placement.Requirements{SSH: true})
	eq(t, p.Provider, "b")
	attempts, err := e.store.Attempts(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || attempts[0].State != "failed" ||
		!regexp.MustCompile(`ssh-up exited 1 with install_failed`).
			MatchString(attempts[0].Error) {
		t.Fatalf("attempts %+v, want a's to fail on ssh-up", attempts)
	}
}

func TestBootSkipsSSHUpWithoutTheNeed(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}),
		map[string]answer{"ssh-up": sshUpOK})
	e := newEnvWith(t, withSSH("a"), a)
	p, _ := e.placeWith(t, "order-1", placement.Requirements{})
	eq(t, a.calls(), []string{"setup sample=false", "setup sample=true"})
	_, err := e.store.SSHHost(t.Context(), p.ID)
	eq(t, errors.Is(err, store.ErrNotFound), true)
	eq(t, aliasShape.MatchString(p.SSHAlias), true)
}

func TestAliasOnlyWithSSH(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.SSHAlias, "")
}

func TestAliasesAreNeverShared(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnvWith(t, withSSH("a"), a)
	seen := map[string]bool{}
	for _, order := range []string{"order-1", "order-2", "order-3"} {
		p, _ := e.placeWith(t, order, placement.Requirements{})
		eq(t, seen[p.SSHAlias], false)
		seen[p.SSHAlias] = true
		got, err := e.store.PlacementBySSHAlias(t.Context(), p.SSHAlias)
		if err != nil || got.ID != p.ID {
			t.Fatalf("by alias: %+v, %v", got, err)
		}
	}
}

func TestPrepareSSHRunsOnceForAReadyPlacement(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}),
		map[string]answer{"ssh-up": sshUpOK})
	e := newEnvWith(t, withSSH("a"), a)
	p, h := e.placeWith(t, "order-1", placement.Requirements{})
	for range 2 {
		if err := e.saga.PrepareSSH(t.Context(), p.ID); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, countOf(a.calls(), "ssh-up"), 1)
	if _, err := e.store.SSHHost(t.Context(), p.ID); err != nil {
		t.Fatal(err)
	}
	// A placement that is not ready, or not there, has nothing to prepare
	eq(t, e.saga.PrepareSSH(t.Context(), uuid.New()), nil)
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	eq(t, e.saga.PrepareSSH(t.Context(), p.ID), nil)
}

func TestPrepareSSHJobIsUniquePerPlacement(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnvWith(t, withSSH("a"), a)
	p, _ := e.placeWith(t, "order-1", placement.Requirements{})
	for range 3 {
		if err := e.saga.EnqueuePrepareSSH(t.Context(), p.ID); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	err := e.pool.QueryRow(t.Context(), `select count(*) from river_job
		where kind = 'prepare_ssh' and queue = 'dataplane' and max_attempts = 3`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, n, 1)
}

func TestAuthorizeGatewayOncePerKey(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}),
		map[string]answer{"ssh-up": sshUpOK})
	e := newEnvWith(t, withSSH("a"), a)
	p, h := e.placeWith(t, "order-1", placement.Requirements{SSH: true})
	ctx := t.Context()
	for range 2 {
		up, err := e.saga.AuthorizeGateway(ctx, p.ID, "restrict ssh-ed25519 AAAA bn-gw",
			"SHA256:gw1")
		if err != nil {
			t.Fatal(err)
		}
		eq(t, up.Host.HostKey, testHostKey)
		eq(t, up.Address, "")
	}
	eq(t, countOf(a.calls(), "ssh-up"), 2)
	if _, err := e.saga.AuthorizeGateway(ctx, p.ID, "x", "SHA256:gw2"); err != nil {
		t.Fatal(err)
	}
	eq(t, countOf(a.calls(), "ssh-up"), 3)
	last := a.argvs()[len(a.argvs())-1]
	eq(t, slices.Contains(last, "ssh-up"), true)

	// The end of the placement forgets its gateway keys
	if _, err := e.saga.Stop(ctx, h); err != nil {
		t.Fatal(err)
	}
	ok, err := e.store.HasGatewayKey(ctx, p.ID, "SHA256:gw1")
	eq(t, err, nil)
	eq(t, ok, false)
}

func TestAuthorizeGatewayBeforePrepareIsPreparing(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}),
		map[string]answer{"ssh-up": sshUpOK})
	e := newEnvWith(t, withSSH("a"), a)
	p, _ := e.placeWith(t, "order-1", placement.Requirements{})
	_, err := e.saga.AuthorizeGateway(t.Context(), p.ID, "x", "SHA256:gw")
	eq(t, errors.Is(err, saga.ErrSSHPreparing), true)
}

func TestSSHSpecForAProvider(t *testing.T) {
	boxd := newScripted(newLocal(t, local.Options{Name: "boxd"}), nil)
	boxd.caps = func(c provider.Capabilities) provider.Capabilities {
		c.ExecUser = "boxd"
		return c
	}
	got := saga.SSHSpecFor(saga.SSHSettings{
		Ports: []int{22, 2022}, InstallTimeout: 90 * time.Second,
	}, boxd, provider.Ref{ID: "x"}, []string{"line"})
	eq(t, got, saga.SSHSpecFields{
		V: 1, User: "boxd", Ports: []int{22, 2022}, SSHDPath: "/usr/sbin/sshd",
		Install: true, InstallTimeoutS: 90, Authorize: []string{"line"},
	})
	root := newScripted(newLocal(t, local.Options{Name: "e2b"}), nil)
	root.caps = func(c provider.Capabilities) provider.Capabilities {
		c.ExecUser = ""
		return c
	}
	got = saga.SSHSpecFor(saga.SSHSettings{}, root, provider.Ref{ID: "x"}, nil)
	eq(t, got.User, "root")
	eq(t, got.Ports, []int{22, 2022})
	eq(t, got.InstallTimeoutS, 120)
	eq(t, got.Authorize, []string{})
}

func countOf(calls []string, sub string) int {
	n := 0
	for _, c := range calls {
		if c == sub {
			n++
		}
	}
	return n
}

// badSSHUps are ssh-up answers the router must refuse, each with the error it gives
func badSSHUps(t *testing.T) map[string]struct {
	a    answer
	want string
} {
	return map[string]struct {
		a    answer
		want string
	}{
		"port not offered": {
			answer{`{"port":1,"host_key":"` + testHostKey + `"}`, 0}, "did not offer",
		},
		"garbage host key": {
			answer{`{"port":$PORT,"host_key":"ssh-ed25519 not-base64!"}`, 0}, "no usable host key",
		},
		"certificate host key": {
			answer{`{"port":$PORT,"host_key":"` + sshkeystest.Cert(t) + `"}`, 0},
			"no usable host key",
		},
	}
}

func TestBootRefusesABadSSHUpAnswer(t *testing.T) {
	for name, c := range badSSHUps(t) {
		t.Run(name, func(t *testing.T) {
			a := newScripted(newLocal(t, local.Options{Name: "a"}),
				map[string]answer{"ssh-up": c.a})
			b := newScripted(newLocal(t, local.Options{Name: "b"}),
				map[string]answer{"ssh-up": sshUpOK})
			e := newEnvWith(t, withSSH("a", "b"), a, b)
			p, _ := e.placeWith(t, "order-1", placement.Requirements{SSH: true})
			eq(t, p.Provider, "b")
			attempts, err := e.store.Attempts(t.Context(), p.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 2 || attempts[0].State != "failed" ||
				!strings.Contains(attempts[0].Error, c.want) {
				t.Fatalf("attempts %+v, want a's to fail with %q", attempts, c.want)
			}
			host, err := e.store.SSHHost(t.Context(), p.ID)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, strconv.Itoa(host.SSHDPort), p.ProviderRef.Meta["sshd_port"])
		})
	}
}

func TestPrepareSSHRefusesABadSSHUpAnswer(t *testing.T) {
	for name, c := range badSSHUps(t) {
		t.Run(name, func(t *testing.T) {
			a := newScripted(newLocal(t, local.Options{Name: "a"}),
				map[string]answer{"ssh-up": c.a})
			e := newEnvWith(t, withSSH("a"), a)
			p, _ := e.placeWith(t, "order-1", placement.Requirements{})
			err := e.saga.PrepareSSH(t.Context(), p.ID)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("prepare ssh: %v, want %q", err, c.want)
			}
			_, err = e.store.SSHHost(t.Context(), p.ID)
			eq(t, errors.Is(err, store.ErrNotFound), true)
		})
	}
}

func TestAuthorizeGatewayRefusesABadSSHUpAnswer(t *testing.T) {
	for name, c := range badSSHUps(t) {
		t.Run(name, func(t *testing.T) {
			answers := map[string]answer{"ssh-up": sshUpOK}
			a := newScripted(newLocal(t, local.Options{Name: "a"}), answers)
			e := newEnvWith(t, withSSH("a"), a)
			p, _ := e.placeWith(t, "order-1", placement.Requirements{SSH: true})
			before, err := e.store.SSHHost(t.Context(), p.ID)
			if err != nil {
				t.Fatal(err)
			}
			answers["ssh-up"] = c.a
			_, err = e.saga.AuthorizeGateway(t.Context(), p.ID, "x", "SHA256:gw")
			if !errors.Is(err, saga.ErrSSHPreparing) || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("authorize: %v, want preparing with %q", err, c.want)
			}
			has, err := e.store.HasGatewayKey(t.Context(), p.ID, "SHA256:gw")
			eq(t, err, nil)
			eq(t, has, false)
			after, err := e.store.SSHHost(t.Context(), p.ID)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, after, before)
		})
	}
}

func TestAuthorizeGatewayAddsNoKeyOnceThePlacementEnds(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}), map[string]answer{"ssh-up": sshUpOK})
	var hook func()
	a.onAnswer = func(sub string) {
		if sub == "ssh-up" && hook != nil {
			hook()
		}
	}
	e := newEnvWith(t, withSSH("a"), a)
	p, _ := e.placeWith(t, "order-1", placement.Requirements{SSH: true})
	ctx := t.Context()
	// The placement starts to end while ssh-up runs
	hook = func() {
		if _, err := e.store.MarkPlacementTerminating(ctx, p.ID, "stop", time.Now()); err != nil {
			t.Error(err)
		}
	}
	_, err := e.saga.AuthorizeGateway(ctx, p.ID, "x", "SHA256:gw")
	eq(t, errors.Is(err, saga.ErrNotReady), true)
	has, err := e.store.HasGatewayKey(ctx, p.ID, "SHA256:gw")
	eq(t, err, nil)
	eq(t, has, false)
}

// nativeServer is a scripted provider with an SSH server of its own
type nativeServer struct {
	scripted
	authorized *[]string
}

func (n nativeServer) AuthorizeNativeSSH(
	_ context.Context, _ provider.Ref, gatewayKey string,
) (provider.NativeSSH, error) {
	n.mu.Lock()
	*n.authorized = append(*n.authorized, gatewayKey)
	n.mu.Unlock()
	return provider.NativeSSH{Address: "203.0.113.10:2201", User: "root", HostKey: testHostKey},
		nil
}

func TestAuthorizeGatewayOnAnyProvidersOwnServer(t *testing.T) {
	for name, transport := range map[string]string{
		"pod": "pod_native", "boat": store.SSHTransportBoatNative,
		"lium": store.SSHTransportLiumNative,
	} {
		t.Run(name, func(t *testing.T) {
			n := nativeServer{
				scripted:   newScripted(newLocal(t, local.Options{Name: name}), nil),
				authorized: &[]string{},
			}
			e := newEnvFeatures(t, nil, provider.Features{
				SSH: []string{name}, NativeSSH: []string{name},
			}, n)
			p, _ := e.placeWith(t, "order-1", placement.Requirements{SSH: true})
			eq(t, countOf(n.calls(), "ssh-up"), 0)
			eq(t, aliasShape.MatchString(p.SSHAlias), true)
			if err := e.saga.PrepareSSH(t.Context(), p.ID); err != nil {
				t.Fatal(err)
			}
			_, err := e.store.SSHHost(t.Context(), p.ID)
			eq(t, errors.Is(err, store.ErrNotFound), true)
			for range 2 {
				up, err := e.saga.AuthorizeGateway(t.Context(), p.ID, "k bn-gw", "SHA256:gw")
				if err != nil {
					t.Fatal(err)
				}
				eq(t, up.Address, "203.0.113.10:2201")
				eq(t, up.Host, store.SSHHost{
					PlacementID: p.ID, Transport: transport, SSHDPort: 2201, SSHUser: "root",
					HostKey: testHostKey, PreparedAt: up.Host.PreparedAt,
				})
			}
			eq(t, *n.authorized, []string{"k bn-gw"})
			host, err := e.store.SSHHost(t.Context(), p.ID)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, host.Transport, transport)
			eq(t, e.reload(t, p.ID).ProviderRef.Meta["ssh_address"], "203.0.113.10:2201")
			eq(t, countOf(n.calls(), "ssh-up"), 0)
		})
	}
}
