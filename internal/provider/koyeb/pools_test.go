package koyeb

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const poolName = "bn-main-medium"

var poolLabels = map[string]string{
	"bugnet": "1", "bugnet_deployment": "main", "bugnet_pool": "medium",
}

func mediumSpec(t *testing.T, p *Provider, target int) provider.PoolSpec {
	t.Helper()
	size, ok := p.SizeFor(provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10})
	if !ok || size.Name != "medium" {
		t.Fatalf("size %+v %v, want medium", size, ok)
	}
	return provider.PoolSpec{Name: poolName, Size: size, Target: target, Labels: poolLabels}
}

func TestKoyebEnsurePoolBody(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 2)), nil)
	pool, ok := f.poolByName(poolName)
	eq(t, ok, true)
	eq(t, pool.size, int64(2))
	def := any(pool.definition)
	for path, want := range map[string]string{
		"type":               `"SANDBOX"`,
		"docker.image":       `"koyeb/sandbox"`,
		"regions":            `["fra"]`,
		"instance_types":     `[{"type": "medium"}]`,
		"scalings.0.min":     `1`,
		"scalings.0.max":     `1`,
		"scalings.0.targets": `[{"sleep_idle_delay": {"light_sleep_value": 60}}]`,
		"mesh":               `"DEPLOYMENT_MESH_AUTO"`,
		"ports":              `[{"port": 3030, "protocol": "http"}, {"port": 3031, "protocol": "http"}]`,
		"routes.0.path":      `"/koyeb-sandbox/"`,
	} {
		if got := field(def, path); !jsonEqual(got, want) {
			t.Errorf("definition.%s = %v, want %s", path, got, want)
		}
	}
	// The member labels in BUGNET_LABELS and no secret of ours, whatever else
	env := definitionEnv(pool.definition)
	eq(t, len(env), 1)
	var labels map[string]string
	eq(t, json.Unmarshal([]byte(env["BUGNET_LABELS"]), &labels), nil)
	eq(t, labels, poolLabels)
	if _, held := labels["placement"]; held {
		t.Fatal("a pool member carries a placement label")
	}
	_, unclaimed := f.poolMembers(poolName)
	eq(t, len(unclaimed), 2)

	// A repeated call changes nothing, a new target changes the size alone
	eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 2)), nil)
	eq(t, f.callCount("POST /v1/service_pools"), 1)
	eq(t, f.callCount("PUT /v1/service_pools/{id}"), 0)
	eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 3)), nil)
	eq(t, f.callCount("POST /v1/service_pools"), 1)
	eq(t, f.callCount("PUT /v1/service_pools/{id}"), 1)
	_, unclaimed = f.poolMembers(poolName)
	eq(t, len(unclaimed), 3)
}

func TestKoyebEnsurePoolMakesNoDuplicateAcrossProviders(t *testing.T) {
	f := newFake(t)
	a, b := f.provider(t, fakeToken), f.provider(t, fakeToken)
	eq(t, a.EnsurePool(t.Context(), mediumSpec(t, a, 1)), nil)
	// The second replica has not seen the pool, and finds it by name
	eq(t, b.EnsurePool(t.Context(), mediumSpec(t, b, 1)), nil)
	eq(t, len(f.pools.pools), 1)
}

func TestKoyebEnsurePoolRefusals(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	spec := mediumSpec(t, p, 1)
	spec.Name = "main-medium"
	if err := p.EnsurePool(t.Context(), spec); !isCode(err, provider.CodeInvalid) {
		t.Fatalf("a pool outside the bn- names: %v", err)
	}
	spec = mediumSpec(t, p, 1)
	spec.Size.Name = "huge"
	if err := p.EnsurePool(t.Context(), spec); !isCode(err, provider.CodeInvalid) {
		t.Fatalf("a size not sold: %v", err)
	}
	spec = mediumSpec(t, p, -1)
	if err := p.EnsurePool(t.Context(), spec); !isCode(err, provider.CodeInvalid) {
		t.Fatalf("a negative target: %v", err)
	}
	eq(t, len(f.pools.pools), 0)
}

func TestKoyebClaimIsIdempotentByKey(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 2)), nil)
	one, err := p.Claim(ctx, poolName, "claim-1")
	eq(t, err, nil)
	again, err := p.Claim(ctx, poolName, "claim-1")
	eq(t, err, nil)
	eq(t, again.Ref.Meta[metaService], one.Ref.Meta[metaService])
	eq(t, again.Ref.ID, one.Ref.ID)
	two, err := p.Claim(ctx, poolName, "claim-2")
	eq(t, err, nil)
	if two.Ref.Meta[metaService] == one.Ref.Meta[metaService] {
		t.Fatal("two keys took one member")
	}
	// Each member has its own secret, and the ref names the executor and the routing key
	if one.Ref.Meta[metaSecret] == "" || one.Ref.Meta[metaSecret] == two.Ref.Meta[metaSecret] {
		t.Fatalf("secrets %q and %q", one.Ref.Meta[metaSecret], two.Ref.Meta[metaSecret])
	}
	for _, c := range []provider.Claim{one, two} {
		if c.Ref.Provider != "koyeb" || c.Ref.Meta[metaExecutor] == "" ||
			c.Ref.Meta[metaRouting] == "" || c.Ref.Meta[metaPooled] != "1" {
			t.Fatalf("ref %+v", c.Ref)
		}
		eq(t, c.Warm, true)
		if c.CreatedAt.IsZero() || time.Since(c.CreatedAt) > time.Minute {
			t.Fatalf("created at %v", c.CreatedAt)
		}
	}
	// A third claim finds the pool empty and Koyeb makes a member on demand
	three, err := p.Claim(ctx, poolName, "claim-3")
	eq(t, err, nil)
	eq(t, three.Warm, false)
	_, err = p.Claim(ctx, "bn-main-large", "claim-4")
	if !isCode(err, provider.CodeNotFound) {
		t.Fatalf("a claim on a pool that does not exist: %v", err)
	}
}

func TestKoyebClaimPollsUntilAService(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 1)), nil)
	f.mu.Lock()
	f.pools.claimPending = 3
	f.mu.Unlock()
	start := time.Now()
	c, err := p.Claim(ctx, poolName, "claim-1")
	eq(t, err, nil)
	if c.Ref.Meta[metaService] == "" {
		t.Fatal("the claim returned no member")
	}
	// Three reads showed no service and the fourth did, 100 ms apart
	times := f.callTimes("GET /v1/claims/{id}")
	eq(t, len(times), 4)
	for i := 1; i < len(times); i++ {
		if gap := times[i].Sub(times[i-1]); gap < 90*time.Millisecond {
			t.Errorf("claim read %d came %v after the one before, want 100 ms", i, gap)
		}
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the poll took far over its 100 ms steps")
	}
}

func TestKoyebClaimGivesUpAfterItsWait(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 1)), nil)
	f.mu.Lock()
	f.pools.claimPending = 1 << 20
	f.mu.Unlock()
	old := claimWait
	claimWait = 350 * time.Millisecond
	t.Cleanup(func() { claimWait = old })
	_, err := p.Claim(ctx, poolName, "claim-1")
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeCapacity || !e.CanFailOver {
		t.Fatalf("a claim no service answered: %v", err)
	}
}

func TestKoyebClaimRefusesAMemberWithoutASecret(t *testing.T) {
	f := newFake(t)
	f.pools.noSecret = true
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 1)), nil)
	_, err := p.Claim(ctx, poolName, "claim-1")
	if err == nil {
		t.Fatal("a member with no executor secret was handed out")
	}
}

func TestKoyebFirstCallAfterClaimIsHTTP1Health(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 1)), nil)
	c, err := p.Claim(ctx, poolName, "claim-1")
	eq(t, err, nil)
	eq(t, p.WaitReady(ctx, c.Ref), nil)
	f.mu.Lock()
	routes, protos := append([]string(nil), f.routes...), append([]string(nil), f.protos...)
	f.mu.Unlock()
	eq(t, routes[0], "GET /health")
	eq(t, protos[0], "HTTP/1.1")
	// The member answers a command under its own secret
	stream, err := p.Exec(ctx, c.Ref, provider.ExecSpec{Argv: []string{"echo", "claimed"}})
	eq(t, err, nil)
	out, err := io.ReadAll(stream)
	eq(t, err, nil)
	_, _ = stream.Wait()
	_ = stream.Close()
	eq(t, string(out), "claimed\n")
}

func TestKoyebListOwnedMarksPoolHeld(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ctx := t.Context()
	eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 2)), nil)
	claimed, err := p.Claim(ctx, poolName, "claim-1")
	eq(t, err, nil)
	// A sandbox the router made for a member has its own app and a member label
	size, _ := p.SizeFor(provider.Resources{})
	made, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: "pool-1", Size: size, Lifetime: time.Hour,
		Labels: map[string]string{
			"bugnet": "1", "bugnet_deployment": "main", "bugnet_pool": "medium",
			"pool_member": "00000000-0000-0000-0000-000000000001",
		},
	})
	eq(t, err, nil)
	held := func() map[string]bool {
		t.Helper()
		owned, err := p.ListOwned(ctx, map[string]string{"bugnet": "1"})
		eq(t, err, nil)
		out := map[string]bool{}
		for _, s := range owned {
			out[s.Ref.Meta[metaService]] = s.PoolHeld
		}
		return out
	}
	got := held()
	eq(t, len(got), 3)
	eq(t, got[claimed.Ref.Meta[metaService]], false)
	eq(t, got[made.Meta[metaService]], false)
	_, unclaimed := f.poolMembers(poolName)
	eq(t, len(unclaimed), 1)
	eq(t, got[unclaimed[0]], true)

	// Claims that cannot be listed leave every member the sweep could not judge held
	f.inject("GET /v1/service_pools/{id}/claims", fault{
		status: 503, code: "unavailable", message: "try again",
	})
	got = held()
	eq(t, got[claimed.Ref.Meta[metaService]], true)
	eq(t, got[unclaimed[0]], true)
	eq(t, got[made.Meta[metaService]], false)
}

func TestKoyebDestroyOfAClaimedMember(t *testing.T) {
	t.Run("its own app goes with it", func(t *testing.T) {
		f := newFake(t)
		p := f.provider(t, fakeToken)
		ctx := t.Context()
		eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 1)), nil)
		c, err := p.Claim(ctx, poolName, "claim-1")
		eq(t, err, nil)
		eq(t, p.Destroy(ctx, c.Ref), nil)
		apps, services := f.counts()
		eq(t, apps, 0)
		eq(t, services, 0)
		// Destroying it again finds it gone
		eq(t, p.Destroy(ctx, c.Ref), nil)
	})
	t.Run("a shared app stays for the other members", func(t *testing.T) {
		f := newFake(t)
		f.pools.sharedApp = true
		p := f.provider(t, fakeToken)
		ctx := t.Context()
		eq(t, p.EnsurePool(ctx, mediumSpec(t, p, 2)), nil)
		one, err := p.Claim(ctx, poolName, "claim-1")
		eq(t, err, nil)
		two, err := p.Claim(ctx, poolName, "claim-2")
		eq(t, err, nil)
		eq(t, one.Ref.ID, two.Ref.ID)
		eq(t, p.Destroy(ctx, one.Ref), nil)
		apps, services := f.counts()
		eq(t, apps, 1)
		eq(t, services, 1)
		eq(t, p.Destroy(ctx, two.Ref), nil)
		apps, services = f.counts()
		eq(t, apps, 0)
		eq(t, services, 0)
	})
}

func TestKoyebPoolIdleCost(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	size, _ := p.SizeFor(provider.Resources{})
	eq(t, p.IdleHourlyCostMicros(size), int64(0))
	q, err := New(Options{APIToken: fakeToken, BaseURL: f.api.URL, PoolIdleHourlyMicros: 120})
	eq(t, err, nil)
	eq(t, q.IdleHourlyCostMicros(size), int64(120))
}
