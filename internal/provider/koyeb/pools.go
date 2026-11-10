package koyeb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// claimPoll is the gap between reads of a claim that no service answers yet, and claimWait bounds
// how long a claim waits for its service
var (
	claimPoll = 100 * time.Millisecond
	claimWait = 30 * time.Second
)

const (
	// poolLightSleepAfter is the idle time after which a pool member goes to Light Sleep
	poolLightSleepAfter = 60
	// metaPooled marks a ref of a pool member, whose app may hold other members
	metaPooled = "pooled"
)

var _ provider.PoolClaimer = (*Provider)(nil)

// poolDefinition is the service every member of a pool runs, the labels in BUGNET_LABELS and no
// SANDBOX_SECRET, since a secret set here would be every member's and Koyeb gives each its own
func (p *Provider) poolDefinition(spec provider.PoolSpec, instanceType string) (
	serviceDefinition, error,
) {
	labels := spec.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	encoded, err := json.Marshal(labels)
	if err != nil {
		return serviceDefinition{}, invalid(err)
	}
	var target scalingTarget
	target.SleepIdleDelay.LightSleepValue = poolLightSleepAfter
	def := serviceDefinition{
		Name: spec.Name,
		Type: "SANDBOX",
		Env:  []envVar{{Key: labelsEnv, Value: string(encoded)}},
		Ports: []portSpec{
			{Port: executorPort, Protocol: "http"}, {Port: appPort, Protocol: "http"},
		},
		Routes: []routeSpec{
			{Port: executorPort, Path: executorPath + "/"}, {Port: appPort, Path: "/"},
		},
		InstanceTypes: []typeSpec{{Type: instanceType}},
		Regions:       []string{p.region},
		// One instance at all times, which Light Sleep suspends while nothing asks for it
		Scalings: []scalingSpec{{Min: 1, Max: 1, Targets: []scalingTarget{target}}},
		Mesh:     "DEPLOYMENT_MESH_AUTO",
	}
	def.Docker.Image = p.image
	return def, nil
}

// EnsurePool creates the pool, or sets its size, and is safe to call again
func (p *Provider) EnsurePool(ctx context.Context, spec provider.PoolSpec) error {
	s, ok := p.size(spec.Size.Name)
	if !ok {
		return invalid(fmt.Errorf("koyeb has no size %q", spec.Size.Name))
	}
	if !strings.HasPrefix(spec.Name, namePrefix) {
		return invalid(fmt.Errorf("a koyeb pool is named bn-<deployment>-<size>, not %q",
			spec.Name))
	}
	if spec.Target < 0 {
		return invalid(fmt.Errorf("a pool target of %d", spec.Target))
	}
	def, err := p.poolDefinition(spec, s.typ.id)
	if err != nil {
		return err
	}
	found, err := p.findPool(ctx, spec.Name)
	if err != nil {
		return err
	}
	if found == nil {
		created, err := p.api.createPool(ctx, createPoolRequest{
			Name: spec.Name, Size: int64(spec.Target), Definition: def,
		})
		if err == nil {
			p.rememberPool(spec.Name, created.ID)
			return nil
		}
		// A second replica made it first, so the pool is read again and sized
		if found, err = p.findPool(ctx, spec.Name); err != nil || found == nil {
			return errors.Join(err, internal(fmt.Errorf("koyeb pool %s was not made", spec.Name)))
		}
	}
	p.rememberPool(spec.Name, found.ID)
	if int64(found.Size) == int64(spec.Target) {
		return nil
	}
	_, err = p.api.updatePool(ctx, found.ID, updatePoolRequest{
		Size: int64(spec.Target), Definition: def,
	})
	return err
}

// findPool returns the live pool of exactly this name, or nil
func (p *Provider) findPool(ctx context.Context, name string) (*poolRecord, error) {
	for offset := 0; ; {
		pg, err := p.api.listPools(ctx, name, offset)
		if err != nil {
			return nil, err
		}
		for _, pool := range pg.items {
			if pool.Name == name && pool.Status != "DELETING" {
				return &pool, nil
			}
		}
		offset += len(pg.items)
		if !pg.hasNext || len(pg.items) == 0 {
			return nil, nil
		}
	}
}

func (p *Provider) rememberPool(name, id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pools == nil {
		p.pools = map[string]string{}
	}
	p.pools[name] = id
}

// poolID is the id of a pool by name, asked of Koyeb once and then remembered
func (p *Provider) poolID(ctx context.Context, name string) (string, error) {
	p.mu.Lock()
	id := p.pools[name]
	p.mu.Unlock()
	if id != "" {
		return id, nil
	}
	found, err := p.findPool(ctx, name)
	if err != nil {
		return "", err
	}
	if found == nil {
		return "", notFound(fmt.Errorf("koyeb has no pool %s", name))
	}
	p.rememberPool(name, found.ID)
	return found.ID, nil
}

// Claim takes one member of the pool under key, and a repeated key answers the same member
//
// The claim is polled every 100 ms for up to 30 s until it names a service. The ref carries that
// service's own secret, executor and routing key, and a member that has no secret is refused, since
// an executor without one would trust anyone who reached it.
func (p *Provider) Claim(ctx context.Context, pool, key string) (provider.Claim, error) {
	id, err := p.poolID(ctx, pool)
	if err != nil {
		return provider.Claim{}, err
	}
	reply, err := p.api.claim(ctx, claimRequest{PoolID: id, RequestID: key})
	if err != nil {
		return provider.Claim{}, err
	}
	service, err := p.claimedService(ctx, reply)
	if err != nil {
		return provider.Claim{}, err
	}
	svc, err := p.api.getService(ctx, service)
	if err != nil {
		return provider.Claim{}, err
	}
	dep, err := p.deployment(ctx, svc.LatestDeploymentID)
	if err != nil {
		return provider.Claim{}, err
	}
	ref := refFor(svc.AppID, svc.ID, svc.CreatedAt, dep)
	if ref.Meta[metaSecret] == "" {
		return provider.Claim{}, internal(fmt.Errorf(
			"koyeb pool member %s has no executor secret, so it is not handed out", svc.ID))
	}
	ref.Meta[metaPooled] = "1"
	return provider.Claim{Ref: ref, Warm: reply.Prewarmed, CreatedAt: svc.CreatedAt.UTC()}, nil
}

// claimedService waits for the service a claim names
func (p *Provider) claimedService(ctx context.Context, reply claimReply) (string, error) {
	if reply.ServiceID != "" {
		return reply.ServiceID, nil
	}
	if reply.ClaimID == "" {
		return "", internal(errors.New("koyeb claim reply names no claim"))
	}
	deadline := time.NewTimer(claimWait)
	defer deadline.Stop()
	poll := time.NewTicker(claimPoll)
	defer poll.Stop()
	for {
		c, err := p.api.getClaim(ctx, reply.ClaimID)
		switch {
		case err != nil && !isCode(err, provider.CodeRateLimited):
			return "", err
		case err == nil && c.ServiceID != "":
			return c.ServiceID, nil
		case err == nil && c.Status == "FAILED":
			return "", &provider.Error{
				Code: provider.CodeCapacity, CanFailOver: true,
				Err: fmt.Errorf("koyeb claim %s failed", reply.ClaimID),
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return "", &provider.Error{
				Code: provider.CodeCapacity, CanFailOver: true,
				Err: fmt.Errorf("koyeb claim %s named no service in %v", reply.ClaimID, claimWait),
			}
		case <-poll.C:
		}
	}
}

// IdleHourlyCostMicros is what one unclaimed member of any size costs the account an hour, which
// Koyeb prices for Light Sleep once it does
func (p *Provider) IdleHourlyCostMicros(provider.Size) int64 { return p.poolIdle }

// claimed lists the ids of the services a claim of any pool took, false when it could not be read
func (p *Provider) claimed(ctx context.Context) (map[string]bool, bool) {
	taken := map[string]bool{}
	for offset := 0; ; {
		pools, err := p.api.listPools(ctx, "", offset)
		if err != nil {
			return nil, false
		}
		for _, pool := range pools.items {
			if !strings.HasPrefix(pool.Name, namePrefix) {
				continue
			}
			for at := 0; ; {
				claims, err := p.api.listClaims(ctx, pool.ID, at)
				if err != nil {
					return nil, false
				}
				for _, c := range claims.items {
					if c.ServiceID != "" {
						taken[c.ServiceID] = true
					}
				}
				at += len(claims.items)
				if !claims.hasNext || len(claims.items) == 0 {
					break
				}
			}
		}
		offset += len(pools.items)
		if !pools.hasNext || len(pools.items) == 0 {
			return taken, true
		}
	}
}

// markPoolHeld flags the pool members in a listing that no claim took, which the pool still holds
//
// A listing of claims that fails flags every member it cannot judge, since the sweep destroys any
// member nobody holds.
func (p *Provider) markPoolHeld(ctx context.Context, owned []provider.Summary) {
	var members []int
	for i, s := range owned {
		// A member the router made has its own app and a pool_member label, a kept one neither
		if s.Labels["bugnet_pool"] != "" && s.Labels["pool_member"] == "" {
			owned[i].Ref.Meta[metaPooled] = "1"
			members = append(members, i)
		}
	}
	if len(members) == 0 {
		return
	}
	taken, ok := p.claimed(ctx)
	if !ok {
		p.log.Warn("koyeb pool claims could not be listed, so its members read as held")
	}
	for _, i := range members {
		owned[i].PoolHeld = !ok || !taken[owned[i].Ref.Meta[metaService]]
	}
}

// destroyMember deletes a pool member's service, and its app once the app holds no other service
func (p *Provider) destroyMember(ctx context.Context, ref provider.Ref) error {
	app, err := appID(ref)
	if err != nil {
		return nil
	}
	if id := ref.Meta[metaService]; id != "" {
		if err := p.api.deleteService(ctx, id); err != nil && !isCode(err, provider.CodeNotFound) {
			return err
		}
	}
	left, err := p.findService(ctx, app, "")
	if err != nil {
		return err
	}
	if left != nil {
		return nil
	}
	err = p.api.deleteApp(ctx, app)
	if err == nil || isCode(err, provider.CodeNotFound) {
		return nil
	}
	return err
}

// DeletePool deletes the pool by name, which leaves the members already claimed alone, and treats
// one that is gone as deleted
func (p *Provider) DeletePool(ctx context.Context, name string) error {
	found, err := p.findPool(ctx, name)
	if err != nil || found == nil {
		return err
	}
	p.mu.Lock()
	delete(p.pools, name)
	p.mu.Unlock()
	err = p.api.deletePool(ctx, found.ID)
	if err == nil || isCode(err, provider.CodeNotFound) {
		return nil
	}
	return err
}
