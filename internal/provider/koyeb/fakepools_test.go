package koyeb

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// fakePool is one service pool, its members services of its own apps unless the fake shares one
type fakePool struct {
	id, name   string
	size       int64
	status     string
	definition map[string]any
	// members are the service ids the pool made, claimed ones included
	members []string
	app     string
}

type fakeClaim struct {
	id, poolID, serviceID, requestID string
	prewarmed                        bool
	// pending is how many more reads still show the claim without its service
	pending int
}

// poolState is what the fake keeps for pools, behind fakeKoyeb's own lock
type poolState struct {
	pools  map[string]*fakePool
	claims map[string]*fakeClaim
	// claimPending is how many reads of a new claim show no service yet, and its reply names none
	claimPending int
	// sharedApp puts every member of a pool in one app
	sharedApp bool
	// noSecret makes members that have no SANDBOX_SECRET, as a pool that shared none would
	noSecret bool
	made     int
}

func (f *fakeKoyeb) poolRoutes() map[string]http.HandlerFunc {
	f.pools = &poolState{pools: map[string]*fakePool{}, claims: map[string]*fakeClaim{}}
	return map[string]http.HandlerFunc{
		"POST /v1/service_pools":            f.createPool,
		"PUT /v1/service_pools/{id}":        f.updatePool,
		"DELETE /v1/service_pools/{id}":     f.deletePool,
		"GET /v1/service_pools":             f.listPools,
		"POST /v1/claim":                    f.claim,
		"GET /v1/claims/{id}":               f.getClaim,
		"GET /v1/service_pools/{id}/claims": f.listClaims,
		"DELETE /v1/services/{id}":          f.deleteService,
	}
}

func (f *fakeKoyeb) poolJSON(p *fakePool) map[string]any {
	unclaimed := 0
	claimed := f.claimedSet()
	for _, m := range p.members {
		if !claimed[m] && f.services[m].status != "DELETED" {
			unclaimed++
		}
	}
	return withFuture(map[string]any{
		"id": p.id, "name": p.name, "size": strconv.FormatInt(p.size, 10),
		"ready_count": strconv.Itoa(unclaimed), "status": p.status, "definition": p.definition,
	})
}

func (f *fakeKoyeb) claimedSet() map[string]bool {
	out := map[string]bool{}
	for _, c := range f.pools.claims {
		out[c.serviceID] = true
	}
	return out
}

// checkPool refuses the definitions a Bugnet pool must never have
func checkPool(body map[string]any) string {
	def, _ := body["definition"].(map[string]any)
	env := definitionEnv(def)
	switch {
	case def == nil || body["name"] == nil:
		return "a pool needs a name and a definition"
	case env["SANDBOX_SECRET"] != "":
		return "a pool must not set SANDBOX_SECRET, every member would share it"
	case env["BUGNET_LABELS"] == "":
		return "a pool needs BUGNET_LABELS"
	case field(def, "type") != "SANDBOX" || field(def, "docker.image") != "koyeb/sandbox":
		return "pool members are koyeb/sandbox sandboxes"
	case !jsonEqual(field(def, "regions"), `["fra"]`):
		return "pool members run in fra"
	case field(def, "scalings.0.targets.0.sleep_idle_delay.light_sleep_value") == nil:
		return "pool members go to Light Sleep"
	}
	return ""
}

// addMember makes one unclaimed member of the pool, its secret its own
func (f *fakeKoyeb) addMember(p *fakePool) *fakeService {
	f.pools.made++
	n := f.pools.made
	var appID string
	if f.pools.sharedApp && p.app != "" {
		appID = p.app
	} else {
		a := &fakeApp{
			id: f.id("app"), name: "bn-poolapp-" + strconv.Itoa(n), created: time.Now().UTC(),
			status: "HEALTHY",
		}
		f.apps[a.id] = a
		appID = a.id
		if f.pools.sharedApp {
			p.app = a.id
		}
	}
	def := map[string]any{}
	raw, _ := json.Marshal(p.definition)
	_ = json.Unmarshal(raw, &def)
	env, _ := def["env"].([]any)
	if !f.pools.noSecret {
		env = append(env, map[string]any{
			"key": "SANDBOX_SECRET", "value": "member-secret-" + strconv.Itoa(n),
		})
	}
	def["env"] = env
	s := &fakeService{
		id: f.id("svc"), appID: appID, name: "bn-member-" + strconv.Itoa(n),
		created: time.Now().UTC(), status: "HEALTHY", body: map[string]any{"definition": def},
	}
	d := &fakeDeployment{
		id: f.id("dep"), serviceID: s.id, appID: appID, definition: def, status: "HEALTHY",
		routingKey: "rk-" + s.id,
	}
	s.deployment = d.id
	f.services[s.id], f.deployments[d.id] = s, d
	p.members = append(p.members, s.id)
	return s
}

// fill makes members until the pool holds size that no claim took
func (f *fakeKoyeb) fill(p *fakePool) {
	claimed := f.claimedSet()
	unclaimed := 0
	for _, m := range p.members {
		if !claimed[m] && f.services[m].status != "DELETED" {
			unclaimed++
		}
	}
	for ; int64(unclaimed) < p.size; unclaimed++ {
		f.addMember(p)
	}
}

func (f *fakeKoyeb) createPool(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "body is not a pool")
		return
	}
	if msg := checkPool(body); msg != "" {
		writeError(w, http.StatusBadRequest, "invalid_argument", msg)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	name, _ := body["name"].(string)
	for _, p := range f.pools.pools {
		if p.name == name && p.status != "DELETING" {
			writeError(w, http.StatusConflict, "invalid_argument", "pool name already exists")
			return
		}
	}
	size, _ := body["size"].(float64)
	p := &fakePool{
		id: f.id("pool"), name: name, size: int64(size), status: "READY",
		definition: body["definition"].(map[string]any),
	}
	f.pools.pools[p.id] = p
	f.fill(p)
	writeJSON(w, http.StatusOK, map[string]any{"service_pool": f.poolJSON(p)})
}

func (f *fakeKoyeb) updatePool(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "body is not a pool")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.pools.pools[r.PathValue("id")]
	if p == nil || p.status == "DELETING" {
		writeError(w, http.StatusNotFound, "unknown_resource", "Pool not found")
		return
	}
	size, _ := body["size"].(float64)
	p.size = int64(size)
	f.fill(p)
	writeJSON(w, http.StatusOK, map[string]any{"service_pool": f.poolJSON(p)})
}

func (f *fakeKoyeb) deletePool(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.pools.pools[r.PathValue("id")]
	if p == nil || p.status == "DELETING" {
		writeError(w, http.StatusNotFound, "unknown_resource", "Pool not found")
		return
	}
	p.status = "DELETING"
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (f *fakeKoyeb) listPools(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.mu.Lock()
	defer f.mu.Unlock()
	var match []*fakePool
	for _, p := range f.pools.pools {
		if strings.Contains(p.name, q.Get("name")) {
			match = append(match, p)
		}
	}
	slices.SortFunc(match, func(a, b *fakePool) int { return strings.Compare(a.id, b.id) })
	start, end, more := f.page(q, len(match))
	out := []map[string]any{}
	for _, p := range match[start:end] {
		out = append(out, f.poolJSON(p))
	}
	writeJSON(w, http.StatusOK, withFuture(map[string]any{
		"service_pools": out, "has_next": more, "count": strconv.Itoa(len(match)),
	}))
}

func (f *fakeKoyeb) claimJSON(c *fakeClaim, hide bool) map[string]any {
	service, status := c.serviceID, "FULFILLED"
	if hide {
		service, status = "", "PENDING"
	}
	return withFuture(map[string]any{
		"id": c.id, "pool_id": c.poolID, "service_id": service, "request_id": c.requestID,
		"status": status,
	})
}

func (f *fakeKoyeb) claim(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PoolID    string `json:"pool_id"`
		RequestID string `json:"request_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "body is not a claim")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.pools.pools[body.PoolID]
	if p == nil || p.status == "DELETING" {
		writeError(w, http.StatusNotFound, "unknown_resource", "Pool not found")
		return
	}
	for _, c := range f.pools.claims {
		if c.poolID == p.id && c.requestID == body.RequestID && body.RequestID != "" {
			writeJSON(w, http.StatusOK, map[string]any{
				"claim_id": c.id, "service_id": c.serviceID, "prewarmed": c.prewarmed,
			})
			return
		}
	}
	claimed := f.claimedSet()
	var member *fakeService
	prewarmed := false
	for _, id := range p.members {
		if !claimed[id] && f.services[id].status != "DELETED" {
			member, prewarmed = f.services[id], true
			break
		}
	}
	if member == nil {
		member = f.addMember(p)
	}
	c := &fakeClaim{
		id: f.id("claim"), poolID: p.id, serviceID: member.id, requestID: body.RequestID,
		prewarmed: prewarmed, pending: f.pools.claimPending,
	}
	f.pools.claims[c.id] = c
	service := c.serviceID
	if c.pending > 0 {
		service = ""
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"claim_id": c.id, "service_id": service, "prewarmed": prewarmed,
	})
}

func (f *fakeKoyeb) getClaim(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.pools.claims[r.PathValue("id")]
	if c == nil {
		writeError(w, http.StatusNotFound, "unknown_resource", "Claim not found")
		return
	}
	hide := c.pending > 0
	if hide {
		c.pending--
	}
	writeJSON(w, http.StatusOK, map[string]any{"claim": f.claimJSON(c, hide)})
}

func (f *fakeKoyeb) listClaims(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.mu.Lock()
	defer f.mu.Unlock()
	var match []*fakeClaim
	for _, c := range f.pools.claims {
		if c.poolID == r.PathValue("id") {
			match = append(match, c)
		}
	}
	slices.SortFunc(match, func(a, b *fakeClaim) int { return strings.Compare(a.id, b.id) })
	start, end, more := f.page(q, len(match))
	out := []map[string]any{}
	for _, c := range match[start:end] {
		out = append(out, f.claimJSON(c, false))
	}
	writeJSON(w, http.StatusOK, withFuture(map[string]any{
		"claims": out, "has_next": more, "count": strconv.Itoa(len(match)),
	}))
}

func (f *fakeKoyeb) deleteService(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.services[r.PathValue("id")]
	if s == nil || s.status == "DELETED" {
		writeError(w, http.StatusNotFound, "unknown_resource", "Service not found")
		return
	}
	s.status = "DELETED"
	if d := f.deployments[s.deployment]; d != nil {
		d.status = "STOPPED"
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

// poolMembers lists the live services of the pool made, claimed or not
func (f *fakeKoyeb) poolMembers(name string) (members, unclaimed []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	claimed := f.claimedSet()
	for _, p := range f.pools.pools {
		if p.name != name {
			continue
		}
		for _, id := range p.members {
			if f.services[id].status == "DELETED" {
				continue
			}
			members = append(members, id)
			if !claimed[id] {
				unclaimed = append(unclaimed, id)
			}
		}
	}
	return members, unclaimed
}

// poolByName returns a copy of the live pool with this name
func (f *fakeKoyeb) poolByName(name string) (fakePool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.pools.pools {
		if p.name == name && p.status != "DELETING" {
			return *p, true
		}
	}
	return fakePool{}, false
}
