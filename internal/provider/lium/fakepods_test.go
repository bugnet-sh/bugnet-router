package lium

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

const fakeUserID = "6f1c2b3a-0d4e-4f5a-9b8c-7d6e5f4a3b2c"

// fakePod is one pod of the key, its fields as GET /pods/{id} writes them
type fakePod struct {
	id, name, status, node, template string
	// created is written as Lium's API may, with no zone
	created                   time.Time
	machine, tier, country    string
	ip                        string
	nodeGPUs, gpus            int
	price                     float64
	sshPort                   int
	removal                   string
	terminationHours          int
	nodeOffline, outage       bool
	billingPaused, diskPaused bool
	// unpriced writes price as null
	unpriced bool
	// keys are the pod's authorized_keys lines, the rent's user_public_key
	keys []string
}

// fakeEvent is one entry of a pod's event log
type fakeEvent struct {
	at                   time.Time
	eventType, reason    string
	fromStatus, toStatus string
}

// idemRecord is the first body and answer of one Idempotency-Key
type idemRecord struct {
	body   []byte
	status int
	answer []byte
}

func (p *fakePod) json() map[string]any {
	ports := map[string]any{}
	if p.sshPort > 0 {
		ports["22"] = p.sshPort
	}
	var removal, outage any
	var price any = p.price
	if p.removal != "" {
		removal = p.removal
	}
	if p.unpriced {
		price = nil
	}
	if p.outage {
		outage = map[string]any{
			"offline_since": "2026-11-11T11:50:00Z", "recoveries": 2,
			"median_recovery_minutes": 12, "last_recovery_minutes": 9,
		}
	}
	created := p.created.UTC().Format("2006-01-02T15:04:05.000000")
	return map[string]any{
		"id": p.id, "pod_name": p.name, "status": p.status, "executor_id": p.node,
		"ports_mapping": ports, "ssh_connect_cmd": fmt.Sprintf("ssh root@%s -p %d", p.ip,
			p.sshPort),
		"gpu_name": p.machine, "gpu_count": strconv.Itoa(p.gpus), "cpu_name": "AMD EPYC",
		"cpu_count": 16, "ram_total": 67108864, "is_favorite": false, "updated_at": created,
		"created_at": created, "removal_scheduled_at": removal,
		"termination_hours": p.terminationHours, "jupyter_installation_status": nil,
		"jupyter_url": nil, "enable_jupyter": false, "enable_volume_encryption": false,
		"volume_encryption_status": nil, "price": price, "pending_price": nil,
		"template": map[string]any{"id": p.template, "name": "bugnet-base"},
		"executor": map[string]any{
			"id": p.node, "machine_name": p.machine,
			"price_per_gpu":       p.price / float64(max(p.gpus, 1)),
			"executor_ip_address": p.ip, "validator_hotkey": "5Val", "gpu_count": p.nodeGPUs,
			"available_gpu_count": 0, "specs": map[string]any{}, "tier": p.tier,
			"location": map[string]any{
				"country": "Somewhere", "country_code": p.country, "timezone": "UTC",
			},
			"ncu_profiling_enabled": false, "disk_health_ok": true,
			"on_infiniband_fabric": false, "on_roce_fabric": false, "on_efa_fabric": false,
			"nvlink": nil,
		},
		"node_offline": p.nodeOffline, "billing_paused": p.billingPaused,
		"disk_paused": p.diskPaused, "node_outage": outage, "last_event": nil,
		"cluster_overlay_ip": nil, "spend_to_date": 0.01,
		// A field Lium may add, which the adapter must ignore
		"pod_future": map[string]any{"x": 1},
	}
}

// addPod puts a pod in the fake as if rented earlier, filling what the test leaves out
func (f *fakeLium) addPod(p fakePod) *fakePod {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	if p.id == "" {
		p.id = fmt.Sprintf("00000000-0000-4000-8000-%012d", f.nextID)
	}
	if p.created.IsZero() {
		p.created = time.Date(2026, 11, 11, 11, 59, 0, 0, time.UTC)
	}
	n := euNode("node-of-" + p.id)
	p.node, p.machine = cmp.Or(p.node, n.id), cmp.Or(p.machine, n.machine)
	p.tier, p.country = cmp.Or(p.tier, n.tier), cmp.Or(p.country, n.country)
	p.ip, p.status = cmp.Or(p.ip, f.podIP), cmp.Or(p.status, "RUNNING")
	if p.gpus == 0 {
		p.gpus, p.nodeGPUs = 1, 1
	}
	if p.price == 0 {
		p.price = n.price
	}
	if p.sshPort == 0 {
		p.sshPort = 40000 + f.nextID
	}
	f.pods[p.id] = &p
	return &p
}

// setPod changes a pod the fake keeps
func (f *fakeLium) setPod(id string, change func(*fakePod)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p, ok := f.pods[id]; ok {
		change(p)
	}
}

// pod returns a copy of a pod, false when the fake has none of that id
func (f *fakeLium) pod(id string) (fakePod, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.pods[id]
	if !ok {
		return fakePod{}, false
	}
	return *p, true
}

// podsNamed lists the pods of one name
func (f *fakeLium) podsNamed(name string) []fakePod {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakePod
	for _, p := range f.pods {
		if p.name == name {
			out = append(out, *p)
		}
	}
	return out
}

func (f *fakeLium) addEvent(id string, e fakeEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events[id] = append(f.events[id], e)
}

func (f *fakeLium) rentFault(node string, ft fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rentFaults[node] = &ft
}

func (f *fakeLium) addTemplate(t map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.templates = append(f.templates, t)
}

func (f *fakeLium) dropTemplates() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.templates = nil
}

// sortedPods lists the pods oldest first, ties by id
func (f *fakeLium) sortedPods() []*fakePod {
	out := make([]*fakePod, 0, len(f.pods))
	for _, p := range f.pods {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b *fakePod) int {
		if c := a.created.Compare(b.created); c != 0 {
			return c
		}
		return strings.Compare(a.id, b.id)
	})
	return out
}

// listPods answers every pod of the key
func (f *fakeLium) listPods(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	out := []map[string]any{}
	for _, p := range f.sortedPods() {
		out = append(out, p.json())
	}
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

func (f *fakeLium) getPod(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	p, ok := f.pods[r.PathValue("id")]
	var out map[string]any
	if ok {
		out = p.json()
	}
	f.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "pod_not_found", "No such pod on this account")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// podEvents answers a pod's events oldest first, also for a deleted pod
func (f *fakeLium) podEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	f.mu.Lock()
	out := []map[string]any{}
	for i, e := range f.events[id] {
		var reason any
		if e.reason != "" {
			reason = e.reason
		}
		out = append(out, map[string]any{
			"id": fmt.Sprintf("ev-%s-%d", id, i), "created_at": e.at.Format(time.RFC3339Nano),
			"event_type": e.eventType, "sub_event_type": nil, "pod_id": id, "pod_name": nil,
			"actor": nil, "from_status": e.fromStatus, "to_status": e.toStatus,
			"reason": reason, "detail": nil, "error": nil,
		})
	}
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// deletePod removes a pod at once, refusing another name than expected_name
func (f *fakeLium) deletePod(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	f.mu.Lock()
	p, ok := f.pods[id]
	want := r.URL.Query().Get("expected_name")
	switch {
	case !ok:
		f.mu.Unlock()
		writeError(w, http.StatusNotFound, "pod_not_found", "No such pod on this account")
		return
	case r.URL.Query().Has("expected_name") && want != p.name:
		f.mu.Unlock()
		writeError(w, http.StatusConflict, "conflict", "The pod has another name")
		return
	}
	delete(f.pods, id)
	sshd := f.sshds[id]
	f.mu.Unlock()
	if sshd != nil {
		sshd.close()
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (f *fakeLium) scheduleRemoval(w http.ResponseWriter, r *http.Request) {
	var body struct {
		At string `json:"removal_scheduled_at"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.At == "" {
		writeError(w, http.StatusUnprocessableEntity, "validation_error", "removal time needed")
		return
	}
	f.mu.Lock()
	p, ok := f.pods[r.PathValue("id")]
	if ok {
		p.removal = body.At
	}
	f.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "pod_not_found", "No such pod on this account")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

// rent keeps each Idempotency-Key's answer as Lium does and rents the node as a new pod
func (f *fakeLium) rent(w http.ResponseWriter, r *http.Request) {
	body := readBody(r)
	key := r.Header.Get("Idempotency-Key")
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec, ok := f.idem[key]; key != "" && ok {
		switch {
		case bytes.Equal(rec.body, body):
			w.Header().Set("Idempotent-Replayed", "true")
			writeRaw(w, rec.status, rec.answer)
			return
		case rec.status < 300:
			writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
				"Idempotency-Key reused with another body")
			return
		}
	}
	status, answer := f.rentLocked(r.PathValue("executor_uuid"), body)
	if key != "" {
		f.idem[key] = idemRecord{body: body, status: status, answer: answer}
	}
	writeRaw(w, status, answer)
}

// rentLocked rents a node as a new pod, a node the listing does not hold being a free one
func (f *fakeLium) rentLocked(node string, body []byte) (int, []byte) {
	refuse := func(status int, code string) (int, []byte) {
		raw, _ := json.Marshal(map[string]any{
			"success": false, "message": code, "status_code": status,
			"error": map[string]any{"code": code, "message": code},
		})
		return status, raw
	}
	if ft := f.rentFaults[node]; ft != nil {
		if ft.times > 0 {
			ft.times--
			if ft.times == 0 {
				delete(f.rentFaults, node)
			}
		}
		return refuse(ft.status, ft.code)
	}
	var req struct {
		Name     string   `json:"pod_name"`
		Template string   `json:"template_id"`
		Hours    int      `json:"termination_hours"`
		Keys     []string `json:"user_public_key"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Name == "" {
		return refuse(http.StatusUnprocessableEntity, "validation_error")
	}
	if req.Template != "" && !slices.ContainsFunc(f.templates, func(t map[string]any) bool {
		return t["id"] == req.Template
	}) {
		return refuse(http.StatusNotFound, "template_not_found")
	}
	n := euNode(node)
	if i := slices.IndexFunc(f.nodes, func(n fakeNode) bool { return n.id == node }); i >= 0 {
		if f.nodes[i].free < f.nodes[i].gpus {
			return refuse(http.StatusBadRequest, "node_unavailable")
		}
		f.nodes[i].free = 0
		n = f.nodes[i]
	}
	f.nextID++
	p := &fakePod{
		id: fmt.Sprintf("00000000-0000-4000-8000-%012d", f.nextID), name: req.Name,
		status: f.startStatus, node: node, template: req.Template,
		created: time.Now().UTC(), machine: n.machine, tier: n.tier, country: n.country,
		ip: f.podIP, nodeGPUs: n.gpus, gpus: n.gpus, price: n.price * float64(n.gpus),
		sshPort: 40000 + f.nextID, terminationHours: req.Hours, keys: req.Keys,
	}
	f.pods[p.id] = p
	f.events[p.id] = append(f.events[p.id], fakeEvent{at: p.created, eventType: "pod_created"})
	raw, _ := json.Marshal(map[string]any{"success": true, "pod_id": p.id})
	return http.StatusOK, raw
}

func (f *fakeLium) listTemplates(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	out := slices.Clone(f.templates)
	f.mu.Unlock()
	if out == nil {
		out = []map[string]any{}
	}
	writeJSON(w, http.StatusOK, out)
}

// makeTemplate saves the body as a private template of the fake's user
func (f *fakeLium) makeTemplate(w http.ResponseWriter, r *http.Request) {
	var t map[string]any
	if err := json.Unmarshal(readBody(r), &t); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "validation_error", "not JSON")
		return
	}
	f.mu.Lock()
	f.nextID++
	t["id"] = fmt.Sprintf("10000000-0000-4000-8000-%012d", f.nextID)
	t["user_id"] = f.userID
	t["status"] = "CREATED"
	f.templates = append(f.templates, t)
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, t)
}

func (f *fakeLium) checkTemplate(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	out := slices.Clone(f.checkIssues)
	f.mu.Unlock()
	if out == nil {
		out = []map[string]any{}
	}
	writeJSON(w, http.StatusOK, out)
}

func (f *fakeLium) me(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	out := map[string]any{
		"email": "dev@bugnet.org", "balance": f.balance, "balance_warning": f.warning,
	}
	if f.balance == nil {
		delete(out, "balance")
	}
	if f.userID != "" {
		out["id"] = f.userID
	}
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// setAccount sets what GET /users/me answers
func (f *fakeLium) setAccount(balance, warning any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.balance, f.warning = balance, warning
}

func readBody(r *http.Request) []byte {
	var b bytes.Buffer
	_, _ = b.ReadFrom(r.Body)
	return b.Bytes()
}

func writeRaw(w http.ResponseWriter, status int, raw []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}
