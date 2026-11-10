package koyeb

import (
	"bufio"
	"bytes"
	"cmp"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const fakeToken = "koyeb-fake-token"

var fakeName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,21}[a-z0-9]$`)

// fault answers matching control plane calls with an error, after serving them when after is set
type fault struct {
	status  int
	code    string
	message string
	header  map[string]string
	drop    bool
	after   bool
	times   int
}

type fakeApp struct {
	id, name string
	created  time.Time
	status   string
}

type fakeService struct {
	id, appID, name string
	created         time.Time
	status          string
	deployment      string
	body            map[string]any
	// hiddenReads is how many more reads show the service without its deployment
	hiddenReads int
}

type fakeDeployment struct {
	id, serviceID, appID string
	definition           map[string]any
	status               string
	// startPolls is how many reads still see the deployment starting
	startPolls int
	routingKey string
	// bound is the port the executor's TCP proxy on 3031 forwards to, empty when none
	bound string
}

type logEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Stream    string    `json:"stream"`
	Data      string    `json:"data"`
}

// fakeProc is one start_process command, its log lines kept for every reader to replay
type fakeProc struct {
	mu      sync.Mutex
	entries []logEntry
	done    bool
	changed chan struct{}
	cmd     *exec.Cmd
	pipes   []io.Closer
}

func (p *fakeProc) add(stream, line string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entries = append(p.entries, logEntry{Timestamp: time.Now().UTC(), Stream: stream, Data: line})
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *fakeProc) finish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done = true
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *fakeProc) since(i int) ([]logEntry, bool, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.entries[i:]), p.done, p.changed
}

// noCut leaves a download whole under a fault that changes only how it is answered
const noCut = -1

// fileFault answers matching executor file calls with status and body, or reshapes a download
type fileFault struct {
	status int
	body   string
	header map[string]string
	// cut ends a download's body after so many bytes, its status and length already sent
	cut int64
	// noETag leaves out the ETag, as an executor without a strong validator would
	noETag bool
	// ignoreRange answers a ranged download whole with 200
	ignoreRange bool
	// shift moves the start of a ranged download this many bytes past the one asked for
	shift int64
	// encoding is the Content-Encoding a download claims
	encoding string
	// pad puts one entry of about so many bytes before a listing's own
	pad   int64
	times int
}

// fakeUpload is one upload session, its bytes in a temp file beside the target like Koyeb's
type fakeUpload struct {
	path, temp string
	size       int64
	mode       fs.FileMode
	modeSet    bool
	sha        string
	ranges     [][2]int64
}

type startCall struct {
	header http.Header
	cmd    string
	cwd    string
	env    map[string]string
}

// fakeKoyeb serves Koyeb's control plane from memory and each sandbox's executor from the host
type fakeKoyeb struct {
	t    *testing.T
	api  *httptest.Server
	exec *httptest.Server
	pool *x509.CertPool
	// shell and path run started commands under dash when the host has it, like the Ubuntu image
	shell, path string

	// conflictStatus is how a taken app name is refused, 409 when zero
	conflictStatus int
	// startPolls is how many reads see a new deployment still starting
	startPolls int
	// noMetadata leaves out the sandbox metadata so the executor is reached by the app domain
	noMetadata bool
	// goneStatus is what the edge answers for a deleted app, 404 when zero
	goneStatus int
	pageSize   int

	mu          sync.Mutex
	apps        map[string]*fakeApp
	services    map[string]*fakeService
	deployments map[string]*fakeDeployment
	procs       map[string]*fakeProc
	cmds        []*exec.Cmd
	nextID      int
	faults      map[string]*fault
	calls       map[string]int
	starts      []startCall
	routes      []string
	protos      []string
	uploads     []*http.Request
	kills       []string
	stamps      map[string][]time.Time
	healthDown  int
	// hideDeployment is how many reads of a new service show no deployment yet
	hideDeployment int
	// failLogs answers every log stream with a 503
	failLogs bool
	// logsDelay holds every log stream this long before it answers
	logsDelay time.Duration
	// killDelay holds every kill_process answer this long
	killDelay time.Duration
	// slowDeployment is a deployment whose reads wait until the caller gives up
	slowDeployment string
	script         *string
	cleanup        []string
	fileFaults     map[string][]*fileFault
	downloads      []http.Header
	sessions       map[string]*fakeUpload
	nextUpload     int
	// org is the organization fields the account route answers with, nil for an ACTIVE one
	org map[string]any
	// usage is the quota usage fields, quotaOrg the organization id the route was asked for
	usage map[string]any
	// usageDetails are the rows GET /v1/usages/details pages through
	usageDetails []map[string]any
	quotaOrg     string
	// refuseBinds answers every bind_port with a 500
	refuseBinds bool
	// instanceEvents is the newest instance event message of each deployment, by deployment id
	instanceEvents map[string]string
	// stealBind is a port another client binds right after each unbind_port
	stealBind string
	binds     []string
	// pools holds the service pools and their claims
	pools *poolState
}

func newFake(t *testing.T) *fakeKoyeb {
	t.Helper()
	f := &fakeKoyeb{
		t: t, apps: map[string]*fakeApp{}, services: map[string]*fakeService{},
		deployments: map[string]*fakeDeployment{}, procs: map[string]*fakeProc{},
		faults: map[string]*fault{}, calls: map[string]int{}, stamps: map[string][]time.Time{},
		fileFaults: map[string][]*fileFault{}, sessions: map[string]*fakeUpload{},
		instanceEvents: map[string]string{},
	}
	api := http.NewServeMux()
	for pattern, h := range map[string]http.HandlerFunc{
		"POST /v1/apps":                           f.createApp,
		"GET /v1/apps":                            f.listApps,
		"GET /v1/apps/{id}":                       f.getApp,
		"DELETE /v1/apps/{id}":                    f.deleteApp,
		"POST /v1/services":                       f.createService,
		"GET /v1/services":                        f.listServices,
		"GET /v1/services/{id}":                   f.getService,
		"GET /v1/deployments/{id}":                f.getDeployment,
		"GET /v1/instances":                       f.listInstances,
		"GET /v1/instance_events":                 f.listInstanceEvents,
		"GET /v1/account/organization":            f.organization,
		"GET /v1/usages/details":                  f.usageDetailPage,
		"GET /v1/quotas/organizations/{id}/usage": f.quotaUsage,
	} {
		api.HandleFunc(pattern, f.gate(pattern, h))
	}
	for pattern, h := range f.poolRoutes() {
		api.HandleFunc(pattern, f.gate(pattern, h))
	}
	api.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("adapter called the unexpected control plane route %s %s", r.Method, r.URL.Path)
		writeError(w, http.StatusNotImplemented, "api_error", "not in the fake")
	})
	f.api = httptest.NewServer(api)

	f.exec = httptest.NewUnstartedServer(http.HandlerFunc(f.edge))
	f.exec.EnableHTTP2 = true
	f.exec.StartTLS()
	f.pool = x509.NewCertPool()
	f.pool.AddCert(f.exec.Certificate())
	f.shell, f.path = "sh", os.Getenv("PATH")
	if _, err := os.Stat("/bin/dash"); err == nil {
		bin := t.TempDir()
		f.shell = filepath.Join(bin, "sh")
		if err := os.Symlink("/bin/dash", f.shell); err != nil {
			t.Fatal(err)
		}
		f.path = bin + string(os.PathListSeparator) + f.path
	}

	t.Cleanup(func() {
		f.api.Close()
		f.exec.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, cmd := range f.cmds {
			if cmd.Process != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
		}
		for _, p := range f.cleanup {
			_ = os.RemoveAll(p)
		}
		for _, proto := range f.protos {
			if proto != "HTTP/1.1" {
				t.Errorf("an executor request used %s, want HTTP/1.1 only", proto)
			}
		}
	})
	return f
}

// provider builds an adapter whose control plane is the fake and that trusts the fake's executor
func (f *fakeKoyeb) provider(t *testing.T, token string) *Provider {
	t.Helper()
	p, err := New(Options{APIToken: token, BaseURL: f.api.URL})
	if err != nil {
		t.Fatal(err)
	}
	f.trust(p)
	p.pollInterval = 10 * time.Millisecond
	p.exitDir = filepath.Join(t.TempDir(), "exec")
	return p
}

func (f *fakeKoyeb) trust(p *Provider) {
	p.exec.Transport.(*http.Transport).TLSClientConfig = &tls.Config{RootCAs: f.pool}
	p.streams.Transport.(*http.Transport).TLSClientConfig = &tls.Config{RootCAs: f.pool}
}

func (f *fakeKoyeb) inject(pattern string, ft fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[pattern] = &ft
}

func (f *fakeKoyeb) clearFaults() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.faults)
}

func (f *fakeKoyeb) take(pattern string) *fault {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[pattern]++
	f.stamps[pattern] = append(f.stamps[pattern], time.Now())
	ft := f.faults[pattern]
	if ft == nil {
		return nil
	}
	if ft.times > 0 {
		ft.times--
		if ft.times == 0 {
			delete(f.faults, pattern)
		}
	}
	return ft
}

func (f *fakeKoyeb) callCount(pattern string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[pattern]
}

func (f *fakeKoyeb) callTimes(pattern string) []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.stamps[pattern])
}

// gate checks the API token and applies any fault injected for the route
func (f *fakeKoyeb) gate(pattern string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+fakeToken {
			writeError(w, http.StatusUnauthorized, "authentication_error",
				"Request unauthenticated with bearer")
			return
		}
		ft := f.take(pattern)
		if ft == nil {
			h(w, r)
			return
		}
		if ft.after {
			h(httptest.NewRecorder(), r)
		}
		if ft.drop {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		for k, v := range ft.header {
			w.Header().Set(k, v)
		}
		writeError(w, ft.status, ft.code, ft.message)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"status": status, "code": code, "message": message})
}

// future holds fields and enum values Koyeb may add, which the adapter must ignore
var future = map[string]any{
	"organization_id": "org-1", "version": "42", "role": "ROLE_NOT_KNOWN_YET",
	"state": map[string]any{"desired": "RUNNING_SOMEWHERE_NEW", "auto_release": nil},
}

func withFuture(m map[string]any) map[string]any {
	for k, v := range future {
		m[k] = v
	}
	return m
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeKoyeb) id(kind string) string {
	f.nextID++
	return fmt.Sprintf("%s-%04d-4c1e-9d6b-%012d", kind, f.nextID, f.nextID)
}

/* Apps */

func (f *fakeKoyeb) appJSON(a *fakeApp) map[string]any {
	return withFuture(map[string]any{
		"id": a.id, "name": a.name, "created_at": a.created, "status": a.status,
		"domains": []map[string]any{{"name": f.domain(a.id), "type": "AUTOASSIGNED"}},
	})
}

// domain is where the fake's edge serves an app, a host with a path so one server serves all
func (f *fakeKoyeb) domain(appID string) string {
	return strings.TrimPrefix(f.exec.URL, "https://") + "/s/" + appID
}

// addApp places an app directly, as an earlier create that stopped before its service would
func (f *fakeKoyeb) addApp(name string) *fakeApp {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := &fakeApp{id: f.id("app"), name: name, created: time.Now().UTC(), status: "HEALTHY"}
	f.apps[a.id] = a
	return a
}

func (f *fakeKoyeb) liveApp(name string) *fakeApp {
	for _, a := range f.apps {
		if a.name == name && a.status != "DELETED" {
			return a
		}
	}
	return nil
}

func (f *fakeKoyeb) createApp(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string `json:"name"`
		LifeCycle struct {
			DeleteWhenEmpty bool `json:"delete_when_empty"`
		} `json:"life_cycle"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "body is not an app")
		return
	}
	name := req.Name
	if !fakeName.MatchString(name) {
		writeError(w, http.StatusBadRequest, "invalid_argument", "app name "+name+" is invalid")
		return
	}
	if !req.LifeCycle.DeleteWhenEmpty {
		writeError(w, http.StatusBadRequest, "invalid_argument", "fake apps must delete when empty")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.liveApp(name) != nil {
		status := cmp.Or(f.conflictStatus, http.StatusConflict)
		writeJSON(w, status, map[string]any{
			"status": status, "code": "invalid_argument", "message": "Validation error",
			"fields": []map[string]string{{"field": "name", "description": "already exists"}},
		})
		return
	}
	a := &fakeApp{id: f.id("app"), name: name, created: time.Now().UTC(), status: "STARTING"}
	f.apps[a.id] = a
	writeJSON(w, http.StatusOK, map[string]any{"app": f.appJSON(a)})
}

// listApps filters names by substring, so the adapter must pick the exact name itself
func (f *fakeKoyeb) listApps(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.mu.Lock()
	defer f.mu.Unlock()
	var match []*fakeApp
	for _, a := range f.apps {
		if strings.Contains(a.name, q.Get("name")) {
			match = append(match, a)
		}
	}
	slices.SortFunc(match, func(a, b *fakeApp) int { return strings.Compare(a.id, b.id) })
	start, end, more := f.page(q, len(match))
	apps := []map[string]any{}
	for _, a := range match[start:end] {
		apps = append(apps, f.appJSON(a))
	}
	writeJSON(w, http.StatusOK, withFuture(map[string]any{
		"apps": apps, "has_next": more, "count": strconv.Itoa(len(match)),
	}))
}

func (f *fakeKoyeb) page(q map[string][]string, n int) (int, int, bool) {
	limit := 20
	if l := first(q["limit"]); l != "" {
		limit, _ = strconv.Atoi(l)
	}
	if f.pageSize > 0 {
		limit = min(limit, f.pageSize)
	}
	start, _ := strconv.Atoi(first(q["offset"]))
	start = min(start, n)
	end := min(start+limit, n)
	return start, end, end < n
}

func first(vs []string) string {
	if len(vs) == 0 {
		return ""
	}
	return vs[0]
}

func (f *fakeKoyeb) getApp(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.apps[r.PathValue("id")]
	if a == nil {
		writeError(w, http.StatusNotFound, "unknown_resource", "App not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"app": f.appJSON(a)})
}

// deleteApp keeps the app and its services as DELETED records, as Koyeb's soft delete does
func (f *fakeKoyeb) deleteApp(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.apps[r.PathValue("id")]
	if a == nil || a.status == "DELETED" {
		writeError(w, http.StatusNotFound, "unknown_resource", "App not found")
		return
	}
	a.status = "DELETED"
	for _, s := range f.services {
		if s.appID == a.id {
			s.status = "DELETED"
			f.deployments[s.deployment].status = "STOPPED"
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

/* Services and deployments */

var fakeInstanceTypes = []string{
	"small", "medium", "large", "xlarge", "2xlarge", "3xlarge", "4xlarge", "5xlarge",
	"gpu-nvidia-rtx-4000-sff-ada", "eco-large",
}

func (f *fakeKoyeb) createService(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "body is not a service")
		return
	}
	if msg := checkSandbox(body); msg != "" {
		writeError(w, http.StatusBadRequest, "invalid_argument", msg)
		return
	}
	appID, name := field(body, "app_id").(string), field(body, "name").(string)
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.apps[appID]
	if a == nil || a.status == "DELETED" {
		writeError(w, http.StatusNotFound, "unknown_resource", "App not found")
		return
	}
	for _, s := range f.services {
		if s.appID == a.id && s.name == name && s.status != "DELETED" {
			writeError(w, http.StatusConflict, "invalid_argument", "service name already exists")
			return
		}
	}
	now := time.Now().UTC()
	s := &fakeService{
		id: f.id("svc"), appID: a.id, name: name, created: now, status: "STARTING", body: body,
		hiddenReads: f.hideDeployment,
	}
	d := &fakeDeployment{
		id: f.id("dep"), serviceID: s.id, appID: a.id,
		definition: field(body, "definition").(map[string]any), status: "PROVISIONING",
		startPolls: f.startPolls, routingKey: "rk-" + s.id,
	}
	s.deployment = d.id
	f.services[s.id] = s
	f.deployments[d.id] = d
	a.status = "HEALTHY"
	writeJSON(w, http.StatusOK, map[string]any{"service": f.serviceJSON(s)})
}

// checkSandbox refuses the definitions a Bugnet sandbox must never have
func checkSandbox(body map[string]any) string {
	def, _ := field(body, "definition").(map[string]any)
	env := definitionEnv(def)
	types, _ := field(def, "instance_types").([]any)
	scalings, _ := field(def, "scalings").([]any)
	life, _ := field(body, "life_cycle.delete_after_create").(float64)
	_, named := field(body, "name").(string)
	_, inApp := field(body, "app_id").(string)
	switch {
	case !named || !inApp || def == nil:
		return "a service needs a name, an app and a definition"
	case field(def, "type") != "SANDBOX":
		return "fake services are sandboxes"
	case field(def, "docker.image") != "koyeb/sandbox":
		return "fake sandboxes run koyeb/sandbox"
	case !jsonEqual(field(def, "regions"), `["fra"]`):
		return "fake sandboxes run in fra"
	case len(types) != 1 || !slices.Contains(fakeInstanceTypes, fmt.Sprint(field(def,
		"instance_types.0.type"))):
		return "unknown instance type"
	case len(scalings) != 1 || field(def, "scalings.0.min") != 1.0 ||
		field(def, "scalings.0.max") != 1.0 || field(def, "scalings.0.targets") != nil:
		return "fake sandboxes never scale to zero"
	case env["SANDBOX_SECRET"] == "":
		return "a sandbox needs SANDBOX_SECRET"
	case life < 60 || life > 86400:
		return "delete_after_create must be 60 s to 24 h"
	}
	return ""
}

// definitionEnv reads the variables of a deployment definition decoded from JSON
func definitionEnv(def map[string]any) map[string]string {
	env := map[string]string{}
	vars, _ := field(def, "env").([]any)
	for _, v := range vars {
		e, _ := v.(map[string]any)
		key, _ := e["key"].(string)
		value, _ := e["value"].(string)
		env[key] = value
	}
	return env
}

func (f *fakeKoyeb) serviceJSON(s *fakeService) map[string]any {
	latest := any(s.deployment)
	if s.deployment == "" {
		latest = nil
	}
	return withFuture(map[string]any{
		"id": s.id, "app_id": s.appID, "name": s.name, "created_at": s.created,
		"type": "SANDBOX", "status": s.status, "latest_deployment_id": latest,
		"active_deployment_id": latest,
		"life_cycle":           map[string]any{"delete_after_create": "86400"},
	})
}

// listServices filters like Koyeb and lists deleted services too, so the adapter must skip them
func (f *fakeKoyeb) listServices(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.mu.Lock()
	defer f.mu.Unlock()
	var match []*fakeService
	for _, s := range f.services {
		if (q.Get("app_id") == "" || q.Get("app_id") == s.appID) &&
			(len(q["types"]) == 0 || slices.Contains(q["types"], "SANDBOX")) &&
			strings.Contains(s.name, q.Get("name")) {
			match = append(match, s)
		}
	}
	slices.SortFunc(match, func(a, b *fakeService) int { return strings.Compare(a.id, b.id) })
	start, end, more := f.page(q, len(match))
	services := []map[string]any{}
	for _, s := range match[start:end] {
		services = append(services, f.serviceJSON(s))
	}
	writeJSON(w, http.StatusOK, withFuture(map[string]any{
		"services": services, "has_next": more, "count": strconv.Itoa(len(match)),
	}))
}

func (f *fakeKoyeb) getService(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.services[r.PathValue("id")]
	if s == nil {
		writeError(w, http.StatusNotFound, "unknown_resource", "Service not found")
		return
	}
	if s.hiddenReads > 0 {
		s.hiddenReads--
		hidden := *s
		hidden.deployment = ""
		s = &hidden
	}
	writeJSON(w, http.StatusOK, map[string]any{"service": f.serviceJSON(s)})
}

func (f *fakeKoyeb) getDeployment(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	slow := f.slowDeployment != "" && f.slowDeployment == r.PathValue("id")
	f.mu.Unlock()
	if slow {
		<-r.Context().Done()
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.deployments[r.PathValue("id")]
	if d == nil {
		writeError(w, http.StatusNotFound, "unknown_resource", "Deployment not found")
		return
	}
	if d.status == "PROVISIONING" {
		if d.startPolls > 0 {
			d.startPolls--
		} else {
			d.status = "HEALTHY"
		}
	}
	dep := withFuture(map[string]any{
		"id": d.id, "service_id": d.serviceID, "app_id": d.appID, "status": d.status,
		"definition": d.definition, "messages": []string{"deployment is " + d.status},
		"provisioning_info": map[string]any{"stages": []any{map[string]any{"name": "NEW"}}},
		"metadata":          nil,
	})
	if !f.noMetadata {
		dep["metadata"] = map[string]any{"sandbox": map[string]any{
			"public_url": "https://" + f.domain(d.appID), "routing_key": d.routingKey,
		}, "proxy_ports": []any{}}
	}
	writeJSON(w, http.StatusOK, map[string]any{"deployment": dep})
}

// listInstances lists one instance for a deployment that has an event, its id the deployment's
func (f *fakeKoyeb) listInstances(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dep := r.URL.Query().Get("deployment_id")
	instances := []map[string]any{}
	if _, ok := f.instanceEvents[dep]; ok {
		instances = append(instances, map[string]any{"id": "inst-" + dep, "status": "STOPPED"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"instances": instances, "count": "1"})
}

func (f *fakeKoyeb) listInstanceEvents(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dep := strings.TrimPrefix(r.URL.Query().Get("instance_id"), "inst-")
	events := []map[string]any{}
	if msg, ok := f.instanceEvents[dep]; ok {
		events = append(events, map[string]any{"type": "INSTANCE_EVENT", "message": msg})
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events, "count": "1"})
}

// setDeployment changes the deployment of the app's live service
func (f *fakeKoyeb) setDeployment(appID string, change func(*fakeDeployment)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.services {
		if s.appID == appID && s.status != "DELETED" {
			change(f.deployments[s.deployment])
		}
	}
}

// sandbox returns the live service of an app and its deployment
func (f *fakeKoyeb) sandbox(appID string) (*fakeService, *fakeDeployment) {
	for _, s := range f.services {
		if s.appID == appID && s.status != "DELETED" {
			return s, f.deployments[s.deployment]
		}
	}
	return nil, nil
}

// box returns copies of the app's live service and deployment
func (f *fakeKoyeb) box(appID string) (fakeService, fakeDeployment, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, d := f.sandbox(appID)
	if s == nil {
		return fakeService{}, fakeDeployment{}, false
	}
	return *s, *d, true
}

func (f *fakeKoyeb) setService(appID string, change func(*fakeService)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, _ := f.sandbox(appID); s != nil {
		change(s)
	}
}

func (f *fakeKoyeb) setHealthDown(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healthDown = n
}

// addForeign places a sandbox another tool made, named without the bn- prefix
func (f *fakeKoyeb) addForeign(name, labels string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now().UTC()
	a := &fakeApp{id: f.id("app"), name: name, created: now, status: "HEALTHY"}
	s := &fakeService{id: f.id("svc"), appID: a.id, name: name, created: now, status: "HEALTHY"}
	d := &fakeDeployment{
		id: f.id("dep"), serviceID: s.id, appID: a.id, status: "HEALTHY", routingKey: "rk",
		definition: map[string]any{"env": []any{
			map[string]any{"key": "SANDBOX_SECRET", "value": "s"},
			map[string]any{"key": "BUGNET_LABELS", "value": labels},
		}},
	}
	s.deployment = d.id
	f.apps[a.id], f.services[s.id], f.deployments[d.id] = a, s, d
}

func (f *fakeKoyeb) counts() (apps, services int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.apps {
		if a.status != "DELETED" {
			apps++
		}
	}
	for _, s := range f.services {
		if s.status != "DELETED" {
			services++
		}
	}
	return apps, services
}

func (f *fakeKoyeb) serviceBodies() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []map[string]any
	for _, s := range f.services {
		out = append(out, s.body)
	}
	return out
}

func fakeEnv(d *fakeDeployment) map[string]string { return definitionEnv(d.definition) }

/* Executor */

// edge routes /s/<app>/koyeb-sandbox/ to the app's executor like Koyeb's load balancer
func (f *fakeKoyeb) edge(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, "/s/")
	appID, route, _ := strings.Cut(rest, "/")
	route, under := strings.CutPrefix("/"+route, executorPath)
	f.mu.Lock()
	var d *fakeDeployment
	if a := f.apps[appID]; ok && under && a != nil && a.status != "DELETED" {
		_, d = f.sandbox(appID)
	}
	down := d != nil && route == "/health" && f.healthDown > 0
	if down {
		f.healthDown--
	}
	goneStatus := cmp.Or(f.goneStatus, http.StatusNotFound)
	var secret, routingKey string
	if d != nil {
		f.protos = append(f.protos, r.Proto)
		f.routes = append(f.routes, r.Method+" "+route)
		secret, routingKey = fakeEnv(d)["SANDBOX_SECRET"], d.routingKey
	}
	f.mu.Unlock()
	switch {
	case d == nil:
		http.Error(w, http.StatusText(goneStatus), goneStatus)
		return
	case !f.noMetadata && r.Header.Get("X-Routing-Key") != routingKey:
		http.Error(w, "no instance for this routing key", http.StatusNotFound)
		return
	case down:
		http.Error(w, "upstream connect error", http.StatusServiceUnavailable)
		return
	case route != "/health" && r.Header.Get("Authorization") != "Bearer "+secret:
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	r.URL.Path = route
	switch r.Method + " " + route {
	case "GET /health":
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	case "POST /start_process":
		f.startProcess(w, r)
	case "GET /process_logs_streaming":
		f.streamLogs(w, r)
	case "POST /kill_process":
		f.killProcess(w, r)
	case "GET /files", "HEAD /files":
		f.download(w, r)
	case "PUT /files":
		f.upload(w, r)
	case "GET /files/list":
		f.list(w, r)
	case "GET /files/stat":
		f.stat(w, r)
	case "POST /make_dir":
		f.makeDir(w, r)
	case "POST /files/uploads":
		f.openUpload(w, r)
	case "POST /bind_port":
		f.bindPort(w, r, appID)
	case "POST /unbind_port":
		f.unbindPort(w, appID)
	default:
		f.session(w, r, route)
	}
}

// session serves the routes of one upload session under /files/uploads/{id}
func (f *fakeKoyeb) session(w http.ResponseWriter, r *http.Request, route string) {
	rest, ok := strings.CutPrefix(route, "/files/uploads/")
	id, complete := strings.CutSuffix(rest, "/complete")
	switch {
	case ok && complete && r.Method == http.MethodPost:
		f.completeUpload(w, id)
	case ok && !strings.Contains(rest, "/") && r.Method == http.MethodPut:
		f.uploadPart(w, r, id)
	case ok && !strings.Contains(rest, "/") && r.Method == http.MethodDelete:
		f.dropUpload(w, id)
	default:
		f.t.Errorf("adapter called the executor route %s %s", r.Method, route)
		http.Error(w, "not in the fake", http.StatusNotFound)
	}
}

// bindPort sets the one port the TCP proxy forwards to, as the executor's handler does
func (f *fakeKoyeb) bindPort(w http.ResponseWriter, r *http.Request, appID string) {
	raw, _ := io.ReadAll(r.Body)
	var body struct {
		Port string `json:"port"`
	}
	bad := json.Unmarshal(raw, &body) != nil
	n, err := strconv.Atoi(body.Port)
	bad = bad || err != nil || n < 1 || n > 65535
	f.mu.Lock()
	defer f.mu.Unlock()
	f.binds = append(f.binds, string(bytes.TrimSpace(raw)))
	_, d := f.sandbox(appID)
	switch {
	case f.refuseBinds:
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "bind failed"})
	case bad:
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid port"})
	case d.bound != "":
		writeJSON(w, http.StatusConflict, map[string]any{"error": "Port already bound"})
	default:
		d.bound = body.Port
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "port": body.Port})
	}
}

// unbindPort clears the bound port
func (f *fakeKoyeb) unbindPort(w http.ResponseWriter, appID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, d := f.sandbox(appID)
	d.bound = f.stealBind
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// bind sets the bound port directly, as an earlier bind would have
func (f *fakeKoyeb) bind(appID, port string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, d := f.sandbox(appID)
	d.bound = port
}

func (f *fakeKoyeb) boundPort(appID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, d := f.sandbox(appID)
	return d.bound
}

// bindBodies lists the body of every bind_port call, oldest first
func (f *fakeKoyeb) bindBodies() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.binds)
}

// totalCalls counts every control plane and executor call the fake served
func (f *fakeKoyeb) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.routes)
	for _, c := range f.calls {
		n += c
	}
	return n
}

func (f *fakeKoyeb) routesCalled() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.routes)
}

func (f *fakeKoyeb) lastStart() startCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.starts) == 0 {
		return startCall{}
	}
	return f.starts[len(f.starts)-1]
}

// replyLogs answers every later log stream with raw and runs no started command
func (f *fakeKoyeb) replyLogs(raw string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.script = &raw
}

func (f *fakeKoyeb) startProcess(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Cmd string            `json:"cmd"`
		Cwd string            `json:"cwd"`
		Env map[string]string `json:"env"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Cmd == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if info, err := os.Stat(req.Cwd); req.Cwd != "" && (err != nil || !info.IsDir()) {
		http.Error(w, "Invalid working directory: "+req.Cwd, http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.starts = append(f.starts, startCall{
		header: r.Header.Clone(), cmd: req.Cmd, cwd: req.Cwd, env: maps.Clone(req.Env),
	})
	id := f.id("proc")
	proc := &fakeProc{changed: make(chan struct{})}
	f.procs[id] = proc
	scripted := f.script != nil
	f.mu.Unlock()
	if scripted {
		writeJSON(w, http.StatusCreated, map[string]any{"id": id, "pid": 1, "status": "running"})
		return
	}

	cmd := exec.Command(f.shell, "-c", req.Cmd)
	cmd.Dir = req.Cwd
	cmd.Env = append(os.Environ(), "PATH="+f.path)
	for k, v := range req.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err1 := cmd.StdoutPipe()
	stderr, err2 := cmd.StderrPipe()
	if err := errors.Join(err1, err2); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := cmd.Start(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to start command: " + err.Error(),
		})
		return
	}
	f.mu.Lock()
	f.cmds = append(f.cmds, cmd)
	f.mu.Unlock()
	proc.mu.Lock()
	proc.cmd, proc.pipes = cmd, []io.Closer{stdout, stderr}
	proc.mu.Unlock()
	go func() {
		var pipes sync.WaitGroup
		for stream, pipe := range map[string]io.Reader{"stdout": stdout, "stderr": stderr} {
			pipes.Go(func() {
				sc := bufio.NewScanner(pipe)
				sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
				for sc.Scan() {
					proc.add(stream, sc.Text())
				}
			})
		}
		pipes.Wait()
		_ = cmd.Wait()
		proc.finish()
	}()
	writeJSON(w, http.StatusCreated, map[string]any{
		"id": id, "pid": cmd.Process.Pid, "status": "running",
	})
}

// killProcess kills the started shell alone like the executor, whose Wait then closes its pipes
func (f *fakeKoyeb) killProcess(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.kills = append(f.kills, req.ID)
	proc, delay := f.procs[req.ID], f.killDelay
	f.mu.Unlock()
	time.Sleep(delay)
	if proc == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "error": "process not found: " + req.ID,
		})
		return
	}
	proc.mu.Lock()
	cmd, pipes, done := proc.cmd, proc.pipes, proc.done
	proc.mu.Unlock()
	if cmd == nil || done {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false, "error": "process is not running (status: completed)",
		})
		return
	}
	_ = cmd.Process.Kill()
	for _, p := range pipes {
		_ = p.Close()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true, "message": "Process killed successfully",
	})
}

func (f *fakeKoyeb) killed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.kills)
}

// streamLogs replays the command's lines, then follows it live until it ends
func (f *fakeKoyeb) streamLogs(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	f.mu.Lock()
	proc, script, fail, delay := f.procs[id], f.script, f.failLogs, f.logsDelay
	f.mu.Unlock()
	time.Sleep(delay)
	if fail {
		http.Error(w, "log streaming is down", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	flush := http.NewResponseController(w).Flush
	if script != nil {
		_, _ = io.WriteString(w, *script)
		return
	}
	if proc == nil {
		_, _ = fmt.Fprintf(w, "event: error\ndata: {\"error\": \"process not found: %s\"}\n\n", id)
		return
	}
	_, _ = io.WriteString(w, ":keepalive\n\n")
	_ = flush()
	sent := 0
	for {
		entries, done, changed := proc.since(sent)
		for _, e := range entries {
			data, _ := json.Marshal(e)
			_, _ = fmt.Fprintf(w, "event: log\ndata: %s\n\n", data)
		}
		sent += len(entries)
		_ = flush()
		// Every line is in before done is set, so the lines sent with it were the last ones
		if done {
			_, _ = io.WriteString(w, "event: complete\ndata: {\"message\": \"stream ended\"}\n\n")
			return
		}
		select {
		case <-changed:
		case <-r.Context().Done():
			return
		}
	}
}

/* Executor files */

func writeFileError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, fs.ErrNotExist):
		status = http.StatusNotFound
	case errors.Is(err, fs.ErrPermission):
		status = http.StatusForbidden
	case errors.Is(err, syscall.EISDIR), errors.Is(err, syscall.ENOTDIR):
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// failFiles queues a fault for route's next times calls, every later call when times is 0
func (f *fakeKoyeb) failFiles(route string, ft fileFault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fileFaults[route] = append(f.fileFaults[route], &ft)
}

// fileFault takes the first queued fault for route, if there is one
func (f *fakeKoyeb) fileFault(route string) *fileFault {
	f.mu.Lock()
	defer f.mu.Unlock()
	queue := f.fileFaults[route]
	if len(queue) == 0 {
		return nil
	}
	ft := queue[0]
	if ft.times > 0 {
		if ft.times--; ft.times == 0 {
			f.fileFaults[route] = queue[1:]
		}
	}
	copied := *ft
	return &copied
}

// failed writes a fault's answer and reports whether there was one to write
func failed(w http.ResponseWriter, ft *fileFault) bool {
	if ft == nil || ft.status == 0 {
		return false
	}
	for k, v := range ft.header {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(ft.status)
	_, _ = io.WriteString(w, ft.body)
	return true
}

// cutWriter aborts the response once its body passed left bytes, as a cut connection would
type cutWriter struct {
	http.ResponseWriter
	left int64
}

func (c *cutWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > c.left {
		n, _ := c.ResponseWriter.Write(b[:c.left])
		_ = http.NewResponseController(c.ResponseWriter).Flush()
		c.left -= int64(n)
		panic(http.ErrAbortHandler)
	}
	c.left -= int64(len(b))
	return c.ResponseWriter.Write(b)
}

// downloadHeaders lists the request headers of every download, oldest first
func (f *fakeKoyeb) downloadHeaders() []http.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.downloads)
}

func (f *fakeKoyeb) download(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.downloads = append(f.downloads, r.Header.Clone())
	f.mu.Unlock()
	ft := f.fileFault("GET /files")
	if failed(w, ft) {
		return
	}
	name := r.URL.Query().Get("path")
	info, err := os.Stat(name)
	if err != nil {
		writeFileError(w, err)
		return
	}
	if !info.Mode().IsRegular() {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": name + " is not a regular file (it is a dir)",
		})
		return
	}
	file, err := os.Open(name)
	if err != nil {
		writeFileError(w, err)
		return
	}
	defer func() { _ = file.Close() }()
	w.Header().Set("Content-Type", "application/octet-stream")
	if info.Size() == 0 {
		return
	}
	if ft == nil || !ft.noETag {
		w.Header().Set("ETag", fmt.Sprintf(`"%x-%x"`, info.Size(), info.ModTime().UnixNano()))
	}
	if ft != nil {
		reshape(w, r, ft)
	}
	if ft != nil && ft.cut >= 0 {
		w = &cutWriter{ResponseWriter: w, left: ft.cut}
	}
	http.ServeContent(w, r, "", info.ModTime(), file)
}

// reshape changes a download's request and headers the way ft asks
func reshape(w http.ResponseWriter, r *http.Request, ft *fileFault) {
	if ft.ignoreRange {
		r.Header.Del("Range")
	}
	if ft.encoding != "" {
		w.Header().Set("Content-Encoding", ft.encoding)
	}
	from, to, ok := strings.Cut(strings.TrimPrefix(r.Header.Get("Range"), "bytes="), "-")
	if start, err := strconv.ParseInt(from, 10, 64); ft.shift != 0 && ok && err == nil {
		r.Header.Set("Range", "bytes="+strconv.FormatInt(start+ft.shift, 10)+"-"+to)
	}
}

// upload writes like the executor, atomically with the given mode and parents when asked
func (f *fakeKoyeb) upload(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("path")
	mode, err := strconv.ParseUint(q.Get("mode"), 8, 32)
	if name == "" || err != nil || mode > 0o777 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path and mode required"})
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	f.mu.Lock()
	f.uploads = append(f.uploads, r.Clone(r.Context()))
	f.mu.Unlock()
	if failed(w, f.fileFault("PUT /files")) {
		return
	}
	sum := sha256.Sum256(data)
	if want := r.Header.Get("X-Content-SHA256"); want != "" && want != hex.EncodeToString(sum[:]) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sha256 mismatch"})
		return
	}
	if info, err := os.Stat(name); err == nil && info.IsDir() {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "destination is a directory"})
		return
	}
	f.track(name)
	if q.Get("parents") == "1" {
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			writeFileError(w, err)
			return
		}
	}
	tmp := filepath.Join(filepath.Dir(name), "."+filepath.Base(name)+".koyeb-tmp-1")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		writeFileError(w, err)
		return
	}
	if err := os.Chmod(tmp, fs.FileMode(mode)); err != nil {
		writeFileError(w, err)
		return
	}
	if err := os.Rename(tmp, name); err != nil {
		_ = os.Remove(tmp)
		writeFileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"size": len(data), "sha256": hex.EncodeToString(sum[:]),
	})
}

func (f *fakeKoyeb) lastUpload() *http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.uploads) == 0 {
		return nil
	}
	return f.uploads[len(f.uploads)-1]
}

// list answers NDJSON entries relative to the directory, one level unless recursive is 1
func (f *fakeKoyeb) list(w http.ResponseWriter, r *http.Request) {
	ft := f.fileFault("GET /files/list")
	if failed(w, ft) {
		return
	}
	q := r.URL.Query()
	root := filepath.Clean(q.Get("path"))
	info, err := os.Stat(root)
	if err != nil {
		writeFileError(w, err)
		return
	}
	if !info.IsDir() {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": root + " is not a directory",
		})
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	if ft != nil && ft.pad > 0 {
		_ = enc.Encode(map[string]any{"path": strings.Repeat("p", int(ft.pad)), "type": "file"})
	}
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == root {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		info, err := d.Info()
		if err != nil {
			return enc.Encode(map[string]any{"path": rel, "size": 0, "error": err.Error()})
		}
		_ = enc.Encode(map[string]any{
			"path": rel, "type": fakeType(info.Mode()), "size": info.Size(),
			"mode":  fmt.Sprintf("%04o", info.Mode().Perm()),
			"mtime": info.ModTime().UTC().Format(time.RFC3339Nano),
		})
		if d.IsDir() && q.Get("recursive") != "1" {
			return filepath.SkipDir
		}
		return nil
	})
}

// fakeType names a mode the way the executor's entries do
func fakeType(mode fs.FileMode) string {
	switch {
	case mode.IsRegular():
		return "file"
	case mode.IsDir():
		return "dir"
	case mode&fs.ModeSymlink != 0:
		return "symlink"
	}
	return "other"
}

// stat follows a final symlink like the executor's GET /files/stat
func (f *fakeKoyeb) stat(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("path")
	info, err := os.Stat(name)
	if err != nil {
		writeFileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"type": fakeType(info.Mode()), "size": info.Size(),
		"mode":  fmt.Sprintf("%04o", info.Mode().Perm()),
		"mtime": info.ModTime().UTC().Format(time.RFC3339Nano),
	})
}

// makeDir answers like the executor's legacy route, 200 with success false on a failure
func (f *fakeKoyeb) makeDir(w http.ResponseWriter, r *http.Request) {
	if failed(w, f.fileFault("POST /make_dir")) {
		return
	}
	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	f.track(req.Path)
	if err := os.MkdirAll(req.Path, 0o755); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

const fakeMaxPart = 100 << 20

func (f *fakeKoyeb) openUpload(w http.ResponseWriter, r *http.Request) {
	if failed(w, f.fileFault("POST /files/uploads")) {
		return
	}
	var req struct {
		Path   string  `json:"path"`
		Size   *int64  `json:"size"`
		Mode   *string `json:"mode"`
		SHA256 string  `json:"sha256"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Path == "" ||
		req.Size == nil || *req.Size < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path and size required"})
		return
	}
	u := &fakeUpload{path: filepath.Clean(req.Path), size: *req.Size, sha: req.SHA256}
	if req.Mode != nil {
		mode, err := strconv.ParseUint(*req.Mode, 8, 32)
		if err != nil || mode > 0o777 {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "mode must be octal permission bits, e.g. 0644",
			})
			return
		}
		u.mode, u.modeSet = fs.FileMode(mode), true
	}
	f.mu.Lock()
	f.nextUpload++
	n := f.nextUpload
	f.mu.Unlock()
	id := fmt.Sprintf("up-%04d", n)
	u.temp = filepath.Join(filepath.Dir(u.path),
		"."+filepath.Base(u.path)+".koyeb-tmp-"+strconv.Itoa(n))
	file, err := os.OpenFile(u.temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		err = errors.Join(file.Truncate(u.size), file.Close())
	}
	if err != nil {
		writeFileError(w, err)
		return
	}
	f.mu.Lock()
	f.sessions[id] = u
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"upload_id": id, "max_part_size": fakeMaxPart})
}

func (f *fakeKoyeb) uploadSession(id string) *fakeUpload {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sessions[id]
}

func (f *fakeKoyeb) uploadPart(w http.ResponseWriter, r *http.Request, id string) {
	u := f.uploadSession(id)
	if u == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown upload session"})
		return
	}
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || offset < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "offset must be a non-negative integer",
		})
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	if failed(w, f.fileFault("PUT /files/uploads/{id}")) {
		return
	}
	switch sum := sha256.Sum256(data); {
	case len(data) > fakeMaxPart:
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{
			"error": "part is larger than max_part_size",
		})
		return
	case offset+int64(len(data)) > u.size:
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "part writes past the end of the upload",
		})
		return
	case r.Header.Get("X-Content-SHA256") != "" &&
		r.Header.Get("X-Content-SHA256") != hex.EncodeToString(sum[:]):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sha256 mismatch"})
		return
	}
	file, err := os.OpenFile(u.temp, os.O_WRONLY, 0)
	if err == nil {
		_, err = file.WriteAt(data, offset)
		err = errors.Join(err, file.Close())
	}
	if err != nil {
		writeFileError(w, err)
		return
	}
	if len(data) > 0 {
		f.mu.Lock()
		u.ranges = append(u.ranges, [2]int64{offset, offset + int64(len(data))})
		f.mu.Unlock()
	}
	w.WriteHeader(http.StatusNoContent)
}

// covered reports whether ranges cover [0, size) without a gap
func covered(ranges [][2]int64, size int64) bool {
	sorted := slices.Clone(ranges)
	slices.SortFunc(sorted, func(a, b [2]int64) int { return cmp.Compare(a[0], b[0]) })
	end := int64(0)
	for _, r := range sorted {
		if r[0] > end {
			return false
		}
		end = max(end, r[1])
	}
	return end >= size
}

func (f *fakeKoyeb) completeUpload(w http.ResponseWriter, id string) {
	if failed(w, f.fileFault("POST /files/uploads/{id}/complete")) {
		return
	}
	u := f.uploadSession(id)
	if u == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown upload session"})
		return
	}
	f.mu.Lock()
	whole := covered(u.ranges, u.size)
	if whole {
		delete(f.sessions, id)
	}
	f.mu.Unlock()
	if !whole {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "ranges of the upload are still missing",
		})
		return
	}
	data, err := os.ReadFile(u.temp)
	if err != nil {
		writeFileError(w, err)
		return
	}
	sum := sha256.Sum256(data)
	if u.sha != "" && u.sha != hex.EncodeToString(sum[:]) {
		_ = os.Remove(u.temp)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sha256 mismatch"})
		return
	}
	mode := fs.FileMode(0o644)
	if info, err := os.Stat(u.path); err == nil {
		mode = info.Mode().Perm()
	}
	if u.modeSet {
		mode = u.mode
	}
	f.track(u.path)
	if err := errors.Join(os.Chmod(u.temp, mode), os.Rename(u.temp, u.path)); err != nil {
		_ = os.Remove(u.temp)
		writeFileError(w, err)
		return
	}
	reply := map[string]any{"size": u.size}
	if u.sha != "" {
		reply["sha256"] = u.sha
	}
	writeJSON(w, http.StatusOK, reply)
}

func (f *fakeKoyeb) dropUpload(w http.ResponseWriter, id string) {
	f.mu.Lock()
	u := f.sessions[id]
	delete(f.sessions, id)
	f.mu.Unlock()
	if u == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown upload session"})
		return
	}
	_ = os.Remove(u.temp)
	w.WriteHeader(http.StatusNoContent)
}

// openSessions counts the upload sessions neither completed nor deleted
func (f *fakeKoyeb) openSessions() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sessions)
}

// track remembers the topmost path a write is about to create so cleanup removes it
func (f *fakeKoyeb) track(name string) {
	top := ""
	for p := name; p != "/" && p != "."; p = filepath.Dir(p) {
		if _, err := os.Lstat(p); err == nil {
			break
		}
		top = p
	}
	if top != "" {
		f.mu.Lock()
		f.cleanup = append(f.cleanup, top)
		f.mu.Unlock()
	}
}

func (f *fakeKoyeb) setOrg(org, usage map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.org, f.usage = org, usage
}

func (f *fakeKoyeb) organization(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	org := map[string]any{"id": "org-1", "status": "ACTIVE", "status_message": "VALID"}
	for k, v := range f.org {
		org[k] = v
	}
	writeJSON(w, http.StatusOK, map[string]any{"organization": org})
}

func (f *fakeKoyeb) usageDetailPage(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset = min(max(offset, 0), len(f.usageDetails))
	end := min(offset+limit, len(f.usageDetails))
	writeJSON(w, http.StatusOK, map[string]any{"usage_details": f.usageDetails[offset:end]})
}

func (f *fakeKoyeb) quotaUsage(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.quotaOrg = r.PathValue("id")
	writeJSON(w, http.StatusOK, f.usage)
}
