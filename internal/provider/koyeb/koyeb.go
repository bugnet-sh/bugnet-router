// Package koyeb places sandboxes on Koyeb as SANDBOX services and runs commands through the
// executor inside each one.
//
// Every sandbox is one app holding one service, both named after the idempotency key, so a
// repeated or lost create finds the app it made instead of making another. Koyeb has no labels,
// so the service carries them as JSON in its BUGNET_LABELS variable beside a SANDBOX_SECRET the
// adapter mints, and a create adopts an existing app only when those labels match. The service
// never scales to zero and deletes itself 24 hours after creation as a backstop, while the
// router's own deadline destroys it sooner. Changing that timer redeploys and kills the running
// processes, so SetLifetime changes nothing and refuses any lifetime past the 24 hours. The
// executor is reached over HTTP/1.1 only, the protocol that can wake a sleeping service.
package koyeb

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptrace"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	kapi "github.com/koyeb/koyeb-api-client-go/api/v1/koyeb"

	"github.com/oynozan/bugnet-router/internal/provider"
)

const (
	providerName   = "koyeb"
	defaultRegion  = "fra"
	defaultBaseURL = "https://app.koyeb.com"
	sandboxImage   = "koyeb/sandbox"
	namePrefix     = "bn-"
	nameHexChars   = 20
	labelsEnv      = "BUGNET_LABELS"
	secretEnv      = "SANDBOX_SECRET"
	executorPort   = 3030
	appPort        = 3031
	executorPath   = "/koyeb-sandbox"
	maxLifetime    = 24 * time.Hour
	secretBytes    = 32
	// pageLimit is the largest page Koyeb's list calls serve
	pageLimit = "100"
	// arch is assumed because Koyeb reports no architecture and runs its fleet on amd64
	arch              = "amd64"
	defaultPoll       = time.Second
	defaultRetryAfter = time.Second
	// afterCreateLimit bounds the lookups and cleanup that settle a create whose outcome is unclear
	afterCreateLimit = 30 * time.Second
)

// Ref.Meta keys
const (
	metaService  = "service_id"
	metaSecret   = "secret"
	metaExecutor = "executor_url"
	metaRouting  = "routing_key"
	metaArch     = "arch"
	metaCreated  = "created_at"
)

var (
	billingWord  = regexp.MustCompile(`(?i)billing|payment|credit card|unpaid|funds`)
	quotaWord    = regexp.MustCompile(`(?i)quota|limit`)
	suspendWord  = regexp.MustCompile(`(?i)suspend|banned|deactivated`)
	capacityWord = regexp.MustCompile(`(?i)capacity|not available|unavailable`)
)

// instanceType is one Koyeb instance type and its price per second in micro-USD
type instanceType struct {
	id           string
	resources    provider.Resources
	secondMicros int64
}

// instanceTypes are the fra types from Koyeb's catalog on 2026-11-04 that sell whole vCPUs
var instanceTypes = []instanceType{
	{"small", provider.Resources{VCPU: 1, MemoryMiB: 1024, DiskGiB: 10}, 4},
	{"medium", provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 20}, 8},
	{"large", provider.Resources{VCPU: 4, MemoryMiB: 4096, DiskGiB: 40}, 16},
	{"xlarge", provider.Resources{VCPU: 8, MemoryMiB: 8192, DiskGiB: 80}, 32},
	{"2xlarge", provider.Resources{VCPU: 16, MemoryMiB: 16384, DiskGiB: 160}, 64},
	{"3xlarge", provider.Resources{VCPU: 24, MemoryMiB: 32768, DiskGiB: 240}, 128},
	{"4xlarge", provider.Resources{VCPU: 32, MemoryMiB: 65536, DiskGiB: 320}, 256},
	{"5xlarge", provider.Resources{VCPU: 40, MemoryMiB: 131072, DiskGiB: 400}, 512},
	{"gpu-nvidia-rtx-4000-sff-ada", provider.Resources{
		VCPU: 6, MemoryMiB: 45056, DiskGiB: 240, GPUCount: 1, GPUMemoryGiB: 20,
	}, 140},
}

func lookupType(id string) (instanceType, bool) {
	i := slices.IndexFunc(instanceTypes, func(t instanceType) bool { return t.id == id })
	if i < 0 {
		return instanceType{}, false
	}
	return instanceTypes[i], true
}

// size is one size this adapter sells and the instance type it deploys
type size struct {
	name string
	typ  instanceType
}

func (s size) gpu() bool { return s.typ.resources.GPUCount > 0 }

// ParseInstanceTypes reads comma-separated size=instanceType entries
func ParseInstanceTypes(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, entry := range strings.Split(s, ",") {
		name, typ, ok := strings.Cut(strings.TrimSpace(entry), "=")
		if !ok || name == "" || typ == "" {
			return nil, fmt.Errorf("koyeb: instance type %q is not size=type", entry)
		}
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("koyeb: size %s is named twice", name)
		}
		out[name] = typ
	}
	if _, err := sizesFor(out); err != nil {
		return nil, err
	}
	return out, nil
}

// sizesFor orders the sizes cheapest first, every instance type under its own name when empty
func sizesFor(bySize map[string]string) ([]size, error) {
	var out []size
	if len(bySize) == 0 {
		for _, t := range instanceTypes {
			out = append(out, size{name: t.id, typ: t})
		}
		return out, nil
	}
	for name, id := range bySize {
		t, ok := lookupType(id)
		if name == "" || !ok {
			return nil, fmt.Errorf("koyeb: size %q needs one of the known instance types, not %q",
				name, id)
		}
		out = append(out, size{name: name, typ: t})
	}
	slices.SortFunc(out, func(a, b size) int {
		return cmp.Or(cmp.Compare(a.typ.secondMicros, b.typ.secondMicros),
			strings.Compare(a.name, b.name))
	})
	return out, nil
}

// Options configures the adapter, BaseURL replaces Koyeb's control plane in tests
type Options struct {
	APIToken string
	// Region is where every sandbox runs, fra when empty
	Region string
	// InstanceTypeBySize maps each size sold to its instance type, every type by name when empty
	InstanceTypeBySize map[string]string
	BaseURL            string
}

// Provider places sandboxes on Koyeb, it keeps no state beyond its configuration
type Provider struct {
	api          *kapi.APIClient
	exec         *http.Client
	region       string
	sizes        []size
	pollInterval time.Duration
	exitDir      string
}

// New checks the options and makes no call to Koyeb
func New(opts Options) (*Provider, error) {
	if opts.APIToken == "" {
		return nil, errors.New("koyeb: an API token is required")
	}
	sizes, err := sizesFor(opts.InstanceTypeBySize)
	if err != nil {
		return nil, err
	}
	cfg := kapi.NewConfiguration()
	cfg.Servers = kapi.ServerConfigurations{{URL: cmp.Or(opts.BaseURL, defaultBaseURL)}}
	cfg.HTTPClient = &http.Client{}
	cfg.UserAgent = "bugnet-router"
	cfg.AddDefaultHeader("Authorization", "Bearer "+opts.APIToken)
	return &Provider{
		api:          kapi.NewAPIClient(cfg),
		exec:         newExecutorClient(),
		region:       cmp.Or(opts.Region, defaultRegion),
		sizes:        sizes,
		pollInterval: defaultPoll,
		exitDir:      defaultExitDir,
	}, nil
}

// Name returns "koyeb"
func (p *Provider) Name() string { return providerName }

// Capabilities reports previews, egress rules, the 24 hour lifetime and a GPU when one is sold
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Preview: true, GPU: slices.ContainsFunc(p.sizes, size.gpu), EgressControl: true,
		NeedsRunnerUpload: true, MaxLifetime: maxLifetime,
	}
}

// SizeFor picks the cheapest size covering floor, with a GPU only when the floor asks for one
func (p *Provider) SizeFor(floor provider.Resources) (provider.Size, bool) {
	wantGPU := floor.GPUCount > 0 || floor.GPUMemoryGiB > 0
	for _, s := range p.sizes {
		r := s.typ.resources
		if s.gpu() == wantGPU && r.VCPU >= floor.VCPU && r.MemoryMiB >= floor.MemoryMiB &&
			r.DiskGiB >= floor.DiskGiB && r.GPUCount >= floor.GPUCount &&
			r.GPUMemoryGiB >= floor.GPUMemoryGiB {
			return provider.Size{Name: s.name, Resources: r}, true
		}
	}
	return provider.Size{}, false
}

// HourlyCostMicros is the instance type's per second price over an hour, 0 for a size not sold
func (p *Provider) HourlyCostMicros(sz provider.Size) int64 {
	if s, ok := p.size(sz.Name); ok {
		return s.typ.secondMicros * 3600
	}
	return 0
}

// BillingIncrement is one second, Koyeb meters by the second
func (p *Provider) BillingIncrement() time.Duration { return time.Second }

func (p *Provider) size(name string) (size, bool) {
	i := slices.IndexFunc(p.sizes, func(s size) bool { return s.name == name })
	if i < 0 {
		return size{}, false
	}
	return p.sizes[i], true
}

/* Sandboxes */

// appName is unique per key and fits Koyeb's names, a key-less create gets a random one
func appName(key string) string {
	if key == "" {
		return namePrefix + strings.ToLower(rand.Text())[:nameHexChars]
	}
	sum := sha256.Sum256([]byte(key))
	return namePrefix + hex.EncodeToString(sum[:])[:nameHexChars]
}

// Create makes the app and service named after the key, or adopts the ones a repeat made
func (p *Provider) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	s, ok := p.size(req.Size.Name)
	if !ok {
		return provider.Ref{}, invalid(fmt.Errorf("koyeb has no size %q", req.Size.Name))
	}
	if req.Lifetime <= 0 || req.Lifetime > maxLifetime {
		return provider.Ref{}, invalid(fmt.Errorf("lifetime %v is not within koyeb's limit of %v",
			req.Lifetime, maxLifetime))
	}
	labels := req.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	encoded, err := json.Marshal(labels)
	if err != nil {
		return provider.Ref{}, invalid(err)
	}
	secret := make([]byte, secretBytes)
	_, _ = rand.Read(secret)
	n := newService{
		name: appName(req.IdempotencyKey), instanceType: s.typ.id, labels: labels,
		labelsJSON: string(encoded), secret: base64.RawURLEncoding.EncodeToString(secret),
		// Taken before sending and cut to the second so SetLifetime never undercounts the age
		created: time.Now().UTC().Truncate(time.Second),
	}
	// Lookups and cleanup still run when ctx ended, since an app may exist and bill
	after, cancel := context.WithTimeout(context.WithoutCancel(ctx), afterCreateLimit)
	defer cancel()

	appID, fresh, err := p.createApp(ctx, after, n.name)
	if err != nil {
		return provider.Ref{}, err
	}
	if !fresh {
		ref, found, err := p.adopt(after, appID, n)
		if err != nil || found {
			return ref, err
		}
	}
	return p.createService(ctx, after, appID, n)
}

// newService is what one create asks Koyeb for
type newService struct {
	name, instanceType, labelsJSON, secret string
	labels                                 map[string]string
	created                                time.Time
}

// createApp makes the app, or finds the one a repeated or lost create made and reports it not fresh
func (p *Provider) createApp(ctx, after context.Context, name string) (string, bool, error) {
	body := kapi.CreateApp{Name: &name, LifeCycle: &kapi.AppLifeCycle{DeleteWhenEmpty: new(true)}}
	reply, err := call(ctx, true, func(ctx context.Context) (*kapi.CreateAppReply, *http.Response,
		error) {
		return p.api.AppsApi.CreateApp(ctx).App(body).Execute()
	})
	if err == nil && reply.App.GetId() != "" {
		return reply.App.GetId(), true, nil
	}
	if err == nil {
		err = unknown(errors.New("koyeb create app reply names no app"))
	}
	if !mayExist(err) {
		return "", false, err
	}
	app, lerr := p.findApp(after, name)
	if lerr != nil {
		return "", false, unsettled(err, lerr)
	}
	if app == nil {
		return "", false, absent(err)
	}
	return app.GetId(), false, nil
}

// createService makes the sandbox service in the app, removing the app when that surely failed
func (p *Provider) createService(
	ctx, after context.Context, appID string, n newService,
) (provider.Ref, error) {
	body := p.serviceSpec(appID, n)
	reply, err := call(ctx, true, func(ctx context.Context) (*kapi.CreateServiceReply,
		*http.Response, error) {
		return p.api.ServicesApi.CreateService(ctx).Service(body).Execute()
	})
	if err == nil && reply.Service.GetId() != "" {
		ref := newRef(appID, reply.Service.GetId(), n.secret)
		ref.Meta[metaCreated] = n.created.Format(time.RFC3339)
		return ref, nil
	}
	if err == nil {
		err = unknown(errors.New("koyeb create service reply names no service"))
	}
	if mayExist(err) {
		ref, found, lerr := p.adopt(after, appID, n)
		switch {
		case lerr != nil:
			return provider.Ref{}, unsettled(err, lerr)
		case found:
			return ref, nil
		}
		err = absent(err)
	}
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeOutcomeUnknown {
		// An app without its service is useless, and Koyeb would only drop it after 15 minutes
		_, _ = call(after, false, func(ctx context.Context) (map[string]any, *http.Response,
			error) {
			return p.api.AppsApi.DeleteApp(ctx, appID).Execute()
		})
	}
	return provider.Ref{}, err
}

func (p *Provider) serviceSpec(appID string, n newService) kapi.CreateService {
	typ, mesh := kapi.DEPLOYMENTDEFINITIONTYPE_SANDBOX, kapi.DEPLOYMENTMESH_AUTO
	def := kapi.DeploymentDefinition{
		Name:   &n.name,
		Type:   &typ,
		Docker: &kapi.DockerSource{Image: new(sandboxImage), Privileged: new(false)},
		Env: []kapi.DeploymentEnv{
			{Key: new(secretEnv), Value: new(n.secret)},
			{Key: new(labelsEnv), Value: new(n.labelsJSON)},
		},
		Ports: []kapi.DeploymentPort{
			{Port: new(int64(executorPort)), Protocol: new("http")},
			{Port: new(int64(appPort)), Protocol: new("http")},
		},
		Routes: []kapi.DeploymentRoute{
			{Port: new(int64(executorPort)), Path: new(executorPath + "/")},
			{Port: new(int64(appPort)), Path: new("/")},
		},
		InstanceTypes: []kapi.DeploymentInstanceType{{Type: new(n.instanceType)}},
		Regions:       []string{p.region},
		// One instance at all times, since a sleeping sandbox loses its processes
		Scalings: []kapi.DeploymentScaling{{Min: new(int64(1)), Max: new(int64(1))}},
		Mesh:     &mesh,
	}
	return kapi.CreateService{
		AppId: &appID, Name: &n.name, Definition: &def,
		LifeCycle: &kapi.ServiceLifeCycle{DeleteAfterCreate: new(int64(maxLifetime.Seconds()))},
	}
}

// adopt returns the sandbox the app already holds when its labels are the ones asked for
func (p *Provider) adopt(
	ctx context.Context, appID string, n newService,
) (provider.Ref, bool, error) {
	svc, err := p.findService(ctx, appID, n.name)
	if err != nil || svc == nil {
		return provider.Ref{}, false, err
	}
	dep, err := p.deployment(ctx, svc.GetLatestDeploymentId())
	if err != nil {
		return provider.Ref{}, false, err
	}
	env := envOf(dep)
	var labels map[string]string
	if err := json.Unmarshal([]byte(env[labelsEnv]), &labels); err != nil ||
		!maps.Equal(labels, n.labels) {
		return provider.Ref{}, false, &provider.Error{
			Code: provider.CodeInternal, CanFailOver: true,
			Err: fmt.Errorf("koyeb app name %s is held by a sandbox of another request", n.name),
		}
	}
	return refFor(appID, svc.GetId(), svc.GetCreatedAt(), dep), true, nil
}

func newRef(appID, serviceID, secret string) provider.Ref {
	return provider.Ref{Provider: providerName, ID: appID, Meta: map[string]string{
		metaService: serviceID, metaSecret: secret, metaArch: arch,
	}}
}

// refFor rebuilds a ref from what Koyeb holds about a sandbox
func refFor(appID, serviceID string, created time.Time, dep *kapi.Deployment) provider.Ref {
	ref := newRef(appID, serviceID, envOf(dep)[secretEnv])
	ref.Meta[metaCreated] = created.UTC().Truncate(time.Second).Format(time.RFC3339)
	if url, key := sandboxURL(dep); url != "" {
		ref.Meta[metaExecutor] = url
		if key != "" {
			ref.Meta[metaRouting] = key
		}
	}
	return ref
}

func envOf(dep *kapi.Deployment) map[string]string {
	env := map[string]string{}
	for _, e := range dep.Definition.GetEnv() {
		env[e.GetKey()] = e.GetValue()
	}
	return env
}

// sandboxURL is the executor's base URL and routing key from the deployment's sandbox metadata
func sandboxURL(dep *kapi.Deployment) (string, string) {
	sb := dep.Metadata.GetSandbox()
	url := strings.TrimRight(sb.GetPublicUrl(), "/")
	if url == "" {
		return "", ""
	}
	if !strings.Contains(url, "://") {
		url = "https://" + url
	}
	return url + executorPath, sb.GetRoutingKey()
}

// appID returns the app id of a ref this provider made
func appID(ref provider.Ref) (string, error) {
	if ref.Provider != providerName || ref.ID == "" {
		return "", &provider.Error{
			Code: provider.CodeNotFound,
			Err:  fmt.Errorf("sandbox %s/%s is not a koyeb sandbox", ref.Provider, ref.ID),
		}
	}
	return ref.ID, nil
}

func gone[T ~string](status T) bool { return status == "DELETING" || status == "DELETED" }

// findApp returns the live app with exactly this name, or nil
func (p *Provider) findApp(ctx context.Context, name string) (*kapi.AppListItem, error) {
	for offset := 0; ; {
		reply, err := call(ctx, false, func(ctx context.Context) (*kapi.ListAppsReply,
			*http.Response, error) {
			return p.api.AppsApi.ListApps(ctx).Name(name).Limit(pageLimit).
				Offset(strconv.Itoa(offset)).Execute()
		})
		if err != nil {
			return nil, err
		}
		for _, a := range reply.Apps {
			if a.GetName() == name && !gone(a.GetStatus()) {
				return &a, nil
			}
		}
		offset += len(reply.Apps)
		if !reply.GetHasNext() || len(reply.Apps) == 0 {
			return nil, nil
		}
	}
}

// findService returns the app's live sandbox service with this name, any name when empty, or nil
func (p *Provider) findService(
	ctx context.Context, appID, name string,
) (*kapi.ServiceListItem, error) {
	var found *kapi.ServiceListItem
	err := p.eachService(ctx, appID, func(s kapi.ServiceListItem) bool {
		if name == "" || s.GetName() == name {
			found = &s
			return false
		}
		return true
	})
	return found, err
}

// eachService hands fn the live sandbox services, of one app when appID is set, until fn says stop
func (p *Provider) eachService(
	ctx context.Context, appID string, fn func(kapi.ServiceListItem) bool,
) error {
	for offset := 0; ; {
		reply, err := call(ctx, false, func(ctx context.Context) (*kapi.ListServicesReply,
			*http.Response, error) {
			list := p.api.ServicesApi.ListServices(ctx).Types([]string{"SANDBOX"}).
				Limit(pageLimit).Offset(strconv.Itoa(offset))
			if appID != "" {
				list = list.AppId(appID)
			}
			return list.Execute()
		})
		if err != nil {
			return err
		}
		for _, s := range reply.Services {
			if !gone(s.GetStatus()) && !fn(s) {
				return nil
			}
		}
		offset += len(reply.Services)
		more := reply.GetHasNext() || int64(offset) < reply.GetCount()
		if !more || len(reply.Services) == 0 {
			return nil
		}
	}
}

func (p *Provider) deployment(ctx context.Context, id string) (*kapi.Deployment, error) {
	if id == "" {
		return nil, unknown(errors.New("koyeb service has no deployment yet"))
	}
	reply, err := call(ctx, false, func(ctx context.Context) (*kapi.GetDeploymentReply,
		*http.Response, error) {
		return p.api.DeploymentsApi.GetDeployment(ctx, id).Execute()
	})
	if err != nil {
		return nil, err
	}
	if reply.Deployment == nil {
		return nil, internal(fmt.Errorf("koyeb deployment %s reply is empty", id))
	}
	return reply.Deployment, nil
}

// service returns the ref's live service, not_found once it or its app is gone
func (p *Provider) service(ctx context.Context, ref provider.Ref) (*kapi.Service, error) {
	app, err := appID(ref)
	if err != nil {
		return nil, err
	}
	id := ref.Meta[metaService]
	if id == "" {
		item, err := p.findService(ctx, app, "")
		if err != nil {
			return nil, err
		}
		if item == nil {
			return nil, notFound(fmt.Errorf("koyeb app %s holds no sandbox", app))
		}
		id = item.GetId()
	}
	reply, err := call(ctx, false, func(ctx context.Context) (*kapi.GetServiceReply,
		*http.Response, error) {
		return p.api.ServicesApi.GetService(ctx, id).Execute()
	})
	if err != nil {
		return nil, err
	}
	svc := reply.Service
	if svc == nil || gone(svc.GetStatus()) || svc.GetAppId() != app {
		return nil, notFound(fmt.Errorf("koyeb sandbox %s is gone", app))
	}
	return svc, nil
}

// WaitReady waits for a healthy deployment and executor and records the executor in ref.Meta
func (p *Provider) WaitReady(ctx context.Context, ref provider.Ref) error {
	svc, err := p.service(ctx, ref)
	if err != nil {
		return err
	}
	var last error
	for {
		t, err := p.readyTarget(ctx, ref, svc)
		if e, ok := provider.AsError(err); ok && (e.Code == provider.CodeNotFound ||
			e.Code == provider.CodeAuth || e.Code == provider.CodeCapacity ||
			e.Code == provider.CodeInvalid) {
			return err
		}
		if err == nil {
			// Any failed probe is retried, since Koyeb's edge may not route to a new sandbox yet
			err = p.health(ctx, t)
		}
		if err == nil {
			// The caller's ref shares this map, so it keeps the executor for later calls
			if ref.Meta != nil {
				ref.Meta[metaExecutor] = t.base
				if t.routing != "" {
					ref.Meta[metaRouting] = t.routing
				}
			}
			return nil
		}
		if ctx.Err() == nil {
			last = err
		}
		if !sleep(ctx, p.pollInterval) {
			if last == nil {
				return fmt.Errorf("koyeb sandbox %s not ready: %w", ref.ID, ctx.Err())
			}
			return fmt.Errorf("koyeb sandbox %s not ready: %w, last %w", ref.ID, ctx.Err(), last)
		}
	}
}

// readyTarget reads the deployment and returns its executor once Koyeb calls it healthy
func (p *Provider) readyTarget(
	ctx context.Context, ref provider.Ref, svc *kapi.Service,
) (target, error) {
	dep, err := p.deployment(ctx, svc.GetLatestDeploymentId())
	if err != nil {
		return target{}, err
	}
	switch status := dep.GetStatus(); status {
	case kapi.DEPLOYMENTSTATUS_HEALTHY, kapi.DEPLOYMENTSTATUS_DEGRADED:
	case kapi.DEPLOYMENTSTATUS_PENDING, kapi.DEPLOYMENTSTATUS_PROVISIONING,
		kapi.DEPLOYMENTSTATUS_SCHEDULED, kapi.DEPLOYMENTSTATUS_ALLOCATING,
		kapi.DEPLOYMENTSTATUS_STARTING:
		return target{}, internal(fmt.Errorf("koyeb deployment is %s", status))
	default:
		return target{}, &provider.Error{
			Code: provider.CodeCapacity, CanFailOver: true,
			Err: fmt.Errorf("koyeb deployment ended %s: %s", status,
				strings.Join(dep.Messages, "; ")),
		}
	}
	return p.targetOf(ctx, ref, dep)
}

// SetLifetime accepts any lifetime within the 24 hours the service deletes itself after
func (p *Provider) SetLifetime(ctx context.Context, ref provider.Ref, until time.Time) error {
	if _, err := appID(ref); err != nil {
		return err
	}
	created, err := time.Parse(time.RFC3339, ref.Meta[metaCreated])
	known := err == nil
	if known && until.Sub(created) > maxLifetime {
		return overLifetime(until, created)
	}
	svc, err := p.service(ctx, ref)
	if err != nil {
		return err
	}
	if !known && until.Sub(svc.GetCreatedAt()) > maxLifetime {
		return overLifetime(until, svc.GetCreatedAt())
	}
	return nil
}

func overLifetime(until, created time.Time) error {
	return invalid(fmt.Errorf("lifetime %v from creation is over koyeb's limit of %v",
		until.Sub(created).Round(time.Second), maxLifetime))
}

// Destroy deletes the app with its service and treats one already gone as destroyed
func (p *Provider) Destroy(ctx context.Context, ref provider.Ref) error {
	app, err := appID(ref)
	if err != nil {
		return nil
	}
	_, err = call(ctx, false, func(ctx context.Context) (map[string]any, *http.Response, error) {
		return p.api.AppsApi.DeleteApp(ctx, app).Execute()
	})
	if err == nil || isCode(err, provider.CodeNotFound) {
		return nil
	}
	// A delete may fail once the app is already on its way out
	reply, gerr := call(ctx, false, func(ctx context.Context) (*kapi.GetAppReply, *http.Response,
		error) {
		return p.api.AppsApi.GetApp(ctx, app).Execute()
	})
	if isCode(gerr, provider.CodeNotFound) || (gerr == nil && gone(reply.App.GetStatus())) {
		return nil
	}
	return err
}

// ListOwned reads the labels of every bn- sandbox service and keeps those carrying all of them
func (p *Provider) ListOwned(
	ctx context.Context, labels map[string]string,
) ([]provider.Summary, error) {
	var items []kapi.ServiceListItem
	err := p.eachService(ctx, "", func(s kapi.ServiceListItem) bool {
		if strings.HasPrefix(s.GetName(), namePrefix) {
			items = append(items, s)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	var owned []provider.Summary
	for _, s := range items {
		dep, err := p.deployment(ctx, s.GetLatestDeploymentId())
		if isCode(err, provider.CodeNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var have map[string]string
		if json.Unmarshal([]byte(envOf(dep)[labelsEnv]), &have) != nil || !hasLabels(have, labels) {
			continue
		}
		owned = append(owned, provider.Summary{
			Ref:       refFor(s.GetAppId(), s.GetId(), s.GetCreatedAt(), dep),
			Labels:    have,
			CreatedAt: s.GetCreatedAt().UTC(),
		})
	}
	return owned, nil
}

func hasLabels(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}
	return true
}

/* Control plane errors */

// call runs one control plane request and classifies its failure, create marks creating calls
func call[T any](
	ctx context.Context, create bool, do func(context.Context) (T, *http.Response, error),
) (T, error) {
	var sent atomic.Bool
	trace := &httptrace.ClientTrace{WroteHeaders: func() { sent.Store(true) }}
	v, resp, err := do(httptrace.WithClientTrace(ctx, trace))
	if rv := reflect.ValueOf(v); err == nil && rv.Kind() == reflect.Pointer && rv.IsNil() {
		err = errors.New("koyeb sent an empty reply")
	}
	if err != nil {
		var zero T
		return zero, apiError(err, resp, sent.Load(), create)
	}
	return v, nil
}

// apiError classifies a failed control plane call by its status, then its code and message
func apiError(err error, resp *http.Response, sent, create bool) error {
	if resp == nil {
		switch {
		case sent && create:
			return unknown(err)
		case sent:
			return internal(err)
		}
		return &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: err}
	}
	var raw []byte
	var gen *kapi.GenericOpenAPIError
	if errors.As(err, &gen) {
		raw = gen.Body()
	}
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Fields  []struct {
			Field       string `json:"field"`
			Description string `json:"description"`
		} `json:"fields"`
	}
	_ = json.Unmarshal(raw, &body)
	msg := cmp.Or(body.Message, strings.TrimSpace(string(raw)), err.Error())
	for _, f := range body.Fields {
		msg += ", " + f.Field + " " + f.Description
	}
	status := resp.StatusCode
	e := &provider.Error{
		Code: provider.CodeInternal,
		Err:  fmt.Errorf("koyeb answered %d %s: %s", status, cmp.Or(body.Code, "error"), msg),
	}
	client := status >= 400 && status < 500
	switch {
	case status < 300 && create:
		// The call went through but its reply could not be read
		e.Code = provider.CodeOutcomeUnknown
	case status < 300:
	case status == http.StatusTooManyRequests || body.Code == "rate_limit_error":
		e.Code, e.CanFailOver = provider.CodeRateLimited, true
		e.RetryAfter = retryAfter(resp.Header)
	case status == http.StatusPaymentRequired || (client && billingWord.MatchString(msg)):
		e.Code, e.CanFailOver = provider.CodeUnfunded, true
	case client && quotaWord.MatchString(msg):
		e.Code, e.CanFailOver = provider.CodeQuota, true
	case (status == http.StatusUnauthorized || status == http.StatusForbidden) &&
		suspendWord.MatchString(msg):
		e.Code, e.CanFailOver = provider.CodeAbuseLock, true
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		e.Code, e.CanFailOver = provider.CodeAuth, true
	case status == http.StatusNotFound:
		e.Code = provider.CodeNotFound
	case client:
		e.Code = provider.CodeInvalid
	case status == http.StatusServiceUnavailable || capacityWord.MatchString(msg):
		e.Code, e.CanFailOver = provider.CodeCapacity, true
	}
	return e
}

// retryAfter reads Retry-After, then Koyeb's rate limit reset, both in seconds
func retryAfter(h http.Header) time.Duration {
	for _, name := range []string{"Retry-After", "X-Ratelimit-Reset"} {
		if s, err := strconv.Atoi(strings.TrimSpace(h.Get(name))); err == nil && s >= 0 {
			return time.Duration(s) * time.Second
		}
	}
	return defaultRetryAfter
}

// mayExist reports whether the resource of a failed create may exist anyway
func mayExist(err error) bool {
	e, ok := provider.AsError(err)
	if !ok {
		return false
	}
	switch e.Code {
	case provider.CodeInvalid, provider.CodeInternal, provider.CodeCapacity,
		provider.CodeOutcomeUnknown:
		return true
	}
	return false
}

// absent marks a failed create that a lookup showed made nothing, so another provider may run it
func absent(err error) error {
	e, ok := provider.AsError(err)
	if ok && (e.Code == provider.CodeInternal || e.Code == provider.CodeCapacity) {
		e.CanFailOver = true
	}
	return err
}

// unsettled is a failed create whose lookup failed too, so whether it made something is unknown
func unsettled(err, lookup error) error {
	if isCode(err, provider.CodeInvalid) {
		return lookup
	}
	return unknown(fmt.Errorf("%w, and the lookup after it failed: %w", err, lookup))
}

func isCode(err error, code provider.Code) bool {
	e, ok := provider.AsError(err)
	return ok && e.Code == code
}

func invalid(err error) error {
	return &provider.Error{Code: provider.CodeInvalid, Err: err}
}

func notFound(err error) error {
	return &provider.Error{Code: provider.CodeNotFound, Err: err}
}

func internal(err error) error {
	return &provider.Error{Code: provider.CodeInternal, Err: err}
}

func unknown(err error) error {
	return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: err}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
