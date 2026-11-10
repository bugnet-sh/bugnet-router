// Package koyeb places sandboxes on Koyeb as SANDBOX services and runs commands through the
// executor inside each one.
//
// Every sandbox is one app holding one service, both named after the idempotency key, so a
// repeated or lost create finds the app it made instead of making another. Koyeb has no labels,
// so the service carries them as JSON in its BUGNET_LABELS variable beside a SANDBOX_SECRET the
// adapter mints, and a create adopts an existing app only when those labels match. The service
// never scales to zero and deletes itself 24 hours after creation as a backstop, while the
// router's own deadline destroys it sooner. Changing that timer redeploys and kills the running
// processes, so SetLifetime changes nothing and refuses any lifetime past the 23 hours 50
// minutes the adapter sells, which keeps every deadline inside the backstop. The control plane
// is called through a small client that reads only the fields it needs. The executor is reached
// over HTTP/1.1 only, the protocol that can wake a sleeping service. File streams buffer each
// upload part in memory, so a part Koyeb's front cuts is sent again, and reissue a download
// that ends early from where it stopped. PrepareGate binds the executor's TCP proxy to the gate,
// which the edges reach at the public URL's root with the executor's own path blocked.
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
	"log/slog"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
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
	// backstop is the delete_after_create Koyeb allows at most, set on every service
	backstop = 24 * time.Hour
	// maxLifetime leaves the saga room to measure from its own earlier clock plus its slack
	maxLifetime = backstop - 10*time.Minute
	secretBytes = 32
	// pageLimit is the largest page Koyeb's list calls serve
	pageLimit = "100"
	// pageRetries is how often one rate limited page is asked again
	pageRetries = 3
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
	capacityWord = regexp.MustCompile(`(?i)capacity|not available|unavailable|no instance`)
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
			return nil, fmt.Errorf("koyeb: size %q needs one of the known instance types", name)
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
	Log                *slog.Logger // slog.Default when nil
	// PartBytes is the size of each buffered upload part, 8 MiB when 0
	PartBytes int64
	// BufferTotal bounds every part buffer of the process together, 256 MiB when 0
	BufferTotal int64
	// OrganizationID is read from the account when empty
	OrganizationID string
	// PoolIdleHourlyMicros is what one unclaimed member of a pool costs an hour, 0 while Light
	// Sleep is free
	PoolIdleHourlyMicros int64
}

// Provider places sandboxes on Koyeb, keeping only the labels it already read per service
type Provider struct {
	api          *api
	exec         *http.Client
	log          *slog.Logger
	region       string
	sizes        []size
	pollInterval time.Duration
	exitDir      string
	// streams carries file streams, which never ask for compression
	streams   *http.Client
	partBytes int64
	buffers   *semaphore.Weighted
	// retryWait spaces out reissued reads and is the first pause between attempts of a part
	retryWait time.Duration
	// orgID is the organization the quota route names, read once when no setting gave it
	orgID string
	// poolIdle is one unclaimed pool member's cost an hour
	poolIdle int64
	mu       sync.Mutex
	known    map[string]knownService // by service id, labels never change after create
	pools    map[string]string       // pool ids by name
}

// knownService is what ListOwned read from one deployment of a service
type knownService struct {
	deployment string
	summary    provider.Summary
}

// ErrTransferSizes marks a part or buffer size New refuses
var ErrTransferSizes = errors.New("koyeb: transfer sizes")

// New checks the options and makes no call to Koyeb
func New(opts Options) (*Provider, error) {
	if opts.APIToken == "" {
		return nil, errors.New("koyeb: an API token is required")
	}
	sizes, err := sizesFor(opts.InstanceTypeBySize)
	if err != nil {
		return nil, err
	}
	part := cmp.Or(opts.PartBytes, defaultPartBytes)
	total := cmp.Or(opts.BufferTotal, defaultBufferTotal)
	switch {
	case part < 0 || total < 0:
		return nil, fmt.Errorf("%w: part and buffer sizes must not be negative", ErrTransferSizes)
	case part > maxPartBytes:
		return nil, fmt.Errorf("%w: a part of %d bytes is over Koyeb's %d per request",
			ErrTransferSizes, part, int64(maxPartBytes))
	case part > total:
		return nil, fmt.Errorf("%w: a part of %d bytes does not fit a buffer total of %d",
			ErrTransferSizes, part, total)
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &Provider{
		api: &api{
			base:   strings.TrimRight(cmp.Or(opts.BaseURL, defaultBaseURL), "/"),
			token:  opts.APIToken,
			client: &http.Client{CheckRedirect: provider.NoRedirect},
		},
		exec:         newExecutorClient(),
		log:          log,
		region:       cmp.Or(opts.Region, defaultRegion),
		sizes:        sizes,
		pollInterval: defaultPoll,
		exitDir:      defaultExitDir,
		streams:      newStreamClient(),
		partBytes:    part,
		buffers:      semaphore.NewWeighted(total),
		retryWait:    defaultRetryWait,
		known:        map[string]knownService{},
		pools:        map[string]string{},
		orgID:        opts.OrganizationID,
		poolIdle:     opts.PoolIdleHourlyMicros,
	}, nil
}

// Name returns "koyeb"
func (p *Provider) Name() string { return providerName }

// Capabilities reports no egress control until live test N1 proves Koyeb's network policy
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		GPU: slices.ContainsFunc(p.sizes, size.gpu), NeedsRunnerUpload: true,
		MaxLifetime: maxLifetime, OneSandboxPerKey: true,
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

// Sizes lists the instance types as sizes
func (p *Provider) Sizes() []provider.Size {
	out := make([]provider.Size, 0, len(p.sizes))
	for _, s := range p.sizes {
		out = append(out, provider.Size{Name: s.name, Resources: s.typ.resources})
	}
	return out
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
		held := fmt.Errorf("koyeb app %s exists already", n.name)
		ref, found, err := p.adopt(after, appID, n, held)
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
	app, err := p.api.createApp(ctx, name)
	if err == nil && app.ID != "" {
		return app.ID, true, nil
	}
	if err == nil {
		err = unknown(errors.New("koyeb create app reply names no app"))
	}
	if !mayExist(err) {
		return "", false, err
	}
	found, lerr := p.findApp(after, name)
	if lerr != nil {
		return "", false, unsettled(err, lerr)
	}
	if found == nil {
		return "", false, absent(err)
	}
	return found.ID, false, nil
}

// createService makes the sandbox service in the app, removing the app when that surely failed
func (p *Provider) createService(
	ctx, after context.Context, appID string, n newService,
) (provider.Ref, error) {
	svc, err := p.api.createService(ctx, p.serviceSpec(appID, n))
	if err == nil && svc.ID != "" {
		ref := newRef(appID, svc.ID, n.secret)
		ref.Meta[metaCreated] = n.created.Format(time.RFC3339)
		return ref, nil
	}
	if err == nil {
		err = unknown(errors.New("koyeb create service reply names no service"))
	}
	if mayExist(err) {
		ref, found, aerr := p.adopt(after, appID, n, err)
		switch {
		case aerr != nil:
			return provider.Ref{}, aerr
		case found:
			return ref, nil
		}
		err = absent(err)
	}
	if !isCode(err, provider.CodeOutcomeUnknown) {
		// An app without its service is useless, and Koyeb would only drop it after 15 minutes
		_ = p.api.deleteApp(after, appID)
	}
	return provider.Ref{}, err
}

func (p *Provider) serviceSpec(appID string, n newService) createServiceRequest {
	def := serviceDefinition{
		Name: n.name,
		Type: "SANDBOX",
		Env: []envVar{
			{Key: secretEnv, Value: n.secret},
			{Key: labelsEnv, Value: n.labelsJSON},
		},
		Ports: []portSpec{
			{Port: executorPort, Protocol: "http"}, {Port: appPort, Protocol: "http"},
		},
		Routes: []routeSpec{
			{Port: executorPort, Path: executorPath + "/"}, {Port: appPort, Path: "/"},
		},
		InstanceTypes: []typeSpec{{Type: n.instanceType}},
		Regions:       []string{p.region},
		// One instance at all times, since a sleeping sandbox loses its processes
		Scalings: []scalingSpec{{Min: 1, Max: 1}},
		Mesh:     "DEPLOYMENT_MESH_AUTO",
	}
	def.Docker.Image = sandboxImage
	req := createServiceRequest{AppID: appID, Name: n.name, Definition: def}
	req.LifeCycle.DeleteAfterCreate = int64(backstop.Seconds())
	return req
}

// adopt returns the sandbox the app already holds when its labels are the ones asked for
func (p *Provider) adopt(
	ctx context.Context, appID string, n newService, cause error,
) (provider.Ref, bool, error) {
	svc, err := p.findService(ctx, appID, n.name)
	if err != nil {
		return provider.Ref{}, false, unsettled(cause, err)
	}
	if svc == nil {
		return provider.Ref{}, false, nil
	}
	dep, err := p.deployment(ctx, svc.LatestDeploymentID)
	if err != nil {
		return provider.Ref{}, false, unsettled(cause, err)
	}
	var labels map[string]string
	if err := json.Unmarshal([]byte(envOf(dep)[labelsEnv]), &labels); err != nil ||
		!maps.Equal(labels, n.labels) {
		return provider.Ref{}, false, &provider.Error{
			Code: provider.CodeInternal, CanFailOver: true,
			Err: fmt.Errorf("koyeb app name %s is held by a sandbox of another request", n.name),
		}
	}
	return refFor(appID, svc.ID, svc.CreatedAt, dep), true, nil
}

func newRef(appID, serviceID, secret string) provider.Ref {
	return provider.Ref{Provider: providerName, ID: appID, Meta: map[string]string{
		metaService: serviceID, metaSecret: secret, metaArch: arch,
	}}
}

// refFor rebuilds a ref from what Koyeb holds about a sandbox
func refFor(appID, serviceID string, created time.Time, dep deploymentRecord) provider.Ref {
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

func envOf(dep deploymentRecord) map[string]string {
	env := map[string]string{}
	for _, e := range dep.Definition.Env {
		env[e.Key] = e.Value
	}
	return env
}

// sandboxURL is the executor's base URL and routing key from the deployment's sandbox metadata
func sandboxURL(dep deploymentRecord) (string, string) {
	sb := dep.Metadata.Sandbox
	url := strings.TrimRight(sb.PublicURL, "/")
	if url == "" {
		return "", ""
	}
	if !strings.Contains(url, "://") {
		url = "https://" + url
	}
	return url + executorPath, sb.RoutingKey
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

func gone(status string) bool { return status == "DELETING" || status == "DELETED" }

// findApp returns the live app with exactly this name, or nil
func (p *Provider) findApp(ctx context.Context, name string) (*appRecord, error) {
	for offset := 0; ; {
		page, err := p.api.listApps(ctx, name, offset)
		if err != nil {
			return nil, err
		}
		for _, a := range page.items {
			if a.Name == name && !gone(a.Status) {
				return &a, nil
			}
		}
		offset += len(page.items)
		if !page.hasNext || len(page.items) == 0 {
			return nil, nil
		}
	}
}

// findService returns the app's live sandbox service with this name, any name when empty, or nil
func (p *Provider) findService(
	ctx context.Context, appID, name string,
) (*serviceRecord, error) {
	var found *serviceRecord
	err := p.eachService(ctx, appID, func(s serviceRecord) bool {
		if name == "" || s.Name == name {
			found = &s
			return false
		}
		return true
	})
	return found, err
}

// eachService hands fn the live sandbox services, of one app when appID is set, until fn says stop
func (p *Provider) eachService(
	ctx context.Context, appID string, fn func(serviceRecord) bool,
) error {
	for offset, retries := 0, 0; ; {
		page, err := p.api.listServices(ctx, appID, offset)
		if e, ok := provider.AsError(err); ok && e.Code == provider.CodeRateLimited &&
			retries < pageRetries && fits(ctx, e.RetryAfter) {
			if !sleep(ctx, e.RetryAfter) {
				return ctx.Err()
			}
			retries++
			continue
		}
		if err != nil {
			return err
		}
		for _, s := range page.items {
			if !gone(s.Status) && !fn(s) {
				return nil
			}
		}
		offset += len(page.items)
		more := page.hasNext || int64(offset) < page.count
		if !more || len(page.items) == 0 {
			return nil
		}
	}
}

// fits reports whether a wait ends before the context's deadline, which a retry needs to have
func fits(ctx context.Context, wait time.Duration) bool {
	deadline, ok := ctx.Deadline()
	return ok && time.Until(deadline) > wait
}

func (p *Provider) deployment(ctx context.Context, id string) (deploymentRecord, error) {
	if id == "" {
		return deploymentRecord{}, unknown(errors.New("koyeb service has no deployment yet"))
	}
	return p.api.getDeployment(ctx, id)
}

// service returns the ref's live service, not_found once it or its app is gone
func (p *Provider) service(ctx context.Context, ref provider.Ref) (serviceRecord, error) {
	app, err := appID(ref)
	if err != nil {
		return serviceRecord{}, err
	}
	id := ref.Meta[metaService]
	if id == "" {
		item, err := p.findService(ctx, app, "")
		if err != nil {
			return serviceRecord{}, err
		}
		if item == nil {
			return serviceRecord{}, notFound(fmt.Errorf("koyeb app %s holds no sandbox", app))
		}
		id = item.ID
	}
	svc, err := p.api.getService(ctx, id)
	if err != nil {
		return serviceRecord{}, err
	}
	if gone(svc.Status) || svc.AppID != app {
		return serviceRecord{}, notFound(fmt.Errorf("koyeb sandbox %s is gone", app))
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
		var t target
		err := error(nil)
		if svc.LatestDeploymentID == "" {
			// A service just created may not name its deployment yet, so it is read again
			svc, err = p.service(ctx, ref)
			if err == nil && svc.LatestDeploymentID == "" {
				err = internal(errors.New("koyeb service has no deployment yet"))
			}
		}
		if err == nil {
			t, err = p.readyTarget(ctx, ref, svc)
		}
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
		wait := p.pollInterval
		if e, ok := provider.AsError(err); ok && e.Code == provider.CodeRateLimited {
			wait = max(wait, e.RetryAfter)
		}
		if !sleep(ctx, wait) {
			if last == nil {
				return fmt.Errorf("koyeb sandbox %s not ready: %w", ref.ID, ctx.Err())
			}
			return fmt.Errorf("koyeb sandbox %s not ready: %w, last %w", ref.ID, ctx.Err(), last)
		}
	}
}

// endedStatuses are the deployment statuses that never turn healthy, as the SDK reads them
var endedStatuses = []string{
	"CANCELING", "CANCELED", "UNHEALTHY", "STOPPING", "STOPPED", "ERRORING", "ERROR", "STASHED",
	"SLEEPING",
}

// readyTarget reads the deployment and returns its executor once Koyeb calls it healthy
func (p *Provider) readyTarget(
	ctx context.Context, ref provider.Ref, svc serviceRecord,
) (target, error) {
	dep, err := p.deployment(ctx, svc.LatestDeploymentID)
	if err != nil {
		return target{}, err
	}
	switch status := dep.Status; {
	case status == "HEALTHY" || status == "DEGRADED":
	case slices.Contains(endedStatuses, status):
		return target{}, &provider.Error{
			Code: provider.CodeCapacity, CanFailOver: true,
			Err: fmt.Errorf("koyeb deployment ended %s: %s", status,
				strings.Join(dep.Messages, "; ")),
		}
	default:
		// A status Koyeb adds later is waited on like the starting ones
		return target{}, internal(fmt.Errorf("koyeb deployment is %s", status))
	}
	return p.targetOf(ctx, ref, dep)
}

// SetLifetime accepts any lifetime within the cap, which the 24 hour backstop covers
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
	if !known && until.Sub(svc.CreatedAt) > maxLifetime {
		return overLifetime(until, svc.CreatedAt)
	}
	return nil
}

func overLifetime(until, created time.Time) error {
	return invalid(fmt.Errorf("lifetime %v from creation is over koyeb's limit of %v",
		until.Sub(created).Round(time.Second), maxLifetime))
}

// Destroy deletes the app with its service and treats one already gone as destroyed
func (p *Provider) Destroy(ctx context.Context, ref provider.Ref) error {
	if ref.Meta[metaPooled] != "" {
		return p.destroyMember(ctx, ref)
	}
	app, err := appID(ref)
	if err != nil {
		return nil
	}
	err = p.api.deleteApp(ctx, app)
	if err == nil || isCode(err, provider.CodeNotFound) {
		return nil
	}
	// A delete may fail once the app is already on its way out
	a, gerr := p.api.getApp(ctx, app)
	if isCode(gerr, provider.CodeNotFound) || (gerr == nil && gone(a.Status)) {
		return nil
	}
	return err
}

// ListOwned keeps the bn- sandboxes carrying every label, and what it found when it is cut short
func (p *Provider) ListOwned(
	ctx context.Context, labels map[string]string,
) ([]provider.Summary, error) {
	items, listErr := p.sandboxServices(ctx)
	owned, err := p.summaries(ctx, items, labels)
	if listErr == nil && err == nil {
		p.forgetAllBut(items)
	}
	p.markPoolHeld(ctx, owned)
	return owned, errors.Join(listErr, err)
}

// sandboxServices lists the bn- services, and the ones it found when the listing is cut short
func (p *Provider) sandboxServices(ctx context.Context) ([]serviceRecord, error) {
	var items []serviceRecord
	err := p.eachService(ctx, "", func(s serviceRecord) bool {
		// A service still being created has no deployment to read labels from yet
		if strings.HasPrefix(s.Name, namePrefix) && s.LatestDeploymentID != "" {
			items = append(items, s)
		}
		return true
	})
	return items, err
}

// summaries reads the summary of each service and keeps the ones carrying every label
func (p *Provider) summaries(
	ctx context.Context, items []serviceRecord, labels map[string]string,
) ([]provider.Summary, error) {
	var owned []provider.Summary
	var wait time.Duration
	for _, s := range items {
		sum, ok := p.knownSummary(s)
		if !ok {
			if wait > 0 && !sleep(ctx, wait) {
				return owned, ctx.Err()
			}
			dep, err := p.deployment(ctx, s.LatestDeploymentID)
			wait = 0
			if e, ok := provider.AsError(err); ok && e.Code == provider.CodeRateLimited {
				wait = e.RetryAfter
			}
			if ctx.Err() != nil {
				return owned, ctx.Err()
			}
			if err != nil {
				// One unreadable sandbox must not stop the sweep, the next sweep reads it again
				if !isCode(err, provider.CodeNotFound) {
					p.log.Warn("koyeb sweep skips a sandbox it cannot read", "app", s.AppID,
						"service", s.ID, "err", err)
				}
				continue
			}
			var have map[string]string
			_ = json.Unmarshal([]byte(envOf(dep)[labelsEnv]), &have)
			sum = provider.Summary{
				Ref:       refFor(s.AppID, s.ID, s.CreatedAt, dep),
				Labels:    have,
				CreatedAt: s.CreatedAt.UTC(),
			}
			p.remember(s, sum)
		}
		// The state and lifetime change under a cached summary, so each listing reads them anew
		if st, ok := serviceStatus(s.Status); ok {
			sum.State = st
		}
		sum.ExpiresAt = expiresAt(s)
		if hasLabels(sum.Labels, labels) {
			owned = append(owned, sum)
		}
	}
	return owned, nil
}

// ListLabelled reads the deployments of up to limit of the bn- services from the cursor on, the
// cursor being the number of services already read. Koyeb needs one read for each.
func (p *Provider) ListLabelled(
	ctx context.Context, labels map[string]string, cursor string, limit int,
) (provider.ForeignPage, error) {
	items, err := p.sandboxServices(ctx)
	if err != nil {
		return provider.ForeignPage{}, err
	}
	slices.SortFunc(items, func(a, b serviceRecord) int { return strings.Compare(a.ID, b.ID) })
	start, _ := strconv.Atoi(cursor)
	start = min(max(start, 0), len(items))
	end := min(start+limit, len(items))
	owned, err := p.summaries(ctx, items[start:end], labels)
	if err != nil {
		return provider.ForeignPage{}, err
	}
	out := provider.ForeignPage{Sandboxes: owned}
	if end < len(items) {
		out.Next = strconv.Itoa(end)
	}
	return out, nil
}

// expiresAt is when the service's backstop deletes it, zero when Koyeb gives none
func expiresAt(s serviceRecord) time.Time {
	if s.LifeCycle.DeleteAfterCreate <= 0 {
		return time.Time{}
	}
	return s.CreatedAt.Add(time.Duration(s.LifeCycle.DeleteAfterCreate) * time.Second).UTC()
}

// serviceStatus maps a service or deployment status, false for one Koyeb added since
func serviceStatus(status string) (provider.State, bool) {
	switch status {
	case "PENDING", "PROVISIONING", "SCHEDULED", "ALLOCATING", "STARTING", "RESUMING":
		return provider.StateStarting, true
	case "HEALTHY", "DEGRADED":
		return provider.StateRunning, true
	case "UNHEALTHY":
		return provider.StateDegraded, true
	case "STOPPING", "STOPPED", "CANCELING", "CANCELED", "PAUSING", "PAUSED", "SLEEPING",
		"STASHED":
		return provider.StateStopped, true
	case "ERRORING", "ERROR":
		return provider.StateFailed, true
	}
	return "", false
}

// Inspect reads the service and its newest deployment, a service Koyeb no longer has being gone
func (p *Provider) Inspect(ctx context.Context, ref provider.Ref) (provider.Status, error) {
	if _, err := appID(ref); err != nil {
		return provider.Status{State: provider.StateGone}, nil
	}
	svc, err := p.service(ctx, ref)
	if isCode(err, provider.CodeNotFound) {
		return provider.Status{State: provider.StateGone}, nil
	}
	if err != nil {
		return provider.Status{}, err
	}
	dep, err := p.deployment(ctx, svc.LatestDeploymentID)
	if err != nil {
		return provider.Status{}, err
	}
	state, ok := serviceStatus(dep.Status)
	if !ok {
		return provider.Status{}, internal(fmt.Errorf("koyeb deployment %s is %s", dep.ID,
			dep.Status))
	}
	st := provider.Status{State: state, Reason: dep.Status, ExpiresAt: expiresAt(svc)}
	if state == provider.StateFailed || state == provider.StateStopped {
		// The event says whether the platform or the process ended it, and may be missing
		if msg := p.lastEvent(ctx, dep.ID); msg != "" {
			st.Reason = msg
		}
	}
	return st, nil
}

// lastEvent is the type and message of the newest instance event, empty when it cannot be read
func (p *Provider) lastEvent(ctx context.Context, deployment string) string {
	inst, ok, err := p.api.newestInstance(ctx, deployment)
	if err != nil || !ok {
		return ""
	}
	ev, ok, err := p.api.newestInstanceEvent(ctx, inst.ID)
	if err != nil || !ok {
		return ""
	}
	return strings.TrimSpace(ev.Type + " " + ev.Message)
}

// knownSummary returns a copy of what an earlier listing read from the service's deployment
func (p *Provider) knownSummary(s serviceRecord) (provider.Summary, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k, ok := p.known[s.ID]
	if !ok || k.deployment != s.LatestDeploymentID {
		return provider.Summary{}, false
	}
	sum := k.summary
	sum.Ref.Meta = maps.Clone(sum.Ref.Meta)
	sum.Labels = maps.Clone(sum.Labels)
	return sum, true
}

func (p *Provider) remember(s serviceRecord, sum provider.Summary) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sum.Ref.Meta = maps.Clone(sum.Ref.Meta)
	sum.Labels = maps.Clone(sum.Labels)
	p.known[s.ID] = knownService{deployment: s.LatestDeploymentID, summary: sum}
}

// forgetAllBut drops what it knew of services a whole listing no longer holds
func (p *Provider) forgetAllBut(items []serviceRecord) {
	live := map[string]bool{}
	for _, s := range items {
		live[s.ID] = true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	maps.DeleteFunc(p.known, func(id string, _ knownService) bool { return !live[id] })
}

func hasLabels(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}
	return true
}

/* Create outcomes */

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

// unsettled is a create whose lookup failed, so whether it made a sandbox stays unknown
func unsettled(err, lookup error) error {
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
