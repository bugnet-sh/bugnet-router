// Package e2b places sandboxes on E2B through its REST API and the envd agent inside each one.
//
// E2B fixes vCPU and memory per template, so every size is a template the account has built.
// Create sends the labels as metadata, the lifetime as the timeout, public traffic and auto
// pause off, and an Idempotency-Key derived from the request's key, and it keeps the envd access
// token from the reply and the creation time in the ref, so SetLifetime can hold the plan's
// lifetime cap. Calls to envd go through E2B's sandbox proxy, routed by headers, and run as root.
// Commands go over envd's Connect Process service, file contents over its /files endpoint and
// listings over its Filesystem service. An upload cannot carry a mode, so WriteFile runs chmod
// when the mode is not the one envd gives new files.
package e2b

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/e2b/api"
	filesystempb "github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/filesystem"
	"github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/filesystem/filesystemconnect"
	processpb "github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/process"
	"github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/process/processconnect"
)

const (
	providerName  = "e2b"
	defaultDomain = "e2b.app"
	envdPort      = "49983"
	rootUser      = "root"
	// keepaliveSeconds asks envd for keepalive events at the interval the JS SDK uses
	keepaliveSeconds    = "50"
	defaultMaxLifetime  = time.Hour
	defaultFileMode     = fs.FileMode(0o644)
	idempotencyKeyChars = 32
	listPageSize        = 100
	maxBodyBytes        = 16 << 20
	// maxMessage bounds one envd message, as boxd's gRPC receive limit does
	maxMessage       = 16 << 20
	maxErrorBytes    = 64 << 10
	maxAttempts      = 4
	defaultRetryBase = 100 * time.Millisecond
	// maxRetryWait is the longest wait spent on a retry, a longer one goes back to the router
	maxRetryWait      = 2 * time.Second
	defaultRetryAfter = time.Second
	defaultPoll       = 500 * time.Millisecond
	// healthTimeout bounds one health probe as the JS SDK does, so a hung probe is retried
	healthTimeout = 5 * time.Second
	// Hourly micro-USD from E2B's per-second vCPU and memory rates, e2b.dev/pricing 2026-11-04
	vcpuHourMicros = 50_400
	gibHourMicros  = 16_200
	// arch is assumed because E2B builds templates for linux/amd64 and reports no architecture
	arch = "amd64"
)

// rootAuth names the user envd runs a call as, with an empty password
var rootAuth = "Basic " + base64.StdEncoding.EncodeToString([]byte(rootUser+":"))

// stableHosts serve envd for every sandbox at sandbox.<domain>, the list the JS SDK keeps
var stableHosts = map[string]bool{
	"e2b.app": true, "e2b.dev": true, "e2b.pro": true, "e2b-staging.dev": true,
	"e2b-tango.dev": true, "e2b-juliett.dev": true, "foxtrot-2.e2b.dev": true,
}

// capacityCodes are E2B error codes saying no sandbox was made for lack of room
var capacityCodes = map[string]bool{
	"sandbox_capacity_unavailable": true, "sandbox_placement_timeout": true,
	"sandbox_no_compatible_node": true, "sandbox_create_failed": true,
}

var (
	concurrentWord = regexp.MustCompile(`(?i)concurren`)
	bannedWord     = regexp.MustCompile(`(?i)\bbanned\b`)
	billingWord    = regexp.MustCompile(`(?i)blocked|billing|spending|payment|credit|funds`)
	capacityWord   = regexp.MustCompile(`(?i)capacity`)
)

// Template is one template built on the account, sold as the size of the same name
type Template struct {
	Name       string
	TemplateID string
	VCPU       int
	MemoryMiB  int
	DiskGiB    int
}

func (t Template) size() provider.Size {
	return provider.Size{Name: t.Name, Resources: provider.Resources{
		VCPU: t.VCPU, MemoryMiB: t.MemoryMiB, DiskGiB: t.DiskGiB,
	}}
}

// ParseTemplates reads comma-separated size=templateID entries, then vcpu, memMiB and diskGiB
func ParseTemplates(s string) ([]Template, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out []Template
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		name, spec, ok := strings.Cut(entry, "=")
		parts := strings.Split(spec, ":")
		if !ok || len(parts) < 4 {
			return nil, fmt.Errorf("e2b: template %q is not name=templateID:vcpu:memMiB:diskGiB",
				entry)
		}
		// A template ID may end in a tag after a colon, so the numbers are read from the end
		n := len(parts) - 3
		var nums [3]int
		for i, p := range parts[n:] {
			v, err := strconv.Atoi(p)
			if err != nil {
				return nil, fmt.Errorf("e2b: template %q: %w", entry, err)
			}
			nums[i] = v
		}
		out = append(out, Template{
			Name: name, TemplateID: strings.Join(parts[:n], ":"),
			VCPU: nums[0], MemoryMiB: nums[1], DiskGiB: nums[2],
		})
	}
	if err := validate(out); err != nil {
		return nil, err
	}
	return out, nil
}

func validate(templates []Template) error {
	seen := map[string]bool{}
	for _, t := range templates {
		switch {
		case t.Name == "" || t.TemplateID == "":
			return fmt.Errorf("e2b: template %+v needs a name and a template ID", t)
		case t.VCPU < 1 || t.MemoryMiB < 1 || t.DiskGiB < 0:
			return fmt.Errorf("e2b: template %s needs vCPUs and memory", t.Name)
		case seen[t.Name]:
			return fmt.Errorf("e2b: two templates are named %s", t.Name)
		}
		seen[t.Name] = true
	}
	return nil
}

// Options configures the adapter, BaseURL and EnvdURL replace E2B's hosts in tests
type Options struct {
	APIKey string
	// Domain is the region's domain, e2b.app when empty and e2b-juliett.dev for EU-West-1
	Domain    string
	Templates []Template
	// MaxLifetime caps a sandbox's lifetime, 1 h for the Hobby plan when zero and 24 h on Pro
	MaxLifetime time.Duration
	// BinaryCodec talks to envd in binary protobuf instead of the JSON the SDKs use
	BinaryCodec bool
	BaseURL     string
	EnvdURL     string
	maxMessage  int // replaces maxMessage in tests
}

// Provider places sandboxes on E2B, it keeps no state beyond its configuration
type Provider struct {
	api          *api.Client
	rest         *http.Client
	envd         *http.Client
	restURL      string
	envdURL      string
	domain       string
	templates    []Template
	maxLifetime  time.Duration
	codec        []connect.ClientOption
	pollInterval time.Duration
	retryBase    time.Duration
}

// New checks the options and makes no call to E2B
func New(opts Options) (*Provider, error) {
	if opts.APIKey == "" {
		return nil, errors.New("e2b: an API key is required")
	}
	if opts.MaxLifetime < 0 {
		return nil, errors.New("e2b: the max lifetime is negative")
	}
	if err := validate(opts.Templates); err != nil {
		return nil, err
	}
	domain := cmp.Or(opts.Domain, defaultDomain)
	p := &Provider{
		rest: &http.Client{CheckRedirect: provider.NoRedirect},
		envd: &http.Client{
			Transport:     proxyTransport{base: http.DefaultTransport},
			CheckRedirect: provider.NoRedirect,
		},
		restURL:      cmp.Or(opts.BaseURL, "https://api."+domain),
		envdURL:      opts.EnvdURL,
		domain:       domain,
		templates:    byCost(opts.Templates),
		maxLifetime:  cmp.Or(opts.MaxLifetime, defaultMaxLifetime),
		pollInterval: defaultPoll,
		retryBase:    defaultRetryBase,
	}
	// envd runs in the sandbox, so every message it sends is bounded, decompressed size too
	p.codec = []connect.ClientOption{connect.WithReadMaxBytes(cmp.Or(opts.maxMessage, maxMessage))}
	if !opts.BinaryCodec {
		p.codec = append(p.codec, connect.WithProtoJSON())
	}
	key := opts.APIKey
	client, err := api.NewClient(p.restURL, api.WithHTTPClient(p.rest),
		api.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
			req.Header.Set("X-API-Key", key)
			return nil
		}))
	if err != nil {
		return nil, fmt.Errorf("e2b: %w", err)
	}
	p.api = client
	return p, nil
}

// byCost orders templates cheapest first so SizeFor picks the cheapest that fits
func byCost(templates []Template) []Template {
	out := slices.Clone(templates)
	slices.SortStableFunc(out, func(a, b Template) int {
		return cmp.Compare(hourlyMicros(a.size().Resources), hourlyMicros(b.size().Resources))
	})
	return out
}

func hourlyMicros(r provider.Resources) int64 {
	return int64(r.VCPU)*vcpuHourMicros + (int64(r.MemoryMiB)*gibHourMicros+1023)/1024
}

// Name returns "e2b"
func (p *Provider) Name() string { return providerName }

// Capabilities reports previews, egress rules and the plan's lifetime cap, and no SSH
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Preview: true, EgressControl: true, NeedsRunnerUpload: true, MaxLifetime: p.maxLifetime,
	}
}

// SizeFor picks the cheapest template covering floor, E2B sandboxes have no GPU
func (p *Provider) SizeFor(floor provider.Resources) (provider.Size, bool) {
	if floor.GPUCount > 0 || floor.GPUMemoryGiB > 0 {
		return provider.Size{}, false
	}
	for _, t := range p.templates {
		if t.VCPU >= floor.VCPU && t.MemoryMiB >= floor.MemoryMiB && t.DiskGiB >= floor.DiskGiB {
			return t.size(), true
		}
	}
	return provider.Size{}, false
}

// HourlyCostMicros prices the size's vCPUs and memory while it runs, disk is free
func (p *Provider) HourlyCostMicros(size provider.Size) int64 {
	return hourlyMicros(size.Resources)
}

// BillingIncrement is one second, E2B meters by the second
func (p *Provider) BillingIncrement() time.Duration { return time.Second }

/* Sandboxes */

// Create starts a sandbox from the size's template, a repeated key replays the first create
func (p *Provider) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	i := slices.IndexFunc(p.templates, func(t Template) bool { return t.Name == req.Size.Name })
	if i < 0 {
		return provider.Ref{}, invalid(fmt.Errorf("e2b has no template for size %q",
			req.Size.Name))
	}
	if req.Lifetime <= 0 || req.Lifetime > p.maxLifetime {
		return provider.Ref{}, invalid(fmt.Errorf("lifetime %v is not within e2b's limit of %v",
			req.Lifetime, p.maxLifetime))
	}
	// V2 has no secure flag because E2B issues an envd access token to every v2 sandbox
	body := api.NewSandboxV2{
		TemplateID: p.templates[i].TemplateID,
		Timeout:    new(seconds(req.Lifetime)),
		AutoPause:  new(false),
		Network:    &api.SandboxNetworkConfig{AllowPublicTraffic: new(false)},
	}
	if len(req.Labels) > 0 {
		body.Metadata = new(api.SandboxMetadata(maps.Clone(req.Labels)))
	}
	params := &api.PostV2SandboxesParams{}
	if req.IdempotencyKey != "" {
		params.IdempotencyKey = new(idempotencyKey(req.IdempotencyKey))
	}
	// Taken before sending and cut to the second so SetLifetime never undercounts the runtime
	created := time.Now().UTC().Truncate(time.Second)
	_, raw, err := p.send(ctx, true, func(ctx context.Context) (*http.Response, error) {
		return p.api.PostV2Sandboxes(ctx, params, body)
	})
	if err != nil {
		return provider.Ref{}, err
	}
	var sb api.Sandbox
	if err := json.Unmarshal(raw, &sb); err != nil || sb.SandboxID == "" {
		return provider.Ref{}, &provider.Error{
			Code: provider.CodeOutcomeUnknown,
			Err:  fmt.Errorf("e2b create reply names no sandbox: %.200s", raw),
		}
	}
	ref := newRef(sb.SandboxID)
	ref.Meta["created_at"] = created.Format(time.RFC3339)
	ref.Meta["domain"] = p.domain
	if sb.Domain != nil && *sb.Domain != "" {
		ref.Meta["domain"] = *sb.Domain
	}
	if sb.EnvdAccessToken != nil && *sb.EnvdAccessToken != "" {
		ref.Meta["envd_token"] = *sb.EnvdAccessToken
	}
	return ref, nil
}

// idempotencyKey fits any key to E2B's ^[a-z0-9-]+$ pattern and keeps distinct keys apart
func idempotencyKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:idempotencyKeyChars]
}

// seconds rounds up and never returns 0, which E2B refuses at create
func seconds(d time.Duration) int32 {
	s := (d + time.Second - 1) / time.Second
	return int32(min(max(s, 1), math.MaxInt32))
}

func newRef(id string) provider.Ref {
	return provider.Ref{Provider: providerName, ID: id, Meta: map[string]string{"arch": arch}}
}

// sandboxID returns the sandbox id of a ref this provider made
func sandboxID(ref provider.Ref) (string, error) {
	if ref.Provider != providerName || ref.ID == "" {
		return "", &provider.Error{
			Code: provider.CodeNotFound,
			Err:  fmt.Errorf("sandbox %s/%s is not an e2b sandbox", ref.Provider, ref.ID),
		}
	}
	return ref.ID, nil
}

// WaitReady polls envd's health check, the sandbox already runs once Create returns
func (p *Provider) WaitReady(ctx context.Context, ref provider.Ref) error {
	id, err := sandboxID(ref)
	if err != nil {
		return err
	}
	var last error
	for {
		err := p.health(ctx, ref)
		if err == nil {
			return nil
		}
		if e, ok := provider.AsError(err); ok && (e.Code == provider.CodeNotFound ||
			e.Code == provider.CodeAuth || e.Code == provider.CodeInvalid) {
			return err
		}
		if ctx.Err() == nil {
			last = err
		}
		if !sleep(ctx, p.pollInterval) {
			if last == nil {
				return fmt.Errorf("e2b sandbox %s envd not up: %w", id, ctx.Err())
			}
			return fmt.Errorf("e2b sandbox %s envd not up: %w, last %w", id, ctx.Err(), last)
		}
	}
}

// SetLifetime replaces the sandbox's timeout, which E2B counts from now, within the plan's cap
func (p *Provider) SetLifetime(ctx context.Context, ref provider.Ref, until time.Time) error {
	id, err := sandboxID(ref)
	if err != nil {
		return err
	}
	created, err := p.createdAt(ctx, ref)
	if err != nil {
		return err
	}
	if life := until.Sub(created); life > p.maxLifetime {
		return invalid(fmt.Errorf("lifetime %v from creation is over e2b's limit of %v",
			life.Round(time.Second), p.maxLifetime))
	}
	body := api.SandboxTimeoutRequest{Timeout: seconds(time.Until(until))}
	_, _, err = p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		return p.api.PostSandboxesSandboxIDTimeout(ctx, id, body)
	})
	return err
}

// createdAt reads the creation time Create kept in the ref, or asks E2B when the ref lacks it
func (p *Provider) createdAt(ctx context.Context, ref provider.Ref) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, ref.Meta["created_at"]); err == nil {
		return t, nil
	}
	_, raw, err := p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		return p.api.GetSandboxesSandboxID(ctx, ref.ID)
	})
	if err != nil {
		return time.Time{}, err
	}
	var sb api.SandboxDetail
	if err := json.Unmarshal(raw, &sb); err != nil || sb.StartedAt.IsZero() {
		return time.Time{}, &provider.Error{
			Code: provider.CodeInternal,
			Err:  fmt.Errorf("e2b sandbox %s reply has no start time: %.200s", ref.ID, raw),
		}
	}
	return sb.StartedAt, nil
}

// Destroy kills the sandbox and treats one already gone as destroyed
func (p *Provider) Destroy(ctx context.Context, ref provider.Ref) error {
	id, err := sandboxID(ref)
	if err != nil {
		return nil
	}
	_, _, err = p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		return p.api.DeleteSandboxesSandboxID(ctx, id)
	})
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
		return nil
	}
	return err
}

// ListOwned pages through running and paused sandboxes whose metadata carries every label
func (p *Provider) ListOwned(
	ctx context.Context, labels map[string]string,
) ([]provider.Summary, error) {
	params := &api.GetV2SandboxesParams{
		State: &[]api.SandboxState{api.Running, api.Paused},
		Limit: new(api.PaginationLimit(listPageSize)),
	}
	if len(labels) > 0 {
		params.Metadata = new(metadataQuery(labels))
	}
	var owned []provider.Summary
	for {
		h, raw, err := p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
			return p.api.GetV2Sandboxes(ctx, params)
		})
		if err != nil {
			return nil, err
		}
		var page []api.ListedSandbox
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, &provider.Error{
				Code: provider.CodeInternal, Err: fmt.Errorf("e2b sandbox list: %w", err),
			}
		}
		for _, s := range page {
			var md map[string]string
			if s.Metadata != nil {
				md = *s.Metadata
			}
			if hasLabels(md, labels) {
				owned = append(owned, provider.Summary{
					Ref: newRef(s.SandboxID), Labels: md, CreatedAt: s.StartedAt.UTC(),
				})
			}
		}
		next := h.Get("X-Next-Token")
		if next == "" {
			return owned, nil
		}
		params.NextToken = &next
	}
}

// metadataQuery builds the list filter, a query string of keys and values each URL encoded
func metadataQuery(labels map[string]string) string {
	q := url.Values{}
	for k, v := range labels {
		q.Set(uriComponent(k), uriComponent(v))
	}
	return q.Encode()
}

// uriComponent escapes like JavaScript's encodeURIComponent, which the SDKs use for the filter
func uriComponent(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

func hasLabels(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}
	return true
}

/* REST calls */

// send retries a REST call that failed before sending or whose answer says Should-Retry
func (p *Provider) send(
	ctx context.Context, create bool, call func(context.Context) (*http.Response, error),
) (http.Header, []byte, error) {
	for attempt := 1; ; attempt++ {
		var sent atomic.Bool
		trace := &httptrace.ClientTrace{WroteHeaders: func() { sent.Store(true) }}
		resp, err := call(httptrace.WithClientTrace(ctx, trace))
		if err != nil {
			if !sent.Load() && attempt < maxAttempts && retrySleep(ctx, p.backoff(attempt)) {
				continue
			}
			return nil, nil, transportError(err, sent.Load(), create)
		}
		raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
		_ = resp.Body.Close()
		if err != nil {
			return nil, nil, transportError(err, true, create)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp.Header, raw, nil
		}
		wait, retry := p.retryWait(resp.Header, attempt)
		if retry && attempt < maxAttempts && retrySleep(ctx, wait) {
			continue
		}
		return nil, nil, restError(resp.StatusCode, resp.Header, raw)
	}
}

func (p *Provider) backoff(attempt int) time.Duration {
	return p.retryBase << (attempt - 1)
}

// retryWait follows Retry-After or backs off, and gives up on a wait the router should decide
func (p *Provider) retryWait(h http.Header, attempt int) (time.Duration, bool) {
	if !strings.EqualFold(strings.TrimSpace(h.Get("Should-Retry")), "true") {
		return 0, false
	}
	wait := p.backoff(attempt)
	if d, ok := retryAfter(h); ok {
		wait = d
	}
	return wait, wait <= maxRetryWait
}

func retryAfter(h http.Header) (time.Duration, bool) {
	s, err := strconv.Atoi(strings.TrimSpace(h.Get("Retry-After")))
	if err != nil || s < 0 {
		return 0, false
	}
	return time.Duration(s) * time.Second, true
}

// retrySleep skips a wait that would outlast ctx, since the retry could not finish in time
func retrySleep(ctx context.Context, d time.Duration) bool {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < d {
		return false
	}
	return sleep(ctx, d)
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

// transportError reports a create that may have reached E2B as an unknown outcome
func transportError(err error, sent, create bool) error {
	switch {
	case sent && create:
		return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: err}
	case sent:
		return &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	return &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: err}
}

// restError classifies an error answer by its error_code, then its status and message
func restError(status int, h http.Header, raw []byte) error {
	var body api.Error
	_ = json.Unmarshal(raw, &body)
	msg := cmp.Or(body.Message, strings.TrimSpace(string(raw)), http.StatusText(status))
	var code string
	if body.ErrorCode != nil {
		code = *body.ErrorCode
	}
	e := &provider.Error{
		Code: provider.CodeInternal,
		Err:  fmt.Errorf("e2b answered %d %s: %s", status, cmp.Or(code, "error"), msg),
	}
	switch {
	case code == "idempotency_outcome_unknown" || code == "idempotency_in_progress":
		e.Code = provider.CodeOutcomeUnknown
	case code == "sandbox_create_refused":
		e.CanFailOver = true
	case capacityCodes[code]:
		e.Code, e.CanFailOver = provider.CodeCapacity, true
	case status == http.StatusTooManyRequests && concurrentWord.MatchString(msg):
		e.Code, e.CanFailOver = provider.CodeQuota, true
	case status == http.StatusTooManyRequests:
		e.Code, e.CanFailOver, e.RetryAfter = provider.CodeRateLimited, true, defaultRetryAfter
		if d, ok := retryAfter(h); ok {
			e.RetryAfter = d
		}
	case status == http.StatusPaymentRequired:
		e.Code, e.CanFailOver = provider.CodeUnfunded, true
	case status == http.StatusForbidden && bannedWord.MatchString(msg):
		e.Code, e.CanFailOver = provider.CodeAbuseLock, true
	case status == http.StatusForbidden && billingWord.MatchString(msg):
		e.Code, e.CanFailOver = provider.CodeUnfunded, true
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		e.Code, e.CanFailOver = provider.CodeAuth, true
	case status == http.StatusNotFound:
		e.Code = provider.CodeNotFound
	case status == http.StatusBadRequest || status == http.StatusConflict ||
		status == http.StatusUnprocessableEntity:
		e.Code = provider.CodeInvalid
	case status == http.StatusServiceUnavailable ||
		(status >= 500 && capacityWord.MatchString(msg)):
		e.Code, e.CanFailOver = provider.CodeCapacity, true
	}
	return e
}

/* envd */

// envdBase is the envd host, shared per domain where E2B serves one and per sandbox elsewhere
func (p *Provider) envdBase(ref provider.Ref) string {
	if p.envdURL != "" {
		return p.envdURL
	}
	domain := cmp.Or(ref.Meta["domain"], p.domain)
	if stableHosts[domain] {
		return "https://sandbox." + domain
	}
	return "https://" + envdPort + "-" + ref.ID + "." + domain
}

// envdHeaders route a call through E2B's proxy to the sandbox's envd and authorize it there
func envdHeaders(h http.Header, ref provider.Ref) {
	h.Set("E2b-Sandbox-Id", ref.ID)
	h.Set("E2b-Sandbox-Port", envdPort)
	if token := ref.Meta["envd_token"]; token != "" {
		h.Set("X-Access-Token", token)
	}
}

// proxyTransport keeps the message of the proxy's 502 answers, which Connect drops for streams
type proxyTransport struct{ base http.RoundTripper }

type proxyError struct{ message string }

func (e *proxyError) Error() string { return "e2b sandbox proxy: " + e.message }

func (t proxyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusBadGateway {
		return resp, err
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	_ = resp.Body.Close()
	var body struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal(raw, &body)
	return nil, &proxyError{message: cmp.Or(body.Message, strings.TrimSpace(string(raw)),
		resp.Status)}
}

// envdError classifies a failed call to envd, refused by the proxy, by envd or by the network
func envdError(err error) error {
	if _, ok := provider.AsError(err); ok {
		return err
	}
	e := &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: err}
	var px *proxyError
	if errors.As(err, &px) {
		if strings.Contains(px.message, "was not found") {
			e.Code, e.CanFailOver = provider.CodeNotFound, false
		}
		return e
	}
	switch connect.CodeOf(err) {
	case connect.CodeNotFound:
		e.Code, e.CanFailOver = provider.CodeNotFound, false
	case connect.CodeInvalidArgument, connect.CodeFailedPrecondition, connect.CodeOutOfRange,
		connect.CodeAlreadyExists:
		e.Code, e.CanFailOver = provider.CodeInvalid, false
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		e.Code = provider.CodeAuth
	case connect.CodeResourceExhausted:
		e.Code = provider.CodeRateLimited
	}
	return e
}

// filesError classifies an error answer of envd's HTTP API, whose body is {code, message}
func filesError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	var body struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal(raw, &body)
	msg := cmp.Or(body.Message, strings.TrimSpace(string(raw)), resp.Status)
	e := &provider.Error{
		Code: provider.CodeInternal, CanFailOver: true,
		Err: fmt.Errorf("e2b envd answered %d: %s", resp.StatusCode, msg),
	}
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusNotAcceptable, http.StatusInsufficientStorage:
		e.Code, e.CanFailOver = provider.CodeInvalid, false
	case http.StatusUnauthorized, http.StatusForbidden:
		e.Code = provider.CodeAuth
	case http.StatusNotFound:
		e.Code, e.CanFailOver = provider.CodeNotFound, false
	case http.StatusTooManyRequests:
		e.Code = provider.CodeRateLimited
	}
	return e
}

func drainClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBytes))
	_ = resp.Body.Close()
}

// health asks envd's health check, which answers without the access token
func (p *Provider) health(ctx context.Context, ref provider.Ref) error {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.envdBase(ref)+"/health", nil)
	if err != nil {
		return &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	envdHeaders(req.Header, ref)
	resp, err := p.envd.Do(req)
	if err != nil {
		return envdError(err)
	}
	defer drainClose(resp)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return filesError(resp)
}

/* Exec */

// Exec starts argv as root without a shell or stdin and returns once envd reports it started
func (p *Provider) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	if _, err := sandboxID(ref); err != nil {
		return nil, err
	}
	if len(spec.Argv) == 0 {
		return nil, invalid(errors.New("exec needs a command"))
	}
	cfg := &processpb.ProcessConfig{Cmd: spec.Argv[0], Args: spec.Argv[1:], Envs: spec.Env}
	if spec.Cwd != "" {
		cfg.Cwd = &spec.Cwd
	}
	req := connect.NewRequest(&processpb.StartRequest{Process: cfg, Stdin: proto.Bool(false)})
	envdHeaders(req.Header(), ref)
	req.Header().Set("Authorization", rootAuth)
	req.Header().Set("Keepalive-Ping-Interval", keepaliveSeconds)

	sctx, cancel := context.WithCancel(ctx)
	client := processconnect.NewProcessClient(p.envd, p.envdBase(ref), p.codec...)
	stream, err := client.Start(sctx, req)
	if err != nil {
		cancel()
		return nil, envdError(err)
	}
	s := &execStream{stream: stream, cancel: cancel}
	for !s.started && !s.done {
		s.next()
	}
	if s.err != nil {
		return nil, s.err
	}
	return s, nil
}

type execStream struct {
	stream  *connect.ServerStreamForClient[processpb.StartResponse]
	cancel  context.CancelFunc
	pending []byte
	started bool
	end     *processpb.ProcessEvent_EndEvent
	err     error
	done    bool
}

func (s *execStream) Read(p []byte) (int, error) {
	for len(s.pending) == 0 {
		if s.done {
			if s.err != nil {
				return 0, s.err
			}
			return 0, io.EOF
		}
		s.next()
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// next takes one event, keeping stdout and the end event and dropping stderr and keepalives
func (s *execStream) next() {
	if !s.stream.Receive() {
		s.done = true
		if err := s.stream.Err(); err != nil {
			s.err = envdError(err)
		}
		s.cancel()
		_ = s.stream.Close()
		return
	}
	switch ev := s.stream.Msg().GetEvent().GetEvent().(type) {
	case *processpb.ProcessEvent_Start:
		s.started = true
	case *processpb.ProcessEvent_Data:
		if out := ev.Data.GetStdout(); len(s.pending) == 0 {
			s.pending = out
		} else {
			s.pending = append(s.pending, out...)
		}
	case *processpb.ProcessEvent_End:
		s.end, s.done = ev.End, true
		s.cancel()
		_ = s.stream.Close()
	}
}

func (s *execStream) Wait() (int, error) {
	for !s.done {
		s.next()
	}
	s.pending = nil
	if s.err != nil {
		return -1, s.err
	}
	if s.end == nil {
		return -1, &provider.Error{
			Code: provider.CodeOutcomeUnknown,
			Err:  errors.New("e2b ended the command stream without an exit code"),
		}
	}
	return int(s.end.GetExitCode()), nil
}

// Close cancels the stream, safe to call while another goroutine reads
func (s *execStream) Close() error {
	s.cancel()
	return nil
}

/* Files */

func (p *Provider) filesRequest(
	ctx context.Context, method string, ref provider.Ref, name string, body io.Reader,
) (*http.Request, error) {
	q := url.Values{"path": {name}, "username": {rootUser}}
	req, err := http.NewRequestWithContext(ctx, method, p.envdBase(ref)+"/files?"+q.Encode(),
		body)
	if err != nil {
		return nil, &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	envdHeaders(req.Header, ref)
	return req, nil
}

// ReadFile asks envd for the byte range and cuts it from the whole file when Range is ignored
func (p *Provider) ReadFile(
	ctx context.Context, ref provider.Ref, name string, offset int64, length int,
) ([]byte, bool, error) {
	if _, err := sandboxID(ref); err != nil {
		return nil, false, err
	}
	if offset < 0 || length < 0 {
		return nil, false, invalid(fmt.Errorf("read of %d bytes at offset %d", length, offset))
	}
	req, err := p.filesRequest(ctx, http.MethodGet, ref, name, nil)
	if err != nil {
		return nil, false, err
	}
	// One byte past the slice tells whether it reaches the end, with or without Content-Range
	last := offset + int64(length)
	req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(last, 10))
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := p.envd.Do(req)
	if err != nil {
		return nil, false, envdError(err)
	}
	defer drainClose(resp)
	switch resp.StatusCode {
	case http.StatusPartialContent:
		data, err := io.ReadAll(io.LimitReader(resp.Body, int64(length)+1))
		if err != nil {
			return nil, false, envdError(err)
		}
		if len(data) > length {
			return data[:length], false, nil
		}
		return data, true, nil
	case http.StatusRequestedRangeNotSatisfiable:
		return nil, true, nil
	case http.StatusOK:
		return readSlice(resp.Body, offset, length)
	}
	return nil, false, filesError(resp)
}

// readSlice cuts the range out of a whole file, reading at most one byte past it
func readSlice(r io.Reader, offset int64, length int) ([]byte, bool, error) {
	if _, err := io.CopyN(io.Discard, r, offset); errors.Is(err, io.EOF) {
		return nil, true, nil
	} else if err != nil {
		return nil, false, envdError(err)
	}
	data, err := io.ReadAll(io.LimitReader(r, int64(length)))
	if err != nil {
		return nil, false, envdError(err)
	}
	if len(data) < length {
		return data, true, nil
	}
	_, err = io.ReadFull(r, make([]byte, 1))
	if errors.Is(err, io.EOF) {
		return data, true, nil
	}
	if err != nil {
		return nil, false, envdError(err)
	}
	return data, false, nil
}

// WriteFile uploads data, envd creates missing parents, then runs chmod for another mode
func (p *Provider) WriteFile(
	ctx context.Context, ref provider.Ref, name string, data []byte, mode fs.FileMode,
) error {
	if _, err := sandboxID(ref); err != nil {
		return err
	}
	req, err := p.filesRequest(ctx, http.MethodPost, ref, name, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := p.envd.Do(req)
	if err != nil {
		return envdError(err)
	}
	defer drainClose(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return filesError(resp)
	}
	if mode.Perm() == defaultFileMode {
		return nil
	}
	return p.chmod(ctx, ref, name, mode.Perm())
}

func (p *Provider) chmod(
	ctx context.Context, ref provider.Ref, name string, mode fs.FileMode,
) error {
	argv := []string{"chmod", "--", strconv.FormatUint(uint64(mode), 8), name}
	stream, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: argv})
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()
	if _, err := io.Copy(io.Discard, stream); err != nil {
		return err
	}
	code, err := stream.Wait()
	if err != nil {
		return err
	}
	if code != 0 {
		return &provider.Error{
			Code: provider.CodeInternal,
			Err:  fmt.Errorf("e2b chmod %o of %s exited %d", mode, name, code),
		}
	}
	return nil
}

// ListFiles asks envd to walk the directory down to depth levels in one call
func (p *Provider) ListFiles(
	ctx context.Context, ref provider.Ref, name string, depth int, into *provider.Listing,
) error {
	if _, err := sandboxID(ref); err != nil {
		return err
	}
	if depth < 1 {
		return invalid(fmt.Errorf("depth %d is below 1", depth))
	}
	req := connect.NewRequest(&filesystempb.ListDirRequest{Path: name, Depth: uint32(depth)})
	envdHeaders(req.Header(), ref)
	req.Header().Set("Authorization", rootAuth)
	client := filesystemconnect.NewFilesystemClient(p.envd, p.envdBase(ref), p.codec...)
	resp, err := client.ListDir(ctx, req)
	if err != nil {
		return envdError(err)
	}
	for _, e := range resp.Msg.GetEntries() {
		if !into.Add(fileEntry(name, e)) {
			break
		}
	}
	return nil
}

func fileEntry(dir string, e *filesystempb.EntryInfo) provider.FileEntry {
	mode := fs.FileMode(e.GetMode()).Perm()
	switch {
	case e.GetIsSymlink() || e.GetType() == filesystempb.FileType_FILE_TYPE_SYMLINK:
		mode |= fs.ModeSymlink
	case e.GetType() == filesystempb.FileType_FILE_TYPE_DIRECTORY:
		mode |= fs.ModeDir
	}
	entry := provider.FileEntry{
		Path:  cmp.Or(e.GetPath(), path.Join(dir, e.GetName())),
		Size:  e.GetSize(),
		IsDir: mode.IsDir(),
		Mode:  mode,
	}
	if e.GetModifiedTime() != nil {
		entry.Modified = e.GetModifiedTime().AsTime().UTC()
	}
	return entry
}

func invalid(err error) error {
	return &provider.Error{Code: provider.CodeInvalid, Err: err}
}
