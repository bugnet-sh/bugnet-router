// Package boat places sandboxes on Boat through its public REST API.
//
// Every sandbox is created with noEnv, snapshots and monitoring off and failFast on, so it gets
// none of the account's secrets, keeps nothing after it ends and is refused at once when no
// machine is ready. Its TTL is the lifetime, which SetLifetime moves by setting a new TTL from
// now. The request's key travels as an Idempotency-Key, so a repeated create returns the first
// sandbox. Commands run detached and their output is followed on the attach stream, which the
// adapter reopens from the delivered offsets whenever it ends early. File calls are JSON with a
// 5 MiB cap, so a larger upload goes through a signed upload URL and a larger read through tail
// in the sandbox. The client in ./api is generated from the vendored OpenAPI document.
package boat

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptrace"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/boat/api"
)

const (
	providerName   = "boat"
	defaultBaseURL = "https://boat.dev/api/v1"
	// maxTTL is the longest ttlSeconds Boat takes on create and update
	maxTTL            = 30 * 24 * time.Hour
	maxLabels         = 20
	maxLabelValue     = 256
	pageLimit         = 200
	maxReplyBytes     = 16 << 20
	maxErrorBytes     = 64 << 10
	defaultPoll       = 500 * time.Millisecond
	defaultReattach   = 500 * time.Millisecond
	defaultRetryAfter = time.Second
	// arch is what every Boat machine type runs, x86_64 per Boat's machine docs
	arch = "amd64"
)

// Ref.Meta keys
const (
	metaArch    = "arch"
	metaCreated = "created_at"
)

var labelKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,62}$`)

// machineType is one Boat machine type and its price in micro-USD per hour
type machineType struct {
	name       string
	resources  provider.Resources
	hourMicros int64
}

// machineTypes are the EU types and USD prices of Boat's machines and pricing pages on 2026-11-04
var machineTypes = []machineType{
	{"small", provider.Resources{VCPU: 2, MemoryMiB: 4 << 10, DiskGiB: 51}, 18_000},
	{"default", provider.Resources{VCPU: 4, MemoryMiB: 8 << 10, DiskGiB: 51}, 36_000},
	{"large", provider.Resources{VCPU: 8, MemoryMiB: 16 << 10, DiskGiB: 111}, 72_000},
	{"xlarge", provider.Resources{VCPU: 16, MemoryMiB: 32 << 10, DiskGiB: 251}, 200_000},
}

// size is one size this adapter sells and the machine type it creates
type size struct {
	name string
	typ  machineType
}

// ParseTypes reads comma-separated size=type entries, where type is one of Boat's machine types
func ParseTypes(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, entry := range strings.Split(s, ",") {
		name, typ, ok := strings.Cut(strings.TrimSpace(entry), "=")
		if !ok || name == "" || typ == "" {
			return nil, fmt.Errorf("boat: type %q is not size=type", entry)
		}
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("boat: size %s is named twice", name)
		}
		out[name] = typ
	}
	if _, err := sizesFor(out); err != nil {
		return nil, err
	}
	return out, nil
}

// sizesFor orders the sizes cheapest first, every machine type under its own name when empty
func sizesFor(bySize map[string]string) ([]size, error) {
	var out []size
	if len(bySize) == 0 {
		for _, t := range machineTypes {
			out = append(out, size{name: t.name, typ: t})
		}
		return out, nil
	}
	for name, typ := range bySize {
		i := slices.IndexFunc(machineTypes, func(t machineType) bool { return t.name == typ })
		if name == "" || i < 0 {
			return nil, fmt.Errorf("boat: size %q needs one of small, default, large or xlarge, "+
				"not %q", name, typ)
		}
		out = append(out, size{name: name, typ: machineTypes[i]})
	}
	slices.SortFunc(out, func(a, b size) int {
		return cmp.Or(cmp.Compare(a.typ.hourMicros, b.typ.hourMicros),
			strings.Compare(a.name, b.name))
	})
	return out, nil
}

// Options configures the adapter, BaseURL replaces Boat's API in tests
type Options struct {
	APIKey string
	// TypeBySize maps each size sold to its machine type, every type by name when empty
	TypeBySize map[string]string
	BaseURL    string
	Log        *slog.Logger // slog.Default when nil
}

// Provider places sandboxes on Boat, it keeps no state beyond its configuration
type Provider struct {
	api *api.Client
	// direct sends uploads to signed URLs, which take no Authorization header
	direct       *http.Client
	log          *slog.Logger
	sizes        []size
	pollInterval time.Duration
	reattachWait time.Duration
	idleTimeout  time.Duration
}

// New checks the options and makes no call to Boat
func New(opts Options) (*Provider, error) {
	if opts.APIKey == "" {
		return nil, errors.New("boat: an API key is required")
	}
	sizes, err := sizesFor(opts.TypeBySize)
	if err != nil {
		return nil, err
	}
	key := opts.APIKey
	client, err := api.NewClient(cmp.Or(opts.BaseURL, defaultBaseURL),
		api.WithHTTPClient(&http.Client{}),
		api.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
			req.Header.Set("Authorization", "Bearer "+key)
			req.Header.Set("User-Agent", "bugnet-router")
			return nil
		}))
	if err != nil {
		return nil, fmt.Errorf("boat: %w", err)
	}
	return &Provider{
		api: client, direct: &http.Client{}, log: cmp.Or(opts.Log, slog.Default()),
		sizes: sizes, pollInterval: defaultPoll, reattachWait: defaultReattach,
		idleTimeout: defaultIdle,
	}, nil
}

// Name returns "boat"
func (p *Provider) Name() string { return providerName }

// Capabilities reports SSH and previews, no GPU or egress rules, and Boat's 30 day TTL
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		SSH: true, Preview: true, NeedsRunnerUpload: true, MaxLifetime: maxTTL,
	}
}

// SizeFor picks the cheapest size covering floor, Boat has no GPU
func (p *Provider) SizeFor(floor provider.Resources) (provider.Size, bool) {
	if floor.GPUCount > 0 || floor.GPUMemoryGiB > 0 {
		return provider.Size{}, false
	}
	for _, s := range p.sizes {
		r := s.typ.resources
		if r.VCPU >= floor.VCPU && r.MemoryMiB >= floor.MemoryMiB && r.DiskGiB >= floor.DiskGiB {
			return provider.Size{Name: s.name, Resources: r}, true
		}
	}
	return provider.Size{}, false
}

// HourlyCostMicros prices a size at its type's rate, and an unknown size as the dearest type
func (p *Provider) HourlyCostMicros(sz provider.Size) int64 {
	if s, ok := p.size(sz.Name); ok {
		return s.typ.hourMicros
	}
	return machineTypes[len(machineTypes)-1].hourMicros
}

// BillingIncrement is one second, Boat meters by the second
func (p *Provider) BillingIncrement() time.Duration { return time.Second }

func (p *Provider) size(name string) (size, bool) {
	i := slices.IndexFunc(p.sizes, func(s size) bool { return s.name == name })
	if i < 0 {
		return size{}, false
	}
	return p.sizes[i], true
}

/* Sandboxes */

// Create asks for a sandbox only if a machine is ready, a repeated key returns the first one
func (p *Provider) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	s, ok := p.size(req.Size.Name)
	if !ok {
		return provider.Ref{}, invalid(fmt.Errorf("boat has no size %q", req.Size.Name))
	}
	if req.Lifetime <= 0 || req.Lifetime > maxTTL {
		return provider.Ref{}, invalid(fmt.Errorf("lifetime %v is not within boat's limit of %v",
			req.Lifetime, maxTTL))
	}
	if err := checkLabels(req.Labels); err != nil {
		return provider.Ref{}, err
	}
	body := api.CreateSandboxRequest{
		Type:       new(api.CreateSandboxRequestType(s.typ.name)),
		TtlSeconds: new(wholeSeconds(req.Lifetime)),
		NoEnv:      new(true),
		Snapshots:  new(false),
		Monitoring: new(false),
		FailFast:   new(true),
	}
	if len(req.Labels) > 0 {
		body.Labels = new(maps.Clone(req.Labels))
	}
	params := &api.CreateParams{}
	if req.IdempotencyKey != "" {
		params.IdempotencyKey = new(idempotencyKey(req.IdempotencyKey))
	}
	created := time.Now().UTC()
	raw, err := p.send(ctx, true, func(ctx context.Context) (*http.Response, error) {
		return p.api.Create(ctx, params, body)
	})
	if err != nil {
		return provider.Ref{}, err
	}
	var reply api.CreateSandboxResponse
	if err := json.Unmarshal(raw, &reply); err != nil || reply.Sandbox.Id == "" {
		return provider.Ref{}, unknown(fmt.Errorf("boat create reply names no sandbox: %.200s",
			raw))
	}
	sb := reply.Sandbox
	if sb.State == api.SandboxStateCancelled {
		return provider.Ref{}, noMachine(sb)
	}
	if sb.CreatedAt != nil {
		created = *sb.CreatedAt
	}
	return newRef(sb.Id, created), nil
}

// checkLabels holds labels to Boat's limits, which refuses the whole create otherwise
func checkLabels(labels map[string]string) error {
	if len(labels) > maxLabels {
		return invalid(fmt.Errorf("boat takes at most %d labels, not %d", maxLabels, len(labels)))
	}
	for k, v := range labels {
		if !labelKey.MatchString(k) {
			return invalid(fmt.Errorf("label key %q does not fit boat's label rules", k))
		}
		if len(v) > maxLabelValue {
			return invalid(fmt.Errorf("label %s is longer than %d bytes", k, maxLabelValue))
		}
	}
	return nil
}

// idempotencyKey fits any request key into Boat's header, which takes at most 255 characters
func idempotencyKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// wholeSeconds rounds up and never returns 0, which Boat refuses
func wholeSeconds(d time.Duration) int {
	if d <= 0 {
		return 1
	}
	return int((d + time.Second - 1) / time.Second)
}

func newRef(id string, created time.Time) provider.Ref {
	return provider.Ref{Provider: providerName, ID: id, Meta: map[string]string{
		metaArch: arch, metaCreated: created.UTC().Truncate(time.Second).Format(time.RFC3339),
	}}
}

// sandboxID returns the sandbox id of a ref this provider made
func sandboxID(ref provider.Ref) (string, error) {
	if ref.Provider != providerName || ref.ID == "" {
		return "", notFound(fmt.Errorf("sandbox %s/%s is not a boat sandbox", ref.Provider,
			ref.ID))
	}
	return ref.ID, nil
}

func noMachine(sb api.Sandbox) error {
	return &provider.Error{
		Code: provider.CodeCapacity, CanFailOver: true,
		Err: fmt.Errorf("boat cancelled sandbox %s without a machine: %s", sb.Id,
			deref(sb.Error)),
	}
}

func (p *Provider) get(ctx context.Context, id string) (api.Sandbox, error) {
	raw, err := p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		return p.api.Get(ctx, id)
	})
	if err != nil {
		return api.Sandbox{}, err
	}
	var reply api.SandboxInfoResponse
	if err := json.Unmarshal(raw, &reply); err != nil {
		return api.Sandbox{}, internal(fmt.Errorf("boat sandbox %s reply: %w", id, err))
	}
	return reply.Sandbox, nil
}

// WaitReady polls until the sandbox is ready, idle or running
func (p *Provider) WaitReady(ctx context.Context, ref provider.Ref) error {
	id, err := sandboxID(ref)
	if err != nil {
		return err
	}
	var last error
	for {
		sb, err := p.get(ctx, id)
		if err == nil {
			switch sb.State {
			case api.SandboxStateReady, api.SandboxStateIdle, api.SandboxStateRunning:
				return nil
			case api.SandboxStateCancelled:
				return noMachine(sb)
			case api.SandboxStateError, api.SandboxStateArchiving, api.SandboxStateArchived:
				return &provider.Error{
					Code: provider.CodeInternal, CanFailOver: true,
					Err: fmt.Errorf("boat sandbox %s is %s: %s", id, sb.State, deref(sb.Error)),
				}
			}
			// The other states, and any Boat adds later, are waited on like provisioning
			err = internal(fmt.Errorf("boat sandbox %s is %s", id, sb.State))
		}
		if e, ok := provider.AsError(err); ok && e.Code != provider.CodeInternal &&
			e.Code != provider.CodeRateLimited && e.Code != provider.CodeOutcomeUnknown {
			return err
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
				return fmt.Errorf("boat sandbox %s not ready: %w", id, ctx.Err())
			}
			return fmt.Errorf("boat sandbox %s not ready: %w, last %w", id, ctx.Err(), last)
		}
	}
}

// SetLifetime sets a TTL that ends at until, since Boat counts a new TTL from now
func (p *Provider) SetLifetime(ctx context.Context, ref provider.Ref, until time.Time) error {
	id, err := sandboxID(ref)
	if err != nil {
		return err
	}
	left := time.Until(until)
	if left > maxTTL {
		return invalid(fmt.Errorf("lifetime %v from now is over boat's limit of %v",
			left.Round(time.Second), maxTTL))
	}
	body := api.UpdateSandboxRequest{TtlSeconds: new(wholeSeconds(left))}
	_, err = p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		return p.api.Update(ctx, id, body)
	})
	return err
}

// Destroy deletes the sandbox and its data and treats one already gone as destroyed
func (p *Provider) Destroy(ctx context.Context, ref provider.Ref) error {
	id, err := sandboxID(ref)
	if err != nil {
		return nil
	}
	params := &api.DeleteSandboxParams{XAsciiConfirmDelete: id}
	_, err = p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		return p.api.DeleteSandbox(ctx, id, params)
	})
	if isCode(err, provider.CodeNotFound) {
		return nil
	}
	return err
}

// ListOwned asks Boat for the sandboxes carrying every label, page by page
func (p *Provider) ListOwned(
	ctx context.Context, labels map[string]string,
) ([]provider.Summary, error) {
	var filter []string
	for k, v := range labels {
		filter = append(filter, k+"="+v)
	}
	slices.Sort(filter)
	params := &api.SandboxesParams{Limit: new(pageLimit)}
	if len(filter) > 0 {
		params.Label = &filter
	}
	var owned []provider.Summary
	for {
		raw, err := p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
			return p.api.Sandboxes(ctx, params)
		})
		if err != nil {
			return nil, err
		}
		var page api.SandboxListResponse
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, internal(fmt.Errorf("boat sandbox list reply: %w", err))
		}
		for _, sb := range page.Sandboxes {
			have := map[string]string{}
			if sb.Labels != nil {
				have = *sb.Labels
			}
			if !hasLabels(have, labels) {
				continue
			}
			s := provider.Summary{Labels: have}
			if sb.CreatedAt != nil {
				s.CreatedAt = sb.CreatedAt.UTC()
			}
			s.Ref = newRef(sb.Id, s.CreatedAt)
			owned = append(owned, s)
		}
		info := page.PageInfo
		if info == nil || !info.HasMore || info.NextCursor == nil || *info.NextCursor == "" {
			return owned, nil
		}
		params.Cursor = info.NextCursor
	}
}

func hasLabels(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}
	return true
}

/* Requests */

// send makes one call and reads a 2xx reply, create marks calls that start something billable
func (p *Provider) send(
	ctx context.Context, create bool, call func(context.Context) (*http.Response, error),
) ([]byte, error) {
	resp, err := p.open(ctx, create, call)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxReplyBytes))
	if err != nil {
		return nil, transportError(fmt.Errorf("boat reply: %w", err), true, create)
	}
	return raw, nil
}

// open makes one call and returns a 2xx response with its body unread
func (p *Provider) open(
	ctx context.Context, create bool, call func(context.Context) (*http.Response, error),
) (*http.Response, error) {
	var sent atomic.Bool
	trace := &httptrace.ClientTrace{WroteHeaders: func() { sent.Store(true) }}
	resp, err := call(httptrace.WithClientTrace(ctx, trace))
	if err != nil {
		return nil, transportError(fmt.Errorf("boat: %w", err), sent.Load(), create)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	_ = resp.Body.Close()
	return nil, apiError(resp.StatusCode, resp.Header, raw, create)
}

/* Errors */

// transportError classifies a call that got no answer, a create that was sent may have run
func transportError(err error, sent, create bool) error {
	switch {
	case sent && create:
		return unknown(err)
	case sent:
		return internal(err)
	}
	return &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: err}
}

// canonical names an error code with the sandbox_ prefix that Boat's boat_ and box_ codes share
func canonical(code string) string {
	for _, prefix := range []string{"boat_", "box_"} {
		if rest, ok := strings.CutPrefix(code, prefix); ok {
			return "sandbox_" + rest
		}
	}
	return code
}

// apiError classifies an error envelope by its code, then its status
func apiError(status int, h http.Header, raw []byte, create bool) error {
	var top map[string]any
	_ = json.Unmarshal(raw, &top)
	inner, _ := top["error"].(map[string]any)
	details, _ := inner["details"].(map[string]any)
	code := canonical(cmp.Or(text(top, "code"), text(inner, "code")))
	msg := cmp.Or(text(top, "message"), text(inner, "message"), strings.TrimSpace(string(raw)),
		http.StatusText(status))
	if id := text(top, "requestId"); id != "" {
		msg += " (request " + id + ")"
	}
	e := &provider.Error{
		Code: provider.CodeInternal,
		Err:  fmt.Errorf("boat answered %d %s: %s", status, cmp.Or(code, "error"), msg),
	}
	client := status >= 400 && status < 500
	switch {
	case code == "no_ready_machine" || code == "out_of_capacity":
		e.Code, e.CanFailOver = provider.CodeCapacity, true
		e.RetryAfter, _ = waitHint(h, top, details)
	case status == http.StatusTooManyRequests && (code == "limit_reached" ||
		code == "member_limit_reached" || code == "trial_compute_limit_reached"):
		e.Code, e.CanFailOver = provider.CodeQuota, true
	case status == http.StatusTooManyRequests:
		e.Code, e.CanFailOver = provider.CodeRateLimited, true
		wait, ok := waitHint(h, top, details)
		if !ok {
			wait = defaultRetryAfter
		}
		e.RetryAfter = wait
	case code == "team_member_cap_reached" || code == "machine_class_plan_required" ||
		code == "trial_machine_class_not_allowed":
		e.Code, e.CanFailOver = provider.CodeQuota, true
	case status == http.StatusPaymentRequired || code == "org_suspended":
		e.Code, e.CanFailOver = provider.CodeUnfunded, true
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		e.Code, e.CanFailOver = provider.CodeAuth, true
	case status == http.StatusNotFound || (client && strings.HasSuffix(code, "not_found")):
		e.Code = provider.CodeNotFound
	case code == "idempotency_in_progress":
		e.Code = provider.CodeOutcomeUnknown
	case code == "monitoring_off" || code == "monitoring_off_for_account" ||
		code == "snapshots_off_for_account":
		e.Code = provider.CodeInvalid
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity:
		e.Code = provider.CodeInvalid
	case code == "sandbox_direct_failed" && details["retryable"] == true:
		// The command provably never ran, so the caller may send it again
	case status >= 500 && create:
		// Boat replays a create of the same key and a command may have started, so both are unsure
		e.Code = provider.CodeOutcomeUnknown
	}
	return e
}

// waitHint reads Boat's wait from the body, its details, then Retry-After, in seconds
func waitHint(h http.Header, top, details map[string]any) (time.Duration, bool) {
	for _, m := range []map[string]any{top, details} {
		for _, k := range []string{"retryAfterSeconds", "retryAfter"} {
			if s, ok := seconds(m[k]); ok {
				return s, true
			}
		}
	}
	if s, ok := seconds(strings.TrimSpace(h.Get("Retry-After"))); ok {
		return s, true
	}
	for _, m := range []map[string]any{top, details} {
		if at, err := time.Parse(time.RFC3339, text(m, "nextStartAt")); err == nil {
			return max(time.Until(at).Round(time.Second), 0), true
		}
	}
	return 0, false
}

// seconds reads a JSON number or a numeric string as a duration in seconds
func seconds(v any) (time.Duration, bool) {
	var f float64
	switch n := v.(type) {
	case float64:
		f = n
	case string:
		var err error
		if f, err = strconv.ParseFloat(n, 64); err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	if f < 0 {
		return 0, false
	}
	return time.Duration(f * float64(time.Second)), true
}

func text(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}

func deref[T any](v *T) T {
	var zero T
	if v == nil {
		return zero
	}
	return *v
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
