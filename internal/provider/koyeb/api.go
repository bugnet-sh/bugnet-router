package koyeb

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// maxReplyBytes bounds one control plane reply, a page of 100 services fits many times over
const maxReplyBytes = 16 << 20

// api calls the few control plane routes the adapter uses
type api struct {
	base, token string
	client      *http.Client
}

// Records hold only the fields the adapter reads, so new fields and enum values never break them

type appRecord struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Domains   []struct {
		Name string `json:"name"`
	} `json:"domains"`
}

type serviceRecord struct {
	ID                 string    `json:"id"`
	AppID              string    `json:"app_id"`
	Name               string    `json:"name"`
	Status             string    `json:"status"`
	CreatedAt          time.Time `json:"created_at"`
	LatestDeploymentID string    `json:"latest_deployment_id"`
	LifeCycle          struct {
		DeleteAfterCreate flexInt `json:"delete_after_create"`
	} `json:"life_cycle"`
}

type instanceRecord struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type instanceEvent struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type deploymentRecord struct {
	ID         string   `json:"id"`
	Status     string   `json:"status"`
	Messages   []string `json:"messages"`
	Definition struct {
		Env []envVar `json:"env"`
	} `json:"definition"`
	Metadata struct {
		Sandbox struct {
			PublicURL  string `json:"public_url"`
			RoutingKey string `json:"routing_key"`
		} `json:"sandbox"`
	} `json:"metadata"`
}

type envVar struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type createAppRequest struct {
	Name      string `json:"name"`
	LifeCycle struct {
		DeleteWhenEmpty bool `json:"delete_when_empty"`
	} `json:"life_cycle"`
}

type createServiceRequest struct {
	AppID      string            `json:"app_id"`
	Name       string            `json:"name"`
	Definition serviceDefinition `json:"definition"`
	LifeCycle  struct {
		DeleteAfterCreate int64 `json:"delete_after_create"`
	} `json:"life_cycle"`
}

type serviceDefinition struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Docker struct {
		Image      string `json:"image"`
		Privileged bool   `json:"privileged"`
	} `json:"docker"`
	Env           []envVar      `json:"env"`
	Ports         []portSpec    `json:"ports"`
	Routes        []routeSpec   `json:"routes"`
	InstanceTypes []typeSpec    `json:"instance_types"`
	Regions       []string      `json:"regions"`
	Scalings      []scalingSpec `json:"scalings"`
	Mesh          string        `json:"mesh"`
}

type portSpec struct {
	Port     int    `json:"port"`
	Protocol string `json:"protocol"`
}

type routeSpec struct {
	Port int    `json:"port"`
	Path string `json:"path"`
}

type typeSpec struct {
	Type string `json:"type"`
}

type scalingSpec struct {
	Min     int             `json:"min"`
	Max     int             `json:"max"`
	Targets []scalingTarget `json:"targets,omitempty"`
}

// scalingTarget carries the idle delay after which an instance goes to light sleep
type scalingTarget struct {
	SleepIdleDelay struct {
		LightSleepValue int64 `json:"light_sleep_value"`
	} `json:"sleep_idle_delay"`
}

// flexInt reads a JSON number or the quoted form protobuf gives 64-bit integers, else zero
type flexInt int64

func (n *flexInt) UnmarshalJSON(raw []byte) error {
	if v, err := strconv.ParseInt(strings.Trim(string(raw), `"`), 10, 64); err == nil {
		*n = flexInt(v)
	}
	return nil
}

// page is one page of a list reply, whose items sit under a key named after the resource
type page[T any] struct {
	items   []T
	count   int64
	hasNext bool
}

/* Routes */

func (a *api) createApp(ctx context.Context, name string) (appRecord, error) {
	req := createAppRequest{Name: name}
	req.LifeCycle.DeleteWhenEmpty = true
	return record[appRecord](ctx, a, true, http.MethodPost, "/v1/apps", nil, req, "app")
}

func (a *api) listApps(ctx context.Context, name string, offset int) (page[appRecord], error) {
	q := url.Values{"name": {name}, "limit": {pageLimit}, "offset": {strconv.Itoa(offset)}}
	return list[appRecord](ctx, a, "/v1/apps", q, "apps")
}

func (a *api) getApp(ctx context.Context, id string) (appRecord, error) {
	return record[appRecord](ctx, a, false, http.MethodGet, "/v1/apps/"+url.PathEscape(id), nil,
		nil, "app")
}

func (a *api) deleteApp(ctx context.Context, id string) error {
	return a.send(ctx, false, http.MethodDelete, "/v1/apps/"+url.PathEscape(id), nil, nil, nil)
}

func (a *api) createService(
	ctx context.Context, req createServiceRequest,
) (serviceRecord, error) {
	return record[serviceRecord](ctx, a, true, http.MethodPost, "/v1/services", nil, req,
		"service")
}

// listServices lists sandbox services, those of one app when appID is set
func (a *api) listServices(
	ctx context.Context, appID string, offset int,
) (page[serviceRecord], error) {
	q := url.Values{"types": {"SANDBOX"}, "limit": {pageLimit}, "offset": {strconv.Itoa(offset)}}
	if appID != "" {
		q.Set("app_id", appID)
	}
	return list[serviceRecord](ctx, a, "/v1/services", q, "services")
}

func (a *api) getService(ctx context.Context, id string) (serviceRecord, error) {
	return record[serviceRecord](ctx, a, false, http.MethodGet,
		"/v1/services/"+url.PathEscape(id), nil, nil, "service")
}

func (a *api) getDeployment(ctx context.Context, id string) (deploymentRecord, error) {
	return record[deploymentRecord](ctx, a, false, http.MethodGet,
		"/v1/deployments/"+url.PathEscape(id), nil, nil, "deployment")
}

// newestInstance lists the newest instance of a deployment, false when it has none
func (a *api) newestInstance(ctx context.Context, deployment string) (instanceRecord, bool, error) {
	q := url.Values{"deployment_id": {deployment}, "limit": {"1"}, "order": {"desc"}}
	pg, err := list[instanceRecord](ctx, a, "/v1/instances", q, "instances")
	if err != nil || len(pg.items) == 0 {
		return instanceRecord{}, false, err
	}
	return pg.items[0], true, nil
}

// newestInstanceEvent reads the newest event of an instance, false when it has none
func (a *api) newestInstanceEvent(
	ctx context.Context, instance string,
) (instanceEvent, bool, error) {
	q := url.Values{"instance_id": {instance}, "limit": {"1"}, "order": {"desc"}}
	pg, err := list[instanceEvent](ctx, a, "/v1/instance_events", q, "events")
	if err != nil || len(pg.items) == 0 {
		return instanceEvent{}, false, err
	}
	return pg.items[0], true, nil
}

/* Pool routes */

type poolRecord struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	Size       flexInt `json:"size"`
	ReadyCount flexInt `json:"ready_count"`
	Status     string  `json:"status"`
}

type claimRecord struct {
	ID        string `json:"id"`
	PoolID    string `json:"pool_id"`
	ServiceID string `json:"service_id"`
	RequestID string `json:"request_id"`
	Status    string `json:"status"`
}

type createPoolRequest struct {
	Name       string            `json:"name"`
	Size       int64             `json:"size"`
	Definition serviceDefinition `json:"definition"`
}

type updatePoolRequest struct {
	Size       int64             `json:"size"`
	Definition serviceDefinition `json:"definition"`
}

type claimRequest struct {
	PoolID    string `json:"pool_id"`
	RequestID string `json:"request_id"`
}

type claimReply struct {
	ClaimID   string `json:"claim_id"`
	ServiceID string `json:"service_id"`
	Prewarmed bool   `json:"prewarmed"`
}

func (a *api) createPool(ctx context.Context, req createPoolRequest) (poolRecord, error) {
	return record[poolRecord](ctx, a, false, http.MethodPost, "/v1/service_pools", nil, req,
		"service_pool")
}

func (a *api) updatePool(
	ctx context.Context, id string, req updatePoolRequest,
) (poolRecord, error) {
	return record[poolRecord](ctx, a, false, http.MethodPut,
		"/v1/service_pools/"+url.PathEscape(id), nil, req, "service_pool")
}

func (a *api) deletePool(ctx context.Context, id string) error {
	return a.send(ctx, false, http.MethodDelete, "/v1/service_pools/"+url.PathEscape(id), nil,
		nil, nil)
}

func (a *api) listPools(ctx context.Context, name string, offset int) (page[poolRecord], error) {
	q := url.Values{"limit": {pageLimit}, "offset": {strconv.Itoa(offset)}}
	if name != "" {
		q.Set("name", name)
	}
	return list[poolRecord](ctx, a, "/v1/service_pools", q, "service_pools")
}

// claim asks for one member of a pool, a repeated request id answering the same claim
func (a *api) claim(ctx context.Context, req claimRequest) (claimReply, error) {
	var out claimReply
	err := a.send(ctx, false, http.MethodPost, "/v1/claim", nil, req, &out)
	return out, err
}

func (a *api) getClaim(ctx context.Context, id string) (claimRecord, error) {
	return record[claimRecord](ctx, a, false, http.MethodGet,
		"/v1/claims/"+url.PathEscape(id), nil, nil, "claim")
}

func (a *api) listClaims(
	ctx context.Context, pool string, offset int,
) (page[claimRecord], error) {
	q := url.Values{"limit": {pageLimit}, "offset": {strconv.Itoa(offset)}}
	return list[claimRecord](ctx, a, "/v1/service_pools/"+url.PathEscape(pool)+"/claims", q,
		"claims")
}

func (a *api) deleteService(ctx context.Context, id string) error {
	return a.send(ctx, false, http.MethodDelete, "/v1/services/"+url.PathEscape(id), nil, nil,
		nil)
}

/* Requests */

// record calls a route whose reply holds one record under key
func record[T any](
	ctx context.Context, a *api, create bool, method, route string, q url.Values, in any,
	key string,
) (T, error) {
	var zero, v T
	var reply map[string]json.RawMessage
	if err := a.send(ctx, create, method, route, q, in, &reply); err != nil {
		return zero, err
	}
	raw, ok := reply[key]
	if !ok || string(raw) == "null" {
		return zero, unreadable(create, fmt.Errorf("koyeb %s reply holds no %s", route, key))
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return zero, unreadable(create, fmt.Errorf("koyeb %s reply: %w", route, err))
	}
	return v, nil
}

// list reads one page of a list route
func list[T any](
	ctx context.Context, a *api, route string, q url.Values, key string,
) (page[T], error) {
	var raw json.RawMessage
	if err := a.send(ctx, false, http.MethodGet, route, q, nil, &raw); err != nil {
		return page[T]{}, err
	}
	var meta struct {
		Count   flexInt `json:"count"`
		HasNext bool    `json:"has_next"`
	}
	// A count or has_next of another type only ends the paging early
	_ = json.Unmarshal(raw, &meta)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return page[T]{}, internal(fmt.Errorf("koyeb %s reply: %w", route, err))
	}
	p := page[T]{count: int64(meta.Count), hasNext: meta.HasNext}
	if items := fields[key]; len(items) > 0 && string(items) != "null" {
		if err := json.Unmarshal(items, &p.items); err != nil {
			return page[T]{}, internal(fmt.Errorf("koyeb %s reply: %w", route, err))
		}
	}
	return p, nil
}

// send makes one call and decodes a 2xx reply into out, create marks calls that make something
func (a *api) send(
	ctx context.Context, create bool, method, route string, q url.Values, in, out any,
) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return invalid(err)
		}
		body = bytes.NewReader(raw)
	}
	u := a.base + route
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	var sent atomic.Bool
	trace := &httptrace.ClientTrace{WroteHeaders: func() { sent.Store(true) }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), method, u, body)
	if err != nil {
		return internal(err)
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "bugnet-router")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return transportError(fmt.Errorf("koyeb %s %s: %w", method, route, err), sent.Load(),
			create)
	}
	defer drainClose(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
		return apiError(resp.StatusCode, resp.Header, raw)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxReplyBytes)).Decode(out); err != nil {
		return unreadable(create, fmt.Errorf("koyeb %s %s reply: %w", method, route, err))
	}
	return nil
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

// unreadable is a 2xx answer whose reply could not be read, a create may have run regardless
func unreadable(create bool, err error) error {
	if create {
		return unknown(err)
	}
	return internal(err)
}

// apiError classifies an error answer by its status, then Koyeb's code and message
func apiError(status int, h http.Header, raw []byte) error {
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Fields  []struct {
			Field       string `json:"field"`
			Description string `json:"description"`
		} `json:"fields"`
	}
	_ = json.Unmarshal(raw, &body)
	msg := cmp.Or(body.Message, strings.TrimSpace(string(raw)), http.StatusText(status))
	for _, f := range body.Fields {
		msg += ", " + f.Field + " " + f.Description
	}
	e := &provider.Error{
		Code: provider.CodeInternal,
		Err:  fmt.Errorf("koyeb answered %d %s: %s", status, cmp.Or(body.Code, "error"), msg),
	}
	client := status >= 400 && status < 500
	auth := status == http.StatusUnauthorized || status == http.StatusForbidden
	switch {
	case status == http.StatusTooManyRequests || body.Code == "rate_limit_error":
		e.Code, e.CanFailOver = provider.CodeRateLimited, true
		e.RetryAfter = retryAfter(h)
	case status == http.StatusPaymentRequired || (client && billingWord.MatchString(msg)):
		e.Code, e.CanFailOver = provider.CodeUnfunded, true
	case client && quotaWord.MatchString(msg):
		e.Code, e.CanFailOver = provider.CodeQuota, true
	case auth && suspendWord.MatchString(msg):
		e.Code, e.CanFailOver = provider.CodeAbuseLock, true
	case auth:
		e.Code, e.CanFailOver = provider.CodeAuth, true
	case status == http.StatusNotFound:
		e.Code = provider.CodeNotFound
	case status == http.StatusServiceUnavailable || capacityWord.MatchString(msg):
		// A refused instance type comes back as a 4xx, and another provider may still have room
		e.Code, e.CanFailOver = provider.CodeCapacity, true
	case client:
		e.Code = provider.CodeInvalid
	}
	return provider.Named(e, providerName)
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
