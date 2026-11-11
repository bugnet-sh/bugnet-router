package lium

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// maxReplyBytes bounds one reply, a full node listing of 152 nodes was 686 KB
	maxReplyBytes     = 32 << 20
	maxErrorBytes     = 64 << 10
	defaultRetryAfter = time.Second
)

// Lium's error codes, stable where its messages are not (developers/error-codes)
const (
	codeUnauthorized        = "unauthorized"
	codeKeyInactive         = "api_key_inactive"
	codeForbidden           = "forbidden"
	codeInsufficientBalance = "insufficient_balance"
	codeKeyBudget           = "API_KEY_BUDGET_EXCEEDED"
	codeBalanceRequired     = "BALANCE_REQUIRED"
	codeEmailNotVerified    = "email_not_verified"
	codeBalanceInvalid      = "account_balance_invalid"
	codePodNotFound         = "pod_not_found"
	codeNodeNotFound        = "node_not_found"
	codeTemplateNotFound    = "template_not_found"
	codeNodeUnavailable     = "node_unavailable"
	codeNodeRentInProgress  = "node_rent_in_progress"
	codeOwnRentInProgress   = "own_rent_in_progress"
	codeNodePaused          = "node_paused_by_provider"
	codeProviderBanned      = "provider_banned"
	codeBelowBandwidth      = "node_below_bandwidth_floor"
	codePriceChanged        = "price_changed"
	codeNodeNotVerified     = "node_not_verified"
	codeConflict            = "conflict"
	codeTemplateInvalid     = "template_invalid"
	codeImageRefInvalid     = "image_reference_invalid"
	codeImageNotFound       = "image_not_found"
	codeImageNeedsCreds     = "image_needs_registry_credentials"
	codeImageCredsRefused   = "image_registry_credentials_refused"
	codeImageNoAMD64        = "image_no_linux_amd64"
	codeGPUCountInvalid     = "gpu_count_invalid"
	codeGPUSplitRefused     = "gpu_split_not_allowed"
	codeTTLInvalid          = "ttl_invalid"
	codeValidation          = "validation_error"
	codeKeyInvalid          = "idempotency_key_invalid"
	codeKeyReused           = "idempotency_key_reused"
	codeInProgress          = "idempotency_in_progress"
	codeOutcomeUnknown      = "idempotency_outcome_unknown"
	codePodStateConflict    = "pod_state_conflict"
	codeRateLimited         = "rate_limited"
)

// codeSet is a set of Lium error codes
type codeSet map[string]bool

func codes(cs ...string) codeSet {
	s := codeSet{}
	for _, c := range cs {
		s[c] = true
	}
	return s
}

var (
	authCodes = codes(codeUnauthorized, codeKeyInactive, codeForbidden, codeEmailNotVerified,
		codeBalanceInvalid)
	unfundedCodes = codes(codeInsufficientBalance, codeKeyBudget, codeBalanceRequired)
	// nodeCodes refuse one node on a rent, so the next node is tried
	nodeCodes = codes(codeNodeUnavailable, codeNodeRentInProgress, codeNodePaused,
		codeProviderBanned, codeBelowBandwidth, codePriceChanged, codeNodeNotFound,
		codeNodeNotVerified)
	// invalidCodes refuse our request, image or template, which another provider may still serve
	invalidCodes = codes(codeTemplateInvalid, codeImageRefInvalid, codeImageNotFound,
		codeImageNeedsCreds, codeImageCredsRefused, codeImageNoAMD64, codeGPUCountInvalid,
		codeGPUSplitRefused, codeTTLInvalid, codeValidation)
	// bugCodes are the adapter's own mistakes, which no other provider changes
	bugCodes = codes(codeKeyInvalid, codeKeyReused, codePodStateConflict, codeConflict)
)

// codeWord is a detail that is itself an error code, as FastAPI writes a bare HTTPException
var codeWord = regexp.MustCompile(`^[A-Za-z_]+$`)

// api calls Lium's REST routes, pacing every call and every rent
type api struct {
	base, key string
	client    *http.Client
	log       *slog.Logger
	// ops paces every call and creates every rent besides, nil for no pacing
	ops, creates *rate.Limiter
}

// call is one request, its body bytes kept as they are so a retried rent is byte-identical
type call struct {
	method, route string
	query         url.Values
	body          []byte
	header        http.Header
	// public calls go without the key, as the node listing needs none
	public bool
	// rent marks a call that may rent a pod, paced as a create and settled by a listing
	rent bool
}

// limiter paces perSecond calls with no burst, nil for 0
func limiter(perSecond int) *rate.Limiter {
	if perSecond <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(perSecond), 1)
}

// do makes one call and decodes a 2xx reply into out, returning the reply's header
func (a *api) do(ctx context.Context, c call, out any) (http.Header, error) {
	if err := a.pace(ctx, c.rent); err != nil {
		return nil, &provider.Error{
			Code: provider.CodeInternal, CanFailOver: true,
			Err: fmt.Errorf("lium %s %s not sent: %w", c.method, c.route, err),
		}
	}
	u := a.base + c.route
	if len(c.query) > 0 {
		u += "?" + c.query.Encode()
	}
	var body io.Reader
	if c.body != nil {
		body = bytes.NewReader(c.body)
	}
	// writes counts each time the transport wrote the request, as it resends a keyed POST itself
	var writes atomic.Int32
	trace := &httptrace.ClientTrace{WroteHeaders: func() { writes.Add(1) }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), c.method, u,
		body)
	if err != nil {
		return nil, &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	for k, vs := range c.header {
		req.Header[k] = vs
	}
	if !c.public {
		req.Header.Set("X-API-Key", a.key)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "bugnet-router")
	if c.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, transportError(fmt.Errorf("lium %s %s: %w", c.method, c.route, err),
			writes.Load() > 0, c.rent)
	}
	defer drainClose(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
		err := a.classify(resp.StatusCode, resp.Header, raw, c.rent)
		if c.rent && writes.Load() > 1 {
			return resp.Header, resentUnseen(c, err)
		}
		return resp.Header, err
	}
	if out == nil {
		return resp.Header, nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxReplyBytes)).Decode(out); err != nil {
		return resp.Header, unreadable(fmt.Errorf("lium %s %s reply: %w", c.method, c.route,
			err), c.rent)
	}
	return resp.Header, nil
}

// pace waits for a token of every bucket the call spends from
func (a *api) pace(ctx context.Context, rent bool) error {
	if err := wait(ctx, a.ops); err != nil || !rent {
		return err
	}
	return wait(ctx, a.creates)
}

// wait takes one token from lim, giving it back when ctx ends first
func wait(ctx context.Context, lim *rate.Limiter) error {
	if lim == nil {
		return nil
	}
	r := lim.Reserve()
	d := r.Delay()
	if d == 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		r.Cancel()
		return ctx.Err()
	}
}

func drainClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBytes))
	_ = resp.Body.Close()
}

/* Errors */

// apiError is one error answer, Code the stable field a caller branches on
type apiError struct {
	Status                   int
	Code, Message, RequestID string
}

func (e *apiError) Error() string {
	s := fmt.Sprintf("lium answered %d %s: %s", e.Status, cmp.Or(e.Code, "error"), e.Message)
	if e.RequestID != "" {
		s += " (request " + e.RequestID + ")"
	}
	return s
}

// codeOf returns the Lium error code in err's chain, empty when it holds none
func codeOf(err error) string {
	var e *apiError
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// transportError classifies a call that got no answer, a rent that was sent may have rented
func transportError(err error, sent, rent bool) error {
	switch {
	case sent && rent:
		return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: err}
	case sent:
		return &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	return &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: err}
}

// resentUnseen is a rent whose first write lost its answer before the transport sent it again
func resentUnseen(c call, err error) error {
	// The answer is written out, not wrapped, so the rent counts as one with no answer
	return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: fmt.Errorf(
		"lium %s %s lost its answer and the transport sent it again, which got %v", c.method,
		c.route, err)}
}

// unreadable is a 2xx answer whose reply could not be read, a rent may have rented regardless
func unreadable(err error, rent bool) error {
	if rent {
		return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: err}
	}
	return &provider.Error{Code: provider.CodeInternal, Err: err}
}

// readError reads Lium's envelope, the older one with only a message, or FastAPI's detail
func readError(status int, h http.Header, raw []byte) *apiError {
	var body struct {
		Error     json.RawMessage `json:"error"`
		Code      string          `json:"code"`
		Message   string          `json:"message"`
		Detail    json.RawMessage `json:"detail"`
		RequestID string          `json:"request_id"`
	}
	var inner, detail struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	}
	// Each part is read on its own, so a field of an unexpected type loses only itself
	_ = json.Unmarshal(raw, &body)
	_ = json.Unmarshal(body.Error, &inner)
	_ = json.Unmarshal(body.Detail, &detail)
	var word string
	if json.Unmarshal(body.Detail, &word) == nil && codeWord.MatchString(word) {
		detail.Code = word
	}
	return &apiError{
		Status: status,
		Code:   cmp.Or(inner.Code, body.Code, detail.Code),
		Message: cmp.Or(inner.Message, body.Message, detail.Message,
			strings.TrimSpace(string(raw)), http.StatusText(status)),
		RequestID: cmp.Or(inner.RequestID, body.RequestID, h.Get("X-Request-Id")),
	}
}

// classify maps an error answer to a provider code by its Lium code, then by its status
func (a *api) classify(status int, h http.Header, raw []byte, rent bool) error {
	ae := readError(status, h, raw)
	e := &provider.Error{Code: provider.CodeInternal, Err: ae}
	code := ae.Code
	switch {
	case status == http.StatusTooManyRequests || code == codeRateLimited:
		e.Code, e.CanFailOver, e.RetryAfter = provider.CodeRateLimited, true, retryAfter(h)
	case authCodes[code]:
		e.Code, e.CanFailOver = provider.CodeAuth, true
	case unfundedCodes[code]:
		e.Code, e.CanFailOver = provider.CodeUnfunded, true
	case code == codePodNotFound:
		e.Code = provider.CodeNotFound
	case nodeCodes[code] || rent && code == codeConflict:
		e.Code, e.CanFailOver = provider.CodeCapacity, true
	case code == codeOwnRentInProgress:
		// Another rent of ours holds the node, which never sends the order elsewhere by itself
		e.Code = provider.CodeCapacity
	case code == codeTemplateNotFound:
		e.CanFailOver = true
	case invalidCodes[code]:
		e.Code, e.CanFailOver = provider.CodeInvalid, true
	case code == codeInProgress:
		e.Code, e.RetryAfter = provider.CodeOutcomeUnknown, retryAfter(h)
	case code == codeOutcomeUnknown:
		e.Code = provider.CodeOutcomeUnknown
	case bugCodes[code]:
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		// Both refuse the key, whatever code a closed account or a new scope answers with
		e.Code, e.CanFailOver = provider.CodeAuth, true
	case status == http.StatusPaymentRequired:
		e.Code, e.CanFailOver = provider.CodeUnfunded, true
	case status >= 400 && status < 500:
		// A refused request rented nothing, and the log shows a code the table lacks
		e.Code, e.CanFailOver = provider.CodeInvalid, true
		a.log.Error("lium: an error answer the adapter does not know", "status", status,
			"code", code, "message", ae.Message, "request_id", ae.RequestID)
	default:
		e.CanFailOver = true
	}
	return provider.Named(e, providerName)
}

// retryAfter reads Retry-After in seconds, a second when it is absent or unreadable
func retryAfter(h http.Header) time.Duration {
	if s, err := strconv.Atoi(strings.TrimSpace(h.Get("Retry-After"))); err == nil && s >= 0 {
		return time.Duration(s) * time.Second
	}
	return defaultRetryAfter
}
