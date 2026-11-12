package hf

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// jobsHost is the one host the Jobs client sends the token to
	jobsHost = "huggingface.co"
	jobsPath = "/api/jobs"
	jobsBase = "https://" + jobsHost + jobsPath
	// maxReplyBytes bounds one reply, a page of Jobs or the hardware list
	maxReplyBytes = 8 << 20
	maxErrorBytes = 64 << 10
	// maxMessageBytes bounds each part of an error answer an error carries
	maxMessageBytes = 512
	// maxPages bounds one listing, so a list that never ends cannot hold a caller
	maxPages = 100
	// defaultRetryAfter is the least a 429 holds the calls back
	defaultRetryAfter = time.Second
	// maxRetryAfter is the Hub's rate limit window, the longest a 429 can rightly ask to wait
	maxRetryAfter = 5 * time.Minute
)

var (
	// jobID is a Job id as one plain path segment, HF's are 24 hex digits
	jobID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	// lockWords are what a 403 says of an account HF suspended or banned, wording UNVERIFIED
	lockWords = regexp.MustCompile(`(?i)\b(suspended|banned)\b`)
	// linkValue is one link of a Link header and the parameters after it
	linkValue = regexp.MustCompile(`<([^>]*)>([^,]*)`)
)

// jobsClient calls HF's Jobs API under one namespace, pacing every call
type jobsClient struct {
	token, namespace string
	client           *http.Client
	log              *slog.Logger
	// ops paces every call, nil for no pacing
	ops *rate.Limiter
	// mu guards heldUntil, before which no call is sent once a 429 asked to wait
	mu        sync.Mutex
	heldUntil time.Time
}

func newJobsClient(token, namespace string, perSecond int, log *slog.Logger) *jobsClient {
	return &jobsClient{
		token: token, namespace: namespace, log: log,
		client: &http.Client{CheckRedirect: provider.NoRedirect},
		ops:    limiter(perSecond),
	}
}

// limiter paces perSecond calls with no burst, nil for 0
func limiter(perSecond int) *rate.Limiter {
	if perSecond <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(perSecond), 1)
}

// call is one Jobs API request
type call struct {
	method string
	// url is a route jobsURL built or a page link HF gave, sent only under jobsBase
	url  string
	body []byte
	// public calls go without the token, as the hardware list needs none
	public bool
	// job marks a call on one Job, whose 404 means the Job is gone
	job bool
	// create marks a call that may start a Job, so one sent without an answer may have started it
	create bool
}

// jobsURL is a route under jobsBase, each segment escaped, with the query when it has one
func jobsURL(query url.Values, segments ...string) string {
	u := jobsBase
	for _, s := range segments {
		u += "/" + url.PathEscape(s)
	}
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	return u
}

// jobURL is a route on one Job of the namespace, refusing an id that is not one plain segment
func (c *jobsClient) jobURL(id string, rest ...string) (string, error) {
	if !jobID.MatchString(id) {
		return "", &provider.Error{
			Code: provider.CodeInvalid, Err: fmt.Errorf("hf: %.40q is not a Job id", id),
		}
	}
	return jobsURL(nil, append([]string{c.namespace, id}, rest...)...), nil
}

// checkURL refuses any URL outside jobsBase, so the token never goes anywhere else
func checkURL(raw string) error {
	u, err := url.Parse(raw)
	if err == nil && u.Scheme == "https" && u.Host == jobsHost && u.User == nil &&
		u.Opaque == "" && (u.Path == jobsPath || strings.HasPrefix(u.Path, jobsPath+"/")) &&
		!slices.ContainsFunc(strings.Split(u.Path, "/"), func(s string) bool {
			return s == "." || s == ".."
		}) {
		return nil
	}
	return errors.New("hf: refused a Jobs call to a URL outside " + jobsBase)
}

// do makes one call and decodes a 2xx reply into out, returning the reply's header
func (c *jobsClient) do(ctx context.Context, cl call, out any) (http.Header, error) {
	if err := checkURL(cl.url); err != nil {
		return nil, &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	route := cl.method + " " + routeOf(cl.url)
	if err := c.pace(ctx); err != nil {
		return nil, &provider.Error{
			Code: provider.CodeInternal, CanFailOver: true,
			Err: fmt.Errorf("hf %s not sent: %w", route, err),
		}
	}
	var body io.Reader
	if cl.body != nil {
		body = bytes.NewReader(cl.body)
	}
	// sent is set once the transport wrote the request, after which a create may have started
	var sent atomic.Bool
	trace := &httptrace.ClientTrace{WroteHeaders: func() { sent.Store(true) }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), cl.method,
		cl.url, body)
	if err != nil {
		return nil, &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	if !cl.public {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "bugnet-router")
	if cl.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, transportError(fmt.Errorf("hf %s: %w", route, err), sent.Load(), cl.create)
	}
	defer drainClose(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
		return resp.Header, c.classify(resp.StatusCode, resp.Header, raw, cl)
	}
	if out == nil {
		return resp.Header, nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxReplyBytes)).Decode(out); err != nil {
		return resp.Header, unreadable(fmt.Errorf("hf %s reply: %w", route, err), cl.create)
	}
	return resp.Header, nil
}

// routeOf is a URL's path, which errors name without the query
func routeOf(raw string) string {
	if u, err := url.Parse(raw); err == nil {
		return u.Path
	}
	return jobsPath
}

// pace waits out a 429's hold, then takes a token of the ops bucket
func (c *jobsClient) pace(ctx context.Context) error {
	for {
		if err := sleep(ctx, c.held()); err != nil {
			return err
		}
		if err := take(ctx, c.ops); err != nil {
			return err
		}
		// A 429 that came while this call waited for its token holds it back too
		if c.held() <= 0 {
			return nil
		}
	}
}

// hold keeps every call back for wait from now
func (c *jobsClient) hold(wait time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if until := time.Now().Add(wait); until.After(c.heldUntil) {
		c.heldUntil = until
	}
}

// held is how long calls are still held back
func (c *jobsClient) held() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Until(c.heldUntil)
}

// sleep waits for d, or until ctx ends
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// take takes one token from lim, giving it back when ctx ends first
func take(ctx context.Context, lim *rate.Limiter) error {
	if lim == nil {
		return ctx.Err()
	}
	r := lim.Reserve()
	if err := sleep(ctx, r.Delay()); err != nil {
		r.Cancel()
		return err
	}
	return nil
}

func drainClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBytes))
	_ = resp.Body.Close()
}

/* Jobs */

// job is the part of HF's Job object the adapter reads
type job struct {
	ID        string            `json:"id"`
	CreatedAt time.Time         `json:"createdAt"`
	Flavor    string            `json:"flavor"`
	Labels    map[string]string `json:"labels"`
	// Timeout is in seconds
	Timeout float64   `json:"timeout"`
	Status  jobStatus `json:"status"`
}

// jobStatus is a Job's stage and why it ended, Message empty while HF writes null
type jobStatus struct {
	Stage        string `json:"stage"`
	Message      string `json:"message"`
	CancelReason string `json:"cancelReason"`
}

// getJob reads one Job of the namespace
func (c *jobsClient) getJob(ctx context.Context, id string) (job, error) {
	u, err := c.jobURL(id)
	if err != nil {
		return job{}, err
	}
	var j job
	_, err = c.do(ctx, call{method: http.MethodGet, url: u, job: true}, &j)
	return j, err
}

// listJobs reads the namespace's Jobs carrying every label in any of the stages, page by page
func (c *jobsClient) listJobs(
	ctx context.Context, labels map[string]string, stages []string,
) ([]job, error) {
	q := url.Values{}
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		q.Add("label", k+"="+labels[k])
	}
	for _, s := range stages {
		q.Add("stage", s)
	}
	var out []job
	next := jobsURL(q, c.namespace)
	for range maxPages {
		var page []job
		h, err := c.do(ctx, call{method: http.MethodGet, url: next}, &page)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next, err = nextLink(next, h); err != nil || next == "" {
			return out, err
		}
	}
	return nil, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
		"hf: the Jobs list ran past %d pages", maxPages)}
}

// nextLink is the Link header's rel="next" read against the page that gave it, empty for none
func nextLink(page string, h http.Header) (string, error) {
	for _, v := range h.Values("Link") {
		for _, m := range linkValue.FindAllStringSubmatch(v, -1) {
			if !relNext(m[2]) {
				continue
			}
			base, err := url.Parse(page)
			if err != nil {
				return "", &provider.Error{Code: provider.CodeInternal, Err: err}
			}
			ref, err := url.Parse(strings.TrimSpace(m[1]))
			if err != nil {
				return "", &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
					"hf: the Jobs list's next page link: %w", err)}
			}
			return base.ResolveReference(ref).String(), nil
		}
	}
	return "", nil
}

// relNext reports whether a link's parameters hold rel with next among its values
func relNext(params string) bool {
	for p := range strings.SplitSeq(params, ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(k), "rel") {
			continue
		}
		for _, r := range strings.Fields(strings.Trim(strings.TrimSpace(v), `"`)) {
			if strings.EqualFold(r, "next") {
				return true
			}
		}
	}
	return false
}

/* Errors */

// apiError is one error answer of the Jobs API
type apiError struct {
	Status                   int
	Code, Message, RequestID string
}

func (e *apiError) Error() string {
	s := fmt.Sprintf("hf answered %d", e.Status)
	if e.Code != "" {
		s += " " + e.Code
	}
	s += ": " + e.Message
	if e.RequestID != "" {
		s += " (request " + e.RequestID + ")"
	}
	return s
}

// readError reads the Hub's error or message field, its X-Error-Message header, or the body
func readError(status int, h http.Header, raw []byte, token string) *apiError {
	var body struct {
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
	}
	var text string
	// Each part is read on its own, so a field of an unexpected type loses only itself
	_ = json.Unmarshal(raw, &body)
	_ = json.Unmarshal(body.Error, &text)
	msg := cmp.Or(text, body.Message, h.Get("X-Error-Message"), strings.TrimSpace(string(raw)),
		http.StatusText(status))
	return &apiError{
		Status: status, Code: scrub(h.Get("X-Error-Code"), token), Message: scrub(msg, token),
		RequestID: scrub(h.Get("X-Request-Id"), token),
	}
}

// scrub replaces the token in a part of an answer, then cuts it at maxMessageBytes
func scrub(s, token string) string {
	// An answer that echoes the token never carries it into an error or a log, even cut
	if token != "" {
		s = strings.ReplaceAll(s, token, "[token]")
	}
	if len(s) > maxMessageBytes {
		s = strings.ToValidUTF8(s[:maxMessageBytes], "") + "..."
	}
	return s
}

// classify maps an error answer to a provider code by its status, design 4.13
func (c *jobsClient) classify(status int, h http.Header, raw []byte, cl call) error {
	ae := readError(status, h, raw, c.token)
	e := &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: ae}
	switch {
	case status == http.StatusTooManyRequests:
		e.Code, e.RetryAfter = provider.CodeRateLimited, retryAfter(h)
		c.hold(e.RetryAfter)
	case status == http.StatusUnauthorized:
		e.Code = provider.CodeAuth
	case status == http.StatusPaymentRequired:
		e.Code = provider.CodeUnfunded
	case status == http.StatusForbidden && lockWords.MatchString(ae.Message):
		e.Code = provider.CodeAbuseLock
	case status == http.StatusForbidden:
		e.Code = provider.CodeAuth
	case status == http.StatusNotFound && cl.job:
		e.Code, e.CanFailOver = provider.CodeNotFound, false
	case status == http.StatusBadRequest || status == http.StatusNotFound ||
		status == http.StatusUnprocessableEntity:
		// A 404 off a Job's route names the namespace, which hf.namespace has wrong
		e.Code = provider.CodeInvalid
	case status >= 500:
	case status >= 400:
		// A refused request started nothing, and the log shows a status the table lacks
		e.Code = provider.CodeInvalid
		c.log.Error("hf: an error answer the adapter does not know", "status", status,
			"message", ae.Message, "request_id", ae.RequestID)
	}
	return provider.Named(e, providerName)
}

// retryAfter is how long a 429 asks to wait, the longer of Retry-After and RateLimit's reset
func retryAfter(h http.Header) time.Duration {
	wait := max(retryAfterHeader(h.Get("Retry-After")), rateLimitReset(h.Values("RateLimit")))
	return min(max(wait, defaultRetryAfter), maxRetryAfter)
}

// retryAfterHeader reads Retry-After in seconds or as an HTTP date, 0 when it reads neither
func retryAfterHeader(v string) time.Duration {
	v = strings.TrimSpace(v)
	if s, err := strconv.ParseInt(v, 10, 64); err == nil && s >= 0 {
		return seconds(s)
	}
	if t, err := http.ParseTime(v); err == nil {
		return time.Until(t)
	}
	return 0
}

// rateLimitReset reads RateLimit's t, the seconds until a window resets, a spent bucket's first
func rateLimitReset(values []string) time.Duration {
	spent, latest := int64(-1), int64(-1)
	for _, v := range values {
		for item := range strings.SplitSeq(v, ",") {
			r, t := int64(-1), int64(-1)
			// The first part names the bucket, the rest are its parameters
			for _, param := range strings.Split(item, ";")[1:] {
				k, val, _ := strings.Cut(strings.TrimSpace(param), "=")
				n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
				switch {
				case err != nil || n < 0:
				case k == "r":
					r = n
				case k == "t":
					t = n
				}
			}
			if t < 0 {
				continue
			}
			latest = max(latest, t)
			if r == 0 {
				spent = max(spent, t)
			}
		}
	}
	if spent >= 0 {
		return seconds(spent)
	}
	return seconds(max(latest, 0))
}

// seconds converts a count of seconds, bounded so the product cannot overflow
func seconds(n int64) time.Duration {
	return time.Duration(min(n, int64(maxRetryAfter/time.Second)+1)) * time.Second
}

// transportError classifies a call that got no answer, a create that was sent may have started
func transportError(err error, sent, create bool) error {
	switch {
	case sent && create:
		return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: err}
	case sent:
		return &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	return &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: err}
}

// unreadable is a 2xx answer whose reply could not be read, a create may have started regardless
func unreadable(err error, create bool) error {
	if create {
		return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: err}
	}
	return &provider.Error{Code: provider.CodeInternal, Err: err}
}
