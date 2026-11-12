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
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"slices"
	"strings"
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
	// maxMessageBytes bounds the part of an error answer an error carries
	maxMessageBytes = 512
)

var (
	// jobID is a Job id as one plain path segment, HF's are 24 hex digits
	jobID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	// lockWords are what a 403 says of an account HF suspended or banned, wording UNVERIFIED
	lockWords = regexp.MustCompile(`(?i)\b(suspended|banned)\b`)
)

// jobsClient calls HF's Jobs API under one namespace, pacing every call
type jobsClient struct {
	token, namespace string
	client           *http.Client
	log              *slog.Logger
	// ops paces every call, nil for no pacing
	ops *rate.Limiter
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

// pace waits for a token of the ops bucket
func (c *jobsClient) pace(ctx context.Context) error {
	if c.ops == nil {
		return ctx.Err()
	}
	r := c.ops.Reserve()
	d := r.Delay()
	if d == 0 {
		return ctx.Err()
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

// readError reads the Hub's {"error": ...} envelope, its X-Error headers, or the bare body
func readError(status int, h http.Header, raw []byte) *apiError {
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
	if len(msg) > maxMessageBytes {
		msg = strings.ToValidUTF8(msg[:maxMessageBytes], "") + "..."
	}
	return &apiError{
		Status: status, Code: h.Get("X-Error-Code"), Message: msg,
		RequestID: h.Get("X-Request-Id"),
	}
}

// classify maps an error answer to a provider code by its status, design 4.13
func (c *jobsClient) classify(status int, h http.Header, raw []byte, cl call) error {
	ae := readError(status, h, raw)
	e := &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: ae}
	switch {
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
