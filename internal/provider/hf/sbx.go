package hf

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// sbxDomain holds every Job's ports on HF's proxy, the only hosts the sandbox token goes to
	sbxDomain = ".hf.jobs"
	// maxHealthBytes bounds a health answer, which the Job's root controls
	maxHealthBytes = 64 << 10
	// maxBodyBytes is sbx-server's cap on a request body, design 4.4
	maxBodyBytes = 16 << 20
	// sbxHeaderTimeout bounds sbx-server's answer once a request went out whole
	sbxHeaderTimeout = 2 * time.Minute
	// busyRetry is how long sbx-server's connection cap asks a call to wait, design 4.13
	busyRetry = time.Second
	// lostTimeout bounds the Job read that tells a gone Job from a failed call
	lostTimeout = 10 * time.Second
)

// sbxClient calls sbx-server in a Job through HF's proxy, carrying the sandbox's own token alone
type sbxClient struct {
	client *http.Client
}

// newSBXClient talks HTTP/1.1 only, so parallel parts get connections of their own, and never
// asks for compression, so lengths stay exact
func newSBXClient() *sbxClient {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Protocols, t.DisableCompression = protocols, true
	t.ResponseHeaderTimeout = sbxHeaderTimeout
	return &sbxClient{client: &http.Client{Transport: t, CheckRedirect: provider.NoRedirect}}
}

// sandbox is how the router reaches one Job's sbx-server
type sandbox struct {
	id, token string
	// end is when HF stops the Job, zero when the ref does not say
	end time.Time
}

// sandbox reads the ref's Job and token, deriving the token from the Job's key when the ref lacks it
func (p *Provider) sandbox(ctx context.Context, ref provider.Ref) (sandbox, error) {
	id, ok := jobOf(ref)
	if !ok {
		return sandbox{}, notHF(ref)
	}
	sb := sandbox{id: id, token: ref.Meta[metaSecret]}
	sb.end, _ = jobEnd(ref)
	if sb.token == "" {
		j, err := p.jobs.getJob(ctx, id)
		if err != nil {
			return sandbox{}, err
		}
		if j.ID != id {
			// A token derived from another Job's labels would open that Job
			return sandbox{}, &provider.Error{Code: provider.CodeInternal,
				Err: fmt.Errorf("hf answered Job %.40q for Job %s", j.ID, id)}
		}
		if sb.token, err = p.tokenOf(j); err != nil {
			return sandbox{}, err
		}
	}
	if !sha256Hex.MatchString(sb.token) {
		return sandbox{}, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"hf Job %s has a sandbox token that is not 64 lowercase hex", id)}
	}
	return sb, nil
}

// tokenOf derives a Job's token from its key label
func (p *Provider) tokenOf(j job) (string, error) {
	key := j.Labels[labelKey]
	if !keyHex.MatchString(key) {
		return "", &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"hf Job %s has no token in its ref and no key to derive one from", j.ID)}
	}
	return p.sandboxToken(key), nil
}

// request builds a call to one route of the sandbox's sbx-server, carrying its token alone
func (c *sbxClient) request(
	ctx context.Context, sb sandbox, method, route string, q url.Values, body []byte,
) (*http.Request, error) {
	u, err := sbxURL(sb.id, sbxPort, route)
	if err != nil {
		return nil, err
	}
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	if err := checkSBXURL(u); err != nil {
		return nil, &provider.Error{Code: provider.CodeInvalid, Err: err}
	}
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return nil, &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	req.Header.Set("X-Sandbox-Token", sb.token)
	req.Header.Set("User-Agent", "bugnet-router")
	return req, nil
}

// do sends a call and returns its answer when 2xx, else the error design 4.13 gives it
func (p *Provider) do(ctx context.Context, sb sandbox, req *http.Request) (*http.Response, error) {
	resp, err := p.sbx.client.Do(req)
	if err != nil {
		return nil, p.lost(ctx, sb, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s sbx-server %s: %w", sb.id, routeName(req), err)})
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer drainClose(resp)
	return nil, p.answerError(ctx, sb, req, resp)
}

// routeName is a call's method and path, which errors name without the query
func routeName(req *http.Request) string { return req.Method + " " + req.URL.Path }

// answerError classifies an error answer of sbx-server, or of HF's proxy in front of it
func (p *Provider) answerError(
	ctx context.Context, sb sandbox, req *http.Request, resp *http.Response,
) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	var body struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(raw, &body)
	// The Job's root may answer for sbx-server and echo the token it got
	msg := scrubToken(cmp.Or(body.Error, strings.TrimSpace(string(raw)),
		http.StatusText(resp.StatusCode)), sb.token)
	status := resp.StatusCode
	e := &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
		"hf Job %s sbx-server %s answered %d: %s", sb.id, routeName(req), status, msg)}
	switch {
	case status == http.StatusBadRequest || status == http.StatusConflict ||
		status == http.StatusRequestEntityTooLarge:
		e.Code = provider.CodeInvalid
	case status == http.StatusForbidden:
		e.Err = fmt.Errorf("hf_token_refused, sbx-server does not take the sandbox's token: %w",
			e.Err)
	case status == http.StatusNotFound:
		e.Code = provider.CodeNotFound
	case status == http.StatusServiceUnavailable:
		e.Code, e.RetryAfter = provider.CodeRateLimited, busyRetry
		return p.lost(ctx, sb, e)
	case status == http.StatusInternalServerError &&
		strings.Contains(strings.ToLower(msg), "no space left on device"):
		e.Code = provider.CodeNoSpace
	case status >= http.StatusBadGateway && status <= http.StatusGatewayTimeout:
		return p.lost(ctx, sb, e)
	}
	return e
}

// lost reads the Job behind a failed call, not_found once HF shows it ended or no longer has it
func (p *Provider) lost(ctx context.Context, sb sandbox, e *provider.Error) error {
	if ctx.Err() != nil {
		return e
	}
	jctx, cancel := context.WithTimeout(ctx, lostTimeout)
	defer cancel()
	j, err := p.jobs.getJob(jctx, sb.id)
	switch {
	case isCode(err, provider.CodeNotFound):
		return &provider.Error{Code: provider.CodeNotFound,
			Err: fmt.Errorf("%w, and hf no longer has the Job", e.Err)}
	case err == nil && !liveStage(j.Status.Stage):
		return &provider.Error{Code: provider.CodeNotFound,
			Err: fmt.Errorf("%w, and hf shows the Job %.40s", e.Err, j.Status.Stage)}
	}
	return e
}

// health is sbx-server's /health answer, mode and auth shown only to its own token
type health struct {
	Status   string `json:"status"`
	Protocol *int   `json:"protocol"`
	Mode     string `json:"mode"`
	Auth     string `json:"auth"`
}

// sbxURL is a route on one port of a Job, refusing an id that is not one plain host label part
func sbxURL(id string, port int, route string) (string, error) {
	if !jobID.MatchString(id) {
		return "", &provider.Error{
			Code: provider.CodeInvalid, Err: fmt.Errorf("hf: %.40q is not a Job id", id),
		}
	}
	return "https://" + id + "--" + strconv.Itoa(port) + sbxDomain + route, nil
}

// checkSBXURL refuses any URL outside *.hf.jobs, so the sandbox token never goes anywhere else
func checkSBXURL(raw string) error {
	u, err := url.Parse(raw)
	if err == nil && u.Scheme == "https" && u.User == nil && u.Opaque == "" && u.Port() == "" &&
		!slices.ContainsFunc(strings.Split(u.Path, "/"), func(s string) bool {
			return s == "." || s == ".."
		}) {
		label, ok := strings.CutSuffix(u.Host, sbxDomain)
		if ok && label != "" && !strings.Contains(label, ".") {
			return nil
		}
	}
	return errors.New("hf: refused a sandbox call to a URL outside *" + sbxDomain)
}

// health reads the Job's sbx-server health with its token, an error for any answer but a 200
func (c *sbxClient) health(ctx context.Context, id, token string) (health, error) {
	u, err := sbxURL(id, sbxPort, "/health")
	if err != nil {
		return health{}, err
	}
	if err := checkSBXURL(u); err != nil {
		return health{}, &provider.Error{Code: provider.CodeInvalid, Err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return health{}, &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	req.Header.Set("X-Sandbox-Token", token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "bugnet-router")
	resp, err := c.client.Do(req)
	if err != nil {
		return health{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s sbx-server health: %w", id, err)}
	}
	defer drainClose(resp)
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxMessageBytes))
		// The Job's root may answer for sbx-server and echo the token it got
		return health{}, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"hf Job %s sbx-server health answered %d: %.200q", id, resp.StatusCode,
			scrubToken(string(raw), token))}
	}
	var h health
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxHealthBytes)).Decode(&h); err != nil {
		return health{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s sbx-server health: %w", id, err)}
	}
	return h, nil
}

// scrubToken replaces the sandbox token in any case, since hex reads the same either way, then
// cuts the text as scrub does
func scrubToken(s, token string) string {
	token = asciiLower(token)
	if token == "" {
		return scrub(s)
	}
	folded := asciiLower(s)
	var b strings.Builder
	for {
		i := strings.Index(folded, token)
		if i < 0 {
			break
		}
		b.WriteString(s[:i] + "[token]")
		s, folded = s[i+len(token):], folded[i+len(token):]
	}
	b.WriteString(s)
	return scrub(b.String())
}

// asciiLower lowers ASCII letters alone, so every byte keeps its place
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}
