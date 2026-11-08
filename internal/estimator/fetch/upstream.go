package fetch

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// upstream calls the configured hosts only, with a cap on bodies and concurrent requests
type upstream struct {
	hc  *http.Client
	sem chan struct{}
}

// newUpstream returns a client that follows only the redirects follow allows
func newUpstream(
	concurrency int, follow func(req *http.Request, via []*http.Request) error,
) *upstream {
	u := &upstream{sem: make(chan struct{}, max(concurrency, 1))}
	u.hc = &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment, MaxIdleConnsPerHost: 4,
			TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 20 * time.Second,
		},
		CheckRedirect: follow,
	}
	return u
}

type response struct {
	status int
	header http.Header
	body   []byte
}

// do sends one request and reads at most limit bytes of the answer
func (u *upstream) do(
	ctx context.Context, method, rawURL string, header map[string]string, body []byte,
	limit int64,
) (response, error) {
	select {
	case u.sem <- struct{}{}:
		defer func() { <-u.sem }()
	case <-ctx.Done():
		return response{}, ctx.Err()
	}
	var rd *bytes.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	var req *http.Request
	var err error
	if rd != nil {
		req, err = http.NewRequestWithContext(ctx, method, rawURL, rd)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, rawURL, nil)
	}
	if err != nil {
		return response{}, fmt.Errorf("fetch: build request: %w", err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	req.Header.Set("User-Agent", "bugnet-router-estimator")
	resp, err := u.hc.Do(req)
	if err != nil {
		// The URL holds only validated parts, but an error text could still echo a host
		return response{}, fmt.Errorf("fetch: %s request failed: %w", method, urlFree(err))
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := readCapped(resp.Body, limit)
	if err != nil {
		return response{status: resp.StatusCode, header: resp.Header}, err
	}
	return response{status: resp.StatusCode, header: resp.Header, body: b}, nil
}

// urlFree unwraps a *url.Error so the message keeps its cause and drops the URL
func urlFree(err error) error {
	if ue, ok := err.(*url.Error); ok { //nolint:errorlint // a transport error is never wrapped
		return ue.Err
	}
	return err
}

// sameHost allows one redirect within host, and under pathPrefix when it is set
func sameHost(host, pathPrefix string, hops int) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) > hops || !strings.EqualFold(req.URL.Host, host) ||
			(pathPrefix != "" && !strings.HasPrefix(req.URL.Path, pathPrefix)) {
			return http.ErrUseLastResponse
		}
		return nil
	}
}

// hostOf returns the host of a base URL
func hostOf(base string) string {
	u, err := url.Parse(base)
	if err != nil {
		return ""
	}
	return u.Host
}
