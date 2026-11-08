// Package gateclient talks to one sandbox's bugnet-run gate through its provider route.
//
// Every call carries the route's provider credentials and the gate key, never follows a
// redirect, and trusts an answer only when it carries Bugnet-Gate-Response, since anything
// else came from the provider's proxy. Errors never quote the route, its credentials or the key.
package gateclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
)

// Wire names the gate shares with bugnet-run and the edges
const (
	HeaderKey      = "Bugnet-Gate"
	HeaderResponse = "Bugnet-Gate-Response"
	HeaderVersion  = "Bugnet-Gate-Version"
	PathHealth     = "/__bugnet/health"
	PathPort       = "/__bugnet/port/"
)

// callTimeout bounds one call, so a provider proxy that holds a request cannot hang the caller
const callTimeout = 10 * time.Second

// maxBody is what a call reads of an answer before dropping the connection
const maxBody = 4 << 10

// ErrNotGate means the answer did not come from the gate, as a provider's own 404 or 502
var ErrNotGate = errors.New("gateclient: the answer did not come from the gate")

// ErrKeyRefused means the gate answered its health with 404, which it gives a wrong key
var ErrKeyRefused = errors.New("gateclient: the gate refused the key")

var client = &http.Client{
	Transport: http.DefaultTransport.(*http.Transport).Clone(),
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// Health asks the gate for its health and answers nil only for its 204
func Health(ctx context.Context, r provider.GateRoute, gateKey string) error {
	status, err := call(ctx, r, gateKey, PathHealth)
	if err != nil {
		return err
	}
	switch status {
	case http.StatusNoContent:
		return nil
	case http.StatusNotFound:
		return ErrKeyRefused
	}
	return fmt.Errorf("gateclient: the gate's health answered %d", status)
}

// PortListening asks the gate whether something inside the sandbox accepts on port
func PortListening(ctx context.Context, r provider.GateRoute, gateKey string,
	port int) (bool, error) {
	if port < 1 || port > 65535 {
		return false, fmt.Errorf("gateclient: port %d is not a TCP port", port)
	}
	status, err := call(ctx, r, gateKey, PathPort+strconv.Itoa(port))
	if err != nil {
		return false, err
	}
	switch status {
	case http.StatusNoContent:
		return true, nil
	case http.StatusNotFound:
		// The gate gives a wrong key the same bare 404, so a closed port needs a healthy gate
		if err := Health(ctx, r, gateKey); err != nil {
			return false, err
		}
		return false, nil
	}
	return false, fmt.Errorf("gateclient: the gate's port check answered %d", status)
}

// unquoted drops the URL, address and host name a transport error carries, keeping its cause
func unquoted(err error) error {
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return errors.New("the route's host did not resolve")
	}
	var op *net.OpError
	if errors.As(err, &op) {
		return op.Err
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// call sends one GET to the gate and answers its status once the gate's header shows it is the gate
func call(ctx context.Context, r provider.GateRoute, gateKey, path string) (int, error) {
	base, err := url.Parse(r.BaseURL)
	if err != nil || (base.Scheme != "https" && base.Scheme != "http") || base.Host == "" {
		return 0, errors.New("gateclient: the route's base URL is not an http or https origin")
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	target := strings.TrimSuffix(r.BaseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, errors.New("gateclient: the route makes no request")
	}
	for k, v := range r.Header {
		req.Header.Set(k, v)
	}
	for name, value := range r.Cookies {
		req.AddCookie(&http.Cookie{Name: name, Value: value})
	}
	req.Header.Set(HeaderKey, gateKey)
	res, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("gateclient: %w", unquoted(err))
	}
	defer func() { _ = res.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, maxBody))
	if res.Header.Get(HeaderResponse) != "1" {
		return 0, fmt.Errorf("%w, status %d", ErrNotGate, res.StatusCode)
	}
	return res.StatusCode, nil
}
