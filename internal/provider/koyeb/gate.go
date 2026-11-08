package koyeb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const routingHeader = "X-Routing-Key"

var _ provider.Gater = (*Provider)(nil)

// PrepareGate binds the executor's TCP proxy to port, unbinding whatever an earlier bind left
func (p *Provider) PrepareGate(
	ctx context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	if _, err := appID(ref); err != nil {
		return nil, err
	}
	if err := provider.CheckGatePort(port); err != nil {
		return nil, err
	}
	t, err := p.target(ctx, ref)
	if err != nil {
		return nil, err
	}
	conflict, err := p.portCall(ctx, ref, t, "/bind_port", strconv.Itoa(port))
	if err == nil && conflict {
		// The executor binds one port at a time and answers 409 while another is bound
		if _, err = p.portCall(ctx, ref, t, "/unbind_port", ""); err == nil {
			conflict, err = p.portCall(ctx, ref, t, "/bind_port", strconv.Itoa(port))
		}
		if err == nil && conflict {
			err = internal(fmt.Errorf(
				"koyeb sandbox %s: another client bound a port between unbind and bind", ref.ID))
		}
	}
	if err != nil {
		return nil, err
	}
	meta := map[string]string{metaExecutor: t.base}
	if t.routing != "" {
		meta[metaRouting] = t.routing
	}
	return meta, nil
}

// portCall posts to a port route and reports a 409, which only bind_port answers
func (p *Provider) portCall(
	ctx context.Context, ref provider.Ref, t target, route, port string,
) (bool, error) {
	body := map[string]string{}
	if port != "" {
		body["port"] = port
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return false, internal(err)
	}
	req, err := t.request(ctx, http.MethodPost, route, nil, bytes.NewReader(raw))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	var ok []int
	if route == "/bind_port" {
		ok = []int{http.StatusConflict}
	}
	resp, err := p.do(ctx, ref, req, ok...)
	if err != nil {
		return false, err
	}
	defer drainClose(resp)
	return resp.StatusCode == http.StatusConflict, nil
}

// GateRoute is the sandbox's public URL, whose root reaches the bound port and never the executor
func (p *Provider) GateRoute(ref provider.Ref, port int) (provider.GateRoute, error) {
	id, err := appID(ref)
	if err != nil {
		return provider.GateRoute{}, err
	}
	if err := provider.CheckGatePort(port); err != nil {
		return provider.GateRoute{}, err
	}
	base := ref.Meta[metaExecutor]
	if base == "" {
		return provider.GateRoute{}, provider.GateUnprepared(providerName, id, metaExecutor)
	}
	origin, ok := strings.CutSuffix(base, executorPath)
	if !ok || origin == "" {
		return provider.GateRoute{}, internal(fmt.Errorf(
			"koyeb sandbox %s has an executor URL outside %s", id, executorPath))
	}
	route := provider.GateRoute{BaseURL: origin, BlockedPrefixes: []string{executorPath + "/"}}
	if key := ref.Meta[metaRouting]; key != "" {
		route.Header = map[string]string{routingHeader: key}
	}
	return route, nil
}
