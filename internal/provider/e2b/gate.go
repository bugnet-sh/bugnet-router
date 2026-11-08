package e2b

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/e2b/api"
)

const (
	metaTraffic   = "traffic_token"
	trafficHeader = "e2b-traffic-access-token"
)

var _ provider.Gater = (*Provider)(nil)

// PrepareGate reads the traffic token of an adopted sandbox, a known token needs no call
func (p *Provider) PrepareGate(
	ctx context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	id, err := sandboxID(ref)
	if err != nil {
		return nil, err
	}
	if err := provider.CheckGatePort(port); err != nil {
		return nil, err
	}
	if ref.Meta[metaTraffic] != "" {
		return map[string]string{}, nil
	}
	// Connect only extends a sandbox's timeout, so one second reads the token and moves nothing
	body := api.ConnectSandboxV2{Timeout: new(int32(1))}
	_, raw, err := p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		return p.api.PostV2SandboxesSandboxIDConnect(ctx, id, body)
	})
	if err != nil {
		return nil, err
	}
	var sb api.Sandbox
	if err := json.Unmarshal(raw, &sb); err != nil {
		return nil, &provider.Error{
			Code: provider.CodeInternal,
			Err:  fmt.Errorf("e2b connect reply for sandbox %s is not a sandbox: %w", id, err),
		}
	}
	if sb.TrafficAccessToken == nil || *sb.TrafficAccessToken == "" {
		return nil, &provider.Error{
			Code: provider.CodeInternal,
			Err:  fmt.Errorf("e2b sandbox %s has no traffic token, its ports are public", id),
		}
	}
	meta := map[string]string{metaTraffic: *sb.TrafficAccessToken}
	if ref.Meta["envd_token"] == "" && sb.EnvdAccessToken != nil && *sb.EnvdAccessToken != "" {
		meta["envd_token"] = *sb.EnvdAccessToken
	}
	if ref.Meta["domain"] == "" && sb.Domain != nil && *sb.Domain != "" {
		meta["domain"] = *sb.Domain
	}
	return meta, nil
}

// GateRoute is the port's host on the sandbox's domain with the traffic token header
func (p *Provider) GateRoute(ref provider.Ref, port int) (provider.GateRoute, error) {
	id, err := sandboxID(ref)
	if err != nil {
		return provider.GateRoute{}, err
	}
	if err := provider.CheckGatePort(port); err != nil {
		return provider.GateRoute{}, err
	}
	token := ref.Meta[metaTraffic]
	if token == "" {
		return provider.GateRoute{}, provider.GateUnprepared(providerName, id, metaTraffic)
	}
	domain := cmp.Or(ref.Meta["domain"], p.domain)
	return provider.GateRoute{
		BaseURL: "https://" + strconv.Itoa(port) + "-" + id + "." + domain,
		Header:  map[string]string{trafficHeader: token},
	}, nil
}
