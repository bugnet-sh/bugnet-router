package boat

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat/api"
)

const (
	metaGateHost  = "gate_host"
	metaPortToken = "_token"
	portAuth      = "_port_auth"
)

var _ provider.Gater = (*Provider)(nil)

// PrepareGate hosts port and keeps the route's host and token, never the URL that joins them
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
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.HostPort(ctx, id, api.HostPortRequest{Port: port})
	})
	if err != nil {
		return nil, err
	}
	var reply struct {
		URL string `json:"url"`
	}
	if err := decode(raw, &reply); err != nil {
		return nil, internal(fmt.Errorf("boat host reply for sandbox %s is not JSON", id))
	}
	host, token, err := hostedRoute(reply.URL)
	if err != nil {
		return nil, internal(fmt.Errorf("boat hosted port %d of sandbox %s: %w", port, id, err))
	}
	return map[string]string{metaGateHost: host, metaPortToken: token}, nil
}

// hostedRoute splits a hosted URL into its host and token, its errors never quoting the URL
func hostedRoute(raw string) (string, string, error) {
	if raw == "" {
		return "", "", errors.New("the reply has no URL")
	}
	u, err := url.Parse(raw)
	switch {
	case err != nil:
		return "", "", errors.New("the URL does not parse")
	case u.Scheme != "https" || u.Host == "" || u.User != nil:
		return "", "", errors.New("the URL is not a plain https origin")
	}
	token := u.Query().Get(metaPortToken)
	if token == "" {
		return "", "", errors.New("the URL carries no token")
	}
	return u.Host, token, nil
}

// GateRoute is the hosted port's origin with the token in the _port_auth cookie
func (p *Provider) GateRoute(ref provider.Ref, port int) (provider.GateRoute, error) {
	id, err := sandboxID(ref)
	if err != nil {
		return provider.GateRoute{}, err
	}
	if err := provider.CheckGatePort(port); err != nil {
		return provider.GateRoute{}, err
	}
	for _, key := range []string{metaGateHost, metaPortToken} {
		if ref.Meta[key] == "" {
			return provider.GateRoute{}, provider.GateUnprepared(providerName, id, key)
		}
	}
	return provider.GateRoute{
		BaseURL: "https://" + ref.Meta[metaGateHost],
		Cookies: map[string]string{portAuth: ref.Meta[metaPortToken]},
	}, nil
}
