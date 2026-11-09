package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// hookDedupeTTL is how long a delivered event id is remembered
	hookDedupeTTL = 24 * time.Hour
	// hookValkeyWait is the longest the dedupe waits on Valkey before the job is left to dedupe
	hookValkeyWait = 200 * time.Millisecond
)

// CheckHooks refuses a hooks.providers entry that has no secret or no verifier
func CheckHooks(
	h config.Hooks, secrets map[string]config.HookSecret, reg *provider.Registry,
) error {
	for _, name := range h.Providers {
		if len(secrets[name].Current) == 0 {
			return fmt.Errorf("hooks.providers lists %s but its webhook secret is not set", name)
		}
		p, ok := reg.Get(name)
		if !ok {
			return fmt.Errorf("hooks.providers lists %s but it is not in providers", name)
		}
		if _, ok := p.(provider.HookVerifier); !ok {
			return fmt.Errorf("hooks.providers lists %s but it verifies no webhooks", name)
		}
	}
	return nil
}

// DeliverHook verifies a provider webhook, drops a duplicate and queues a hook_hint job
func (e *EdgeServer) DeliverHook(
	ctx context.Context, req *connect.Request[routerv1.DeliverHookRequest],
) (*connect.Response[routerv1.DeliverHookResponse], error) {
	s, m := e.s, req.Msg
	verifier, secret, ok := s.hookVerifier(m.GetProvider())
	if !ok {
		return nil, withInfo(connect.NewError(connect.CodeNotFound,
			errors.New("no webhooks for this provider")), reasonHookUnknownProvider)
	}
	h := make(http.Header, len(m.GetHeaders()))
	for _, kv := range m.GetHeaders() {
		h.Add(kv.GetName(), kv.GetValue())
	}
	ev, err := verifyAny(verifier, h, m.GetBody(), secret)
	if err != nil {
		s.d.Log.Info("api: webhook refused", "provider", m.GetProvider(),
			"delivery", cmp.Or(h.Get("X-Ascii-Delivery"), h.Get("e2b-delivery-id")),
			"client", m.GetClientIp())
		return nil, withInfo(connect.NewError(connect.CodePermissionDenied,
			errors.New("the webhook signature is not valid")), reasonHookBadSignature)
	}
	done := connect.NewResponse(&routerv1.DeliverHookResponse{Status: http.StatusNoContent})
	key := s.d.Prefix + "hook:" + m.GetProvider() + ":" + ev.ID
	fresh, err := s.hookOnce(ctx, key, hookDedupeTTL)
	if err != nil {
		s.d.Log.Warn("api: webhook dedupe skipped", "provider", m.GetProvider())
	} else if !fresh {
		return done, nil
	}
	if err := s.queueHint(ctx, m.GetProvider(), ev); err != nil {
		if fresh {
			// The provider retries this event id, which the dedupe must not swallow
			s.forget(key)
		}
		s.d.Log.Warn("api: webhook job not queued", "provider", m.GetProvider(), "err", err)
		return nil, connect.NewError(connect.CodeUnavailable,
			errors.New("the webhook could not be queued, try again"))
	}
	return done, nil
}

// hookVerifier finds the provider's verifier and its secrets, current first
func (s *Server) hookVerifier(name string) (provider.HookVerifier, [][]byte, bool) {
	if !slices.Contains(s.d.Hooks.Providers, name) || s.d.Registry == nil {
		return nil, nil, false
	}
	p, ok := s.d.Registry.Get(name)
	if !ok {
		return nil, nil, false
	}
	v, ok := p.(provider.HookVerifier)
	sec := s.d.HookSecrets[name]
	if !ok || len(sec.Current) == 0 {
		return nil, nil, false
	}
	secrets := [][]byte{sec.Current}
	if len(sec.Previous) > 0 {
		secrets = append(secrets, sec.Previous)
	}
	return v, secrets, true
}

// verifyAny returns the event of the first secret that passes, the current one tried first
func verifyAny(
	v provider.HookVerifier, h http.Header, body []byte, secrets [][]byte,
) (provider.HookEvent, error) {
	for _, secret := range secrets {
		ev, err := v.VerifyHook(h, body, secret)
		if err == nil {
			return ev, nil
		}
		if !errors.Is(err, provider.ErrBadSignature) {
			return provider.HookEvent{}, err
		}
	}
	return provider.HookEvent{}, provider.ErrBadSignature
}

// hookOnce marks key and reports whether this call was first, an error when Valkey is slow
func (s *Server) hookOnce(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, hookValkeyWait)
	defer cancel()
	err := s.d.Valkey.Do(ctx, s.d.Valkey.B().Set().Key(key).Value("1").Nx().
		Ex(ttl).Build()).Error()
	switch {
	case valkey.IsValkeyNil(err):
		return false, nil
	case err != nil:
		return false, err
	}
	return true, nil
}

// forget drops a dedupe mark, on its own short deadline since the caller's may be spent
func (s *Server) forget(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), hookValkeyWait)
	defer cancel()
	_ = s.d.Valkey.Do(ctx, s.d.Valkey.B().Del().Key(key).Build()).Error()
}

// queueHint inserts the hint unless one for the same sandbox ran or waits within hint_unique
func (s *Server) queueHint(ctx context.Context, name string, ev provider.HookEvent) error {
	if s.d.River == nil {
		return errors.New("no job client")
	}
	unique := s.d.Hooks.HintUnique
	if unique <= 0 {
		unique = 10 * time.Second
	}
	gate := s.d.Prefix + "hook:hint:" + name + ":" + ev.SandboxID
	fresh, err := s.hookOnce(ctx, gate, unique)
	if err == nil && !fresh {
		return nil
	}
	_, err = s.d.River.Insert(ctx, jobargs.HookHint{
		Provider: name, SandboxID: ev.SandboxID, EventID: ev.ID, EventType: ev.Type,
		PlacementLabel:  ev.Labels["placement"],
		DeploymentLabel: ev.Labels["bugnet_deployment"],
	}, nil)
	if err != nil {
		s.forget(gate)
	}
	return err
}
