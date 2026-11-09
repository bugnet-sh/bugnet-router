package main

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	openapi_types "github.com/oapi-codegen/runtime/types"
	"golang.org/x/term"

	boatapi "github.com/bugnet-sh/bugnet-router/internal/provider/boat/api"
	e2bapi "github.com/bugnet-sh/bugnet-router/internal/provider/e2b/api"
)

const (
	hookSecretBytes = 32
	// hookPathBytes is the decoded length of the edge's BUGNET_HOOK_PATH_SECRET
	hookPathBytes   = 32
	hookDeliveries  = 5
	boatDefaultBase = "https://boat.dev/api/v1"
)

// hookEvents are the events each provider's endpoint subscribes to
var hookEvents = map[string][]string{
	"e2b":  {"sandbox.lifecycle.killed", "sandbox.lifecycle.paused"},
	"boat": {"sandbox.error", "sandbox.archived", "sandbox.degraded", "sandbox.recovered"},
}

// hooks registers, lists, rotates and deletes the provider webhook endpoints of one deployment
func (c *cli) hooks(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: hooks takes register, list, rotate or delete", errUsage)
	}
	fs := newFlags("hooks " + args[0])
	if err := parse(fs, args[1:], true); err != nil {
		return err
	}
	rest := fs.Args()
	want := map[string]int{"register": 1, "list": 1, "rotate": 1, "delete": 2}[args[0]]
	if want == 0 {
		return fmt.Errorf("%w: hooks takes register, list, rotate or delete", errUsage)
	}
	if len(rest) != want {
		return fmt.Errorf("%w: hooks %s takes %s", errUsage, args[0],
			map[int]string{1: "a provider", 2: "a provider and an endpoint id"}[want])
	}
	prov := rest[0]
	if _, ok := hookEvents[prov]; !ok {
		return fmt.Errorf("%w: hooks work for e2b and boat", errUsage)
	}
	h, err := c.newHooks(prov)
	if err != nil {
		return err
	}
	switch args[0] {
	case "register":
		return h.register()
	case "list":
		return h.list()
	case "rotate":
		return h.rotate()
	}
	return h.remove(rest[1])
}

// hookClient is one provider's webhook API as one command uses it
type hookClient struct {
	c    *cli
	prov string
	// url is the endpoint this deployment registers, holding the path secret
	url string
	// prefix tells this deployment's endpoints from other deployments' in a listing
	prefix string
	e2b    *e2bapi.ClientWithResponses
	boat   *boatapi.ClientWithResponses
}

// newHooks builds the client, asking for the path secret and the Boat key with echo off
func (c *cli) newHooks(prov string) (*hookClient, error) {
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	if cfg.Names.HooksHost == "" {
		return nil, errors.New("names.hooks_host is not set in the settings file")
	}
	h := &hookClient{c: c, prov: prov}
	path := c.getenv("BUGNET_HOOK_PATH_SECRET")
	if path == "" {
		if path, err = c.secretInput("BUGNET_HOOK_PATH_SECRET: "); err != nil {
			return nil, err
		}
	}
	if raw, err := base64.RawURLEncoding.DecodeString(path); err != nil ||
		len(raw) != hookPathBytes {
		return nil, errors.New("BUGNET_HOOK_PATH_SECRET must be 32 bytes in base64url")
	}
	h.prefix = "https://" + cfg.Names.HooksHost + "/hooks/" + prov + "/"
	h.url = h.prefix + path
	switch prov {
	case "e2b":
		key := c.getenv("E2B_API_KEY")
		if key == "" {
			return nil, errors.New("E2B_API_KEY is not set")
		}
		base := c.hookBase["e2b"]
		if base == "" {
			base = "https://api." + cmp.Or(cfg.E2B.Domain, "e2b.app")
		}
		h.e2b, err = e2bapi.NewClientWithResponses(base, e2bapi.WithHTTPClient(c.hookHTTP()),
			e2bapi.WithRequestEditorFn(func(_ context.Context, r *http.Request) error {
				r.Header.Set("X-API-Key", key)
				return nil
			}))
	case "boat":
		key := c.getenv("BOAT_ADMIN_API_KEY")
		if key == "" {
			if key, err = c.secretInput("Boat admin API key: "); err != nil {
				return nil, err
			}
		}
		base := cmp.Or(c.hookBase["boat"], boatDefaultBase)
		h.boat, err = boatapi.NewClientWithResponses(base, boatapi.WithHTTPClient(c.hookHTTP()),
			boatapi.WithRequestEditorFn(func(_ context.Context, r *http.Request) error {
				r.Header.Set("Authorization", "Bearer "+key)
				return nil
			}))
	}
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (c *cli) hookHTTP() *http.Client {
	return &http.Client{
		Timeout: time.Minute,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// secretInput reads one line from the terminal with echo off, or from ask in tests
func (c *cli) secretInput(label string) (string, error) {
	if c.ask != nil {
		return c.ask(label)
	}
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return "", fmt.Errorf("%s is not set and stdin is not a terminal", strings.TrimSuffix(
			strings.TrimSuffix(label, ": "), ":"))
	}
	_, _ = fmt.Fprint(c.stderr, label)
	b, err := term.ReadPassword(fd)
	_, _ = fmt.Fprintln(c.stderr)
	if err != nil {
		return "", err
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return "", errors.New("nothing was typed")
	}
	return v, nil
}

// shown hides the path secret of this deployment's endpoint in a URL it prints
func (h *hookClient) shown(u string) string {
	if rest, ok := strings.CutPrefix(u, h.prefix); ok && rest != "" {
		return h.prefix + "<path secret>"
	}
	return u
}

// ours reports whether u is this deployment's endpoint for the provider
func (h *hookClient) ours(u string) bool {
	return strings.HasPrefix(u, h.prefix)
}

func (h *hookClient) out(format string, args ...any) {
	_, _ = fmt.Fprintf(h.c.stdout, format+"\n", args...)
}

// fail reports a refused call without quoting the provider's words
func fail(what string, status int) error {
	return fmt.Errorf("%s failed with HTTP %d", what, status)
}

func (h *hookClient) register() error {
	switch h.prov {
	case "e2b":
		return h.registerE2B()
	}
	return h.registerBoat()
}

func (h *hookClient) registerE2B() error {
	raw := make([]byte, hookSecretBytes)
	_, _ = rand.Read(raw)
	secret := base64.RawURLEncoding.EncodeToString(raw)
	resp, err := h.e2b.PostEventsWebhooksWithResponse(h.c.ctx, e2bapi.WebhookCreate{
		Name: "bugnet", Url: h.url, Events: hookEvents["e2b"], SignatureSecret: secret,
	})
	if err != nil {
		return err
	}
	if resp.JSON201 == nil {
		return fail("registering the webhook", resp.StatusCode())
	}
	h.out("E2B_WEBHOOK_SECRET=%s", secret)
	h.out("team_id = %q", resp.JSON201.TeamId)
	return nil
}

func (h *hookClient) registerBoat() error {
	name := "bugnet"
	events := make([]boatapi.WebhookEventType, 0, len(hookEvents["boat"]))
	for _, e := range hookEvents["boat"] {
		events = append(events, boatapi.WebhookEventType(e))
	}
	resp, err := h.boat.CreateWebhookWithResponse(h.c.ctx,
		boatapi.WebhookCreateRequest{Name: &name, Url: h.url, Events: events})
	if err != nil {
		return err
	}
	if resp.JSON201 == nil {
		return fail("registering the webhook", resp.StatusCode())
	}
	h.out("BOAT_WEBHOOK_SECRET=%s", resp.JSON201.Secret)
	return nil
}

func (h *hookClient) list() error {
	if h.prov == "boat" {
		resp, err := h.boat.ListWebhooksWithResponse(h.c.ctx)
		if err != nil {
			return err
		}
		if resp.JSON200 == nil {
			return fail("listing the webhooks", resp.StatusCode())
		}
		for _, w := range resp.JSON200.Webhooks {
			events := make([]string, len(w.Events))
			for i, e := range w.Events {
				events[i] = string(e)
			}
			h.out("%s %s %s", w.Id, h.shown(w.Url), strings.Join(events, ","))
		}
		return nil
	}
	resp, err := h.e2b.GetEventsWebhooksWithResponse(h.c.ctx)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fail("listing the webhooks", resp.StatusCode())
	}
	for _, w := range *resp.JSON200 {
		h.out("%s %s %s enabled=%t", w.Id, h.shown(w.Url), strings.Join(w.Events, ","), w.Enabled)
		if err := h.deliveries(w.Id); err != nil {
			return err
		}
	}
	return nil
}

// deliveries prints the latest attempts of one E2B endpoint
func (h *hookClient) deliveries(id string) error {
	uid, err := parseWebhookID(id)
	if err != nil {
		return err
	}
	limit := int32(hookDeliveries)
	resp, err := h.e2b.GetEventsWebhooksWebhookIDDeliveriesWithResponse(h.c.ctx, uid,
		&e2bapi.GetEventsWebhooksWebhookIDDeliveriesParams{Limit: &limit})
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fail("listing the deliveries", resp.StatusCode())
	}
	for _, g := range resp.JSON200.Data {
		for _, a := range g.Attempts {
			code := "none"
			if a.ResponseHttpStatusCode != nil {
				code = fmt.Sprint(*a.ResponseHttpStatusCode)
			}
			h.out("  %s %s %s http %s", a.Timestamp.UTC().Format(time.RFC3339), a.EventType,
				a.Status, code)
		}
	}
	return nil
}

func parseWebhookID(id string) (openapi_types.UUID, error) {
	var u openapi_types.UUID
	if err := u.UnmarshalText([]byte(id)); err != nil {
		return u, fmt.Errorf("%w: %q is not an E2B webhook id", errUsage, id)
	}
	return u, nil
}

// find returns the one endpoint of this deployment, an error for none or several
func (h *hookClient) find() (string, error) {
	var ids []string
	if h.prov == "boat" {
		resp, err := h.boat.ListWebhooksWithResponse(h.c.ctx)
		if err != nil {
			return "", err
		}
		if resp.JSON200 == nil {
			return "", fail("listing the webhooks", resp.StatusCode())
		}
		for _, w := range resp.JSON200.Webhooks {
			if h.ours(w.Url) {
				ids = append(ids, w.Id)
			}
		}
	} else {
		resp, err := h.e2b.GetEventsWebhooksWithResponse(h.c.ctx)
		if err != nil {
			return "", err
		}
		if resp.JSON200 == nil {
			return "", fail("listing the webhooks", resp.StatusCode())
		}
		for _, w := range *resp.JSON200 {
			if h.ours(w.Url) {
				ids = append(ids, w.Id)
			}
		}
	}
	switch len(ids) {
	case 1:
		return ids[0], nil
	case 0:
		return "", fmt.Errorf("no %s webhook of this deployment is registered", h.prov)
	}
	return "", fmt.Errorf("%d %s webhooks of this deployment are registered, "+
		"delete the extra ones with hooks delete", len(ids), h.prov)
}

func (h *hookClient) rotate() error {
	id, err := h.find()
	if err != nil {
		return err
	}
	if h.prov == "boat" {
		resp, err := h.boat.RotateWebhookSigningSecretWithResponse(h.c.ctx, id)
		if err != nil {
			return err
		}
		if resp.JSON200 == nil {
			return fail("rotating the secret", resp.StatusCode())
		}
		h.out("BOAT_WEBHOOK_SECRET=%s", resp.JSON200.Secret)
		return nil
	}
	uid, err := parseWebhookID(id)
	if err != nil {
		return err
	}
	raw := make([]byte, hookSecretBytes)
	_, _ = rand.Read(raw)
	secret := base64.RawURLEncoding.EncodeToString(raw)
	resp, err := h.e2b.PatchEventsWebhooksWebhookIDWithResponse(h.c.ctx, uid,
		e2bapi.WebhookConfiguration{SignatureSecret: &secret})
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fail("rotating the secret", resp.StatusCode())
	}
	h.out("E2B_WEBHOOK_SECRET=%s", secret)
	return nil
}

func (h *hookClient) remove(id string) error {
	if h.prov == "boat" {
		resp, err := h.boat.DeleteWebhookWithResponse(h.c.ctx, id)
		if err != nil {
			return err
		}
		if resp.StatusCode() != http.StatusOK {
			return fail("deleting the webhook", resp.StatusCode())
		}
	} else {
		uid, err := parseWebhookID(id)
		if err != nil {
			return err
		}
		resp, err := h.e2b.DeleteEventsWebhooksWebhookIDWithResponse(h.c.ctx, uid)
		if err != nil {
			return err
		}
		if resp.StatusCode() >= 300 {
			return fail("deleting the webhook", resp.StatusCode())
		}
	}
	h.out("deleted %s", id)
	return nil
}
