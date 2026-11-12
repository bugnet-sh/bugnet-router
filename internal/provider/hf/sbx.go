package hf

import (
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

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// sbxDomain holds every Job's ports on HF's proxy, the only hosts the sandbox token goes to
	sbxDomain = ".hf.jobs"
	// maxHealthBytes bounds a health answer, which the Job's root controls
	maxHealthBytes = 64 << 10
)

// sbxClient calls sbx-server in a Job through HF's proxy, carrying the sandbox's own token alone
type sbxClient struct {
	client *http.Client
}

func newSBXClient() *sbxClient {
	return &sbxClient{client: &http.Client{CheckRedirect: provider.NoRedirect}}
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
			scrub(string(raw), token))}
	}
	var h health
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxHealthBytes)).Decode(&h); err != nil {
		return health{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s sbx-server health: %w", id, err)}
	}
	return h, nil
}
