package lium

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// templateCommand keeps the container running, our image having no command of its own
	templateCommand  = "sleep infinity"
	templateCategory = "UBUNTU"
	sshPort          = 22
	defaultVolume    = "/workspace"
)

var (
	digestWord = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	nameWord   = regexp.MustCompile(`[^a-z0-9]+`)
)

// imageRef is an OCI reference by digest, split as a template names it
type imageRef struct{ repo, tag, digest string }

// parseImageRef splits a ref with an optional tag and a sha256 digest, refusing one without
func parseImageRef(ref string) (imageRef, error) {
	name, digest, ok := strings.Cut(ref, "@")
	if !ok || name == "" || !digestWord.MatchString(digest) {
		return imageRef{}, invalid(fmt.Errorf("lium image %q is not named by digest", ref), true)
	}
	r := imageRef{repo: name, digest: digest}
	// A tag follows the last colon after the last slash, a registry port comes before it
	if i := strings.LastIndex(name, ":"); i > strings.LastIndex(name, "/") {
		r.repo, r.tag = name[:i], name[i+1:]
	}
	if r.repo == "" {
		return imageRef{}, invalid(fmt.Errorf("lium image %q names no repository", ref), true)
	}
	return r, nil
}

// templateSpec is the body of POST /templates and what a listed template must equal
type templateSpec struct {
	Name                     string            `json:"name"`
	DockerImage              string            `json:"docker_image"`
	DockerImageTag           string            `json:"docker_image_tag,omitempty"`
	DockerImageDigest        string            `json:"docker_image_digest"`
	Category                 string            `json:"category"`
	IsPrivate                bool              `json:"is_private"`
	Volumes                  []string          `json:"volumes"`
	InternalPorts            []int             `json:"internal_ports"`
	Environment              map[string]string `json:"environment"`
	StartupCommands          string            `json:"startup_commands"`
	OneTimeTemplate          bool              `json:"one_time_template"`
	IsTemporary              bool              `json:"is_temporary"`
	SupportsDocker           bool              `json:"supports_docker"`
	SupportsVolumeEncryption bool              `json:"supports_volume_encryption"`
}

// templateSpecFor builds the private template for one image ref, the configured image taking
// the configured version as its tag
func (p *Provider) templateSpecFor(ref string) (templateSpec, error) {
	r, err := parseImageRef(ref)
	if err != nil {
		return templateSpec{}, err
	}
	if r.tag == "" && ref == p.image {
		r.tag = p.imageVersion
	}
	words := []string{path.Base(r.repo)}
	if r.tag != "" {
		words = append(words, r.tag)
	}
	words = append(words, strings.TrimPrefix(r.digest, "sha256:")[:12])
	name := strings.Trim(nameWord.ReplaceAllString(strings.ToLower(strings.Join(words, "-")),
		"-"), "-")
	return templateSpec{
		Name: name, DockerImage: r.repo, DockerImageTag: r.tag, DockerImageDigest: r.digest,
		Category: templateCategory, IsPrivate: true, Volumes: []string{p.volume},
		InternalPorts: []int{sshPort}, Environment: map[string]string{},
		StartupCommands: templateCommand,
	}, nil
}

// listedTemplate holds the fields of one GET /templates entry the choice reads
type listedTemplate struct {
	ID              string                     `json:"id"`
	Name            string                     `json:"name"`
	DockerImage     string                     `json:"docker_image"`
	Digest          *string                    `json:"docker_image_digest"`
	IsPrivate       *bool                      `json:"is_private"`
	UserID          *string                    `json:"user_id"`
	Volumes         []string                   `json:"volumes"`
	InternalPorts   []json.RawMessage          `json:"internal_ports"`
	Environment     map[string]json.RawMessage `json:"environment"`
	Entrypoint      *string                    `json:"entrypoint"`
	StartupCommands *string                    `json:"startup_commands"`
	OneTimeTemplate *bool                      `json:"one_time_template"`
	IsTemporary     *bool                      `json:"is_temporary"`
}

// takes reports whether t is our own private template of spec, owner the account id or empty
// when the account names none
func (t listedTemplate) takes(spec templateSpec, owner string) bool {
	ports := make([]int, 0, len(t.InternalPorts))
	for _, raw := range t.InternalPorts {
		n, ok := intOf(raw)
		if !ok {
			return false
		}
		ports = append(ports, n)
	}
	return t.ID != "" && is(t.IsPrivate) && (owner == "" || deref(t.UserID) == owner) &&
		t.Name == spec.Name && t.DockerImage == spec.DockerImage &&
		deref(t.Digest) == spec.DockerImageDigest &&
		deref(t.StartupCommands) == spec.StartupCommands && deref(t.Entrypoint) == "" &&
		slices.Equal(ports, spec.InternalPorts) && slices.Equal(t.Volumes, spec.Volumes) &&
		len(t.Environment) == 0 && !is(t.OneTimeTemplate) && !is(t.IsTemporary)
}

func is(b *bool) bool { return b != nil && *b }

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// intOf reads a JSON number or a number in a string
func intOf(raw json.RawMessage) (int, bool) {
	var n int
	if json.Unmarshal(raw, &n) == nil {
		return n, true
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	return n, err == nil
}

// template finds or makes the private template for an image ref, once per process
func (p *Provider) template(ctx context.Context, ref string) (string, error) {
	p.tmu.Lock()
	defer p.tmu.Unlock()
	if id, ok := p.templates[ref]; ok {
		return id, nil
	}
	spec, err := p.templateSpecFor(ref)
	if err != nil {
		return "", err
	}
	owner, err := p.accountID(ctx)
	if err != nil {
		return "", err
	}
	id, err := p.findTemplate(ctx, spec, owner)
	if err != nil {
		return "", err
	}
	if id == "" {
		if id, err = p.makeTemplate(ctx, spec, owner); err != nil {
			return "", err
		}
	}
	p.templates[ref] = id
	return id, nil
}

// dropTemplate forgets the template of ref when it is still id, so the next create looks again
func (p *Provider) dropTemplate(ref, id string) {
	p.tmu.Lock()
	defer p.tmu.Unlock()
	if p.templates[ref] == id {
		delete(p.templates, ref)
	}
}

// accountID reads our account's id once, empty when GET /users/me names none
func (p *Provider) accountID(ctx context.Context) (string, error) {
	if p.account != nil {
		return *p.account, nil
	}
	var me map[string]any
	if _, err := p.api.do(ctx, call{method: http.MethodGet, route: "/users/me"}, &me); err != nil {
		return "", err
	}
	id, _ := me["id"].(string)
	p.account = &id
	return id, nil
}

// findTemplate returns the id of a listed template takes accepts, empty when none does
func (p *Provider) findTemplate(
	ctx context.Context, spec templateSpec, owner string,
) (string, error) {
	var raw []json.RawMessage
	_, err := p.api.do(ctx, call{method: http.MethodGet, route: "/templates"}, &raw)
	if err != nil {
		return "", err
	}
	for _, r := range raw {
		var t listedTemplate
		// A template another account wrote in a shape we cannot read is never taken
		if json.Unmarshal(r, &t) == nil && t.takes(spec, owner) {
			return t.ID, nil
		}
	}
	return "", nil
}

// makeTemplate has Lium check the template, saves it and returns its id
func (p *Provider) makeTemplate(
	ctx context.Context, spec templateSpec, owner string,
) (string, error) {
	body, err := json.Marshal(spec)
	if err != nil {
		return "", &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	var issues []struct {
		Field, Code, Message string
	}
	_, err = p.api.do(ctx, call{method: http.MethodPost, route: "/templates/check", body: body},
		&issues)
	if err != nil {
		return "", err
	}
	if len(issues) > 0 {
		words := make([]string, 0, len(issues))
		for _, i := range issues {
			words = append(words, fmt.Sprintf("%s %s %s", i.Field, i.Code, i.Message))
		}
		return "", invalid(fmt.Errorf("lium refuses template %s, %s", spec.Name,
			strings.Join(words, ", ")), true)
	}
	var made struct {
		ID string `json:"id"`
	}
	_, err = p.api.do(ctx, call{method: http.MethodPost, route: "/templates", body: body}, &made)
	if err != nil {
		return "", err
	}
	if made.ID != "" {
		return made.ID, nil
	}
	id, err := p.findTemplate(ctx, spec, owner)
	if err == nil && id == "" {
		err = &provider.Error{Code: provider.CodeInternal, CanFailOver: true,
			Err: errors.New("lium saved template " + spec.Name + " but names and lists no id")}
	}
	return id, err
}

// invalid is a refused request of ours, which another provider may still serve
func invalid(err error, failOver bool) error {
	return &provider.Error{Code: provider.CodeInvalid, CanFailOver: failOver, Err: err}
}
