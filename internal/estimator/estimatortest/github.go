// Package estimatortest holds the fake GitHub, the fake Hugging Face Hub and the flaky Valkey that
// the estimator's tests share
package estimatortest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Repo is one fake repository
type Repo struct {
	Owner, Name string
	Private     bool
	// Branches and Tags map a name to a commit
	Branches map[string]string
	Tags     map[string]string
	// Commits map a commit to its files, path to content
	Commits map[string]map[string]string
	// Default is the branch HEAD names
	Default string
}

// GitHub is a fake of api.github.com, github.com's smart HTTP and raw.githubusercontent.com on
// one test server
type GitHub struct {
	Srv *httptest.Server
	// Counters of the calls served
	Refs, GraphQL, Tree, Raw, Other atomic.Int32
	// Delay slows every answer
	Delay time.Duration
	// Cost is the GraphQL rateLimit cost it reports, 1 when zero
	Cost int
	// Remaining and Limit go into the rate limit headers of API answers, absent when Limit is 0
	Remaining, Limit int
	// Reset is the x-ratelimit-reset epoch second, an hour from the start when zero
	Reset int64
	// Moved lists blob paths whose oid GraphQL reports different from the tree's
	Moved map[string]bool
	// PrivateTokenRepos makes /user/repos list a private repository
	PrivateTokenRepos bool

	mu      sync.Mutex
	repos   map[string]*Repo
	queries []string
	auth    []string
}

// NewGitHub starts a fake with repos, closed when the test ends
func NewGitHub(t *testing.T, repos ...*Repo) *GitHub {
	t.Helper()
	g := &GitHub{repos: map[string]*Repo{}, Moved: map[string]bool{}}
	for _, r := range repos {
		g.repos[strings.ToLower(r.Owner+"/"+r.Name)] = r
	}
	g.Srv = httptest.NewServer(http.HandlerFunc(g.serve))
	t.Cleanup(g.Srv.Close)
	return g
}

// URL is the base URL of every host the fake plays
func (g *GitHub) URL() string { return g.Srv.URL }

// Queries returns the GraphQL query texts received, in order
func (g *GitHub) Queries() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.queries...)
}

// Auths returns the Authorization header of each API call, in order
func (g *GitHub) Auths() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.auth...)
}

// Oid is the blob id the fake gives content
func Oid(content string) string {
	s := sha256.Sum256([]byte(content))
	return hex.EncodeToString(s[:])[:40]
}

func pkt(s string) string { return fmt.Sprintf("%04x%s", len(s)+4, s) }

func (g *GitHub) repo(owner, name string) *Repo {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.repos[strings.ToLower(owner+"/"+strings.TrimSuffix(name, ".git"))]
}

func (g *GitHub) headers(w http.ResponseWriter) {
	if g.Limit == 0 {
		return
	}
	reset := g.Reset
	if reset == 0 {
		reset = time.Now().Add(time.Hour).Unix()
	}
	w.Header().Set("X-Ratelimit-Limit", fmt.Sprint(g.Limit))
	w.Header().Set("X-Ratelimit-Remaining", fmt.Sprint(g.Remaining))
	w.Header().Set("X-Ratelimit-Reset", fmt.Sprint(reset))
}

var treePath = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/git/trees/([^/]+)$`)
var rawPath = regexp.MustCompile(`^/([^/]+)/([^/]+)/([0-9a-f]{7,40})/(.+)$`)
var refsPath = regexp.MustCompile(`^/([^/]+)/([^/]+)\.git/info/refs$`)
var aliasVar = regexp.MustCompile(`(\w+): object\(expression: \$(\w+)\)`)

func (g *GitHub) serve(w http.ResponseWriter, r *http.Request) {
	if g.Delay > 0 {
		select {
		case <-time.After(g.Delay):
		case <-r.Context().Done():
			return
		}
	}
	p := r.URL.Path
	switch {
	case refsPath.MatchString(p):
		g.refs(w, r)
	case p == "/graphql":
		g.graphql(w, r)
	case treePath.MatchString(p):
		g.tree(w, r)
	case p == "/rate_limit":
		g.Other.Add(1)
		_, _ = io.WriteString(w, `{"resources":{"core":{"limit":5000,"remaining":4999},`+
			`"graphql":{"limit":5000,"remaining":5000}}}`)
	case p == "/user/repos":
		g.Other.Add(1)
		if g.PrivateTokenRepos {
			_, _ = io.WriteString(w, `[{"full_name":"x/private"}]`)
			return
		}
		_, _ = io.WriteString(w, `[]`)
	case rawPath.MatchString(p):
		g.raw(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (g *GitHub) refs(w http.ResponseWriter, r *http.Request) {
	g.Refs.Add(1)
	m := refsPath.FindStringSubmatch(r.URL.Path)
	repo := g.repo(m[1], m[2])
	if repo == nil {
		http.NotFound(w, r)
		return
	}
	var b strings.Builder
	b.WriteString(pkt("# service=git-upload-pack\n") + "0000")
	head := repo.Branches[repo.Default]
	b.WriteString(pkt(head + " HEAD\x00multi_ack symref=HEAD:refs/heads/" + repo.Default + "\n"))
	var names []string
	for n := range repo.Branches {
		names = append(names, "refs/heads/"+n)
	}
	for n := range repo.Tags {
		names = append(names, "refs/tags/"+n)
	}
	sort.Strings(names)
	for _, n := range names {
		sha := repo.Branches[strings.TrimPrefix(n, "refs/heads/")]
		if t, ok := strings.CutPrefix(n, "refs/tags/"); ok {
			sha = repo.Tags[t]
		}
		b.WriteString(pkt(sha + " " + n + "\n"))
	}
	b.WriteString("0000")
	_, _ = io.WriteString(w, b.String())
}

// files returns a commit's files, nil when the commit is unknown
func (r *Repo) files(sha string) map[string]string {
	for c, f := range r.Commits {
		if strings.HasPrefix(c, sha) {
			return f
		}
	}
	return nil
}

func (g *GitHub) tree(w http.ResponseWriter, r *http.Request) {
	g.Tree.Add(1)
	g.mu.Lock()
	g.auth = append(g.auth, r.Header.Get("Authorization"))
	g.mu.Unlock()
	g.headers(w)
	w.Header().Set("X-Ratelimit-Resource", "core")
	m := treePath.FindStringSubmatch(r.URL.Path)
	repo := g.repo(m[1], m[2])
	if repo == nil {
		http.NotFound(w, r)
		return
	}
	files := repo.files(m[3])
	if files == nil {
		http.NotFound(w, r)
		return
	}
	type entry struct {
		Path string `json:"path"`
		Type string `json:"type"`
		SHA  string `json:"sha"`
		Size int64  `json:"size"`
	}
	var tree []entry
	for p, c := range files {
		tree = append(tree, entry{p, "blob", Oid(c), int64(len(c))})
	}
	sort.Slice(tree, func(i, j int) bool { return tree[i].Path < tree[j].Path })
	_ = json.NewEncoder(w).Encode(map[string]any{"truncated": false, "tree": tree})
}

func (g *GitHub) raw(w http.ResponseWriter, r *http.Request) {
	g.Raw.Add(1)
	m := rawPath.FindStringSubmatch(r.URL.Path)
	repo := g.repo(m[1], m[2])
	if repo == nil {
		http.NotFound(w, r)
		return
	}
	files := repo.files(m[3])
	c, ok := files[m[4]]
	if !ok {
		http.NotFound(w, r)
		return
	}
	_, _ = io.WriteString(w, c)
}

func (g *GitHub) graphql(w http.ResponseWriter, r *http.Request) {
	g.GraphQL.Add(1)
	var req struct {
		Query     string            `json:"query"`
		Variables map[string]string `json:"variables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad", http.StatusBadRequest)
		return
	}
	g.mu.Lock()
	g.queries = append(g.queries, req.Query)
	g.auth = append(g.auth, r.Header.Get("Authorization"))
	g.mu.Unlock()
	g.headers(w)
	w.Header().Set("X-Ratelimit-Resource", "graphql")
	cost := g.Cost
	if cost == 0 {
		cost = 1
	}
	repo := g.repo(req.Variables["owner"], req.Variables["name"])
	out := map[string]any{"rateLimit": map[string]any{"cost": cost, "remaining": 4999}}
	if repo == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"rateLimit": out["rateLimit"], "repository": nil},
			"errors": []map[string]string{{"type": "NOT_FOUND"}}})
		return
	}
	sha, _, _ := strings.Cut(req.Variables["c"], ":")
	files := repo.files(sha)
	full := sha
	for c := range repo.Commits {
		if strings.HasPrefix(c, sha) {
			full = c
		}
	}
	rep := map[string]any{"isPrivate": repo.Private, "commit": map[string]string{"oid": full}}
	blob := func(p, content string) map[string]any {
		oid := Oid(content)
		if g.Moved[p] {
			oid = Oid(content + "moved")
		}
		return map[string]any{"oid": oid, "byteSize": len(content), "isBinary": false,
			"isTruncated": false, "text": content}
	}
	for _, m := range aliasVar.FindAllStringSubmatch(req.Query, -1) {
		alias, variable := m[1], m[2]
		if alias == "commit" {
			continue
		}
		_, p, _ := strings.Cut(req.Variables[variable], ":")
		if strings.HasPrefix(alias, "d") {
			var entries []map[string]any
			var names []string
			for fp := range files {
				if strings.HasPrefix(fp, p+"/") && !strings.Contains(fp[len(p)+1:], "/") {
					names = append(names, fp)
				}
			}
			sort.Strings(names)
			for _, fp := range names {
				entries = append(entries, map[string]any{"name": path.Base(fp), "type": "blob",
					"object": blob(fp, files[fp])})
			}
			if len(entries) > 0 {
				rep[alias] = map[string]any{"entries": entries}
			}
			continue
		}
		if c, ok := files[p]; ok {
			rep[alias] = blob(p, c)
		}
	}
	out["repository"] = rep
	_ = json.NewEncoder(w).Encode(map[string]any{"data": out})
}
