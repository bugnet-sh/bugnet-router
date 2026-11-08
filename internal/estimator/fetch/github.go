package fetch

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/canon"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/source"
)

const (
	// maxExtraLocks and maxExtraLFS are the files read beyond the signal files
	maxExtraLocks = 8
	maxExtraLFS   = 8
	// pointer sizes of a Git LFS pointer file, version, oid and size lines
	lfsPointerMin = 120
	lfsPointerMax = 140
)

// GitHub reads repositories through GraphQL with a token, or through the tree and raw without one
type GitHub struct {
	cfg   config.Estimator
	token string
	cache *Cache
	guard *Guard
	log   *slog.Logger
	up    *upstream
	query string
	slots []slot
}

// slot is one signal path the GraphQL query asks for, as an alias and a variable
type slot struct {
	alias, variable string
	path            string
	dir, rootOnly   bool
}

// NewGitHub returns a client over the configured hosts, token empty for anonymous reads
func NewGitHub(
	cfg config.Estimator, token string, cache *Cache, guard *Guard, log *slog.Logger,
) *GitHub {
	if log == nil {
		log = slog.Default()
	}
	apiHost := hostOf(cfg.GithubAPI)
	g := &GitHub{cfg: cfg, token: token, cache: cache, guard: guard, log: log,
		up: newUpstream(cfg.GithubConcurrency, func(req *http.Request, via []*http.Request) error {
			if len(via) > 1 || !strings.EqualFold(req.URL.Host, apiHost) ||
				!strings.EqualFold(via[0].URL.Host, apiHost) {
				return http.ErrUseLastResponse
			}
			return nil
		})}
	g.slots = signalSlots()
	g.query = buildQuery(g.slots)
	return g
}

var _ Fetcher = (*GitHub)(nil)

// signalSlots is the fixed alias table, the signal files and then the signal directories
func signalSlots() []slot {
	var out []slot
	for _, p := range facts.SignalFiles {
		out = append(out, slot{path: p})
	}
	for _, p := range facts.SignalDirs {
		out = append(out, slot{path: p, dir: true, rootOnly: p == ".github/workflows"})
	}
	for i := range out {
		n := strconv.Itoa(i + 1)
		out[i].variable = "e" + n
		out[i].alias = "f" + n
		if out[i].dir {
			out[i].alias = "d" + n
		}
	}
	return out
}

const blobFields = "oid byteSize isBinary isTruncated text"

// buildQuery is the one query text, a constant of the alias table, with variables for every value
func buildQuery(slots []slot) string {
	var b strings.Builder
	b.WriteString("query($owner: String!, $name: String!, $c: String!")
	for _, s := range slots {
		b.WriteString(", $" + s.variable + ": String!")
	}
	b.WriteString(") { rateLimit { cost remaining resetAt } " +
		"repository(owner: $owner, name: $name) { isPrivate " +
		"commit: object(expression: $c) { ... on Commit { oid } }")
	for _, s := range slots {
		if s.dir {
			fmt.Fprintf(&b, " %s: object(expression: $%s) { ... on Tree { entries { name type "+
				"object { ... on Blob { %s } } } } }", s.alias, s.variable, blobFields)
			continue
		}
		fmt.Fprintf(&b, " %s: object(expression: $%s) { ... on Blob { %s } }", s.alias,
			s.variable, blobFields)
	}
	b.WriteString(" } }")
	return b.String()
}

// Query returns the GraphQL query text, which is the same for every source
func (g *GitHub) Query() string { return g.query }

func (g *GitHub) authHeader() map[string]string {
	h := map[string]string{"Accept": "application/vnd.github+json"}
	if g.token != "" {
		h["Authorization"] = "Bearer " + g.token
	}
	return h
}

func hash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// repoKey is the lowercased cache key, the subdirectory kept as given after it
func facts1Key(src source.Source, subdir []string) string {
	k := src.Key()
	if len(subdir) > 0 {
		k += "/" + strings.Join(subdir, "/")
	}
	return k
}

// splitFallback is the ref and subdirectory an unread source reports, the first segment as ref
func splitFallback(src source.Source) (string, []string) {
	ref, sub, err := src.Split(nil)
	if err != nil {
		return "", nil
	}
	return ref, sub
}

// Facts reads the repository at the commit the source names, or its default branch
func (g *GitHub) Facts(ctx context.Context, src source.Source) (facts.Facts, Resolved, error) {
	repoKey := src.Key()
	fbRef, fbSub := splitFallback(src)
	unread := func(r rules.UnreadReason) (facts.Facts, Resolved, error) {
		return facts.Facts{}, Resolved{Ref: fbRef, Subdir: fbSub, Unread: r}, nil
	}
	if r, ok := g.cache.Neg(ctx, "github", repoKey); ok {
		return unread(r)
	}
	refHash := hash(strings.Join(src.RefPath, "/"))
	lookup := func(ctx context.Context) (facts.Facts, Resolved, bool) {
		v, ok := g.cache.Ref(ctx, "github", repoKey, refHash)
		if !ok {
			return facts.Facts{}, Resolved{}, false
		}
		sha, ref, _ := strings.Cut(v, "\n")
		n := 0
		if ref != "" {
			n = strings.Count(ref, "/") + 1
		}
		if n > len(src.RefPath) {
			return facts.Facts{}, Resolved{}, false
		}
		sub := slices.Clone(src.RefPath[n:])
		if len(sub) == 0 {
			sub = nil
		}
		f, ok := g.cache.Facts(ctx, "github", facts1Key(src, sub), sha)
		return f, Resolved{Ref: ref, Subdir: sub, Revision: sha}, ok
	}
	if f, r, ok := lookup(ctx); ok {
		return f, r, nil
	}
	return g.cache.Do(ctx, "github:"+repoKey+":"+refHash, lookup,
		func(ctx context.Context) (facts.Facts, Resolved, error) {
			return g.run(ctx, src, refHash)
		})
}

// run resolves the ref to a commit, then reads the facts unless another replica cached them
func (g *GitHub) run(
	ctx context.Context, src source.Source, refHash string,
) (facts.Facts, Resolved, error) {
	repoKey := src.Key()
	ref, sub, sha, tag, unread, err := g.resolve(ctx, src)
	res := Resolved{Ref: ref, Subdir: sub, Revision: sha, Unread: unread}
	if err != nil || unread != "" {
		if unread == "" {
			res.Unread = rules.UnreadUpstreamError
		}
		if negative(unread) && sha == "" {
			g.cache.PutNeg(ctx, "github", repoKey, unread)
		}
		return facts.Facts{}, res, nil
	}
	g.cache.PutRef(ctx, "github", repoKey, refHash, sha+"\n"+ref, tag)
	key := facts1Key(src, sub)
	if f, ok := g.cache.Facts(ctx, "github", key, sha); ok {
		return f, res, nil
	}
	f, unread, err := g.read(ctx, src, sub, sha, key)
	if err != nil {
		g.log.Warn("estimator: read github source", "err", err)
		res.Unread = rules.UnreadUpstreamError
		return facts.Facts{}, res, nil
	}
	if unread != "" {
		if negative(unread) {
			g.cache.PutNeg(ctx, "github", repoKey, unread)
		}
		res.Unread = unread
		return facts.Facts{}, res, nil
	}
	return f, res, nil
}

/* Refs */

// refList is a repository's ref advertisement
type refList struct {
	refs  map[string]string
	names []string
}

// resolve turns the source's ref path into a ref, a subdirectory and a commit
func (g *GitHub) resolve(ctx context.Context, src source.Source) (
	ref string, sub []string, sha string, tag bool, unread rules.UnreadReason, err error,
) {
	if commit := src.CommitRef(); commit != "" {
		ref, sub, err = src.Split(nil)
		return ref, sub, strings.ToLower(commit), false, "", err
	}
	list, unread, err := g.listRefs(ctx, src)
	if err != nil || unread != "" {
		return "", nil, "", false, unread, err
	}
	ref, sub, err = src.Split(list.names)
	if err != nil {
		return "", nil, "", false, rules.UnreadNotFound, nil
	}
	if ref == "" {
		sha = list.refs["HEAD"]
	} else if sha = list.refs["refs/heads/"+ref]; sha == "" {
		sha = cmp.Or(list.refs["refs/tags/"+ref+"^{}"], list.refs["refs/tags/"+ref])
		tag = sha != ""
	}
	if sha == "" {
		return ref, sub, "", false, rules.UnreadNotFound, nil
	}
	return ref, sub, sha, tag, "", nil
}

// listRefs reads the repository's ref advertisement from smart HTTP, which costs no REST quota
func (g *GitHub) listRefs(
	ctx context.Context, src source.Source,
) (refList, rules.UnreadReason, error) {
	u := g.cfg.GithubGit + "/" + url.PathEscape(src.Owner) + "/" + url.PathEscape(src.Repo) +
		".git/info/refs?service=git-upload-pack"
	resp, err := g.up.do(ctx, http.MethodGet, u, nil, nil, maxRefsBytes)
	switch {
	case errors.Is(err, errTooBig):
		return refList{}, rules.UnreadUpstreamError, nil
	case err != nil:
		return refList{}, "", err
	case resp.status != http.StatusOK:
		return refList{}, classify(resp.status, resp.header, true), nil
	}
	list, err := parseRefs(resp.body)
	if err != nil {
		return refList{}, rules.UnreadUpstreamError, nil
	}
	if len(list.refs) == 0 {
		return refList{}, rules.UnreadNotFound, nil
	}
	return list, "", nil
}

// parseRefs reads a v0 smart HTTP advertisement
func parseRefs(b []byte) (refList, error) {
	list := refList{refs: map[string]string{}}
	for len(b) >= 4 {
		n, err := strconv.ParseUint(string(b[:4]), 16, 32)
		if err != nil {
			return refList{}, errors.New("fetch: bad pkt-line length")
		}
		if n == 0 {
			b = b[4:]
			continue
		}
		if n < 4 || int(n) > len(b) {
			return refList{}, errors.New("fetch: truncated pkt-line")
		}
		line := b[4:n]
		b = b[n:]
		if bytes.HasPrefix(line, []byte("#")) {
			continue
		}
		if i := bytes.IndexByte(line, 0); i >= 0 {
			line = line[:i]
		}
		sha, name, ok := strings.Cut(strings.TrimRight(string(line), "\n"), " ")
		if !ok || len(sha) != 40 || name == "" {
			continue
		}
		list.refs[name] = sha
		if rest, ok := strings.CutPrefix(name, "refs/heads/"); ok {
			list.names = append(list.names, rest)
		} else if rest, ok := strings.CutPrefix(name, "refs/tags/"); ok {
			list.names = append(list.names, strings.TrimSuffix(rest, "^{}"))
		}
	}
	slices.Sort(list.names)
	list.names = slices.Compact(list.names)
	return list, nil
}

/* Reading */

// treeEntry is one entry of the recursive tree answer
type treeEntry struct {
	Path string `json:"path"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
	Size int64  `json:"size"`
}

type treeAnswer struct {
	Truncated bool        `json:"truncated"`
	Tree      []treeEntry `json:"tree"`
}

// read fetches the facts of a source at a commit, the GraphQL way when the budget allows
func (g *GitHub) read(
	ctx context.Context, src source.Source, sub []string, sha, key string,
) (facts.Facts, rules.UnreadReason, error) {
	if ok, until := g.guard.Allow(ctx, ResourceGitHubCore); !ok {
		g.log.Warn("estimator: github rest budget low", "until", until)
		return facts.Facts{}, rules.UnreadRateLimited, nil
	}
	files := map[string][]byte{}
	oids := map[string]string{}
	gql := false
	if g.token != "" {
		if ok, _ := g.guard.Allow(ctx, ResourceGitHubGraphQL); ok {
			var unread rules.UnreadReason
			var err error
			files, oids, unread, err = g.graphql(ctx, src, sub, sha)
			if err != nil || unread != "" {
				return facts.Facts{}, unread, err
			}
			gql = true
		}
	}
	tree, unread, err := g.tree(ctx, src, sha)
	if err != nil || unread != "" {
		return facts.Facts{}, unread, err
	}
	byPath := make(map[string]treeEntry, len(tree))
	for _, e := range tree {
		byPath[e.Path] = e
	}
	prefix := strings.Join(sub, "/")
	abs := func(rel string) string {
		if prefix == "" || strings.HasPrefix(rel, ".github/workflows/") {
			return rel
		}
		return prefix + "/" + rel
	}
	var want []fileReq
	if gql {
		// A blob that moved between the calls, or came back cut short, is read again at the commit
		for rel, oid := range oids {
			if e, ok := byPath[abs(rel)]; !ok || e.SHA != oid {
				want = append(want, fileReq{rel, abs(rel)})
			}
		}
	} else {
		for _, rel := range g.anonymousFiles(tree, prefix) {
			want = append(want, fileReq{rel, abs(rel)})
		}
	}
	for _, w := range want {
		delete(files, w.key)
	}
	for _, p := range g.extraFiles(tree, files, prefix, abs) {
		want = append(want, fileReq{p, p})
	}
	slices.SortFunc(want, func(a, b fileReq) int { return cmp.Compare(a.key, b.key) })
	if err := g.raw(ctx, src, sha, want, files); err != nil {
		return facts.Facts{}, "", err
	}
	blobs := make([]facts.Blob, 0, len(tree))
	for _, e := range tree {
		blobs = append(blobs, facts.Blob{Path: e.Path, SHA: e.SHA, Size: e.Size, Type: e.Type})
	}
	f := facts.Extract(files, blobs)
	b, err := canon.Marshal(f)
	if err != nil {
		return facts.Facts{}, "", err
	}
	if int64(len(b)) > g.cfg.MaxFactsBytes {
		g.log.Warn("estimator: facts past the size cap", "bytes", len(b))
		return facts.Facts{}, rules.UnreadUpstreamError, nil
	}
	g.cache.PutFacts(ctx, "github", key, sha, b, sum(b))
	return f, "", nil
}

func sum(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}

// anonymousFiles picks at most max_files signal files that the tree shows exist
func (g *GitHub) anonymousFiles(tree []treeEntry, prefix string) []string {
	in := map[string]bool{}
	for _, e := range tree {
		if e.Type == "blob" && e.Size <= g.cfg.MaxFileBytes {
			in[e.Path] = true
		}
	}
	join := func(p string) string {
		if prefix == "" {
			return p
		}
		return prefix + "/" + p
	}
	var out []string
	for _, p := range facts.SignalFiles {
		if in[join(p)] {
			out = append(out, p)
		}
	}
	for _, d := range facts.SignalDirs {
		dir := join(d)
		if d == ".github/workflows" {
			dir = d
		}
		var names []string
		for p := range in {
			if rest, ok := strings.CutPrefix(p, dir+"/"); ok && !strings.Contains(rest, "/") {
				names = append(names, p)
			}
		}
		slices.Sort(names)
		for _, p := range names {
			rel := p
			if d != ".github/workflows" && prefix != "" {
				rel = strings.TrimPrefix(p, prefix+"/")
			}
			if !slices.Contains(out, rel) {
				out = append(out, rel)
			}
		}
	}
	if len(out) > g.cfg.MaxFiles {
		out = out[:g.cfg.MaxFiles]
	}
	return out
}

// extraFiles names the lockfiles found only in the tree and the LFS pointers worth reading
func (g *GitHub) extraFiles(
	tree []treeEntry, files map[string][]byte, prefix string, abs func(string) string,
) []string {
	have := map[string]bool{}
	for rel := range files {
		have[abs(rel)] = true
	}
	var locks []treeEntry
	lfs := false
	for _, e := range tree {
		if e.Type != "blob" || e.Size > g.cfg.MaxFileBytes {
			continue
		}
		if path.Base(e.Path) == ".gitattributes" && !strings.Contains(e.Path, "/") {
			lfs = bytes.Contains(files[".gitattributes"], []byte("filter=lfs"))
		}
		if !have[e.Path] && lockfile(path.Base(e.Path)) {
			locks = append(locks, e)
		}
	}
	slices.SortFunc(locks, func(a, b treeEntry) int {
		ina, inb := underPrefix(a.Path, prefix), underPrefix(b.Path, prefix)
		return cmp.Or(cmp.Compare(boolRank(inb), boolRank(ina)),
			cmp.Compare(strings.Count(a.Path, "/"), strings.Count(b.Path, "/")),
			cmp.Compare(a.Path, b.Path))
	})
	var out []string
	for _, e := range locks[:min(len(locks), maxExtraLocks)] {
		out = append(out, e.Path)
	}
	if lfs {
		n := 0
		for _, e := range tree {
			if n == maxExtraLFS {
				break
			}
			if e.Type == "blob" && e.Size >= lfsPointerMin && e.Size <= lfsPointerMax &&
				!have[e.Path] && !slices.Contains(out, e.Path) {
				out = append(out, e.Path)
				n++
			}
		}
	}
	return out
}

func underPrefix(p, prefix string) bool {
	return prefix == "" || strings.HasPrefix(p, prefix+"/")
}

func boolRank(b bool) int {
	if b {
		return 1
	}
	return 0
}

// lockfile says a file name is one of the lockfile kinds the extractor counts
func lockfile(base string) bool {
	return slices.Contains(facts.SignalFiles, base) && (strings.HasSuffix(base, ".lock") ||
		strings.HasSuffix(base, "-lock.json") || base == "go.sum" || base == "pnpm-lock.yaml" ||
		base == "bun.lock" || base == "uv.lock" || base == "yarn.lock")
}

// graphql asks for every signal file and directory in one query
func (g *GitHub) graphql(
	ctx context.Context, src source.Source, sub []string, sha string,
) (files map[string][]byte, oids map[string]string, unread rules.UnreadReason, err error) {
	vars := map[string]string{"owner": src.Owner, "name": src.Repo, "c": sha}
	for _, s := range g.slots {
		var subdir []string
		if !s.rootOnly {
			subdir = sub
		}
		vars[s.variable] = source.Expression(sha, subdir, s.path)
	}
	body, err := json.Marshal(map[string]any{"query": g.query, "variables": vars})
	if err != nil {
		return nil, nil, "", err
	}
	h := g.authHeader()
	h["Content-Type"] = "application/json"
	resp, err := g.up.do(ctx, http.MethodPost, g.cfg.GithubAPI+"/graphql", h, body, maxGraphQL)
	if errors.Is(err, errTooBig) {
		return nil, nil, rules.UnreadUpstreamError, nil
	}
	if err != nil {
		return nil, nil, "", err
	}
	g.guard.Record(ctx, ResourceGitHubGraphQL, resp.header)
	if resp.status != http.StatusOK {
		g.limited(ctx, ResourceGitHubGraphQL, resp)
		return nil, nil, classify(resp.status, resp.header, false), nil
	}
	var ans struct {
		Data struct {
			RateLimit struct {
				Cost      int `json:"cost"`
				Remaining int `json:"remaining"`
			} `json:"rateLimit"`
			Repository map[string]json.RawMessage `json:"repository"`
		} `json:"data"`
		Errors []struct {
			Type string `json:"type"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(resp.body, &ans); err != nil {
		return nil, nil, rules.UnreadUpstreamError, nil
	}
	if c := ans.Data.RateLimit.Cost; c > 1 {
		g.log.Error("estimator: the github query cost more than one point", "cost", c)
	}
	if ans.Data.Repository == nil {
		for _, e := range ans.Errors {
			if e.Type == "NOT_FOUND" {
				return nil, nil, rules.UnreadNotFound, nil
			}
			if e.Type == "RATE_LIMITED" {
				g.guard.Exhaust(ctx, ResourceGitHubGraphQL, time.Now().Add(time.Minute))
				return nil, nil, rules.UnreadRateLimited, nil
			}
		}
		return nil, nil, rules.UnreadNotFound, nil
	}
	var private bool
	_ = json.Unmarshal(ans.Data.Repository["isPrivate"], &private)
	if private {
		// Nothing about a private repository is kept or returned
		return nil, nil, rules.UnreadNotFound, nil
	}
	files, oids = map[string][]byte{}, map[string]string{}
	for _, s := range g.slots {
		raw := ans.Data.Repository[s.alias]
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		if s.dir {
			g.readDir(raw, s.path, files, oids)
			continue
		}
		if b, oid, ok := g.readBlob(raw); ok {
			files[s.path], oids[s.path] = b, oid
		} else if oid != "" {
			oids[s.path] = oid
		}
	}
	return files, oids, "", nil
}

type gqlBlob struct {
	OID         string `json:"oid"`
	ByteSize    int64  `json:"byteSize"`
	IsBinary    bool   `json:"isBinary"`
	IsTruncated bool   `json:"isTruncated"`
	Text        string `json:"text"`
}

// readBlob returns a blob's text when it came whole, and its oid whenever it has one
func (g *GitHub) readBlob(raw json.RawMessage) ([]byte, string, bool) {
	var b gqlBlob
	if json.Unmarshal(raw, &b) != nil || b.OID == "" {
		return nil, "", false
	}
	if b.IsBinary || b.IsTruncated || b.ByteSize > g.cfg.MaxFileBytes ||
		int64(len(b.Text)) != b.ByteSize {
		return nil, b.OID, false
	}
	return []byte(b.Text), b.OID, true
}

func (g *GitHub) readDir(
	raw json.RawMessage, dir string, files map[string][]byte, oids map[string]string,
) {
	var t struct {
		Entries []struct {
			Name   string          `json:"name"`
			Type   string          `json:"type"`
			Object json.RawMessage `json:"object"`
		} `json:"entries"`
	}
	if json.Unmarshal(raw, &t) != nil {
		return
	}
	for _, e := range t.Entries {
		if e.Type != "blob" || strings.ContainsAny(e.Name, "/\x00") {
			continue
		}
		p := dir + "/" + e.Name
		if b, oid, ok := g.readBlob(e.Object); ok {
			files[p], oids[p] = b, oid
		} else if oid != "" {
			oids[p] = oid
		}
	}
}

// limited records a secondary rate limit or an exhausted bucket from a failed answer
func (g *GitHub) limited(ctx context.Context, resource string, resp response) {
	if classify(resp.status, resp.header, false) != rules.UnreadRateLimited {
		return
	}
	wait := time.Duration(max(retryAfter(resp.header), 60)) * time.Second
	g.guard.Exhaust(ctx, resource, time.Now().Add(wait))
}

// tree reads the recursive tree of a commit
func (g *GitHub) tree(
	ctx context.Context, src source.Source, sha string,
) ([]treeEntry, rules.UnreadReason, error) {
	u := g.cfg.GithubAPI + "/repos/" + url.PathEscape(src.Owner) + "/" +
		url.PathEscape(src.Repo) + "/git/trees/" + url.PathEscape(sha) + "?recursive=1"
	resp, err := g.up.do(ctx, http.MethodGet, u, g.authHeader(), nil, maxTreeBytes)
	if errors.Is(err, errTooBig) {
		return nil, rules.UnreadUpstreamError, nil
	}
	if err != nil {
		return nil, "", err
	}
	g.guard.Record(ctx, ResourceGitHubCore, resp.header)
	if resp.status != http.StatusOK {
		g.limited(ctx, ResourceGitHubCore, resp)
		return nil, classify(resp.status, resp.header, false), nil
	}
	var t treeAnswer
	if json.Unmarshal(resp.body, &t) != nil {
		return nil, rules.UnreadUpstreamError, nil
	}
	if t.Truncated {
		g.log.Warn("estimator: github tree answer was truncated")
	}
	return t.Tree, "", nil
}

// fileReq is a file to read, its key in the facts' file map and its path from the repository root
type fileReq struct{ key, full string }

// raw reads files at a commit from the raw host into files, skipping a file that is missing or
// larger than max_file_bytes
func (g *GitHub) raw(
	ctx context.Context, src source.Source, sha string, reqs []fileReq, files map[string][]byte,
) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var first error
	for _, r := range reqs {
		wg.Go(func() {
			u := g.cfg.GithubRaw + "/" + url.PathEscape(src.Owner) + "/" +
				url.PathEscape(src.Repo) + "/" + url.PathEscape(sha) + "/" +
				source.EscapePath(strings.Split(r.full, "/"))
			resp, err := g.up.do(ctx, http.MethodGet, u, nil, nil, g.cfg.MaxFileBytes)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case errors.Is(err, errTooBig):
			case err != nil:
				first = cmp.Or(first, err)
			case resp.status == http.StatusOK:
				files[r.key] = resp.body
			}
		})
	}
	wg.Wait()
	return first
}

// CheckToken reads the token's limit and turns the token off when it can read a private
// repository, so the estimator never reads private code
func (g *GitHub) CheckToken(ctx context.Context) {
	if g.token == "" {
		return
	}
	resp, err := g.up.do(ctx, http.MethodGet, g.cfg.GithubAPI+"/rate_limit", g.authHeader(), nil,
		1<<20)
	if err == nil && resp.status == http.StatusOK {
		var ans struct {
			Resources struct {
				Core    struct{ Limit, Remaining int } `json:"core"`
				GraphQL struct{ Limit, Remaining int } `json:"graphql"`
			} `json:"resources"`
		}
		if json.Unmarshal(resp.body, &ans) == nil {
			g.log.Info("estimator: github token limits", "core", ans.Resources.Core.Limit,
				"graphql", ans.Resources.GraphQL.Limit)
		}
	} else if err != nil || resp.status == http.StatusUnauthorized {
		g.log.Error("estimator: the github token was refused, reading anonymously")
		g.token = ""
		return
	}
	resp, err = g.up.do(ctx, http.MethodGet,
		g.cfg.GithubAPI+"/user/repos?visibility=private&per_page=1", g.authHeader(), nil, 1<<20)
	if err != nil || resp.status != http.StatusOK {
		g.log.Warn("estimator: could not check whether the github token reads private code")
		return
	}
	var repos []json.RawMessage
	if json.Unmarshal(resp.body, &repos) == nil && len(repos) > 0 {
		g.log.Error("estimator: the github token reads private repositories, so it is never " +
			"used, restart with a public-only token")
		g.token = ""
	}
}

// Authenticated says the client holds a token it will use
func (g *GitHub) Authenticated() bool { return g.token != "" }
