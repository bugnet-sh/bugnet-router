// Package source parses Estimate's source string and finds secret-shaped values in agent text
package source

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"
)

// ErrInvalid is SOURCE_INVALID, a source that parses as nothing or breaks a rule
var ErrInvalid = errors.New("source invalid")

// Kind says what a source names
type Kind string

// The four kinds of CP 4.3
const (
	KindNone        Kind = "none"
	KindGitHub      Kind = "github"
	KindHuggingFace Kind = "huggingface"
	KindURL         Kind = "url"
)

// maxInput bounds the source string before any parsing
const maxInput = 2048

// Source is a parsed source string, holding nothing of a URL source past its scheme and host
type Source struct {
	Kind Kind
	// Owner and Repo name a GitHub repository with the case given
	Owner, Repo string
	// RefPath holds the segments after tree, a ref and a subdirectory not yet told apart
	RefPath []string
	// Model is a Hugging Face model id with the case given
	Model string
	// Revision is the Hugging Face revision asked for, empty for main
	Revision string
	// URL is a URL source's scheme and host
	URL string
}

var (
	ghOwner   = regexp.MustCompile(`^[A-Za-z0-9-]{1,39}$`)
	ghRepo    = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)
	hfPart    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$`)
	subdirSeg = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)
	commitSHA = regexp.MustCompile(`^[0-9a-fA-F]{7,40}$`)
	bareHF    = regexp.MustCompile(`^([^/@]+)/([^/@]+)(?:@(.+))?$`)
	sshForm   = regexp.MustCompile(`^(?:ssh://)?git@github\.com[:/]([^/]+)/([^/#?]+?)/?$`)
	knownHost = regexp.MustCompile(`(?i)^(?:www\.)?(?:github\.com|huggingface\.co|hf\.co|` +
		`raw\.githubusercontent\.com|gist\.github\.com|gist\.githubusercontent\.com)(?:/|$)`)
)

var githubPages = map[string]bool{
	"about": true, "apps": true, "collections": true, "enterprise": true, "explore": true,
	"features": true, "login": true, "marketplace": true, "notifications": true, "orgs": true,
	"pricing": true, "search": true, "settings": true, "sponsors": true, "topics": true,
	"trending": true, "users": true,
}

var huggingfacePages = map[string]bool{
	"api": true, "blog": true, "chat": true, "collections": true, "datasets": true, "docs": true,
	"join": true, "learn": true, "login": true, "models": true, "organizations": true,
	"papers": true, "posts": true, "pricing": true, "settings": true, "spaces": true,
	"tasks": true,
}

func invalid(why string) error { return fmt.Errorf("%w: %s", ErrInvalid, why) }

// Parse reads a source string by the rules of CP 4.3, refusing what breaks them with ErrInvalid
func Parse(s string) (Source, error) {
	if len(s) > maxInput {
		return Source{}, invalid("too long")
	}
	s = strings.TrimSpace(s)
	s = strings.TrimLeft(s, "<\"'`(")
	s = strings.TrimRight(s, ">\"'`)")
	if s == "" {
		return Source{Kind: KindNone}, nil
	}
	if strings.IndexFunc(s, hidden) >= 0 {
		return Source{}, invalid("control or invisible characters")
	}
	if m := sshForm.FindStringSubmatch(s); m != nil {
		return github(m[1], m[2], nil)
	}
	s = strings.TrimPrefix(s, "git+")
	if !strings.Contains(s, "://") {
		if !knownHost.MatchString(s) {
			return bare(s)
		}
		s = "https://" + s
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" || u.Opaque != "" {
		return Source{}, invalid("not a URL")
	}
	if u.User != nil {
		return Source{}, invalid("a URL with userinfo")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return Source{}, invalid("not an http or https URL")
	}
	segs, err := segments(u.EscapedPath())
	if err != nil {
		return Source{}, err
	}
	host := strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	switch {
	case host == "github.com" && u.Port() == "":
		if src, ok, err := fromGitHub(segs); ok || err != nil {
			return src, err
		}
	case (host == "huggingface.co" || host == "hf.co") && u.Port() == "":
		if src, ok, err := fromHuggingFace(segs); ok || err != nil {
			return src, err
		}
	}
	return Source{Kind: KindURL, URL: u.Scheme + "://" + strings.ToLower(u.Host)}, nil
}

// hidden is a control, format or line-breaking character, which no source needs
func hidden(r rune) bool {
	return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == ' ' || r == ' '
}

// segments splits an escaped path and unescapes each segment on its own
func segments(p string) ([]string, error) {
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return nil, nil
	}
	raw := strings.Split(p, "/")
	out := make([]string, len(raw))
	for i, r := range raw {
		seg, err := url.PathUnescape(r)
		if err != nil || seg == "" {
			return nil, invalid("an empty or badly escaped path segment")
		}
		out[i] = seg
	}
	return out, nil
}

func bare(s string) (Source, error) {
	m := bareHF.FindStringSubmatch(s)
	if m == nil {
		return Source{}, invalid("not a URL, GitHub repository or Hugging Face model id")
	}
	return huggingface(m[1], m[2], m[3])
}

// fromGitHub reads a github.com path, ok false when it names a page and not a repository
func fromGitHub(segs []string) (Source, bool, error) {
	if len(segs) < 2 || githubPages[strings.ToLower(segs[0])] {
		return Source{}, false, nil
	}
	owner, repo, rest := segs[0], segs[1], segs[2:]
	var refPath []string
	switch {
	case len(rest) < 2:
	case rest[0] == "tree":
		refPath = rest[1:]
	case rest[0] == "blob" || rest[0] == "raw":
		refPath = rest[1 : len(rest)-1]
		if len(refPath) == 0 {
			refPath = rest[1:]
		}
	case rest[0] == "commit" || rest[0] == "commits":
		refPath = rest[1:2]
	case rest[0] == "releases" && len(rest) >= 3 && (rest[1] == "tag" || rest[1] == "download"):
		refPath = rest[2:3]
	}
	src, err := github(owner, repo, refPath)
	return src, true, err
}

func github(owner, repo string, refPath []string) (Source, error) {
	repo = strings.TrimSuffix(repo, ".git")
	if !ghOwner.MatchString(owner) || !ghRepo.MatchString(repo) || repo == "." || repo == ".." {
		return Source{}, invalid("not a GitHub owner and repository")
	}
	src := Source{Kind: KindGitHub, Owner: owner, Repo: repo}
	if len(refPath) == 0 {
		return src, nil
	}
	src.RefPath = append([]string(nil), refPath...)
	for i := 1; i <= len(refPath); i++ {
		if _, _, err := src.splitAt(i); err == nil {
			return src, nil
		}
	}
	return Source{}, invalid("no reading of the path gives a valid ref and subdirectory")
}

// fromHuggingFace reads a huggingface.co path, ok false when it names a page and not a model
func fromHuggingFace(segs []string) (Source, bool, error) {
	if len(segs) >= 2 && segs[0] == "api" && segs[1] == "models" {
		segs = segs[2:]
	}
	if len(segs) == 0 || huggingfacePages[segs[0]] {
		return Source{}, false, nil
	}
	revAt := func(i int) string {
		if len(segs) > i+1 && revisionKinds[segs[i]] {
			return segs[i+1]
		}
		return ""
	}
	if len(segs) == 1 || revisionKinds[segs[1]] {
		src, err := huggingface("", segs[0], revAt(1))
		return src, true, err
	}
	src, err := huggingface(segs[0], segs[1], revAt(2))
	return src, true, err
}

var revisionKinds = map[string]bool{"tree": true, "blob": true, "resolve": true, "commit": true}

func huggingface(org, name, rev string) (Source, error) {
	if (org != "" && !hfPart.MatchString(org)) || !hfPart.MatchString(name) {
		return Source{}, invalid("not a Hugging Face model id")
	}
	if rev != "" && !CheckRef(rev) {
		return Source{}, invalid("a revision git would refuse")
	}
	id := name
	if org != "" {
		id = org + "/" + name
	}
	return Source{Kind: KindHuggingFace, Model: id, Revision: rev}, nil
}

// CheckRef applies git's check-ref-format rules plus the parser's refusal of space, colon,
// quote and backslash
func CheckRef(ref string) bool {
	if ref == "" || len(ref) > 255 || ref == "@" || strings.Contains(ref, "..") ||
		strings.Contains(ref, "@{") || strings.HasSuffix(ref, ".") {
		return false
	}
	if strings.ContainsFunc(ref, func(r rune) bool {
		return hidden(r) || unicode.IsSpace(r) || strings.ContainsRune("~^:?*[\\\"\x7f", r)
	}) {
		return false
	}
	for _, c := range strings.Split(ref, "/") {
		if c == "" || strings.HasPrefix(c, ".") || strings.HasSuffix(c, ".lock") {
			return false
		}
	}
	return true
}

// splitAt reads the first i segments of RefPath as the ref and the rest as the subdirectory
func (s Source) splitAt(i int) (string, []string, error) {
	ref := strings.Join(s.RefPath[:i], "/")
	if !CheckRef(ref) {
		return "", nil, invalid("a ref git would refuse")
	}
	sub := s.RefPath[i:]
	for _, seg := range sub {
		if !subdirSeg.MatchString(seg) || seg == "." || seg == ".." {
			return "", nil, invalid("a subdirectory segment outside the allowed characters")
		}
	}
	if len(sub) == 0 {
		sub = nil
	}
	return ref, sub, nil
}

// CommitRef is the commit SHA a GitHub source names, which skips ref resolution
func (s Source) CommitRef() string {
	if len(s.RefPath) > 0 && commitSHA.MatchString(s.RefPath[0]) {
		return s.RefPath[0]
	}
	return ""
}

// Split tells the ref from the subdirectory by the longest listed ref that prefixes RefPath,
// falling back to the first segment when none does
func (s Source) Split(listed []string) (string, []string, error) {
	if len(s.RefPath) == 0 {
		return "", nil, nil
	}
	if s.CommitRef() != "" {
		return s.splitAt(1)
	}
	known := make(map[string]bool, len(listed))
	for _, r := range listed {
		known[r] = true
	}
	for i := len(s.RefPath); i >= 1; i-- {
		if known[strings.Join(s.RefPath[:i], "/")] {
			return s.splitAt(i)
		}
	}
	return s.splitAt(1)
}

// Key is the lowercased cache key, github.com/owner/repo or huggingface.co/id, empty otherwise
func (s Source) Key() string {
	switch s.Kind {
	case KindGitHub:
		return strings.ToLower("github.com/" + s.Owner + "/" + s.Repo)
	case KindHuggingFace:
		return strings.ToLower("huggingface.co/" + s.Model)
	}
	return ""
}

// Canonical is ResolvedSource.canonical, with the subdirectory when one was named
func (s Source) Canonical(subdir []string) string {
	switch s.Kind {
	case KindGitHub:
		c := "github.com/" + s.Owner + "/" + s.Repo
		if len(subdir) > 0 {
			c += "/" + strings.Join(subdir, "/")
		}
		return c
	case KindHuggingFace:
		return "huggingface.co/" + s.Model
	case KindURL:
		return s.URL
	}
	return ""
}

// Expression is a GraphQL object expression, sent only as a variable, never in query text
func Expression(ref string, subdir []string, path string) string {
	if ref == "" {
		ref = "HEAD"
	}
	parts := append(append([]string(nil), subdir...), path)
	return ref + ":" + strings.Join(parts, "/")
}

// EscapePath URL-escapes each segment on its own and joins them with slashes
func EscapePath(segs []string) string {
	out := make([]string, len(segs))
	for i, s := range segs {
		out[i] = url.PathEscape(s)
	}
	return strings.Join(out, "/")
}

var secretShape = regexp.MustCompile(`(?:^|[^A-Za-z0-9])(?:(?:sk-|gh[pousr]_|github_pat_|hf_|` +
	`xox[abpr]-|glpat-|r8_)[A-Za-z0-9_-]{20,}|AKIA[0-9A-Z]{16}(?:$|[^A-Za-z0-9]))`)

// ContainsSecret says whether s holds a value of one of the gateway's secret shapes
func ContainsSecret(s string) bool {
	return secretShape.MatchString(s)
}
