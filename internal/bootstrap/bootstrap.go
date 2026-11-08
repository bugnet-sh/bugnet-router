// Package bootstrap puts a placement's GitHub source in its sandbox (CP 4.9, DP 10.7).
//
// A public repository is cloned by an ordinary job started from the sandbox. A private one is
// fetched by the router through the egress proxy, which swaps the placeholder of the
// placement's GitHub secret for the token, and is written into the sandbox as an archive that a
// job unpacks. The state lives on the placement row, so any replica can answer for a fetch
// another one runs, and a fetch whose replica went silent reads as interrupted.
package bootstrap

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/source"
	"github.com/bugnet-sh/bugnet-router/internal/jobs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/transfer"
)

// The failure reasons of Bootstrap.failure_reason
const (
	ReasonCloneFailed   = "CLONE_FAILED"
	ReasonFetchFailed   = "FETCH_FAILED"
	ReasonTooLarge      = "TOO_LARGE"
	ReasonInterrupted   = "INTERRUPTED"
	ReasonToolMissing   = "TOOL_MISSING"
	ReasonBudgetSpent   = "BUDGET_SPENT"
	ReasonAttemptsSpent = "ATTEMPTS_SPENT"
)

// Refusals Fetch answers as errors, the rest being states of the Bootstrap
var (
	// ErrNoSource means the placement has no GitHub source
	ErrNoSource = errors.New("bootstrap: the placement has no GitHub source")
	// ErrToolMissing means the provider's image has no git, or no tar for a private source
	ErrToolMissing = errors.New("bootstrap: the sandbox image lacks the tool the fetch needs")
	// ErrSecretRequired means a private source needs a GitHub secret on the placement
	ErrSecretRequired = errors.New("bootstrap: add a GitHub secret to read this repository")
	// ErrRateLimited means this replica runs as many private fetches as it may
	ErrRateLimited = errors.New("bootstrap: this replica runs as many private fetches as it may")
	// ErrNotConfigured means the router has no settings for private fetches
	ErrNotConfigured = errors.New("bootstrap: private fetches are not configured")
)

// RetryAfter is what a refused call is told to wait before it asks again
const RetryAfter = 10 * time.Second

const (
	githubHost    = "api.github.com"
	reservedKey   = "_bootstrap:"
	beatEvery     = 10 * time.Second
	startWait     = time.Millisecond
	archiveName   = "src.tar.gz"
	archiveDir    = ".bugnet"
	maxRevisionHD = 128
)

var (
	ownerRE = regexp.MustCompile(`^[A-Za-z0-9-]{1,39}$`)
	repoRE  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)
	refRE   = regexp.MustCompile(`^[A-Za-z0-9._/-]{1,255}$`)
	shaRE   = regexp.MustCompile(`^[0-9a-f]{40}$`)
	subRE   = regexp.MustCompile(`^[A-Za-z0-9._ -]{1,255}$`)
)

// Counter counts the bytes a fetch streams against the placement's egress budget
type Counter interface {
	// Left is what remains of the placement's egress budget
	Left(ctx context.Context, p store.Placement) (uint64, error)
	// Add counts n streamed bytes and answers what remains
	Add(ctx context.Context, p store.Placement, n uint64) (uint64, error)
}

// Deps are the services and settings a Service works with
type Deps struct {
	Store    *store.Store
	Jobs     *jobs.Service
	Registry *provider.Registry
	Keys     dpkeys.Keys
	Counter  Counter
	Log      *slog.Logger // slog.Default when nil
	Settings config.Bootstrap
	Fetch    config.Fetch
	// Images is each provider's tool manifest
	Images map[string]config.ImageTools
	// EgressHost is the proxy's name, and CAFile the Bugnet CA that signs its inner certificates
	EgressHost, CAFile string
	// FetchToken is the bnf_ token the proxy honours from the router's own address
	FetchToken string
	// Hooks for tests, each zero value meaning production
	Upstream
}

// Service runs FetchSource
type Service struct {
	d       Deps
	life    context.Context
	end     context.CancelFunc
	wg      sync.WaitGroup
	running atomic.Int64
}

// New builds a service over deps
func New(deps Deps) *Service {
	if deps.Log == nil {
		deps.Log = slog.Default()
	}
	deps.Upstream = deps.orDefaults()
	life, end := context.WithCancel(context.Background())
	return &Service{d: deps, life: life, end: end}
}

// Close stops the private fetches this replica runs and waits for them, leaving their rows to
// read as interrupted
func (s *Service) Close() {
	s.end()
	s.wg.Wait()
}

// source is a placement's GitHub source in its parts
type sourceParts struct {
	owner, repo string
	subdir      []string
	// ref is the commit, else the ref the estimate named, else empty for the default branch
	rev, ref string
}

// partsOf splits the placement's source, and says whether every part is safe to pass on
func partsOf(p store.Placement) (sourceParts, bool) {
	segs := strings.Split(p.SourceCanonical, "/")
	if len(segs) < 3 || segs[0] != "github.com" {
		return sourceParts{}, false
	}
	sp := sourceParts{owner: segs[1], repo: segs[2], subdir: segs[3:]}
	if shaRE.MatchString(p.SourceRevision) {
		sp.rev = p.SourceRevision
	}
	sp.ref = p.SourceRef
	ok := ownerRE.MatchString(sp.owner) && repoRE.MatchString(sp.repo) &&
		sp.repo != "." && sp.repo != ".." && (sp.ref == "" || refOK(sp.ref))
	for _, d := range sp.subdir {
		ok = ok && subRE.MatchString(d) && d != "." && d != ".."
	}
	return sp, ok
}

// refOK holds for a ref with no empty or .. segment that no option can be mistaken for
func refOK(ref string) bool {
	if !refRE.MatchString(ref) || strings.HasPrefix(ref, "-") {
		return false
	}
	return !slices.ContainsFunc(strings.Split(ref, "/"), func(s string) bool {
		return s == "" || s == ".." || s == "."
	})
}

// target is the revision or ref a fetch asks for, empty for the default branch
func (sp sourceParts) target() string {
	if sp.rev != "" {
		return sp.rev
	}
	return sp.ref
}

// shown is Bootstrap.path, relative to the home as ~/<dir>/<repo> plus the subdirectory
func (s *Service) shown(sp sourceParts) string {
	return path.Join(append([]string{"~", s.d.Settings.Dir, sp.repo}, sp.subdir...)...)
}

// dest is the repository's directory under the exec user's home, which jobs run in
func (s *Service) dest(sp sourceParts) string {
	return path.Join(s.d.Settings.Dir, sp.repo)
}

// isGitHub says whether the placement has a GitHub source
func isGitHub(p store.Placement) bool { return p.SourceKind == string(source.KindGitHub) }

// View is the placement's bootstrap as the row and its job show it, nil for no GitHub source
func (s *Service) View(ctx context.Context, p store.Placement) (*routerv1.Bootstrap, error) {
	if !isGitHub(p) {
		return nil, nil //nolint:nilnil // no GitHub source has no bootstrap
	}
	row, err := s.d.Store.BootstrapOf(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	return s.view(ctx, p, row)
}

func (s *Service) view(
	ctx context.Context, p store.Placement, row store.Bootstrap,
) (*routerv1.Bootstrap, error) {
	b := &routerv1.Bootstrap{
		Path: row.Path, Revision: cmpRevision(row, p), JobId: row.JobID,
	}
	fail := func(reason string) (*routerv1.Bootstrap, error) {
		b.State, b.FailureReason = routerv1.BootstrapState_BOOTSTRAP_STATE_FAILED, reason
		return b, nil
	}
	switch row.State {
	case "":
		b.State = routerv1.BootstrapState_BOOTSTRAP_STATE_PENDING
		return b, nil
	case store.BootstrapNeedsSecret:
		b.State = routerv1.BootstrapState_BOOTSTRAP_STATE_NEEDS_SECRET
		return b, nil
	case store.BootstrapFailed:
		return fail(row.Reason)
	}
	if row.JobID == "" {
		if row.Heartbeat == nil || time.Since(*row.Heartbeat) > s.d.Settings.HeartbeatStale {
			return fail(ReasonInterrupted)
		}
		b.State = routerv1.BootstrapState_BOOTSTRAP_STATE_RUNNING
		return b, nil
	}
	job, err := s.d.Store.JobByID(ctx, row.JobID)
	if err != nil {
		return nil, fmt.Errorf("read the job of the bootstrap: %w", err)
	}
	switch {
	case job.State == store.JobRunning:
		b.State = routerv1.BootstrapState_BOOTSTRAP_STATE_RUNNING
	case job.State == store.JobExited && job.ExitCode != nil && *job.ExitCode == 0:
		b.State = routerv1.BootstrapState_BOOTSTRAP_STATE_DONE
	case job.State == store.JobExited && job.ExitCode != nil && *job.ExitCode == 127:
		return fail(ReasonToolMissing)
	default:
		return fail(ReasonCloneFailed)
	}
	return b, nil
}

// cmpRevision is the commit the row learned, else the one the estimate resolved
func cmpRevision(row store.Bootstrap, p store.Placement) string {
	if row.Revision != "" {
		return row.Revision
	}
	if row.State != "" && shaRE.MatchString(p.SourceRevision) {
		return p.SourceRevision
	}
	return ""
}

// githubSecret finds the placement's active secret whose hosts include api.github.com
func (s *Service) githubSecret(ctx context.Context, p store.Placement) (store.Secret, bool, error) {
	all, err := s.d.Store.Secrets(ctx, p.ID)
	if err != nil {
		return store.Secret{}, false, err
	}
	for _, x := range all {
		if x.State == store.SecretActive && slices.Contains(x.Hosts, githubHost) {
			return x, true, nil
		}
	}
	return store.Secret{}, false, nil
}

// Fetch puts the placement's GitHub source in its sandbox, answering the state as it is when a
// fetch already runs or finished, and starting one otherwise
func (s *Service) Fetch(ctx context.Context, p store.Placement) (*routerv1.Bootstrap, error) {
	if !isGitHub(p) {
		return nil, ErrNoSource
	}
	sp, valid := partsOf(p)
	row, err := s.d.Store.BootstrapOf(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	cur, err := s.view(ctx, p, row)
	if err != nil {
		return nil, err
	}
	switch cur.State {
	case routerv1.BootstrapState_BOOTSTRAP_STATE_RUNNING:
		return cur, nil
	case routerv1.BootstrapState_BOOTSTRAP_STATE_DONE:
		return s.learnRevision(ctx, p, sp, row, cur), nil
	}
	if !valid {
		// A source that could never be fetched is refused before any request leaves
		return &routerv1.Bootstrap{
			State:         routerv1.BootstrapState_BOOTSTRAP_STATE_FAILED,
			FailureReason: ReasonFetchFailed,
		}, nil
	}
	prov, ok := s.d.Registry.Get(p.Provider)
	if !ok || p.ProviderRef == nil {
		return nil, fmt.Errorf("provider %q is not configured", p.Provider)
	}
	tools := s.d.Images[p.Provider]
	if tools.Git == "none" {
		return nil, ErrToolMissing
	}
	public := p.SourceRead || p.SourceUnread != string(rules.UnreadNotFound)
	secret, haveSecret, err := s.githubSecret(ctx, p)
	if err != nil {
		return nil, err
	}
	failedPublic := row.State == store.BootstrapPublic
	retryPublic := failedPublic && haveSecret
	if public && !retryPublic && row.State != store.BootstrapPrivate &&
		row.State != store.BootstrapFailed {
		return s.startPublic(ctx, p, prov, sp, row)
	}
	if tools.Tar == "none" {
		return nil, ErrToolMissing
	}
	if !haveSecret {
		s.mark(ctx, p, row, store.Bootstrap{State: store.BootstrapNeedsSecret})
		return nil, ErrSecretRequired
	}
	return s.startPrivate(ctx, p, prov, sp, row, secret)
}

// mark keeps a state that needs no job, leaving the attempt counters as they are
func (s *Service) mark(ctx context.Context, p store.Placement, from, to store.Bootstrap) {
	to.Attempt, to.PrivateAttempts = from.Attempt, from.PrivateAttempts
	to.Path = cmp.Or(to.Path, from.Path)
	if _, err := s.d.Store.ClaimBootstrap(ctx, p.ID, from, to); err != nil {
		s.d.Log.Warn("bootstrap: record a state", "placement", p.ID, "err", err)
	}
}

// reread answers the row as it is now, for a call that lost the race to start a fetch
func (s *Service) reread(ctx context.Context, p store.Placement) (*routerv1.Bootstrap, error) {
	row, err := s.d.Store.BootstrapOf(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	return s.view(ctx, p, row)
}

// home is the exec user's home in the placement's sandbox
func home(prov provider.Provider, p store.Placement) string {
	return transfer.Home(prov, *p.ProviderRef, prov.Capabilities())
}
