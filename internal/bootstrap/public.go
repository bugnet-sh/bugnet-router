package bootstrap

import (
	"context"
	"path"
	"strconv"
	"strings"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/jobs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// publicScript clones owner $1 repo $2 at revision or ref $3 into $4, 127 without git
const publicScript = `command -v git >/dev/null 2>&1 || exit 127
set -e
mkdir -p "$4"
cd "$4"
git init -q
git remote remove origin 2>/dev/null || true
git remote add origin "https://github.com/$1/$2.git"
want=${3:-HEAD}
if git fetch -q --depth 1 origin "$want" && git checkout -q --detach FETCH_HEAD; then
	exit 0
fi
git fetch -q origin '+refs/heads/*:refs/remotes/origin/*' '+refs/tags/*:refs/tags/*'
if [ "$want" = HEAD ]; then
	git remote set-head origin -a >/dev/null
	want=origin/HEAD
fi
git checkout -q --detach "$want" 2>/dev/null || git checkout -q --detach "origin/$want"
`

// publicArgv is the job that clones a public repository, every argument already validated
func publicArgv(sp sourceParts, dir string) []string {
	return []string{"sh", "-c", publicScript, "sh", sp.owner, sp.repo, sp.target(), dir}
}

// cloneDisplay is how the session record shows the clone, never the script itself
func cloneDisplay(sp sourceParts) []string {
	argv := []string{"git", "clone", "https://github.com/" + sp.owner + "/" + sp.repo}
	if t := sp.target(); t != "" {
		argv = append(argv, "@"+t)
	}
	return argv
}

// startPublic starts the clone job under the next _bootstrap key
func (s *Service) startPublic(
	ctx context.Context, p store.Placement, prov provider.Provider, sp sourceParts,
	row store.Bootstrap,
) (*routerv1.Bootstrap, error) {
	to := row
	to.State, to.JobID, to.Reason, to.Revision = store.BootstrapPublic, "", "", ""
	to.Attempt++
	to.Path = s.shown(sp)
	claimed, err := s.d.Store.ClaimBootstrap(ctx, p.ID, row, to)
	if err != nil {
		return nil, err
	}
	if !claimed {
		return s.reread(ctx, p)
	}
	h := home(prov, p)
	shown := jobs.WithDisplay(ctx, cloneDisplay(sp))
	res, err := s.d.Jobs.Exec(shown, p, publicArgv(sp, s.dest(sp)), h,
		map[string]string{"GIT_TERMINAL_PROMPT": "0"}, reservedKey+strconv.Itoa(to.Attempt),
		startWait, jobs.Until{})
	if err != nil {
		// With no job to read, the row reads as interrupted at once
		epoch := store.Bootstrap{State: store.BootstrapPublic, Heartbeat: &time.Time{}}
		if _, uerr := s.d.Store.UpdateBootstrap(ctx, p.ID, to, epoch); uerr != nil {
			s.d.Log.Warn("bootstrap: release a clone that did not start", "err", uerr)
		}
		return nil, err
	}
	to.JobID = res.JobID
	if _, err := s.d.Store.UpdateBootstrap(ctx, p.ID, to, to); err != nil {
		return nil, err
	}
	return s.reread(ctx, p)
}

// learnRevision reads the commit a finished public clone checked out, when none is known yet
func (s *Service) learnRevision(
	ctx context.Context, p store.Placement, sp sourceParts, row store.Bootstrap,
	cur *routerv1.Bootstrap,
) *routerv1.Bootstrap {
	if cur.Revision != "" || row.State != store.BootstrapPublic {
		return cur
	}
	prov, ok := s.d.Registry.Get(p.Provider)
	if !ok || p.ProviderRef == nil {
		return cur
	}
	file := path.Join(home(prov, p), s.dest(sp), ".git", "HEAD")
	data, _, err := prov.ReadFile(ctx, *p.ProviderRef, file, 0, maxRevisionHD)
	if err != nil {
		return cur
	}
	if rev := strings.TrimSpace(string(data)); shaRE.MatchString(rev) {
		to := row
		to.Revision = rev
		if _, err := s.d.Store.UpdateBootstrap(ctx, p.ID, row, to); err != nil {
			s.d.Log.Warn("bootstrap: record the commit", "placement", p.ID, "err", err)
		}
		cur.Revision = rev
	}
	return cur
}
