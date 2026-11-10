package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// Errors Resolve and the staging helpers answer with
var (
	ErrPathRefused = errors.New("transfer: the path must be absolute, ~ or ~/rest")
	ErrIsDirectory = errors.New("transfer: the path is a directory")
	ErrNotRegular  = errors.New("transfer: the path is not a regular file")
	ErrFileChanged = errors.New("transfer: the file changed since the URL was made")
)

const (
	// RunnerPath is where the router uploads bugnet-run, a base image's own is image.RunnerFor
	RunnerPath = image.UploadedRunnerPath
	// stageDir holds each staged transfer's own directory, under the exec user's home
	stageDir = ".bugnet/xfer"
	// maxOutput bounds what one helper command may print
	maxOutput = 64 << 10
)

// Target is where a transfer reads or writes, and whether it goes through a staging directory
type Target struct {
	Path   string
	Staged bool
	// StageDir is <home>/.bugnet/xfer, which holds one directory per staged transfer
	StageDir string
}

// StagedPath is where transfer id stages its file, inside its own directory
func (t Target) StagedPath(id string) string {
	return path.Join(t.StageDir, id, path.Base(t.Path))
}

// Home is the exec user's home, the provider's own for a Pather
func Home(prov provider.Provider, ref provider.Ref, caps provider.Capabilities) string {
	if p, ok := prov.(provider.Pather); ok {
		return p.Home(ref)
	}
	if caps.ExecUser == "" || caps.ExecUser == "root" {
		return "/root"
	}
	return "/home/" + caps.ExecUser
}

// Expand checks a path's form and expands ~, refusing other relative paths, NUL and newlines
func Expand(p, home string) (string, error) {
	switch {
	case p == "" || strings.ContainsAny(p, "\x00\n"):
		return "", ErrPathRefused
	case p == "~":
		return home, nil
	case strings.HasPrefix(p, "~/"):
		return path.Join(home, p[2:]) + trailing(p), nil
	case !path.IsAbs(p):
		return "", ErrPathRefused
	}
	return p, nil
}

// trailing keeps a final slash, which marks a directory that no upload may replace
func trailing(p string) string {
	if strings.HasSuffix(p, "/") {
		return "/"
	}
	return ""
}

// Resolve expands and resolves p and decides whether the transfer is staged (DP 8.3.5)
func Resolve(
	ctx context.Context, prov provider.Provider, ref provider.Ref, caps provider.Capabilities,
	direction routerv1.TransferDirection, p string,
) (Target, error) {
	home := Home(prov, ref, caps)
	expanded, err := Expand(p, home)
	if err != nil {
		return Target{}, err
	}
	if direction == routerv1.TransferDirection_TRANSFER_DIRECTION_UPLOAD &&
		strings.HasSuffix(expanded, "/") {
		return Target{}, ErrIsDirectory
	}
	t := Target{StageDir: path.Join(home, stageDir)}
	if pather, ok := prov.(provider.Pather); ok {
		if t.Path, err = pather.RealPath(ctx, ref, expanded); err != nil {
			return Target{}, err
		}
		return t, nil
	}
	if t.Path, err = realPath(ctx, prov, ref, caps, expanded); err != nil {
		return Target{}, err
	}
	if !nonRoot(caps) {
		return t, nil
	}
	if !within(t.Path, home) && !within(t.Path, "/tmp") {
		t.Staged = true
		return t, nil
	}
	mode := "w"
	if direction == routerv1.TransferDirection_TRANSFER_DIRECTION_DOWNLOAD {
		mode = "r"
	}
	_, code, err := run(ctx, prov, ref, false, "sh", "-c", accessScript, "sh", mode, t.Path)
	if err != nil {
		return Target{}, err
	}
	t.Staged = code != 0
	return t, nil
}

// accessScript exits 0 when the exec user can read $2, or write it in place, as the API would
const accessScript = `p=$2
if [ "$1" = r ]; then exec test -r "$p"; fi
d=$(dirname "$p")
if [ -e "$p" ]; then
	test -w "$p" && test -w "$d" || exit 1
	if [ -k "$d" ]; then test -O "$p" || exit 1; fi
	exit 0
fi
while [ ! -e "$d" ]; do d=$(dirname "$d"); done
test -d "$d" && test -w "$d"`

// nonRoot reports whether the exec user needs sudo for paths it does not own
func nonRoot(caps provider.Capabilities) bool {
	return caps.ExecUser != "" && caps.ExecUser != "root"
}

func within(p, dir string) bool {
	return p == dir || strings.HasPrefix(p, dir+"/")
}

// realPath runs realpath -m, under sudo for a non-root exec user so unreadable parents resolve
func realPath(
	ctx context.Context, prov provider.Provider, ref provider.Ref, caps provider.Capabilities,
	p string,
) (string, error) {
	out, code, err := run(ctx, prov, ref, nonRoot(caps), "realpath", "-m", "--", p)
	if err != nil {
		return "", err
	}
	got := strings.TrimSuffix(string(out), "\n")
	if code != 0 || !path.IsAbs(got) || strings.ContainsAny(got, "\x00\n") {
		return "", &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("realpath exited %d", code)}
	}
	return got, nil
}

// run runs argv through Exec, under sudo -n when sudo is set, and answers its stdout and code
func run(
	ctx context.Context, prov provider.Provider, ref provider.Ref, sudo bool, argv ...string,
) ([]byte, int, error) {
	if sudo {
		argv = append([]string{"sudo", "-n"}, argv...)
	}
	stream, err := prov.Exec(ctx, ref, provider.ExecSpec{Argv: argv})
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = stream.Close() }()
	var out bytes.Buffer
	if _, err := io.Copy(&out, io.LimitReader(stream, maxOutput)); err != nil {
		return nil, 0, err
	}
	// What a helper prints past the cap is never read, so it must not block its exit
	_, _ = io.Copy(io.Discard, stream)
	code, err := stream.Wait()
	if err != nil {
		return nil, 0, err
	}
	return out.Bytes(), code, nil
}
