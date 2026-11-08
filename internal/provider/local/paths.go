package local

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// maxLinkHops bounds the symlinks one RealPath follows, as Linux's ELOOP limit does
const maxLinkHops = 40

var _ provider.Pather = (*Provider)(nil)

// Home is /root, the sandbox path of HOME, whatever host user Exec runs as
func (p *Provider) Home(provider.Ref) string { return "/root" }

// RealPath resolves links as sandbox paths, so an absolute link target stays inside fs
func (p *Provider) RealPath(_ context.Context, ref provider.Ref, name string) (string, error) {
	s, err := p.open(ref)
	if err != nil {
		return "", err
	}
	if !path.IsAbs(name) {
		return "", invalid(fmt.Errorf("path %q is not absolute", name))
	}
	rest := strings.Split(name, "/")
	done := "/"
	for hops := 0; len(rest) > 0; {
		part := rest[0]
		rest = rest[1:]
		switch part {
		case "", ".":
			continue
		case "..":
			done = path.Dir(done)
			continue
		}
		next := path.Join(done, part)
		info, err := os.Lstat(filepath.Join(s.fsDir(), filepath.FromSlash(next)))
		switch {
		case errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR):
			done = next
			continue
		case err != nil:
			return "", fileErr(err)
		case info.Mode()&fs.ModeSymlink == 0:
			done = next
			continue
		}
		if hops++; hops > maxLinkHops {
			return "", invalid(fmt.Errorf("path %q has too many links", name))
		}
		target, err := os.Readlink(filepath.Join(s.fsDir(), filepath.FromSlash(next)))
		if err != nil {
			return "", fileErr(err)
		}
		if path.IsAbs(target) {
			done = "/"
		}
		rest = append(strings.Split(target, "/"), rest...)
	}
	return done, nil
}
