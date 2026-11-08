// Package transfertest serves local sandboxes as a provider whose exec user is not root.
//
// Sandbox gives each sandbox of a local provider a home at /home/<user> and a file API that, as
// on boxd and Boat, reaches only the home and /tmp and nothing marked root's. It runs realpath,
// the access test, sha256sum and bugnet-run's stat, stage-out and place itself when the transfer
// package sends them through Exec, the last three only under sudo. Every other command runs on
// the host with its absolute arguments mapped into the sandbox. Each command is recorded, so a
// test can tell what was staged.
package transfertest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/local"
	"github.com/oynozan/bugnet-router/internal/transfer"
)

// Sandbox is a local provider seen through an exec user who is not root
type Sandbox struct {
	// Provider hides the local provider's own Pather, so paths resolve through Exec
	provider.Provider
	l    *local.Provider
	root string
	user string
	mu   sync.Mutex
	ran  [][]string
	deny map[string]bool
}

// New wraps l, whose sandboxes live under root, with user as the exec user
func New(l *local.Provider, root, user string) *Sandbox {
	return &Sandbox{Provider: l, l: l, root: root, user: user, deny: map[string]bool{}}
}

// Capabilities are the local provider's with the exec user set
func (s *Sandbox) Capabilities() provider.Capabilities {
	c := s.l.Capabilities()
	c.ExecUser = s.user
	return c
}

// Home is the exec user's home as a sandbox path
func (s *Sandbox) Home() string {
	if s.user == "root" {
		return "/root"
	}
	return "/home/" + s.user
}

// Host maps a sandbox path of ref to its host path
func (s *Sandbox) Host(ref provider.Ref, p string) string {
	return filepath.Join(s.root, ref.ID, "fs", filepath.FromSlash(p))
}

// Prepare makes the exec user's home in ref
func (s *Sandbox) Prepare(ref provider.Ref) error {
	return os.MkdirAll(s.Host(ref, s.Home()), 0o755)
}

// RootOnly marks a sandbox path and everything under it as root's, out of the exec user's reach
func (s *Sandbox) RootOnly(p string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deny[path.Clean(p)] = true
}

// Ran lists every command run so far, sudo -n first where it was used
func (s *Sandbox) Ran() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]string(nil), s.ran...)
}

// denied reports whether p or a directory above it is root's
func (s *Sandbox) denied(p string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for q := path.Clean(p); ; q = path.Dir(q) {
		if s.deny[q] {
			return true
		}
		if q == "/" {
			return false
		}
	}
}

// reachable is what the exec user's file API may touch
func (s *Sandbox) reachable(p string) bool {
	in := func(dir string) bool { return p == dir || strings.HasPrefix(p, dir+"/") }
	return (in(s.Home()) || in("/tmp")) && !s.denied(p)
}

func refused(p string) error {
	return &provider.Error{Code: provider.CodeInvalid, Err: fmt.Errorf("%s is out of reach", p)}
}

// StatFile refuses what the exec user cannot reach
func (s *Sandbox) StatFile(
	ctx context.Context, ref provider.Ref, p string,
) (provider.FileEntry, error) {
	if !s.reachable(p) {
		return provider.FileEntry{}, refused(p)
	}
	return s.l.StatFile(ctx, ref, p)
}

// OpenReader refuses what the exec user cannot reach
func (s *Sandbox) OpenReader(
	ctx context.Context, ref provider.Ref, p string, offset int64,
) (provider.FileReader, error) {
	if !s.reachable(p) {
		return nil, refused(p)
	}
	return s.l.OpenReader(ctx, ref, p, offset)
}

// OpenWriter refuses what the exec user cannot reach
func (s *Sandbox) OpenWriter(
	ctx context.Context, ref provider.Ref, p string, spec provider.WriteSpec,
) (provider.FileWriter, error) {
	if !s.reachable(p) {
		return nil, refused(p)
	}
	return s.l.OpenWriter(ctx, ref, p, spec)
}

// transferCommands are the runner subcommands the fake answers itself
var transferCommands = map[string]bool{"stat": true, "stage-out": true, "place": true}

// Exec runs the transfer package's commands itself and the rest on the host
func (s *Sandbox) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	argv := spec.Argv
	root := len(argv) > 2 && argv[0] == "sudo" && argv[1] == "-n"
	s.mu.Lock()
	s.ran = append(s.ran, append([]string(nil), argv...))
	s.mu.Unlock()
	if root {
		argv = argv[2:]
	}
	switch {
	case len(argv) == 4 && argv[0] == "realpath":
		got, err := s.l.RealPath(ctx, ref, argv[3])
		if err != nil {
			return nil, err
		}
		return done(got+"\n", 0), nil
	case len(argv) == 6 && argv[0] == "sh" && (argv[4] == "r" || argv[4] == "w"):
		return done("", s.access(ref, argv[4], argv[5])), nil
	case len(argv) == 3 && argv[0] == "sha256sum":
		return s.digest(ref, argv[2], argv[2], root), nil
	case len(argv) == 5 && argv[0] == "sh" && strings.HasPrefix(argv[2], "sha256sum <"):
		return s.digest(ref, argv[4], "-", root), nil
	case len(argv) > 1 && argv[0] == transfer.RunnerPath && transferCommands[argv[1]]:
		return s.runner(ref, argv[1], argv[2:], root), nil
	case len(argv) > 1 && argv[0] == transfer.RunnerPath:
		// The boot's own commands, such as setup, run the real runner under the sandbox's root
		return s.l.Exec(ctx, ref, spec)
	}
	mapped := append([]string(nil), argv...)
	for i, a := range mapped {
		if i > 0 && path.IsAbs(a) {
			mapped[i] = s.Host(ref, a)
		}
	}
	return s.l.Exec(ctx, ref, provider.ExecSpec{Argv: mapped, Cwd: spec.Cwd, Env: spec.Env})
}

// access is the access test, which fails on root's paths and a missing path's root parent
func (s *Sandbox) access(ref provider.Ref, mode, p string) int {
	if s.denied(p) {
		return 1
	}
	if _, err := os.Lstat(s.Host(ref, p)); err == nil || mode == "r" {
		return boolCode(err != nil)
	}
	for d := path.Dir(p); ; d = path.Dir(d) {
		if _, err := os.Stat(s.Host(ref, d)); err == nil {
			return boolCode(s.denied(d))
		}
	}
}

func boolCode(fail bool) int {
	if fail {
		return 1
	}
	return 0
}

// digest prints p's sum as GNU sha256sum does, marking and escaping a name with \ or a newline
func (s *Sandbox) digest(ref provider.Ref, p, name string, root bool) provider.ExecStream {
	if !root && !s.reachable(p) {
		return done("", 1)
	}
	b, err := os.ReadFile(s.Host(ref, p))
	if err != nil {
		return done("", 1)
	}
	sum := sha256.Sum256(b)
	line := hex.EncodeToString(sum[:]) + "  " + name
	if strings.ContainsAny(name, "\\\n") {
		line = `\` + hex.EncodeToString(sum[:]) + "  " +
			strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(name)
	}
	return done(line+"\n", 0)
}

/* bugnet-run */

type info struct {
	Path             string `json:"path"`
	Size             int64  `json:"size"`
	Mode             uint32 `json:"mode"`
	ModifiedUnixNano int64  `json:"modified_unix_nano"`
	IsDir            bool   `json:"is_dir"`
	Regular          bool   `json:"regular"`
}

func answer(v any) provider.ExecStream {
	b, _ := json.Marshal(v)
	return done(string(b)+"\n", 0)
}

func failure(reason string) provider.ExecStream {
	b, _ := json.Marshal(map[string]string{"error": reason})
	return done(string(b)+"\n", 1)
}

func (s *Sandbox) describe(ref provider.Ref, p string) provider.ExecStream {
	fi, err := os.Lstat(s.Host(ref, p))
	if errors.Is(err, fs.ErrNotExist) {
		return failure("not_found")
	}
	if err != nil {
		return failure("stat_failed")
	}
	return answer(info{
		Path: p, Size: fi.Size(), Mode: uint32(fi.Mode().Perm()),
		ModifiedUnixNano: fi.ModTime().UnixNano(), IsDir: fi.IsDir(),
		Regular: fi.Mode().IsRegular(),
	})
}

// runner answers bugnet-run's stat, stage-out and place as root
func (s *Sandbox) runner(
	ref provider.Ref, sub string, args []string, root bool,
) provider.ExecStream {
	flags := flag.NewFlagSet(sub, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	p := flags.String("path", "", "")
	from := flags.String("from", "", "")
	to := flags.String("to", "", "")
	_ = flags.String("owner", "", "")
	mode := flags.String("mode", "0644", "")
	if !root || flags.Parse(args) != nil {
		return failure("bad_args")
	}
	switch sub {
	case "stat":
		return s.describe(ref, *p)
	case "stage-out":
		return s.stageOut(ref, *from, *to)
	case "place":
		return s.place(ref, *from, *to, *mode)
	}
	return failure("bad_args")
}

func (s *Sandbox) stageOut(ref provider.Ref, from, to string) provider.ExecStream {
	fi, err := os.Lstat(s.Host(ref, from))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return failure("not_found")
	case err != nil:
		return failure("stage_failed")
	case fi.IsDir():
		return failure("is_directory")
	case !fi.Mode().IsRegular():
		return failure("not_regular")
	}
	b, err := os.ReadFile(s.Host(ref, from))
	if err == nil {
		err = os.MkdirAll(filepath.Dir(s.Host(ref, to)), 0o700)
	}
	if err == nil {
		err = os.WriteFile(s.Host(ref, to), b, 0o600)
	}
	if err == nil {
		err = os.Chtimes(s.Host(ref, to), fi.ModTime(), fi.ModTime())
	}
	if err != nil {
		return failure("stage_failed")
	}
	return s.describe(ref, to)
}

func (s *Sandbox) place(ref provider.Ref, from, to, modeText string) provider.ExecStream {
	m, err := strconv.ParseUint(modeText, 8, 32)
	if err != nil {
		return failure("bad_args")
	}
	if fi, err := os.Lstat(s.Host(ref, to)); err == nil && fi.IsDir() {
		return failure("is_directory")
	}
	err = os.MkdirAll(filepath.Dir(s.Host(ref, to)), 0o755)
	if err == nil {
		err = os.Chmod(s.Host(ref, from), fs.FileMode(m))
	}
	if err == nil {
		err = os.Rename(s.Host(ref, from), s.Host(ref, to))
	}
	if errors.Is(err, fs.ErrNotExist) {
		return failure("not_found")
	}
	if err != nil {
		return failure("place_failed")
	}
	return s.describe(ref, to)
}

/* Streams */

type stream struct {
	r    io.Reader
	code int
}

func done(out string, code int) provider.ExecStream {
	return &stream{r: bytes.NewReader([]byte(out)), code: code}
}

func (s *stream) Read(p []byte) (int, error) { return s.r.Read(p) }

func (s *stream) Wait() (int, error) { return s.code, nil }

func (s *stream) Close() error { return nil }
