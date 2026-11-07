package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
)

// info is what bugnet-run's stat, stage-out and place print
type info struct {
	Path             string `json:"path"`
	Size             int64  `json:"size"`
	Mode             uint32 `json:"mode"`
	ModifiedUnixNano int64  `json:"modified_unix_nano"`
	IsDir            bool   `json:"is_dir"`
	Regular          bool   `json:"regular"`
	Error            string `json:"error"`
}

// runnerErrors maps the error values bugnet-run prints to the errors the API answers
var runnerErrors = map[string]error{
	"is_directory": ErrIsDirectory, "not_regular": ErrNotRegular, "file_changed": ErrFileChanged,
}

// runner runs a bugnet-run subcommand under sudo and answers the file it describes
func runner(
	ctx context.Context, prov provider.Provider, ref provider.Ref, args ...string,
) (provider.FileEntry, error) {
	out, code, err := run(ctx, prov, ref, true, append([]string{RunnerPath}, args...)...)
	if err != nil {
		return provider.FileEntry{}, err
	}
	var got info
	if jerr := json.Unmarshal(out, &got); jerr != nil || code != 0 && got.Error == "" {
		return provider.FileEntry{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("bugnet-run %s exited %d", args[0], code)}
	}
	switch {
	case got.Error == "not_found":
		return provider.FileEntry{}, &provider.Error{Code: provider.CodeNotFound,
			Err: fmt.Errorf("bugnet-run %s found no file", args[0])}
	case runnerErrors[got.Error] != nil:
		return provider.FileEntry{}, runnerErrors[got.Error]
	case got.Error != "":
		return provider.FileEntry{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("bugnet-run %s failed with %s", args[0], got.Error)}
	}
	mode := fs.FileMode(got.Mode).Perm()
	if got.IsDir {
		mode |= fs.ModeDir
	} else if !got.Regular {
		mode |= fs.ModeIrregular
	}
	return provider.FileEntry{
		Path: got.Path, Size: got.Size, IsDir: got.IsDir, Mode: mode, Regular: got.Regular,
		Modified: time.Unix(0, got.ModifiedUnixNano).UTC(),
	}, nil
}

// Stat describes the target, through bugnet-run stat under sudo when it is staged
func Stat(
	ctx context.Context, prov provider.Provider, ref provider.Ref, t Target,
) (provider.FileEntry, error) {
	if t.Staged {
		return runner(ctx, prov, ref, "stat", "--path", t.Path)
	}
	return prov.StatFile(ctx, ref, t.Path)
}

// StageOut copies the target to staged, owned by the exec user and readable by it alone
func StageOut(
	ctx context.Context, prov provider.Provider, ref provider.Ref, caps provider.Capabilities,
	t Target, staged string,
) (provider.FileEntry, error) {
	return runner(ctx, prov, ref, "stage-out", "--from", t.Path, "--to", staged,
		"--owner", caps.ExecUser)
}

// Place moves a staged upload over the target with mode, creating its parents
func Place(
	ctx context.Context, prov provider.Provider, ref provider.Ref, staged string, t Target,
	mode fs.FileMode,
) (provider.FileEntry, error) {
	return runner(ctx, prov, ref, "place", "--from", staged, "--to", t.Path,
		"--mode", strconv.FormatUint(uint64(mode.Perm()), 8))
}

// Unstage removes a staged transfer's own directory, as the exec user who owns it
func Unstage(ctx context.Context, prov provider.Provider, ref provider.Ref, staged string) error {
	dir := path.Dir(staged)
	if !strings.Contains(dir, "/"+stageDir+"/") {
		return fmt.Errorf("transfer: %s is no staging directory", Short(path.Base(dir)))
	}
	_, code, err := run(ctx, prov, ref, false, "rm", "-rf", "--", dir)
	if err == nil && code != 0 {
		err = &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf("rm exited %d", code)}
	}
	return err
}

// sweepScript removes staging directories and temporary files untouched for an hour
const sweepScript = `find "$1" -mindepth 1 -maxdepth 1 -mmin +60 -exec rm -rf -- {} + 2>/dev/null
find "$2" -maxdepth 1 -type f -name '.*.bugnet-*.part' -mmin +60 -exec rm -f -- {} + 2>/dev/null
exit 0`

// Sweep removes leftovers older than an hour from the staging root and the target's directory
func Sweep(ctx context.Context, prov provider.Provider, ref provider.Ref, t Target) error {
	if _, ok := prov.(provider.Pather); ok {
		return nil
	}
	_, _, err := run(ctx, prov, ref, false, "sh", "-c", sweepScript, "sh", t.StageDir,
		path.Dir(t.Path))
	return err
}

// digestScript reads the file on stdin, since sha256sum escapes a name holding \ or a newline
const digestScript = `sha256sum < "$1"`

// Digest is the target's SHA-256, by sha256sum in the sandbox or read through a Pather's file API
func Digest(
	ctx context.Context, prov provider.Provider, ref provider.Ref, caps provider.Capabilities,
	t Target,
) ([]byte, error) {
	if _, ok := prov.(provider.Pather); ok {
		r, err := prov.OpenReader(ctx, ref, t.Path, 0)
		if err != nil {
			return nil, err
		}
		defer func() { _ = r.Close() }()
		h := sha256.New()
		if _, err := io.Copy(h, r); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}
	out, code, err := run(ctx, prov, ref, t.Staged && nonRoot(caps), "sh", "-c", digestScript,
		"sh", t.Path)
	if err != nil {
		return nil, err
	}
	sum, _, _ := strings.Cut(string(out), " ")
	b, herr := hex.DecodeString(sum)
	if code != 0 || herr != nil || len(b) != sha256.Size {
		return nil, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("sha256sum exited %d", code)}
	}
	return b, nil
}
