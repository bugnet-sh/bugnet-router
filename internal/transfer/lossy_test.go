package transfer_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/transfer"
)

// standInRealpath is realpath -m for the test, one link of which points at a name that is not UTF-8
const standInRealpath = `#!/bin/sh
[ "$1" = -m ] && [ "$2" = -- ] || exit 2
case $3 in
/root/link) printf '/root/caf\351.txt\n' ;;
/root/gone) exit 1 ;;
*) printf '%s\n' "$3" ;;
esac
`

// lossyExec runs commands on the host and decodes their output as Rust's from_utf8_lossy does
type lossyExec struct {
	provider.Provider // nil, so a call realPath should not make panics
	bin               string
	mu                sync.Mutex
	ran               [][]string
}

func newLossyExec(t *testing.T) *lossyExec {
	t.Helper()
	bin := t.TempDir()
	err := os.WriteFile(filepath.Join(bin, "realpath"), []byte(standInRealpath), 0o755)
	if err != nil {
		t.Fatal(err)
	}
	return &lossyExec{bin: bin}
}

func (f *lossyExec) Exec(
	ctx context.Context, _ provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	f.mu.Lock()
	f.ran = append(f.ran, slices.Clone(spec.Argv))
	f.mu.Unlock()
	name := spec.Argv[0]
	if name == "realpath" {
		name = filepath.Join(f.bin, name)
	}
	cmd := exec.CommandContext(ctx, name, spec.Argv[1:]...)
	cmd.Env = append(os.Environ(), "PATH="+f.bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := cmd.Output()
	var exit *exec.ExitError
	code := 0
	switch {
	case errors.As(err, &exit):
		code = exit.ExitCode()
	case err != nil:
		return nil, err
	}
	return &lossyStream{Reader: strings.NewReader(fromUTF8Lossy(out)), code: code}, nil
}

func (f *lossyExec) commands() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ran)
}

type lossyStream struct {
	io.Reader
	code int
}

func (s *lossyStream) Wait() (int, error) { return s.code, nil }
func (s *lossyStream) Close() error       { return nil }

// fromUTF8Lossy replaces each maximal ill-formed subsequence with one U+FFFD, as Rust does
func fromUTF8Lossy(b []byte) string {
	var out bytes.Buffer
	for len(b) > 0 {
		r, n := utf8.DecodeRune(b)
		if r != utf8.RuneError || n > 1 {
			out.Write(b[:n])
			b = b[n:]
			continue
		}
		out.WriteRune(utf8.RuneError)
		b = b[maximalSubpart(b):]
	}
	return out.String()
}

// maximalSubpart is the length of the ill-formed sequence at the start of b, at least 1
func maximalSubpart(b []byte) int {
	need, lo, hi := 0, byte(0x80), byte(0xBF)
	switch c := b[0]; {
	case c >= 0xC2 && c <= 0xDF:
		need = 1
	case c == 0xE0:
		need, lo = 2, 0xA0
	case c >= 0xE1 && c <= 0xEC, c == 0xEE, c == 0xEF:
		need = 2
	case c == 0xED:
		need, hi = 2, 0x9F
	case c == 0xF0:
		need, lo = 3, 0x90
	case c >= 0xF1 && c <= 0xF3:
		need = 3
	case c == 0xF4:
		need, hi = 3, 0x8F
	}
	n := 1
	for n <= need && n < len(b) && b[n] >= lo && b[n] <= hi {
		n, lo, hi = n+1, 0x80, 0xBF
	}
	return n
}

func TestRealPathLossy(t *testing.T) {
	eq(t, fromUTF8Lossy([]byte("a\xe9\xe2\x82\xff\xffb")), "a����b")
	f := newLossyExec(t)
	lossy := provider.Capabilities{LossyExecText: true}
	got, err := resolveWith(t, f, lossy, "/root/link")
	eq(t, err, nil)
	eq(t, got, "/root/caf\xe9.txt")
	eq(t, f.commands(), [][]string{{"sh", "-c",
		`p=$(realpath -m -- "$1") && printf %s "$p" | base64 -w 0`, "sh", "/root/link"}})

	// The same name read as plain text comes back mangled, a path that does not exist
	got, err = resolveWith(t, f, provider.Capabilities{}, "/root/link")
	eq(t, err, nil)
	eq(t, got, "/root/caf�.txt")

	// realpath's own exit code reaches the caller through the encoding
	_, err = resolveWith(t, f, lossy, "/root/gone")
	pe, ok := provider.AsError(err)
	if !ok || pe.Code != provider.CodeInternal || !strings.Contains(err.Error(), "exited 1") {
		t.Fatalf("a failed realpath gave %v, want an internal error naming exit 1", err)
	}
}

func resolveWith(
	t *testing.T, prov provider.Provider, caps provider.Capabilities, p string,
) (string, error) {
	t.Helper()
	got, err := transfer.Resolve(t.Context(), prov, provider.Ref{ID: "x"}, caps, upload, p)
	return got.Path, err
}
