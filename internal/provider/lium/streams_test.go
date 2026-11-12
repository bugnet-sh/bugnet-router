package lium

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/contract"
)

func TestLiumStreamsContract(t *testing.T) {
	f, p := liveFake(t)
	ref, _ := readyRef(t, f, p)
	contract.Streams(t, p, ref, t.TempDir())
}

func TestLiumListLimit(t *testing.T) {
	f, p := liveFake(t)
	ref, _ := readyRef(t, f, p)
	contract.ListLimit(t, p, ref, t.TempDir())
}

// TestLiumStreamsLargeFile moves a file of several read chunks both ways
func TestLiumStreamsLargeFile(t *testing.T) {
	f, p := liveFake(t)
	ref, _ := readyRef(t, f, p)
	ctx := context.Background()
	data := make([]byte, 3<<20+17)
	_, _ = rand.Read(data)
	name := t.TempDir() + "/big.bin"
	w, err := p.OpenWriter(ctx, ref, name, provider.WriteSpec{Size: int64(len(data)),
		Mode: 0o600})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	entry, err := w.Commit(ctx, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	if entry.Path != name || entry.Size != int64(len(data)) || !entry.Regular {
		t.Fatalf("Commit described %+v", entry)
	}
	r, err := p.OpenReader(ctx, ref, name, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, data[5:]) {
		t.Fatalf("read back %d bytes, %v", len(got), err)
	}
}

// TestLiumCommitChecksTheSum runs sha256sum on the pod and keeps the target on a mismatch
func TestLiumCommitChecksTheSum(t *testing.T) {
	f, p := liveFake(t)
	ref, _ := readyRef(t, f, p)
	ctx := context.Background()
	name := t.TempDir() + "/f"
	if err := os.WriteFile(name, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := p.OpenWriter(ctx, ref, name, provider.WriteSpec{Size: 3, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	wrong := sha256.Sum256([]byte("other"))
	_, err = w.Commit(ctx, wrong[:])
	wantCode(t, err, provider.CodeInternal, false)
	if got, _ := os.ReadFile(name); string(got) != "old" {
		t.Fatalf("a mismatched sum left %q", got)
	}
	ran := false
	for _, line := range f.execLines() {
		ran = ran || strings.Contains(line, "sha256sum")
	}
	if !ran {
		t.Fatal("no sha256sum ran on the pod")
	}
}
