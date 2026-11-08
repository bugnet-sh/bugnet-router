package transfer_test

import (
	"crypto/sha256"
	"os"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/transfer"
)

func TestStagedStatAndStageOut(t *testing.T) {
	s := newSandbox(t, "boxd")
	s.write(t, "/opt/app/key", "secret")
	got, err := resolve(t, s, download, "/opt/app/key")
	if err != nil || !got.Staged {
		t.Fatalf("resolve /opt/app/key: %+v, %v", got, err)
	}
	entry, err := transfer.Stat(t.Context(), s.prov, s.ref, got)
	if err != nil || entry.Size != 6 || !entry.Regular || entry.Modified.IsZero() {
		t.Fatalf("staged stat: %+v, %v", entry, err)
	}
	staged := got.StagedPath("abc")
	eq(t, staged, "/home/boxd/.bugnet/xfer/abc/key")
	copied, err := transfer.StageOut(t.Context(), s.prov, s.ref, s.caps(), got, staged)
	if err != nil || copied.Size != 6 || !copied.Modified.Equal(entry.Modified) {
		t.Fatalf("stage-out: %+v, %v, want the source's size and time", copied, err)
	}
	eq(t, s.read(t, staged), "secret")
	if err := transfer.Unstage(t.Context(), s.prov, s.ref, staged); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.host("/home/boxd/.bugnet/xfer/abc")); !os.IsNotExist(err) {
		t.Fatalf("the staging directory outlived Unstage: %v", err)
	}
	if err := transfer.Unstage(t.Context(), s.prov, s.ref, "/home/boxd/f"); err == nil {
		t.Fatal("Unstage removed a directory outside the staging root")
	}

	for p, want := range map[string]error{
		"/opt/app": transfer.ErrIsDirectory, "/opt/none": nil,
	} {
		tg, err := resolve(t, s, download, p)
		if err != nil {
			t.Fatal(err)
		}
		_, err = transfer.StageOut(t.Context(), s.prov, s.ref, s.caps(), tg, staged)
		if want != nil {
			is(t, err, want)
			continue
		}
		pe, ok := provider.AsError(err)
		if !ok || pe.Code != provider.CodeNotFound {
			t.Fatalf("stage-out of a missing file gave %v, want not_found", err)
		}
	}
}

func TestPlaceMovesAStagedUpload(t *testing.T) {
	s := newSandbox(t, "boxd")
	got, err := resolve(t, s, upload, "/opt/new/x")
	if err != nil || !got.Staged {
		t.Fatalf("resolve /opt/new/x: %+v, %v", got, err)
	}
	staged := got.StagedPath("id1")
	s.write(t, staged, "data")
	placed, err := transfer.Place(t.Context(), s.prov, s.ref, staged, got, 0o600)
	if err != nil || placed.Path != "/opt/new/x" || placed.Size != 4 {
		t.Fatalf("place: %+v, %v", placed, err)
	}
	eq(t, s.read(t, "/opt/new/x"), "data")
	fi, err := os.Stat(s.host("/opt/new/x"))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("placed file: %v, %v, want mode 0600", fi, err)
	}
	ran := s.fake.Ran()
	eq(t, ran[len(ran)-1], []string{"sudo", "-n", transfer.RunnerPath, "place", "--from",
		staged, "--to", "/opt/new/x", "--mode", "600"})
}

func TestDigest(t *testing.T) {
	want := sha256.Sum256([]byte("abc"))
	for _, user := range []string{"", "boxd"} {
		s := newSandbox(t, user)
		// A staged and an unstaged name with a backslash, which sha256sum would escape
		for _, p := range []string{"/opt/f", `/opt/a\b`, `/tmp/c\d`} {
			s.write(t, p, "abc")
			got, err := resolve(t, s, download, p)
			if err != nil {
				t.Fatal(err)
			}
			sum, err := transfer.Digest(t.Context(), s.prov, s.ref, s.caps(), got)
			if err != nil {
				t.Fatalf("digest of %s as %q: %v", p, user, err)
			}
			eq(t, sum, want[:])
		}
	}
	s := newSandbox(t, "boxd")
	s.write(t, "/opt/f", "abc")
	got, _ := resolve(t, s, download, "/opt/f")
	if _, err := transfer.Digest(t.Context(), s.prov, s.ref, s.caps(), got); err != nil {
		t.Fatal(err)
	}
	ran := s.fake.Ran()
	eq(t, ran[len(ran)-1],
		[]string{"sudo", "-n", "sh", "-c", transfer.DigestScript, "sh", "/opt/f"})
}

func TestLeftoversOlderThanAnHourRemoved(t *testing.T) {
	s := newSandbox(t, "boxd")
	old := time.Now().Add(-2 * time.Hour)
	for _, p := range []string{
		"/home/boxd/.bugnet/xfer/old/f", "/home/boxd/.bugnet/xfer/new/f",
		"/home/boxd/out/.a.bugnet-0011aabb.part", "/home/boxd/out/.b.bugnet-2233ccdd.part",
		"/home/boxd/out/kept",
	} {
		s.write(t, p, "x")
	}
	for _, p := range []string{
		"/home/boxd/.bugnet/xfer/old", "/home/boxd/out/.a.bugnet-0011aabb.part",
		"/home/boxd/out/kept",
	} {
		if err := os.Chtimes(s.host(p), old, old); err != nil {
			t.Fatal(err)
		}
	}
	got, err := resolve(t, s, upload, "~/out/next")
	if err != nil {
		t.Fatal(err)
	}
	if err := transfer.Sweep(t.Context(), s.prov, s.ref, got); err != nil {
		t.Fatal(err)
	}
	for p, kept := range map[string]bool{
		"/home/boxd/.bugnet/xfer/old":            false,
		"/home/boxd/.bugnet/xfer/new/f":          true,
		"/home/boxd/out/.a.bugnet-0011aabb.part": false,
		"/home/boxd/out/.b.bugnet-2233ccdd.part": true,
		"/home/boxd/out/kept":                    true,
	} {
		_, err := os.Stat(s.host(p))
		if (err == nil) != kept {
			t.Errorf("%s: %v, want kept %v", p, err, kept)
		}
	}
	local := newSandbox(t, "")
	tg, _ := resolve(t, local, upload, "~/x")
	eq(t, transfer.Sweep(t.Context(), local.prov, local.ref, tg), nil)
}
