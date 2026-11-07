package transfer_test

import (
	"os"
	"testing"

	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/oynozan/bugnet-router/internal/transfer"
)

const (
	upload   = routerv1.TransferDirection_TRANSFER_DIRECTION_UPLOAD
	download = routerv1.TransferDirection_TRANSFER_DIRECTION_DOWNLOAD
)

func resolve(
	t *testing.T, s sandbox, d routerv1.TransferDirection, p string,
) (transfer.Target, error) {
	t.Helper()
	return transfer.Resolve(t.Context(), s.prov, s.ref, s.caps(), d, p)
}

func resolved(t *testing.T, s sandbox, p string) string {
	t.Helper()
	got, err := resolve(t, s, upload, p)
	if err != nil {
		t.Fatalf("resolve %q: %v", p, err)
	}
	return got.Path
}

func TestCreateTransferPathForms(t *testing.T) { // review focus 2
	boxdLike, rootLike, local := newSandbox(t, "boxd"), newSandbox(t, "root"), newSandbox(t, "")
	eq(t, resolved(t, boxdLike, "~/out.csv"), "/home/boxd/out.csv")
	eq(t, resolved(t, rootLike, "~/out.csv"), "/root/out.csv")
	eq(t, resolved(t, rootLike, "~"), "/root")
	eq(t, resolved(t, local, "~/out.csv"), "/root/out.csv")
	eq(t, resolved(t, boxdLike, "/home/boxd/a/../b.txt"), "/home/boxd/b.txt")
	for _, s := range []sandbox{boxdLike, rootLike, local} {
		for _, p := range []string{"out.csv", "./x", "", "a\nb", "a\x00b", "~boxd/x", "~x"} {
			_, err := resolve(t, s, upload, p)
			is(t, err, transfer.ErrPathRefused)
		}
	}
}

func TestResolveFollowsLinksOnEveryProvider(t *testing.T) {
	for _, user := range []string{"boxd", "root", ""} {
		s := newSandbox(t, user)
		home := transfer.Home(s.prov, s.ref, s.caps())
		s.write(t, home+"/real/f", "x")
		if err := os.Symlink("real", s.host(home+"/link")); err != nil {
			t.Fatal(err)
		}
		eq(t, resolved(t, s, "~/link/f"), home+"/real/f")
	}
}

func TestUploadPathEndingInASlashIsADirectory(t *testing.T) {
	s := newSandbox(t, "boxd")
	for _, p := range []string{"/home/boxd/new/", "~/", "/"} {
		_, err := resolve(t, s, upload, p)
		is(t, err, transfer.ErrIsDirectory)
	}
	got, err := resolve(t, s, download, "/home/boxd/new/")
	eq(t, err, nil)
	eq(t, got.Path, "/home/boxd/new")
}

func TestStagingDecisions(t *testing.T) {
	s := newSandbox(t, "boxd")
	s.write(t, "/tmp/mine", "x")
	s.write(t, "/tmp/roots", "x")
	s.fake.RootOnly("/tmp/roots")
	s.fake.RootOnly("/home/boxd/locked")
	for _, c := range []struct {
		d      routerv1.TransferDirection
		path   string
		staged bool
	}{
		{upload, "/opt/x", true},
		{download, "/etc/hostname", true},
		{upload, "/tmp/mine", false},
		{download, "/tmp/mine", false},
		{upload, "/tmp/roots", true},
		{download, "/tmp/roots", true},
		{upload, "/tmp/new/deep/f", false},
		{upload, "/home/boxd/locked/f", true},
		{upload, "~/fresh", false},
	} {
		got, err := resolve(t, s, c.d, c.path)
		if err != nil || got.Staged != c.staged {
			t.Errorf("%v %s: staged %v, %v, want %v", c.d, c.path, got.Staged, err, c.staged)
		}
		eq(t, got.StageDir, "/home/boxd/.bugnet/xfer")
	}
	for _, s := range []sandbox{newSandbox(t, "root"), newSandbox(t, "")} {
		got, err := resolve(t, s, upload, "/opt/x")
		if err != nil || got.Staged {
			t.Errorf("a root exec user staged /opt/x: %+v, %v", got, err)
		}
	}
}

func TestResolveUsesSudoOnlyForANonRootUser(t *testing.T) {
	s := newSandbox(t, "boxd")
	if _, err := resolve(t, s, upload, "/opt/x"); err != nil {
		t.Fatal(err)
	}
	eq(t, s.fake.Ran(), [][]string{{"sudo", "-n", "realpath", "-m", "--", "/opt/x"}})
	r := newSandbox(t, "root")
	if _, err := resolve(t, r, upload, "/opt/x"); err != nil {
		t.Fatal(err)
	}
	eq(t, r.fake.Ran(), [][]string{{"realpath", "-m", "--", "/opt/x"}})
}
