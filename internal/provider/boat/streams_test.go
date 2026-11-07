package boat

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
)

const (
	routeDownloadURL = "POST /api/v1/sandboxes/{id}/files/download-url"
	routeUploadURL   = "POST /api/v1/sandboxes/{id}/files/upload-url"
	routePermissions = "POST /api/v1/sandboxes/{id}/files/permissions"
	routeMove        = "POST /api/v1/sandboxes/{id}/files/move"
	routeList        = "GET /api/v1/sandboxes/{id}/files/list"
	routeFolder      = "POST /api/v1/sandboxes/{id}/files/folder"
	routeDeleteFile  = "DELETE /api/v1/sandboxes/{id}/files"
)

// missingAnswer is how Boat's sandbox answered a file call on a path that does not exist
var missingAnswer = fault{
	status: http.StatusBadRequest, code: "sandbox_direct_failed",
	message: "No such file or directory: /home/user/bugnet-streams/missing", times: 1,
}

func TestBoatStreamsContract(t *testing.T) {
	for _, c := range []struct {
		name  string
		whole bool
	}{
		{"CutUploadsKept", false},
		{"WholeUploadsOnly", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := newFake(t)
			f.wholeUploads = c.whole
			p := f.provider(t, fakeKey)
			contract.Streams(t, p, readySandbox(t, f), fakeDir(t))
		})
	}
}

// streamUp streams data into name through OpenWriter and commits it
func streamUp(
	t *testing.T, p *Provider, ref provider.Ref, name string, data []byte, mode fs.FileMode,
) provider.FileEntry {
	t.Helper()
	w, err := p.OpenWriter(t.Context(), ref, name,
		provider.WriteSpec{Size: int64(len(data)), Mode: mode})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	entry, err := w.Commit(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestBoatStreamWritesBesideTheTarget(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	dir := fakeDir(t)
	name := dir + "/f"
	entry := streamUp(t, p, ref, name, []byte("0123456789"), 0o600)
	if entry.Path != name || entry.Size != 10 || !entry.Regular {
		t.Errorf("Commit described %+v, want a regular file of 10 bytes at %s", entry, name)
	}
	moves := f.movesSeen()
	if len(moves) != 1 {
		t.Fatalf("Commit made the moves %v, want one", moves)
	}
	temp := regexp.MustCompile(`^` + regexp.QuoteMeta(dir) + `/\.f\.bugnet-[0-9a-f]{8}\.part$`)
	if !temp.MatchString(moves[0][0]) || moves[0][1] != name {
		t.Errorf("Commit moved %v, want a temp file beside %s moved over it", moves, name)
	}
	eq(t, f.permissionCalls(), []string{moves[0][0] + " 600"})
	eq(t, f.grantTTLs(), []int{3600})
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("committed file %v, %v, want mode 0600", info, err)
	}
	got, _ := os.ReadFile(name)
	eq(t, string(got), "0123456789")
}

func TestBoatDownloadSkipsOn200(t *testing.T) {
	f := newFake(t)
	f.ignoreRange = true
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	name := fakeDir(t) + "/blob"
	data := make([]byte, 100<<10+7)
	_, _ = rand.Read(data)
	if err := os.WriteFile(name, data, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, 37, int64(len(data)) - 1} {
		r, err := p.OpenReader(t.Context(), ref, name, offset)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil || !bytes.Equal(got, data[offset:]) {
			t.Errorf("read from %d of a 200 answer gave %d bytes, %v", offset, len(got), err)
		}
	}
	var ranges []string
	for _, h := range f.downloadHeaders() {
		ranges = append(ranges, h.Get("Range"))
		eq(t, h.Get("Accept-Encoding"), "identity")
	}
	last := strconv.Itoa(len(data) - 1)
	eq(t, ranges, []string{"bytes=0-" + last, "bytes=37-" + last, "bytes=" + last + "-" + last})
	eq(t, f.grantTTLs(), []int{3600, 3600, 3600})
}

func TestBoatReaderFromAnOffsetWithRange(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	name := fakeDir(t) + "/blob"
	data := make([]byte, 64<<10)
	_, _ = rand.Read(data)
	if err := os.WriteFile(name, data, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, 1, int64(len(data))} {
		r, err := p.OpenReader(t.Context(), ref, name, offset)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil || !bytes.Equal(got, data[offset:]) {
			t.Errorf("read from %d gave %d bytes, %v", offset, len(got), err)
		}
	}
	// a read at the end asks for no URL
	eq(t, f.callCount(routeDownloadURL), 2)
}

func TestBoatReaderThatEndsEarlyFails(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	name := fakeDir(t) + "/f"
	if err := os.WriteFile(name, bytes.Repeat([]byte("x"), 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	f.shortDownload = true
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	got, err := io.ReadAll(r)
	if len(got) != 32<<10 || err == nil {
		t.Fatalf("a download cut in half read %d bytes, %v, want 32 KiB and an error", len(got),
			err)
	}
}

func TestBoatRefusesPathsOutsideHomeAndTmp(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	spec := provider.WriteSpec{Size: 1, Mode: 0o644}
	for _, bad := range []string{
		"/opt/x", "/etc/passwd", "/home/username/f", "relative/f", "", "/tmp/../etc/x", "/",
		"/root/f", "/tmpfile", "/home",
	} {
		_, err := p.StatFile(t.Context(), ref, bad)
		eq(t, isCode(err, provider.CodeInvalid), true)
		_, err = p.OpenReader(t.Context(), ref, bad, 0)
		eq(t, isCode(err, provider.CodeInvalid), true)
		_, err = p.OpenWriter(t.Context(), ref, bad, spec)
		eq(t, isCode(err, provider.CodeInvalid), true)
	}
	eq(t, f.totalCalls(), 0)
}

func TestBoatStatsTheRootsAsDirectories(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	for _, root := range []string{"/tmp", "/tmp/", "/tmp/."} {
		entry, err := p.StatFile(t.Context(), ref, root)
		if err != nil || !entry.IsDir || entry.Regular || entry.Path != "/tmp" {
			t.Fatalf("StatFile of %q gave %+v, %v, want the directory /tmp", root, entry, err)
		}
	}
	_, err := p.OpenReader(t.Context(), ref, "/tmp", 0)
	eq(t, isCode(err, provider.CodeInvalid), true)
	calls := f.totalCalls()
	for _, root := range []string{"/tmp", "/home/user", "/home/user/"} {
		_, err = p.OpenWriter(t.Context(), ref, root, provider.WriteSpec{Size: 1, Mode: 0o644})
		eq(t, isCode(err, provider.CodeInvalid), true)
	}
	eq(t, f.totalCalls(), calls)
	eq(t, f.callCount(routeDownloadURL), 0)
}

func TestBoatFifoIsNotRegular(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	dir := fakeDir(t)
	name := dir + "/fifo"
	if err := syscall.Mkfifo(name, 0o644); err != nil {
		t.Fatal(err)
	}
	entry, err := p.StatFile(t.Context(), ref, name)
	if err != nil || entry.Regular || entry.IsDir || entry.Mode&fs.ModeIrregular == 0 {
		t.Fatalf("StatFile of a FIFO gave %+v, %v, want an irregular entry", entry, err)
	}
	_, err = p.OpenReader(t.Context(), ref, name, 0)
	eq(t, isCode(err, provider.CodeInvalid), true)
	eq(t, f.callCount(routeDownloadURL), 0)

	if err := os.WriteFile(dir+"/file", []byte("abc"), 0o640); err != nil {
		t.Fatal(err)
	}
	file, err := p.StatFile(t.Context(), ref, dir+"/file")
	if err != nil || !file.Regular || file.Size != 3 || file.Mode != 0o640 {
		t.Fatalf("StatFile of a file gave %+v, %v", file, err)
	}
	_, err = p.StatFile(t.Context(), ref, dir+"/missing")
	eq(t, isCode(err, provider.CodeNotFound), true)
}

func TestBoatTypeCheckSeesTheEntryNow(t *testing.T) {
	for _, c := range []struct {
		name string
		swap func(name string) error
		ok   func(provider.FileEntry, error) bool
	}{
		{"Link", func(name string) error {
			return errors.Join(os.Remove(name), os.Symlink("other", name))
		}, func(e provider.FileEntry, err error) bool {
			return err == nil && e.Mode&fs.ModeSymlink != 0 && !e.Regular && !e.IsDir
		}},
		{"Dir", func(name string) error {
			return errors.Join(os.Remove(name), os.Mkdir(name, 0o755))
		}, func(e provider.FileEntry, err error) bool {
			return err == nil && e.IsDir && e.Mode.IsDir() && !e.Regular
		}},
		{"Missing", os.Remove, func(_ provider.FileEntry, err error) bool {
			return isCode(err, provider.CodeNotFound)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			ref := readySandbox(t, f)
			name := fakeDir(t) + "/f"
			if err := os.WriteFile(name, []byte("abc"), 0o644); err != nil {
				t.Fatal(err)
			}
			// the info still describes the file, so only the shell's answer shows the change
			f.swapAfterInfo(name, func() {
				if err := c.swap(name); err != nil {
					t.Error(err)
				}
			})
			entry, err := p.StatFile(t.Context(), ref, name)
			if !c.ok(entry, err) {
				t.Fatalf("StatFile of a file that became a %s gave %+v, %v", c.name, entry, err)
			}
		})
	}
}

func TestBoatReaderRefusesAnotherRange(t *testing.T) {
	for _, c := range []struct {
		name  string
		shift int64
		grow  []byte
	}{
		{"Start", 5, nil},
		{"Total", 0, []byte("more")},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			f.shiftRange = c.shift
			p := f.provider(t, fakeKey)
			ref := readySandbox(t, f)
			name := fakeDir(t) + "/f"
			if err := os.WriteFile(name, bytes.Repeat([]byte("x"), 64<<10), 0o644); err != nil {
				t.Fatal(err)
			}
			if c.grow != nil {
				f.growAfterInfo(name, c.grow)
			}
			r, err := p.OpenReader(t.Context(), ref, name, 0)
			if err == nil {
				_ = r.Close()
				t.Fatal("a read whose 206 does not match the stat opened")
			}
			eq(t, isCode(err, provider.CodeInternal), true)
			eq(t, len(f.downloadHeaders()), 1)
		})
	}
}

func TestBoatWriteBlocksUnderBackPressure(t *testing.T) {
	f := newFake(t)
	f.holdUploads = make(chan struct{})
	release := sync.OnceFunc(func() { close(f.holdUploads) })
	t.Cleanup(release)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	name := fakeDir(t) + "/big"
	const size = 64 << 20
	w, err := p.OpenWriter(t.Context(), ref, name, provider.WriteSpec{Size: size, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	wrote := make(chan error, 1)
	go func() {
		_, err := w.Write(make([]byte, size))
		wrote <- err
	}()
	select {
	case err := <-wrote:
		t.Fatalf("a 64 MiB Write returned %v while Boat read nothing, want it to block", err)
	case <-time.After(500 * time.Millisecond):
	}
	release()
	if err := <-wrote; err != nil {
		t.Fatalf("Write once Boat read gave %v", err)
	}
	entry, err := w.Commit(t.Context(), nil)
	if err != nil || entry.Size != size || !entry.Regular {
		t.Fatalf("Commit gave %+v, %v, want a regular file of 64 MiB", entry, err)
	}
}

func TestBoatStreamErrorsHideTheSignedURL(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := "http://" + lis.Addr().String()
	_ = lis.Close()
	f := newFake(t)
	f.uploadBase = closed
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	name := fakeDir(t) + "/f"
	if err := os.WriteFile(name, []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	var errs []error
	if r, err := p.OpenReader(t.Context(), ref, name, 0); err == nil {
		_ = r.Close()
		t.Fatal("a read from an unreachable signed URL opened")
	} else {
		errs = append(errs, err)
	}
	w, err := p.OpenWriter(t.Context(), ref, name, provider.WriteSpec{Size: 1, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Write([]byte("x"))
	if err == nil {
		_, err = w.Commit(t.Context(), nil)
	}
	errs = append(errs, err)
	for _, err := range errs {
		if err == nil || strings.Contains(err.Error(), "/upload/") ||
			strings.Contains(err.Error(), "/download/") {
			t.Errorf("a stream error %v names the signed URL", err)
		}
	}
	got, _ := os.ReadFile(name)
	eq(t, string(got), "abc")
}

func TestBoatRefusedSignedURLIsNotAuth(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	dir := fakeDir(t)
	name := dir + "/f"
	if err := os.WriteFile(name, []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.expireGrants()
	var errs []error
	if r, err := p.OpenReader(t.Context(), ref, name, 0); err == nil {
		_ = r.Close()
		t.Fatal("a read from an expired signed URL opened")
	} else {
		errs = append(errs, err)
	}
	w, err := p.OpenWriter(t.Context(), ref, name, provider.WriteSpec{Size: 1, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte("x")); err == nil {
		_, err = w.Commit(t.Context(), nil)
	}
	errs = append(errs, err)
	// past Boat's 5 MiB JSON body once encoded, so WriteFile takes a signed URL too
	errs = append(errs, p.WriteFile(t.Context(), ref, dir+"/big", make([]byte, 4<<20), 0o644))
	for _, err := range errs {
		e, ok := provider.AsError(err)
		if !ok || e.Code != provider.CodeInvalid || e.CanFailOver {
			t.Errorf("a signed URL refused with 403 gave %v, want invalid without fail-over", err)
		}
	}
	got, _ := os.ReadFile(name)
	eq(t, string(got), "abc")
	eq(t, f.uploadCount(), 2)
}

func TestBoatStreamsAskForIdentity(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	name := fakeDir(t) + "/f"
	streamUp(t, p, ref, name, []byte("0123456789"), 0o644)
	r, err := p.OpenReader(t.Context(), ref, name, 3)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	eq(t, string(got), "3456789")
	eq(t, err, nil)
	tr := p.streams.Transport.(*http.Transport)
	eq(t, tr.DisableCompression, true)
	eq(t, [3]bool{tr.Protocols.HTTP1(), tr.Protocols.HTTP2(), tr.Protocols.UnencryptedHTTP2()},
		[3]bool{true, false, false})
	eq(t, p.streams.CheckRedirect != nil, true)
	eq(t, f.callCount(routeUploadURL), 1)
	eq(t, f.callCount(routeMove), 1)
	eq(t, f.callCount(routePermissions), 1)
	if _, err := os.Stat(name); errors.Is(err, fs.ErrNotExist) {
		t.Fatal("the target is gone")
	}
}

// fileOp is one adapter call on the file f inside dir, the sandbox's only file at the start
type fileOp func(t *testing.T, p *Provider, ref provider.Ref, dir string) error

// commitThree writes three bytes over f through OpenWriter and commits them
func commitThree(t *testing.T, p *Provider, ref provider.Ref, dir string) error {
	w, err := p.OpenWriter(t.Context(), ref, dir+"/f", provider.WriteSpec{Size: 3, Mode: 0o644})
	if err != nil {
		return err
	}
	if _, err := w.Write([]byte("xyz")); err != nil {
		return err
	}
	_, err = w.Commit(t.Context(), nil)
	return err
}

func TestBoatMissingPathOnEveryFileRoute(t *testing.T) {
	for _, c := range []struct {
		name  string
		route string
		op    fileOp
	}{
		{"Stat", routeInfo, func(t *testing.T, p *Provider, ref provider.Ref, dir string) error {
			_, err := p.StatFile(t.Context(), ref, dir+"/f")
			return err
		}},
		{"Read", routeRead, func(t *testing.T, p *Provider, ref provider.Ref, dir string) error {
			_, _, err := p.ReadFile(t.Context(), ref, dir+"/f", 0, 3)
			return err
		}},
		{"List", routeList, func(t *testing.T, p *Provider, ref provider.Ref, dir string) error {
			_, err := listAll(t.Context(), p, ref, dir, 1)
			return err
		}},
		{"Write", routeWrite, func(t *testing.T, p *Provider, ref provider.Ref, dir string) error {
			return p.WriteFile(t.Context(), ref, dir+"/f", []byte("xyz"), 0o644)
		}},
		{"Folder", routeFolder,
			func(t *testing.T, p *Provider, ref provider.Ref, dir string) error {
				return p.WriteFile(t.Context(), ref, dir+"/sub/f", []byte("xyz"), 0o644)
			}},
		{"DownloadURL", routeDownloadURL,
			func(t *testing.T, p *Provider, ref provider.Ref, dir string) error {
				r, err := p.OpenReader(t.Context(), ref, dir+"/f", 0)
				if err == nil {
					_ = r.Close()
				}
				return err
			}},
		{"UploadURL", routeUploadURL, commitThree},
		{"Permissions", routePermissions, commitThree},
		{"Move", routeMove, commitThree},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			ref := readySandbox(t, f)
			dir := fakeDir(t)
			if err := os.WriteFile(dir+"/f", []byte("abc"), 0o644); err != nil {
				t.Fatal(err)
			}
			f.inject(c.route, missingAnswer)
			err := c.op(t, p, ref, dir)
			e, ok := provider.AsError(err)
			if !ok || e.Code != provider.CodeNotFound || e.CanFailOver {
				t.Fatalf("a missing path answered on %s gave %v, want not_found", c.route, err)
			}
			eq(t, f.callCount(c.route), 1)
			got, _ := os.ReadFile(dir + "/f")
			eq(t, string(got), "abc")
		})
	}
}

func TestBoatAbortOfATempNeverMadeIsNil(t *testing.T) {
	for _, c := range []struct {
		name    string
		prepare func(f *fakeBoat)
		data    string
	}{
		{"Refused", (*fakeBoat).expireGrants, ""},
		{"Unfinished", func(f *fakeBoat) { f.wholeUploads = true }, "01234"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			c.prepare(f)
			p := f.provider(t, fakeKey)
			ref := readySandbox(t, f)
			dir := fakeDir(t)
			name := dir + "/f"
			if err := os.WriteFile(name, []byte("old"), 0o644); err != nil {
				t.Fatal(err)
			}
			w, err := p.OpenWriter(t.Context(), ref, name,
				provider.WriteSpec{Size: 10, Mode: 0o644})
			if err != nil {
				t.Fatal(err)
			}
			if c.data != "" {
				if _, err := w.Write([]byte(c.data)); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, func() bool { return f.uploadCount() == 1 })
			eq(t, w.Abort(), nil)
			eq(t, f.callCount(routeDeleteFile), 1)
			got, _ := os.ReadFile(name)
			eq(t, string(got), "old")
			left, _ := os.ReadDir(dir)
			eq(t, len(left), 1)
		})
	}
}

func TestBoatReaderRefusesAnEncoding(t *testing.T) {
	f := newFake(t)
	f.downloadEncoding = "gzip"
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	name := fakeDir(t) + "/f"
	if err := os.WriteFile(name, bytes.Repeat([]byte("x"), 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err == nil {
		_ = r.Close()
		t.Fatal("a download in gzip encoding opened")
	}
	eq(t, isCode(err, provider.CodeInternal), true)
	eq(t, len(f.downloadHeaders()), 1)
}

func TestBoatReaderChecksTheLengthOfA200(t *testing.T) {
	f := newFake(t)
	f.ignoreRange = true
	p := f.provider(t, fakeKey)
	ref := readySandbox(t, f)
	name := fakeDir(t) + "/f"
	if err := os.WriteFile(name, bytes.Repeat([]byte("x"), 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	// the whole file Boat sends is longer than the one its info described
	f.growAfterInfo(name, []byte("more"))
	r, err := p.OpenReader(t.Context(), ref, name, 10)
	if err == nil {
		got, _ := io.ReadAll(r)
		_ = r.Close()
		t.Fatalf("a 200 of another length than the stat's opened and read %d bytes", len(got))
	}
	eq(t, isCode(err, provider.CodeInternal), true)
	eq(t, len(f.downloadHeaders()), 1)
}

func TestBoatSignedURLAnswersReadAMissingPath(t *testing.T) {
	missing := []byte(`{"ok":false,"status":400,"code":"sandbox_direct_failed",` +
		`"message":"No such file or directory: /home/user/x"}`)
	err := signedError(http.StatusBadRequest, http.Header{}, missing)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeNotFound || e.CanFailOver {
		t.Errorf("a signed URL's missing path answer gave %v, want not_found", err)
	}
	err = signedError(http.StatusForbidden, http.Header{}, []byte("invalid or expired URL"))
	eq(t, isCode(err, provider.CodeInvalid), true)
}
