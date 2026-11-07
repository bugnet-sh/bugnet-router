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
)

func TestBoatStreamsContract(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	contract.Streams(t, p, readySandbox(t, f), fakeDir(t))
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
