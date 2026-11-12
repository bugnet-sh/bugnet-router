package hf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// jobMv is GNU mv in our Ubuntu image, whose -T never moves a file into a directory
	jobMv = "/bin/mv"
	// maxStatBytes bounds one stat answer
	maxStatBytes = 64 << 10
	// maxListBytes bounds one list page, past which the listing is cut
	maxListBytes = 16 << 20
	// listPage is the most entries one list call asks for, so a page of long paths fits the bound
	listPage = 2000
	// digestScript reads the file on stdin, since sha256sum escapes a name holding \ or a newline
	digestScript = `sha256sum < "$1"`
	// maxCommandOut bounds what the output of the adapter's own commands is read to
	maxCommandOut = 256
)

// filePath cleans an absolute path, refusing one sbx-server would read as other text
func filePath(name string) (string, error) {
	if !plainText(name) {
		return "", invalid(errors.New("a path that is not UTF-8 text without NUL bytes"), false)
	}
	clean := path.Clean(name)
	if !path.IsAbs(clean) {
		return "", invalid(fmt.Errorf("path %.200q is not absolute", name), false)
	}
	return clean, nil
}

// targetPath is a path a write may replace, never one that names a directory
func targetPath(name string) (string, error) {
	clean, err := filePath(name)
	if err != nil {
		return "", err
	}
	if clean == "/" || strings.HasSuffix(name, "/") || strings.HasSuffix(name, "/.") ||
		strings.HasSuffix(name, "/..") {
		return "", invalid(fmt.Errorf("path %.200q names no file", name), false)
	}
	return clean, nil
}

/* Stat */

// fileJSON is one entry of sbx-server's list and stat routes
type fileJSON struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Size    int64  `json:"size"`
	MTimeMS int64  `json:"mtime_ms"`
	Mode    string `json:"mode"`
}

// entry describes the entry at p, which the router names, never the answer
func (e fileJSON) entry(p string) provider.FileEntry {
	perm, _ := strconv.ParseUint(strings.TrimPrefix(e.Mode, "0o"), 8, 32)
	m := fs.FileMode(perm).Perm()
	switch e.Type {
	case "dir":
		m |= fs.ModeDir
	case "symlink":
		m |= fs.ModeSymlink
	case "file":
	default:
		m |= fs.ModeIrregular
	}
	out := provider.FileEntry{
		Path: p, Size: max(e.Size, 0), IsDir: e.Type == "dir", Mode: m,
		// sbx-server names FIFOs, sockets and devices file too, design 4.4
		Regular: e.Type == "file",
	}
	if e.MTimeMS > 0 {
		out.Modified = time.UnixMilli(e.MTimeMS).UTC()
	}
	return out
}

// StatFile describes a path from its symlink metadata, so a final symlink is not followed
func (p *Provider) StatFile(
	ctx context.Context, ref provider.Ref, name string,
) (provider.FileEntry, error) {
	if _, ok := jobOf(ref); !ok {
		return provider.FileEntry{}, notHF(ref)
	}
	clean, err := filePath(name)
	if err != nil {
		return provider.FileEntry{}, err
	}
	sb, err := p.sandbox(ctx, ref)
	if err != nil {
		return provider.FileEntry{}, err
	}
	return p.stat(ctx, sb, clean)
}

func (p *Provider) stat(
	ctx context.Context, sb sandbox, clean string,
) (provider.FileEntry, error) {
	req, err := p.sbx.request(ctx, sb, http.MethodGet, "/v1/files/stat",
		url.Values{"path": {clean}}, nil)
	if err != nil {
		return provider.FileEntry{}, err
	}
	resp, err := p.do(ctx, sb, req)
	if err != nil {
		return provider.FileEntry{}, err
	}
	defer drainClose(resp)
	var e fileJSON
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxStatBytes)).Decode(&e); err != nil {
		return provider.FileEntry{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s stat of %s: %w", sb.id, clean, err)}
	}
	return e.entry(clean), nil
}

/* Reads */

// ReadFile reads up to length bytes at offset, asking one more to tell whether they reach the end
func (p *Provider) ReadFile(
	ctx context.Context, ref provider.Ref, name string, offset int64, length int,
) ([]byte, bool, error) {
	if _, ok := jobOf(ref); !ok {
		return nil, false, notHF(ref)
	}
	clean, err := filePath(name)
	if err != nil {
		return nil, false, err
	}
	if offset < 0 || length < 0 {
		return nil, false, invalid(fmt.Errorf("a read of %d bytes at offset %d", length,
			offset), false)
	}
	sb, err := p.sandbox(ctx, ref)
	if err != nil {
		return nil, false, err
	}
	e, err := p.stat(ctx, sb, clean)
	switch {
	case err != nil:
		return nil, false, err
	case e.IsDir:
		return nil, false, invalid(fmt.Errorf("%s is a directory", clean), false)
	case e.Regular && offset > e.Size:
		return nil, true, nil
	}
	// The read follows a final symlink, whose stat size is the link's own
	data, err := p.readRange(ctx, sb, clean, offset, int64(length)+1)
	if err != nil {
		return nil, false, err
	}
	if len(data) > length {
		return data[:length], false, nil
	}
	return data, true, nil
}

// readRange reads at most n bytes at offset, an error for an answer longer or cut short
func (p *Provider) readRange(
	ctx context.Context, sb sandbox, clean string, offset, n int64,
) ([]byte, error) {
	req, err := p.sbx.request(ctx, sb, http.MethodGet, "/v1/files/read", readQuery(clean,
		offset, n), nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.do(ctx, sb, req)
	if err != nil {
		return nil, err
	}
	defer drainClose(resp)
	if resp.ContentLength > n {
		return nil, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"hf Job %s answered %d bytes of %s for a read of %d", sb.id, resp.ContentLength,
			clean, n)}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, n+1))
	switch {
	case err != nil:
		return nil, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s read of %s: %w", sb.id, clean, err)}
	case int64(len(data)) > n:
		return nil, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"hf Job %s answered more of %s than the %d bytes asked", sb.id, clean, n)}
	}
	return data, nil
}

// readQuery asks the read route for n bytes at offset
func readQuery(clean string, offset, n int64) url.Values {
	return url.Values{"path": {clean}, "offset": {strconv.FormatInt(offset, 10)},
		"length": {strconv.FormatInt(n, 10)}}
}

/* Writes */

// WriteFile writes a temporary file beside the target in parts, then moves it over the target
func (p *Provider) WriteFile(
	ctx context.Context, ref provider.Ref, name string, data []byte, mode fs.FileMode,
) error {
	if _, ok := jobOf(ref); !ok {
		return notHF(ref)
	}
	spec := provider.WriteSpec{Size: int64(len(data)), Mode: mode}
	if err := spec.Check(); err != nil {
		return err
	}
	target, err := targetPath(name)
	if err != nil {
		return err
	}
	sb, err := p.sandbox(ctx, ref)
	if err != nil {
		return err
	}
	u := p.newUpload(ctx, sb, target, spec)
	_, err = u.Write(data)
	if err == nil {
		err = u.finish(ctx, nil)
	}
	if err != nil {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), discardTimeout)
		defer cancel()
		_ = u.Discard(dctx)
	}
	return err
}

// replace moves temp over target, which rename(2) does at once
func (p *Provider) replace(ctx context.Context, sb sandbox, temp, target string) error {
	_, code, err := p.command(ctx, sb, []string{p.mv, "-fT", "--", temp, target})
	if err != nil {
		return err
	}
	if code != 0 {
		return invalid(fmt.Errorf("hf Job %s could not move %s over %s, mv exited %d", sb.id,
			temp, target, code), false)
	}
	return nil
}

// digest is a file's SHA-256 as sha256sum computes it in the Job
func (p *Provider) digest(ctx context.Context, sb sandbox, name string) ([]byte, error) {
	out, code, err := p.command(ctx, sb, []string{"/bin/sh", "-c", digestScript, "sh", name})
	if err != nil {
		return nil, err
	}
	field, _, _ := strings.Cut(string(out), " ")
	sum, herr := hex.DecodeString(field)
	if code != 0 || herr != nil || len(sum) != sha256.Size {
		return nil, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("sha256sum of %s in hf Job %s exited %d", name, sb.id, code)}
	}
	return sum, nil
}

// command runs one of the adapter's own commands and returns the start of its output
func (p *Provider) command(
	ctx context.Context, sb sandbox, argv []string,
) ([]byte, int, error) {
	call := newCallID()
	body, err := p.execBody(sb, p.groupArgv(argv, call), nil, "")
	if err != nil {
		return nil, 0, err
	}
	s, err := p.startExec(ctx, sb, body, call)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = s.Close() }()
	out, err := io.ReadAll(io.LimitReader(s, maxCommandOut))
	if err != nil {
		return nil, 0, err
	}
	code, err := s.Wait()
	return out, code, err
}

// remove deletes a file, nil when it is already gone
func (p *Provider) remove(ctx context.Context, sb sandbox, name string) error {
	req, err := p.sbx.request(ctx, sb, http.MethodDelete, "/v1/files/delete",
		url.Values{"path": {name}}, nil)
	if err != nil {
		return err
	}
	resp, err := p.do(ctx, sb, req)
	if isCode(err, provider.CodeNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	drainClose(resp)
	return nil
}

/* Listings */

// ListFiles lists a directory a page at a time, descending into subdirectories to depth
func (p *Provider) ListFiles(
	ctx context.Context, ref provider.Ref, name string, depth int, into *provider.Listing,
) error {
	if _, ok := jobOf(ref); !ok {
		return notHF(ref)
	}
	if depth < 1 {
		return invalid(fmt.Errorf("depth %d is below 1", depth), false)
	}
	clean, err := filePath(name)
	if err != nil {
		return err
	}
	sb, err := p.sandbox(ctx, ref)
	if err != nil {
		return err
	}
	dirs := []string{clean}
	for level := 1; level <= depth && len(dirs) > 0; level++ {
		var next []string
		for _, dir := range dirs {
			if !into.Visit() {
				return nil
			}
			subdirs, err := p.listDir(ctx, sb, dir, into)
			if err != nil {
				// A subdirectory may vanish or refuse listing while the walk goes on
				if level > 1 && (isCode(err, provider.CodeNotFound) ||
					isCode(err, provider.CodeInvalid)) {
					continue
				}
				return err
			}
			if into.Full() && into.Truncated {
				return nil
			}
			next = append(next, subdirs...)
		}
		dirs = next
	}
	return nil
}

// listAnswer is one page of GET /v1/files/list
type listAnswer struct {
	Entries   []fileJSON `json:"entries"`
	Truncated bool       `json:"truncated"`
	Next      *string    `json:"next"`
}

// listDir adds one directory's entries to into and returns its subdirectories
func (p *Provider) listDir(
	ctx context.Context, sb sandbox, dir string, into *provider.Listing,
) ([]string, error) {
	var subdirs []string
	after := ""
	for {
		q := url.Values{"path": {dir}, "limit": {strconv.Itoa(min(into.Room(), listPage))}}
		if after != "" {
			q.Set("after", after)
		}
		page, cut, err := p.listPage(ctx, sb, q)
		if err != nil {
			return nil, err
		}
		added := 0
		for _, e := range page.Entries {
			// The answer comes from inside the Job, so a name is taken only as one plain entry
			if !entryName(e.Name) || after != "" && e.Name <= after {
				continue
			}
			fe := e.entry(path.Join(dir, e.Name))
			if !into.Add(fe) {
				return subdirs, nil
			}
			added++
			if fe.IsDir {
				subdirs = append(subdirs, fe.Path)
			}
		}
		switch {
		case cut:
			into.Truncated = true
			return subdirs, nil
		case !page.Truncated:
			return subdirs, nil
		case added == 0 || page.Next == nil || *page.Next <= after || into.Full():
			// A page that adds nothing or never moves on, or no room for the next, ends the listing
			into.Truncated = true
			return subdirs, nil
		}
		after = *page.Next
	}
}

// listPage reads one page, cut reporting an answer past maxListBytes
func (p *Provider) listPage(
	ctx context.Context, sb sandbox, q url.Values,
) (listAnswer, bool, error) {
	req, err := p.sbx.request(ctx, sb, http.MethodGet, "/v1/files/list", q, nil)
	if err != nil {
		return listAnswer{}, false, err
	}
	resp, err := p.do(ctx, sb, req)
	if err != nil {
		return listAnswer{}, false, err
	}
	defer drainClose(resp)
	body := &cappedReader{r: resp.Body, left: maxListBytes}
	var page listAnswer
	if err := json.NewDecoder(body).Decode(&page); err != nil {
		if body.cut {
			return listAnswer{}, true, nil
		}
		return listAnswer{}, false, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s list of %s: %w", sb.id, q.Get("path"), err)}
	}
	return page, false, nil
}

// entryName reports a name that is one plain directory entry
func entryName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.Contains(name, "/") &&
		plainText(name)
}

// cappedReader ends a body after left bytes and records that it cut the body short
type cappedReader struct {
	r    io.Reader
	left int64
	cut  bool
}

func (c *cappedReader) Read(b []byte) (int, error) {
	if c.left <= 0 {
		c.cut = true
		return 0, io.EOF
	}
	n, err := c.r.Read(b[:min(int64(len(b)), c.left)])
	c.left -= int64(n)
	return n, err
}
