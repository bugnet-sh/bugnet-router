package boat

import (
	"bytes"
	"context"
	"encoding/base64"
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

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/boat/api"
)

const (
	// fileCap is the most a JSON file call carries, Boat's 5 MiB, counted on the encoded body
	fileCap = 5 << 20
	// uploadTTLSeconds keeps a signed upload URL valid only as long as an upload needs
	uploadTTLSeconds = 600
)

// standing are directories every sandbox has, which a write never needs to create
var standing = map[string]bool{"/": true, ".": true, "/tmp": true, "/home/user": true}

// ReadFile reads a file of up to 5 MiB whole and cuts the range, a larger one through tail
func (p *Provider) ReadFile(
	ctx context.Context, ref provider.Ref, name string, offset int64, length int,
) ([]byte, bool, error) {
	id, err := sandboxID(ref)
	if err != nil {
		return nil, false, err
	}
	if offset < 0 || length < 0 {
		return nil, false, invalid(fmt.Errorf("read of %d bytes at offset %d", length, offset))
	}
	info, err := p.info(ctx, id, name)
	if err != nil {
		return nil, false, err
	}
	if info.IsDir {
		return nil, false, invalid(fmt.Errorf("%s is a directory", name))
	}
	size := int64(info.Size)
	if offset >= size {
		return nil, true, nil
	}
	if size > fileCap {
		return p.readTail(ctx, ref, name, offset, length)
	}
	data, err := p.readWhole(ctx, id, name)
	if isCode(err, provider.CodeInvalid) {
		// Boat refuses a whole read past 5 MiB, which a file still being written can reach
		if now, ierr := p.info(ctx, id, name); ierr == nil && now.Size > fileCap {
			return p.readTail(ctx, ref, name, offset, length)
		}
	}
	if err != nil {
		return nil, false, err
	}
	if offset >= int64(len(data)) {
		return nil, true, nil
	}
	end := min(offset+int64(length), int64(len(data)))
	return data[offset:end], end >= int64(len(data)), nil
}

// fileInfo is the part of a file info reply the adapter reads
type fileInfo struct {
	IsDir bool  `json:"isDir"`
	Size  int64 `json:"size"`
}

// fileItem is one entry of a directory listing reply as the adapter reads it
type fileItem struct {
	Path       string `json:"path"`
	Name       string `json:"name"`
	IsDir      bool   `json:"isDir"`
	IsSymlink  bool   `json:"isSymlink"`
	Mode       string `json:"mode"`
	Size       int64  `json:"size"`
	ModifiedAt string `json:"modifiedAt"`
}

type fileListing struct {
	Files     []fileItem `json:"files"`
	Truncated bool       `json:"truncated"`
}

type fileContent struct {
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
}

type uploadReply struct {
	URL string `json:"url"`
}

func (p *Provider) info(ctx context.Context, id, name string) (fileInfo, error) {
	params := &api.GetFileInfoParams{Path: name}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.GetFileInfo(ctx, id, params)
	})
	if err != nil {
		return fileInfo{}, err
	}
	var info fileInfo
	if err := decode(raw, &info); err != nil {
		return fileInfo{}, internal(fmt.Errorf("boat file info of %s: %w", name, err))
	}
	return info, nil
}

func (p *Provider) readWhole(ctx context.Context, id, name string) ([]byte, error) {
	params := &api.ReadFileParams{Path: name, Encoding: new(api.ReadFileParamsEncodingBase64)}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.ReadFile(ctx, id, params)
	})
	if err != nil {
		return nil, err
	}
	var reply fileContent
	if err := decode(raw, &reply); err != nil {
		return nil, internal(fmt.Errorf("boat read of %s: %w", name, err))
	}
	if reply.Encoding == string(api.FileReadResponseEncodingUtf8) {
		return []byte(reply.Content), nil
	}
	data, err := base64.StdEncoding.DecodeString(reply.Content)
	if err != nil {
		return nil, internal(fmt.Errorf("boat read of %s: %w", name, err))
	}
	return data, nil
}

// readTail cuts length bytes at offset in the sandbox, base64 keeping them whole on the stream
func (p *Provider) readTail(
	ctx context.Context, ref provider.Ref, name string, offset int64, length int,
) ([]byte, bool, error) {
	// One byte past length tells the end of file apart without a size that may be stale
	stream, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: []string{
		"sh", "-c", `tail -c +"$1" -- "$2" | head -c "$3" | base64`, "sh",
		strconv.FormatInt(offset+1, 10), name, strconv.FormatInt(int64(length)+1, 10),
	}})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = stream.Close() }()
	out, err := io.ReadAll(stream)
	if err != nil {
		return nil, false, err
	}
	code, err := stream.Wait()
	if err != nil {
		return nil, false, err
	}
	if code != 0 {
		return nil, false, internal(fmt.Errorf("boat read of %s exited %d", name, code))
	}
	data, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(string(out)), ""))
	if err != nil {
		return nil, false, internal(fmt.Errorf("boat read of %s: %w", name, err))
	}
	if len(data) > length {
		return data[:length], false, nil
	}
	return data, true, nil
}

// WriteFile makes the parent, writes in one JSON call or through an upload URL, then sets mode
func (p *Provider) WriteFile(
	ctx context.Context, ref provider.Ref, name string, data []byte, mode fs.FileMode,
) error {
	id, err := sandboxID(ref)
	if err != nil {
		return err
	}
	if dir := path.Dir(name); !standing[dir] {
		folder := api.FileFolderRequest{Path: dir}
		_, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
			return p.api.CreateFolder(ctx, id, folder)
		})
		if err != nil {
			return err
		}
	}
	body, err := json.Marshal(api.FileWriteRequest{
		Path: name, Content: base64.StdEncoding.EncodeToString(data),
		Encoding: new(api.FileWriteRequestEncodingBase64),
	})
	if err != nil {
		return invalid(err)
	}
	if len(body) <= fileCap {
		_, err = p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
			return p.api.WriteFileWithBody(ctx, id, "application/json", bytes.NewReader(body))
		})
	} else {
		err = p.upload(ctx, id, name, data)
	}
	if err != nil {
		return err
	}
	perm := api.FilePermissionsRequest{Path: name, Mode: fmt.Sprintf("%03o", mode.Perm())}
	_, err = p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.SetFilePermissions(ctx, id, perm)
	})
	return err
}

// upload PUTs data to a URL the sandbox serves itself, which has no size cap and takes no key
func (p *Provider) upload(ctx context.Context, id, name string, data []byte) error {
	req := api.FileUrlRequest{Path: name, TtlSeconds: new(uploadTTLSeconds)}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.CreateUploadUrl(ctx, id, req)
	})
	if err != nil {
		return err
	}
	var grant uploadReply
	if err := decode(raw, &grant); err != nil || grant.URL == "" {
		return internal(fmt.Errorf("boat upload URL reply for %s holds no URL", name))
	}
	put, err := http.NewRequestWithContext(ctx, http.MethodPut, grant.URL, bytes.NewReader(data))
	if err != nil {
		return internal(fmt.Errorf("boat upload URL for %s does not parse", name))
	}
	put.Header.Set("Content-Type", "application/octet-stream")
	resp, err := p.direct.Do(put)
	if err != nil {
		return internal(fmt.Errorf("boat upload of %s: %w", name, redact(err)))
	}
	defer func() { _ = resp.Body.Close() }()
	reply, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return signedError(resp.StatusCode, resp.Header, reply)
	}
	return nil
}

// signedError classifies a signed URL's error answer, a refusal blaming the URL, never the key
func signedError(status int, h http.Header, raw []byte) error {
	err := apiError(status, h, raw, noEffect)
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeAuth {
		e.Code, e.CanFailOver = provider.CodeInvalid, false
	}
	return err
}

// redact drops the signed URL from a transport error, since it grants writes until it expires
func redact(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: %w", ue.Op, ue.Err)
	}
	return err
}

// ListFiles lists one directory per call and descends into directories, never symlinks, to depth
func (p *Provider) ListFiles(
	ctx context.Context, ref provider.Ref, name string, depth int, into *provider.Listing,
) error {
	id, err := sandboxID(ref)
	if err != nil {
		return err
	}
	if depth < 1 {
		return invalid(fmt.Errorf("depth %d is below 1", depth))
	}
	dirs := []string{name}
	for level := 1; level <= depth && len(dirs) > 0; level++ {
		var next []string
		for _, dir := range dirs {
			if !into.Visit() {
				return nil
			}
			entries, err := p.list(ctx, id, dir, into)
			if err != nil {
				// Commands in the sandbox may delete a directory while the listing runs
				if level > 1 && (isCode(err, provider.CodeNotFound) ||
					isCode(err, provider.CodeInvalid)) {
					continue
				}
				return err
			}
			for _, e := range entries {
				if !into.Add(e) {
					return nil
				}
				if e.IsDir {
					next = append(next, e.Path)
				}
			}
		}
		dirs = next
	}
	return nil
}

// list reads one directory, marking the listing truncated when Boat cut it at 10,000 entries
func (p *Provider) list(
	ctx context.Context, id, dir string, into *provider.Listing,
) ([]provider.FileEntry, error) {
	params := &api.ListFilesParams{Path: new(dir)}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.ListFiles(ctx, id, params)
	})
	if err != nil {
		return nil, err
	}
	var reply fileListing
	if err := decode(raw, &reply); err != nil {
		return nil, internal(fmt.Errorf("boat listing of %s: %w", dir, err))
	}
	if reply.Truncated {
		into.Truncated = true
	}
	out := make([]provider.FileEntry, 0, len(reply.Files))
	for _, e := range reply.Files {
		out = append(out, fileEntry(dir, e))
	}
	return out, nil
}

func fileEntry(dir string, e fileItem) provider.FileEntry {
	perm, _ := strconv.ParseUint(e.Mode, 8, 32)
	mode := fs.FileMode(perm).Perm()
	switch {
	case e.IsSymlink:
		mode |= fs.ModeSymlink
	case e.IsDir:
		mode |= fs.ModeDir
	}
	name := e.Path
	if name == "" {
		name = path.Join(dir, e.Name)
	}
	// A time in a form parseTime does not know leaves the entry without one
	modified, _ := parseTime(e.ModifiedAt)
	return provider.FileEntry{
		Path: name, Size: e.Size, IsDir: mode.IsDir(), Mode: mode, Modified: modified,
	}
}
