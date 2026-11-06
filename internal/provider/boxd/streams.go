package boxd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/boxd/boxdpb"
)

// reasonTrailer is where boxd names the exact reason a file call failed
const reasonTrailer = "x-boxd-error-reason"

var _ provider.FileStreamer = (*Provider)(nil)

// fsError classifies a file call's failure, boxd's reason trailer before its gRPC code
func fsError(ctx context.Context, err error, trailer metadata.MD) error {
	if err == nil {
		return nil
	}
	if _, ok := provider.AsError(err); ok {
		return err
	}
	if reasons := trailer.Get(reasonTrailer); len(reasons) > 0 && reasons[0] == "no_space" {
		return &provider.Error{Code: provider.CodeNoSpace, Err: withCtx(ctx, err)}
	}
	return classify(ctx, err)
}

// StatFile asks FsStat, which types a final symlink as a link instead of its target
func (p *Provider) StatFile(
	ctx context.Context, ref provider.Ref, name string,
) (provider.FileEntry, error) {
	vmID, err := machineID(ref)
	if err != nil {
		return provider.FileEntry{}, err
	}
	var trailer metadata.MD
	resp, err := p.api.FsStat(ctx, &boxdpb.FsStatRequest{VmId: vmID, Path: []byte(name)},
		grpc.Trailer(&trailer))
	if err != nil {
		return provider.FileEntry{}, fsError(ctx, err, trailer)
	}
	if resp.GetEntry() == nil {
		return provider.FileEntry{}, &provider.Error{
			Code: provider.CodeInternal, Err: fmt.Errorf("boxd stat of %s named no entry", name),
		}
	}
	entry := fileEntry(path.Dir(name), resp.GetEntry())
	entry.Path = name
	return entry, nil
}

// OpenReader stats the file, then streams FsRead from offset for the bytes it had then
func (p *Provider) OpenReader(
	ctx context.Context, ref provider.Ref, name string, offset int64,
) (provider.FileReader, error) {
	info, err := p.StatFile(ctx, ref, name)
	if err != nil {
		return nil, err
	}
	if !info.Regular {
		return nil, invalid(fmt.Errorf("%s is not a regular file", name))
	}
	if offset < 0 || offset > info.Size {
		return nil, invalid(fmt.Errorf("offset %d is outside a file of %d bytes", offset,
			info.Size))
	}
	if offset == info.Size {
		return provider.NewFileReader(ctx, io.NopCloser(bytes.NewReader(nil)), info, offset,
			nil), nil
	}
	sctx, cancel := context.WithCancel(ctx)
	stream, err := p.streams.FsRead(sctx, &boxdpb.FsReadRequest{
		VmId: ref.ID, Path: []byte(name), Offset: uint64(offset),
		Length: proto.Uint64(uint64(info.Size - offset)),
	})
	if err != nil {
		cancel()
		return nil, classify(ctx, err)
	}
	body := &readBody{ctx: ctx, name: name, stream: stream, cancel: cancel,
		want: uint64(info.Size - offset)}
	// The first chunk carries the entry, so a file swapped for a link or a device fails here
	if err := body.first(); err != nil {
		cancel()
		return nil, err
	}
	return provider.NewFileReader(ctx, body, info, offset, nil), nil
}

// readBody is one FsRead stream, checking the byte count boxd reports at its end
type readBody struct {
	ctx     context.Context
	name    string
	stream  grpc.ServerStreamingClient[boxdpb.FsReadChunk]
	cancel  context.CancelFunc
	want    uint64
	got     uint64
	pending []byte
	err     error
}

func (b *readBody) first() error {
	b.next()
	if b.err != nil && !errors.Is(b.err, io.EOF) {
		return b.err
	}
	return nil
}

// next takes one chunk, and once every wanted byte is in, the chunk that ends the stream
func (b *readBody) next() {
	c, err := b.stream.Recv()
	switch {
	case errors.Is(err, io.EOF):
		b.err = io.EOF
		return
	case err != nil:
		b.err = fsError(b.ctx, err, b.stream.Trailer())
		return
	}
	if e := c.GetEntry(); e != nil && e.GetType() != boxdpb.FsEntryType_FS_ENTRY_TYPE_FILE {
		b.err = invalid(fmt.Errorf("%s is no longer a regular file", b.name))
		return
	}
	b.got += uint64(len(c.GetData()))
	b.pending = c.GetData()
	if c.GetEof() {
		b.end(c)
		return
	}
	if b.got >= b.want {
		// The last bytes are held until the eof chunk confirms boxd sent no fewer
		b.verify()
	}
}

// verify reads past the wanted bytes for the eof chunk, a host that ignored the length aside
func (b *readBody) verify() {
	c, err := b.stream.Recv()
	switch {
	case err != nil && !errors.Is(err, io.EOF):
		b.fail(fsError(b.ctx, err, b.stream.Trailer()))
	case err == nil && c.GetEof():
		b.got += uint64(len(c.GetData()))
		b.end(c)
	default:
		b.err = io.EOF
	}
}

// end checks the eof chunk's count against the bytes that arrived
func (b *readBody) end(c *boxdpb.FsReadChunk) {
	if c.GetBytesRead() != b.got {
		b.fail(&provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"boxd counted %d bytes of %s sent but %d arrived", c.GetBytesRead(), b.name, b.got)})
		return
	}
	b.err = io.EOF
}

// fail drops what is pending, so the read that would end the file fails instead
func (b *readBody) fail(err error) {
	b.pending, b.err = nil, err
}

func (b *readBody) Read(p []byte) (int, error) {
	for len(b.pending) == 0 {
		if b.err != nil {
			return 0, b.err
		}
		b.next()
	}
	n := copy(p, b.pending)
	b.pending = b.pending[n:]
	return n, nil
}

func (b *readBody) Close() error {
	b.cancel()
	return nil
}

// OpenWriter streams FsWrite into a hidden file beside the target, which Commit renames over it
func (p *Provider) OpenWriter(
	ctx context.Context, ref provider.Ref, name string, spec provider.WriteSpec,
) (provider.FileWriter, error) {
	vmID, err := machineID(ref)
	if err != nil {
		return nil, err
	}
	if err := spec.Check(); err != nil {
		return nil, err
	}
	target := path.Clean(name)
	base := path.Base(target)
	if name == "" || base == "/" || base == "." || base == ".." {
		return nil, invalid(fmt.Errorf("path %q names no file", name))
	}
	temp := path.Join(path.Dir(target), provider.TempName(base))
	sctx, cancel := context.WithCancel(ctx)
	stream, err := p.streams.FsWrite(sctx)
	if err != nil {
		cancel()
		return nil, classify(ctx, err)
	}
	u := &upload{
		p: p, ctx: ctx, vmID: vmID, target: target, temp: temp, size: uint64(spec.Size),
		stream: stream, cancel: cancel,
	}
	err = stream.Send(&boxdpb.FsWriteChunk{
		VmId: vmID, Path: []byte(temp), Mode: proto.Uint32(uint32(spec.Mode.Perm())),
		ExpectedSize: proto.Uint64(uint64(spec.Size)),
	})
	if err != nil {
		cancel()
		return nil, u.sendFailed(err)
	}
	return provider.NewFileWriter(ctx, spec, u), nil
}

// upload is one FsWrite stream into a temp file, never closed before Commit
type upload struct {
	p      *Provider
	ctx    context.Context
	vmID   string
	target string
	temp   string
	size   uint64
	stream grpc.ClientStreamingClient[boxdpb.FsWriteChunk, boxdpb.FsWriteResponse]
	cancel context.CancelFunc
}

// Write sends p in chunks of at most 1 MiB, each Send waiting for boxd's flow control
func (u *upload) Write(p []byte) (int, error) {
	for sent := 0; sent < len(p); {
		n := min(len(p)-sent, writeChunkBytes)
		if err := u.stream.Send(&boxdpb.FsWriteChunk{Data: p[sent : sent+n]}); err != nil {
			return sent, u.sendFailed(err)
		}
		sent += n
	}
	return len(p), nil
}

// sendFailed reads the status behind a failed Send, which gRPC reports as io.EOF
func (u *upload) sendFailed(err error) error {
	if errors.Is(err, io.EOF) {
		_, err = u.stream.CloseAndRecv()
		if err == nil {
			err = errors.New("boxd ended the write before its end")
		}
	}
	return fsError(u.ctx, err, u.stream.Trailer())
}

// Finish ends the stream, then renames the temp file over the target
func (u *upload) Finish(ctx context.Context, _ []byte) (provider.FileEntry, error) {
	defer u.cancel()
	resp, err := u.stream.CloseAndRecv()
	if err != nil {
		return provider.FileEntry{}, fsError(ctx, err, u.stream.Trailer())
	}
	if resp.GetBytesWritten() != u.size {
		return provider.FileEntry{}, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"boxd wrote %d of %d bytes to %s", resp.GetBytesWritten(), u.size, u.temp)}
	}
	var trailer metadata.MD
	_, err = u.p.api.FsRename(ctx, &boxdpb.FsRenameRequest{
		VmId: u.vmID, OldPath: []byte(u.temp), NewPath: []byte(u.target),
	}, grpc.Trailer(&trailer))
	if err != nil {
		return provider.FileEntry{}, fsError(ctx, err, trailer)
	}
	if resp.GetEntry() == nil {
		return u.p.StatFile(ctx, provider.Ref{Provider: providerName, ID: u.vmID}, u.target)
	}
	entry := fileEntry(path.Dir(u.target), resp.GetEntry())
	entry.Path = u.target
	return entry, nil
}

// Discard ends the stream so a blocked Send returns, then removes the temp file
func (u *upload) Discard(ctx context.Context) error {
	u.cancel()
	var trailer metadata.MD
	_, err := u.p.api.FsRemove(ctx, &boxdpb.FsRemoveRequest{
		VmId: u.vmID, Path: []byte(u.temp),
	}, grpc.Trailer(&trailer))
	if err == nil {
		return nil
	}
	err = fsError(ctx, err, trailer)
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
		return nil
	}
	return err
}
