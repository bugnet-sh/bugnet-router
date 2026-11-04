// Package boxd places sandboxes on boxd machines through boxd's public gRPC API.
//
// Every machine is created from a machine spec with the isolated flag, the request's labels and
// a delete-after timer equal to the lifetime. boxd has no create idempotency key, so the machine
// name derives from the IdempotencyKey and a label holds the key's digest. A create that meets
// the name already taken, or whose reply was lost, adopts the machine when the labels match.
// Label keys at boxd may not contain underscores, so each underscore travels as a dot and the
// adapter refuses router keys with dots of their own. Exec takes one shell command string, built
// here from argv with single-quote escaping, and closing either side of its stream kills the
// command.
package boxd

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/boxd/boxdpb"
)

const (
	providerName     = "boxd"
	defaultRegion    = "fra"
	homeHost         = "boxd.sh"
	apiPort          = "9443"
	keyLabel         = "bugnet/key"
	namePrefix       = "bn-"
	nameHashChars    = 20
	maxLabelValue    = 256
	writeChunkBytes  = 1 << 20
	maxRecvMsgBytes  = 16 << 20
	diskGiB          = 100
	defaultPoll      = 500 * time.Millisecond
	afterCreateLimit = 15 * time.Second
	// usdPerEURCents is a conservative fixed 1.15 USD per EUR, set on 2026-11-04
	usdPerEURCents = 115
	// fallbackArch is assumed because boxd's API reports no architecture
	fallbackArch = "amd64"
)

type sizeClass struct {
	size       provider.Size
	eurPerHour int64
}

// Size classes and their maximum running prices in EUR micros per hour, boxd.sh/pricing 2026-11-04
var sizes = []sizeClass{
	{provider.Size{Name: "1x4", Resources: res(1, 4)}, 100_000},
	{provider.Size{Name: "2x8", Resources: res(2, 8)}, 180_000},
	{provider.Size{Name: "4x16", Resources: res(4, 16)}, 360_000},
	{provider.Size{Name: "8x32", Resources: res(8, 32)}, 700_000},
}

func res(vcpu, memGiB int) provider.Resources {
	return provider.Resources{VCPU: vcpu, MemoryMiB: memGiB << 10, DiskGiB: diskGiB}
}

var (
	routerLabelKey = regexp.MustCompile(`^[a-z0-9]([a-z0-9_-]{0,61}[a-z0-9])?$`)
	envKey         = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	rateWord       = regexp.MustCompile(`(?i)\brate(limit\w*)?\b`)
)

// Options configures the adapter, an empty Endpoint follows Region and Insecure is only for tests
type Options struct {
	APIKey   string
	Endpoint string
	Insecure bool
	Region   string
	Log      *slog.Logger // slog.Default when nil
}

// Provider places sandboxes on boxd over one long-lived gRPC connection
type Provider struct {
	conn         *grpc.ClientConn
	api          boxdpb.BoxdApiClient
	log          *slog.Logger
	pollInterval time.Duration
}

// New connects lazily to boxd, the first call dials the endpoint
func New(opts Options) (*Provider, error) {
	if opts.APIKey == "" {
		return nil, errors.New("boxd: an API key is required")
	}
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	if opts.Insecure {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(endpoint(opts),
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(bearer{key: opts.APIKey, secure: !opts.Insecure}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMsgBytes)),
	)
	if err != nil {
		return nil, fmt.Errorf("boxd: %w", err)
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &Provider{
		conn: conn, api: boxdpb.NewBoxdApiClient(conn), log: log, pollInterval: defaultPoll,
	}, nil
}

// endpoint serves Frankfurt from boxd's home host and any other region from its derived name
func endpoint(opts Options) string {
	if opts.Endpoint != "" {
		return opts.Endpoint
	}
	if opts.Region == "" || opts.Region == defaultRegion {
		return homeHost + ":" + apiPort
	}
	return opts.Region + "." + homeHost + ":" + apiPort
}

// bearer sends the API key itself, which boxd accepts over TLS without a token exchange
type bearer struct {
	key    string
	secure bool
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.key}, nil
}

func (b bearer) RequireTransportSecurity() bool { return b.secure }

// Close releases the connection
func (p *Provider) Close() error { return p.conn.Close() }

// Name returns "boxd"
func (p *Provider) Name() string { return providerName }

// Capabilities reports SSH, previews and egress rules, and no lifetime cap
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		SSH: true, Preview: true, EgressControl: true, NeedsRunnerUpload: true,
	}
}

// SizeFor picks the smallest size class covering floor
func (p *Provider) SizeFor(floor provider.Resources) (provider.Size, bool) {
	for _, s := range sizes {
		r := s.size.Resources
		if r.VCPU >= floor.VCPU && r.MemoryMiB >= floor.MemoryMiB && r.DiskGiB >= floor.DiskGiB &&
			floor.GPUCount == 0 && floor.GPUMemoryGiB == 0 {
			return s.size, true
		}
	}
	return provider.Size{}, false
}

// HourlyCostMicros prices a size at its maximum running rate, and an unknown size as the dearest
func (p *Provider) HourlyCostMicros(size provider.Size) int64 {
	eur := sizes[len(sizes)-1].eurPerHour
	for _, s := range sizes {
		if s.size.Name == size.Name {
			eur = s.eurPerHour
		}
	}
	return (eur*usdPerEURCents + 99) / 100
}

// BillingIncrement is one second, boxd meters by the second
func (p *Provider) BillingIncrement() time.Duration { return time.Second }

/* Machines */

// Create makes an isolated machine named after the idempotency key and adopts one it already made
func (p *Provider) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	if !slices.ContainsFunc(sizes, func(s sizeClass) bool { return s.size.Name == req.Size.Name }) {
		return provider.Ref{}, invalid(fmt.Errorf("boxd has no size %q", req.Size.Name))
	}
	labels, err := encodeLabels(req.Labels)
	if err != nil {
		return provider.Ref{}, err
	}
	in := &boxdpb.CreateVmRequest{
		Config: &boxdpb.VmConfig{Srf: &boxdpb.SrfConfig{AutoSuspendTimeoutSecs: proto.Uint32(0)}},
		Spec: &boxdpb.MachineSpec{
			Version: 1, Size: req.Size.Name, Labels: labels, Isolated: true,
		},
	}
	if req.IdempotencyKey != "" {
		in.Name = machineName(req.IdempotencyKey)
		labels[keyLabel] = keyDigest(req.IdempotencyKey)
	}
	if req.Lifetime > 0 {
		in.DeleteAfterSecs = proto.Uint64(wholeSeconds(req.Lifetime))
	}

	resp, err := p.api.CreateVm(ctx, in)
	// Lookups and the timer still run when ctx ended, since a machine may exist and bill
	after, cancel := context.WithTimeout(context.WithoutCancel(ctx), afterCreateLimit)
	defer cancel()
	var vm *boxdpb.GetVmResponse
	switch code := status.Code(err); {
	case err == nil:
		vm = resp.GetMachine()
		if vm == nil {
			vm = &boxdpb.GetVmResponse{VmId: resp.GetVmId(), Name: resp.GetName()}
		}
	case code == codes.AlreadyExists && in.Name != "":
		if vm, err = p.adopt(after, in.Name, labels); err != nil {
			return provider.Ref{}, err
		}
	case code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.Canceled:
		if in.Name != "" {
			vm, _ = p.adopt(after, in.Name, labels)
		}
		if vm == nil {
			return provider.Ref{}, &provider.Error{
				Code: provider.CodeOutcomeUnknown, Err: withCtx(ctx, err),
			}
		}
	default:
		return provider.Ref{}, classify(ctx, err)
	}
	if err := p.ensureTimer(after, vm, req.Lifetime); err != nil {
		// A machine without its timer would bill until a sweep finds it
		_, _ = p.api.DestroyVm(after, &boxdpb.DestroyVmRequest{VmId: vm.GetVmId()})
		return provider.Ref{}, err
	}
	return newRef(vm.GetVmId()), nil
}

// adopt returns the machine holding name when its labels carry everything the create asked for
func (p *Provider) adopt(
	ctx context.Context, name string, labels map[string]string,
) (*boxdpb.GetVmResponse, error) {
	vm, err := p.api.GetVm(ctx, &boxdpb.GetVmRequest{VmId: name})
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, classify(ctx, err)
	}
	if err != nil || !hasLabels(vm.GetLabels(), labels) {
		return nil, &provider.Error{
			Code: provider.CodeInternal, CanFailOver: true,
			Err: fmt.Errorf("boxd machine name %s is held by a machine of another request", name),
		}
	}
	return vm, nil
}

// ensureTimer sets the delete-after timer when the server did not record it at create
func (p *Provider) ensureTimer(
	ctx context.Context, vm *boxdpb.GetVmResponse, lifetime time.Duration,
) error {
	if lifetime <= 0 || vm.GetDeleteAtMs() != 0 {
		return nil
	}
	return p.setDeleteAfter(ctx, vm.GetVmId(), wholeSeconds(lifetime))
}

func (p *Provider) setDeleteAfter(ctx context.Context, id string, secs uint64) error {
	resp, err := p.api.SetDeleteAfter(ctx, &boxdpb.SetDeleteAfterRequest{
		VmId: id, DeleteAfterSecs: secs,
	})
	if err != nil {
		return classify(ctx, err)
	}
	if resp.GetDeleteAtMs() == 0 {
		return &provider.Error{
			Code: provider.CodeInternal,
			Err:  fmt.Errorf("boxd left machine %s without a timer", id),
		}
	}
	return nil
}

// machineName is unique per key and uses only lowercase letters, digits and hyphens
func machineName(key string) string {
	return namePrefix + keyDigest(key)[:nameHashChars]
}

func keyDigest(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// wholeSeconds rounds up and never returns 0, which boxd reads as no timer
func wholeSeconds(d time.Duration) uint64 {
	if d <= 0 {
		return 1
	}
	return uint64((d + time.Second - 1) / time.Second)
}

func newRef(id string) provider.Ref {
	meta := map[string]string{"arch": fallbackArch}
	return provider.Ref{Provider: providerName, ID: id, Meta: meta}
}

// machineID returns the machine id of a ref this provider made
func machineID(ref provider.Ref) (string, error) {
	if ref.Provider != providerName || ref.ID == "" {
		return "", &provider.Error{
			Code: provider.CodeNotFound,
			Err:  fmt.Errorf("sandbox %s/%s is not a boxd machine", ref.Provider, ref.ID),
		}
	}
	return ref.ID, nil
}

// WaitReady waits for status running, then for an exec to succeed, and tries to stop hibernation
func (p *Provider) WaitReady(ctx context.Context, ref provider.Ref) error {
	vmID, err := machineID(ref)
	if err != nil {
		return err
	}
	for {
		vm, err := p.api.GetVm(ctx, &boxdpb.GetVmRequest{VmId: vmID})
		if err != nil {
			return classify(ctx, err)
		}
		if s := vm.GetStatus(); s == "running" {
			break
		} else if s == "failed" || s == "stopped" || s == "destroyed" {
			return &provider.Error{
				Code: provider.CodeInternal, CanFailOver: true,
				Err: fmt.Errorf("boxd machine %s is %s", vmID, s),
			}
		}
		if err := p.pause(ctx); err != nil {
			return fmt.Errorf("boxd machine %s not running: %w", vmID, err)
		}
	}
	for {
		err := p.probe(ctx, ref)
		if err == nil {
			break
		}
		if e, ok := provider.AsError(err); ok && (e.Code == provider.CodeNotFound ||
			e.Code == provider.CodeAuth || e.Code == provider.CodeInvalid) {
			return err
		}
		if perr := p.pause(ctx); perr != nil {
			return fmt.Errorf("boxd machine %s agent not up: %w, last %w", vmID, perr, err)
		}
	}
	// An idle machine hibernates after four hours, freezing a long command without traffic
	_, err = p.api.SetAutoHibernateTimeout(ctx, &boxdpb.SetAutoHibernateTimeoutRequest{
		VmId: vmID, TimeoutSecs: 0,
	})
	if err != nil {
		// The machine runs and its delete-after timer still bounds it, so a refusal only warns
		p.log.Warn("boxd kept auto hibernate on", "machine", vmID, "err", err)
	}
	return nil
}

func (p *Provider) pause(ctx context.Context) error {
	t := time.NewTimer(p.pollInterval)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// probe runs true in the machine, which succeeds once its agent takes commands
func (p *Provider) probe(ctx context.Context, ref provider.Ref) error {
	stream, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"true"}})
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()
	if _, err := io.Copy(io.Discard, stream); err != nil {
		return err
	}
	_, err = stream.Wait()
	return err
}

// SetLifetime replaces the machine's delete-after timer so it fires at until
func (p *Provider) SetLifetime(ctx context.Context, ref provider.Ref, until time.Time) error {
	vmID, err := machineID(ref)
	if err != nil {
		return err
	}
	return p.setDeleteAfter(ctx, vmID, wholeSeconds(time.Until(until)))
}

// Destroy deletes the machine and treats one already gone as destroyed
func (p *Provider) Destroy(ctx context.Context, ref provider.Ref) error {
	vmID, err := machineID(ref)
	if err != nil {
		return nil
	}
	_, err = p.api.DestroyVm(ctx, &boxdpb.DestroyVmRequest{VmId: vmID})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return classify(ctx, err)
}

// ListOwned asks boxd for the machines carrying every label and hides the adapter's own labels
func (p *Provider) ListOwned(
	ctx context.Context, labels map[string]string,
) ([]provider.Summary, error) {
	want, err := encodeLabels(labels)
	if err != nil {
		return nil, err
	}
	selector := make([]string, 0, len(want))
	for k, v := range want {
		selector = append(selector, k+"="+v)
	}
	slices.Sort(selector)
	resp, err := p.api.ListVms(ctx, &boxdpb.ListVmsRequest{LabelSelector: selector})
	if err != nil {
		return nil, classify(ctx, err)
	}
	var owned []provider.Summary
	for _, vm := range resp.GetVms() {
		if !hasLabels(vm.GetLabels(), want) {
			continue
		}
		s := provider.Summary{Ref: newRef(vm.GetVmId()), Labels: decodeLabels(vm.GetLabels())}
		if vm.GetCreatedAt() > 0 {
			s.CreatedAt = time.Unix(vm.GetCreatedAt(), 0).UTC()
		}
		owned = append(owned, s)
	}
	return owned, nil
}

/* Labels */

// encodeLabels maps router labels to boxd's key rules, where a dot stands for an underscore
func encodeLabels(labels map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		if !routerLabelKey.MatchString(k) {
			return nil, invalid(fmt.Errorf("label key %q does not fit boxd's label rules", k))
		}
		if len(v) > maxLabelValue || strings.ContainsFunc(v, unicode.IsControl) {
			return nil, invalid(fmt.Errorf("label %s has a value boxd refuses", k))
		}
		out[strings.ReplaceAll(k, "_", ".")] = v
	}
	return out, nil
}

// decodeLabels reverses encodeLabels and drops the adapter's own keys, which hold a slash
func decodeLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		if !strings.Contains(k, "/") {
			out[strings.ReplaceAll(k, ".", "_")] = v
		}
	}
	return out
}

func hasLabels(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}
	return true
}

/* Exec */

// Exec runs argv through the machine's shell with stdin from /dev/null, Close kills the command
func (p *Provider) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	vmID, err := machineID(ref)
	if err != nil {
		return nil, err
	}
	command, err := commandLine(spec)
	if err != nil {
		return nil, err
	}
	sctx, cancel := context.WithCancel(ctx)
	stream, err := p.api.Exec(sctx)
	if err != nil {
		cancel()
		return nil, classify(ctx, err)
	}
	// The send side stays open while the command runs, since boxd kills a command on half-close
	err = stream.Send(&boxdpb.ExecChunk{VmId: vmID, Command: command})
	// A failed send reports io.EOF and leaves the status for Recv, so Read surfaces it
	if err != nil && !errors.Is(err, io.EOF) {
		cancel()
		return nil, classify(ctx, err)
	}
	return &execStream{ctx: ctx, stream: stream, cancel: cancel}, nil
}

// commandLine builds the one shell command boxd's Exec takes, quoting every word
func commandLine(spec provider.ExecSpec) (string, error) {
	if len(spec.Argv) == 0 {
		return "", invalid(errors.New("exec needs a command"))
	}
	var b strings.Builder
	if spec.Cwd != "" {
		b.WriteString("cd " + quote(spec.Cwd) + " && ")
	}
	if len(spec.Env) > 0 {
		// env would read an argv0 holding = as one more variable and one starting with - as a flag
		if strings.Contains(spec.Argv[0], "=") || strings.HasPrefix(spec.Argv[0], "-") {
			return "", invalid(fmt.Errorf("command %q cannot follow env", spec.Argv[0]))
		}
		keys := make([]string, 0, len(spec.Env))
		for k := range spec.Env {
			if !envKey.MatchString(k) {
				return "", invalid(fmt.Errorf("environment variable name %q", k))
			}
			keys = append(keys, k)
		}
		slices.Sort(keys)
		b.WriteString("env")
		for _, k := range keys {
			b.WriteString(" " + quote(k+"="+spec.Env[k]))
		}
		b.WriteString(" ")
	}
	for i, a := range spec.Argv {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(quote(a))
	}
	b.WriteString(" </dev/null")
	line := b.String()
	if strings.ContainsRune(line, 0) || !utf8.ValidString(line) {
		return "", invalid(errors.New("boxd takes commands as UTF-8 text without NUL bytes"))
	}
	return line, nil
}

// quote wraps s in single quotes, closing them around each single quote it holds
func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

type execStream struct {
	ctx     context.Context
	stream  grpc.BidiStreamingClient[boxdpb.ExecChunk, boxdpb.ExecChunk]
	cancel  context.CancelFunc
	ending  sync.Once
	pending []byte
	code    int
	exited  bool
	err     error
	done    bool
}

func (s *execStream) Read(p []byte) (int, error) {
	for len(s.pending) == 0 {
		if s.done {
			if s.err != nil {
				return 0, s.err
			}
			return 0, io.EOF
		}
		s.next()
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// next takes one chunk, the first one with no data or a non-zero code carries the exit code
func (s *execStream) next() {
	c, err := s.stream.Recv()
	if err != nil {
		s.done = true
		if !errors.Is(err, io.EOF) {
			s.err = classify(s.ctx, err)
		}
		s.end()
		return
	}
	if !s.exited && (len(c.GetData()) == 0 || c.GetExitCode() != 0) {
		s.code, s.exited = int(c.GetExitCode()), true
	}
	if !c.GetIsStderr() {
		s.pending = c.GetData()
	}
}

func (s *execStream) Wait() (int, error) {
	for !s.done {
		s.next()
	}
	s.pending = nil
	if s.err != nil {
		return -1, s.err
	}
	if !s.exited {
		return -1, &provider.Error{
			Code: provider.CodeOutcomeUnknown,
			Err:  errors.New("boxd ended the exec stream without an exit code"),
		}
	}
	return s.code, nil
}

func (s *execStream) Close() error {
	s.end()
	return nil
}

// end half-closes and then cancels the stream, once the command finished or the caller is done
func (s *execStream) end() {
	s.ending.Do(func() {
		_ = s.stream.CloseSend()
		s.cancel()
	})
}

/* Files */

// ReadFile reports eof once the bytes read reach the size the first chunk announces
func (p *Provider) ReadFile(
	ctx context.Context, ref provider.Ref, name string, offset int64, length int,
) ([]byte, bool, error) {
	vmID, err := machineID(ref)
	if err != nil {
		return nil, false, err
	}
	if offset < 0 || length < 0 {
		return nil, false, invalid(fmt.Errorf("read of %d bytes at offset %d", length, offset))
	}
	stream, err := p.api.FsRead(ctx, &boxdpb.FsReadRequest{
		VmId: vmID, Path: []byte(name), Offset: uint64(offset),
		Length: proto.Uint64(uint64(length)),
	})
	if err != nil {
		return nil, false, classify(ctx, err)
	}
	var data []byte
	size := int64(-1)
	for {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, false, classify(ctx, err)
		}
		if e := c.GetEntry(); e != nil {
			size = int64(e.GetSize())
		}
		data = append(data, c.GetData()...)
	}
	data = data[:min(len(data), length)]
	if size < 0 {
		return data, len(data) < length, nil
	}
	return data, offset+int64(len(data)) >= size, nil
}

// WriteFile streams data in 1 MiB chunks, boxd creates missing parents and swaps the file in
func (p *Provider) WriteFile(
	ctx context.Context, ref provider.Ref, name string, data []byte, mode fs.FileMode,
) error {
	vmID, err := machineID(ref)
	if err != nil {
		return err
	}
	stream, err := p.api.FsWrite(ctx)
	if err != nil {
		return classify(ctx, err)
	}
	first := &boxdpb.FsWriteChunk{
		VmId: vmID, Path: []byte(name), Mode: proto.Uint32(uint32(mode.Perm())),
		ExpectedSize: proto.Uint64(uint64(len(data))),
	}
	chunk, sent := first, 0
	for {
		n := min(len(data)-sent, writeChunkBytes)
		chunk.Data = data[sent : sent+n]
		sent += n
		// A failed send means the server ended the stream, and CloseAndRecv carries its status
		if err := stream.Send(chunk); err != nil || sent == len(data) {
			break
		}
		chunk = &boxdpb.FsWriteChunk{}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return classify(ctx, err)
	}
	if resp.GetBytesWritten() != uint64(len(data)) {
		return &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"boxd wrote %d of %d bytes to %s", resp.GetBytesWritten(), len(data), name)}
	}
	return nil
}

// ListFiles lists one directory per call and recurses into directories, never into symlinks
func (p *Provider) ListFiles(
	ctx context.Context, ref provider.Ref, name string, depth int,
) ([]provider.FileEntry, error) {
	vmID, err := machineID(ref)
	if err != nil {
		return nil, err
	}
	if depth < 1 {
		return nil, invalid(fmt.Errorf("depth %d is below 1", depth))
	}
	var out []provider.FileEntry
	if err := p.list(ctx, vmID, name, depth, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (p *Provider) list(
	ctx context.Context, vmID, dir string, depth int, out *[]provider.FileEntry,
) error {
	stream, err := p.api.FsList(ctx, &boxdpb.FsListRequest{VmId: vmID, Path: []byte(dir)})
	if err != nil {
		return classify(ctx, err)
	}
	var subdirs []string
	for {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return classify(ctx, err)
		}
		for _, e := range c.GetEntries() {
			entry := fileEntry(dir, e)
			*out = append(*out, entry)
			if entry.IsDir && depth > 1 {
				subdirs = append(subdirs, entry.Path)
			}
		}
	}
	for _, sub := range subdirs {
		err := p.list(ctx, vmID, sub, depth-1, out)
		// Commands in the sandbox may delete a directory while the listing runs
		if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func fileEntry(dir string, e *boxdpb.FsEntry) provider.FileEntry {
	mode := fs.FileMode(e.GetMode()).Perm()
	switch e.GetType() {
	case boxdpb.FsEntryType_FS_ENTRY_TYPE_DIRECTORY:
		mode |= fs.ModeDir
	case boxdpb.FsEntryType_FS_ENTRY_TYPE_SYMLINK:
		mode |= fs.ModeSymlink
	case boxdpb.FsEntryType_FS_ENTRY_TYPE_OTHER:
		mode |= fs.ModeIrregular
	}
	return provider.FileEntry{
		Path:     path.Join(dir, string(e.GetName())),
		Size:     int64(e.GetSize()),
		IsDir:    mode.IsDir(),
		Mode:     mode,
		Modified: time.Unix(0, e.GetMtimeNs()).UTC(),
	}
}

/* Errors */

func invalid(err error) error {
	return &provider.Error{Code: provider.CodeInvalid, Err: err}
}

// classify maps a gRPC status to a provider code and keeps the status message in Err
func classify(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := provider.AsError(err); ok {
		return err
	}
	st := status.Convert(err)
	e := &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: withCtx(ctx, err)}
	switch st.Code() {
	case codes.ResourceExhausted:
		e.Code = provider.CodeCapacity
		if rateWord.MatchString(st.Message()) {
			e.Code = provider.CodeRateLimited
		}
	case codes.Unauthenticated, codes.PermissionDenied:
		e.Code = provider.CodeAuth
	case codes.NotFound:
		e.Code, e.CanFailOver = provider.CodeNotFound, false
	case codes.InvalidArgument, codes.FailedPrecondition, codes.AlreadyExists, codes.OutOfRange:
		e.Code, e.CanFailOver = provider.CodeInvalid, false
	}
	return e
}

// withCtx lets callers match the context error behind a cancelled or expired call
func withCtx(ctx context.Context, err error) error {
	code := status.Code(err)
	ended := code == codes.Canceled || code == codes.DeadlineExceeded
	if cerr := ctx.Err(); cerr != nil && ended {
		return fmt.Errorf("%w: %w", cerr, err)
	}
	return err
}
