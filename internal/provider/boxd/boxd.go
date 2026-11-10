// Package boxd places sandboxes on boxd machines through boxd's public gRPC API.
//
// Every machine is created from a machine spec with the isolated flag, the request's labels and
// a delete-after timer equal to the lifetime. boxd has no create idempotency key, so the machine
// name derives from the IdempotencyKey and a label holds the key's digest. A create that meets
// the name already taken, or whose reply was lost, adopts the machine when the labels match.
// Label keys at boxd may not contain underscores, so each underscore travels as a dot and the
// adapter refuses router keys with dots of their own. Exec takes one shell command string, which
// package shell builds from argv with single-quote escaping, and closing either side of its
// stream kills the command. Before Create returns a machine, its default proxy is pinned to the
// gate port and boxd's own SSH is off, so no app inside it is public by accident. PrepareGate
// adds a bn subdomain proxy to the gate, or turns bot protection off on the default proxy, and
// keeps the route's host in the ref. File streams run on a second connection, write only to a
// hidden temp file beside the target and rename it over the target on Commit, since FsWrite
// overwrites some targets in place.
package boxd

import (
	"cmp"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd/boxdpb"
	"github.com/bugnet-sh/bugnet-router/internal/provider/shell"
)

const (
	providerName  = "boxd"
	defaultRegion = "fra"
	homeHost      = "boxd.sh"
	apiPort       = "9443"
	keyLabel      = "bugnet/key"
	// boxd names a machine's default proxy with the empty string, "default" is not found
	defaultProxy     = ""
	hibernateOffMeta = "hibernate_off"
	namePrefix       = "bn-"
	nameHashChars    = 20
	maxLabelValue    = 256
	writeChunkBytes  = 1 << 20
	maxRecvMsgBytes  = 16 << 20
	diskGiB          = 100
	defaultPoll      = 500 * time.Millisecond
	afterCreateLimit = 15 * time.Second
	// probeTimeout bounds one agent probe as E2B and Koyeb bound theirs, so a hung one is retried
	probeTimeout = 5 * time.Second
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
	rateWord       = regexp.MustCompile(`(?i)\brate(limit\w*)?\b`)
	// machineLimitWord marks the organization's own machine limit, which a poll corrects
	machineLimitWord = regexp.MustCompile(`(?i)machine limit`)
	// goneStatus is how boxd refuses a call on a machine it keeps listed after destroying it
	goneStatus = regexp.MustCompile(`status '(destroyed|destroying)'`)
)

// Options configures the adapter, an empty Endpoint follows Region and Insecure is only for tests
type Options struct {
	APIKey   string
	Endpoint string
	Insecure bool
	Region   string
	Log      *slog.Logger // slog.Default when nil
	// GatePort is where every machine's default proxy points, provider.DefaultGatePort when 0
	GatePort int
	// GateRoute is default for the pinned default proxy, or subdomain for a bn proxy to the gate
	GateRoute string
	// ParkedGiB is the disk written per hibernated machine, which sets its idle cost, 5 when 0
	ParkedGiB int
	// WakeCheck is the command Unpark runs until it exits 0, the runner's ping when empty
	WakeCheck []string
	// Snapshots maps each size to the snapshot its machines restore, none for boxd's own image
	Snapshots map[string]string
}

// Provider places sandboxes on boxd over one gRPC connection and streams files over a second
type Provider struct {
	conn         *grpc.ClientConn
	api          boxdpb.BoxdApiClient
	streamConn   *grpc.ClientConn
	streams      boxdpb.BoxdApiClient
	log          *slog.Logger
	pollInterval time.Duration
	afterCreate  time.Duration
	probeWithin  time.Duration
	gatePort     int
	gateRoute    string
	parkedGiB    int
	// pingArgv is the command Unpark runs until it exits 0
	pingArgv  []string
	snapshots map[string]string
	// imagePoll and imageWait pace SaveImage's wait for a snapshot to turn ready
	imagePoll, imageWait time.Duration
}

// New connects lazily to boxd, the first call dials the endpoint
func New(opts Options) (*Provider, error) {
	if opts.APIKey == "" {
		return nil, errors.New("boxd: an API key is required")
	}
	if opts.ParkedGiB < 0 {
		return nil, fmt.Errorf("boxd: parked GiB %d is negative", opts.ParkedGiB)
	}
	if opts.GatePort < 0 || opts.GatePort > 65535 {
		return nil, fmt.Errorf("boxd: gate port %d is not a TCP port", opts.GatePort)
	}
	route := cmp.Or(opts.GateRoute, routeDefault)
	if route != routeSubdomain && route != routeDefault {
		return nil, fmt.Errorf("boxd: gate route %q is not subdomain or default", route)
	}
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	if opts.Insecure {
		creds = insecure.NewCredentials()
	}
	dial := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(bearer{key: opts.APIKey, secure: !opts.Insecure}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMsgBytes)),
	}
	conn, err := grpc.NewClient(endpoint(opts), dial...)
	if err != nil {
		return nil, fmt.Errorf("boxd: %w", err)
	}
	// A second connection keeps a long file stream off the TCP connection Exec uses
	streamConn, err := grpc.NewClient(endpoint(opts), dial...)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("boxd: %w", err)
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	pingArgv := runnerPing
	if len(opts.WakeCheck) > 0 {
		pingArgv = slices.Clone(opts.WakeCheck)
	}
	return &Provider{
		conn: conn, api: boxdpb.NewBoxdApiClient(conn), streamConn: streamConn,
		streams: boxdpb.NewBoxdApiClient(streamConn), log: log, pollInterval: defaultPoll,
		afterCreate: afterCreateLimit, probeWithin: probeTimeout,
		gatePort:  cmp.Or(opts.GatePort, provider.DefaultGatePort),
		gateRoute: route, parkedGiB: cmp.Or(opts.ParkedGiB, defaultParkedGiB),
		pingArgv: pingArgv, snapshots: maps.Clone(opts.Snapshots),
		imagePoll: snapshotPoll, imageWait: snapshotWait,
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

// Close releases both connections
func (p *Provider) Close() error { return errors.Join(p.conn.Close(), p.streamConn.Close()) }

// Name returns "boxd"
func (p *Provider) Name() string { return providerName }

// Capabilities reports egress rules, the runner upload and Exec as boxd, and no lifetime cap
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{EgressControl: true, NeedsRunnerUpload: true, ExecUser: "boxd"}
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

// Sizes lists the machine sizes boxd sells
func (p *Provider) Sizes() []provider.Size {
	out := make([]provider.Size, 0, len(sizes))
	for _, s := range sizes {
		out = append(out, s.size)
	}
	return out
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
	in, labels, err := p.createRequest(req)
	if err != nil {
		return provider.Ref{}, err
	}
	if req.Lifetime > 0 {
		in.DeleteAfterSecs = proto.Uint64(wholeSeconds(req.Lifetime))
	}

	resp, err := p.api.CreateVm(ctx, in)
	if networkRefused(err) {
		// The spec may cover config.network, and then SetSsh turns SSH off instead
		in.Config.Network = nil
		resp, err = p.api.CreateVm(ctx, in)
	}
	// Lookups and the timer still run when ctx ended, since a machine may exist and bill
	after, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.afterCreate)
	defer cancel()
	var vm *boxdpb.GetVmResponse
	switch code := status.Code(err); {
	case err == nil:
		vm = resp.GetMachine()
		if vm == nil {
			vm = &boxdpb.GetVmResponse{VmId: resp.GetVmId(), Name: resp.GetName()}
		}
	case nameTaken(err) && in.Name != "":
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
		return provider.Ref{}, p.missingSnapshot(classify(ctx, err), snapshotOf(in))
	}
	if err := p.closeRoutes(after, vm); err != nil {
		p.discard(ctx, vm.GetVmId())
		return provider.Ref{}, err
	}
	if err := p.ensureTimer(after, vm, req.Lifetime); err != nil {
		// A machine without its timer would bill until a sweep finds it
		p.discard(ctx, vm.GetVmId())
		return provider.Ref{}, err
	}
	return p.stopHibernation(after, vm.GetVmId()), nil
}

// egressList is the machine's egress allowlist, the hosts and the proxy's IPv4, never empty
func egressList(e *provider.EgressAllow) []string {
	out := slices.Clone(e.Hosts)
	for _, ip := range e.Proxy {
		// boxd's list takes IPv4 addresses and ranges alone
		if ip.Is4() {
			out = append(out, ip.String())
		}
	}
	if len(out) == 0 {
		// An empty list means unrestricted to boxd, and none means nothing is reachable
		return []string{"none"}
	}
	return out
}

// networkRefused reports a create boxd refused for its network config
func networkRefused(err error) bool {
	st, _ := status.FromError(err)
	return st.Code() == codes.InvalidArgument && strings.Contains(st.Message(), "network")
}

// closeRoutes pins the default proxy to the gate port and turns boxd's own SSH off
func (p *Provider) closeRoutes(ctx context.Context, vm *boxdpb.GetVmResponse) error {
	id := vm.GetVmId()
	_, err := p.api.SetProxyPort(ctx, &boxdpb.SetProxyPortRequest{
		Vm: id, Name: defaultProxy, Port: strconv.Itoa(p.gatePort),
	})
	if err != nil {
		return exposed(ctx, "pin the default proxy of", id, err)
	}
	if vm.GetSshDisabled() {
		return nil
	}
	if _, err := p.api.SetSsh(ctx, &boxdpb.SetSshRequest{VmId: id, Enabled: false}); err != nil {
		return exposed(ctx, "turn SSH off on", id, err)
	}
	return nil
}

// stopHibernation turns auto hibernation off and records in the ref whether boxd accepted
func (p *Provider) stopHibernation(ctx context.Context, id string) provider.Ref {
	ref := newRef(id)
	ref.Meta[hibernateOffMeta] = "1"
	// An idle machine hibernates after four hours, freezing a long command without traffic
	_, err := p.api.SetAutoHibernateTimeout(ctx, &boxdpb.SetAutoHibernateTimeoutRequest{
		VmId: id, TimeoutSecs: 0,
	})
	if err != nil {
		// The delete-after timer still bounds the machine, so a refusal only warns
		p.log.Warn("boxd kept auto hibernate on", "machine", id, "err", err)
		ref.Meta[hibernateOffMeta] = "0"
	}
	return ref
}

// exposed is the error for a machine left reachable, which the saga places elsewhere
func exposed(ctx context.Context, what, id string, err error) error {
	return &provider.Error{
		Code: provider.CodeInternal, CanFailOver: true,
		Err: fmt.Errorf("boxd could not %s machine %s: %w", what, id, withCtx(ctx, err)),
	}
}

// discard destroys a machine Create gives up on, with its own deadline since ctx may be spent
func (p *Provider) discard(ctx context.Context, id string) {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.afterCreate)
	defer cancel()
	_, err := p.api.DestroyVm(dctx, &boxdpb.DestroyVmRequest{VmId: id})
	if err != nil && status.Code(err) != codes.NotFound {
		p.log.Warn("boxd could not destroy a machine Create gave up on", "machine", id, "err", err)
	}
}

// nameTaken reports a create refused for its name, which boxd answers with FailedPrecondition
func nameTaken(err error) bool {
	st, _ := status.FromError(err)
	return st.Code() == codes.AlreadyExists ||
		st.Code() == codes.FailedPrecondition && strings.Contains(st.Message(), "already taken")
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

// WaitReady waits for status running, then for an exec to succeed
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
	ctx, cancel := context.WithTimeout(ctx, p.probeWithin)
	defer cancel()
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

// statusOf maps a machine's status word, false for a status boxd added since
func statusOf(vm *boxdpb.GetVmResponse) (provider.Status, bool) {
	st := provider.Status{Reason: vm.GetStatus()}
	if ms := vm.GetDeleteAtMs(); ms > 0 {
		st.ExpiresAt = time.UnixMilli(ms).UTC()
	}
	switch vm.GetStatus() {
	case "pending", "starting":
		st.State = provider.StateStarting
	case "running", "migrating":
		st.State = provider.StateRunning
	case "suspended", "hibernated", "stopped":
		st.State = provider.StateStopped
	case "failed":
		st.State = provider.StateFailed
	default:
		return provider.Status{}, false
	}
	return st, true
}

// Inspect reads the machine, NOT_FOUND meaning boxd no longer has it
func (p *Provider) Inspect(ctx context.Context, ref provider.Ref) (provider.Status, error) {
	vmID, err := machineID(ref)
	if err != nil {
		return provider.Status{State: provider.StateGone}, nil
	}
	vm, err := p.api.GetVm(ctx, &boxdpb.GetVmRequest{VmId: vmID})
	if status.Code(err) == codes.NotFound {
		return provider.Status{State: provider.StateGone}, nil
	}
	if err != nil {
		return provider.Status{}, classify(ctx, err)
	}
	st, ok := statusOf(vm)
	if !ok {
		return provider.Status{}, &provider.Error{
			Code: provider.CodeInternal,
			Err:  fmt.Errorf("boxd machine %s has status %q", vmID, vm.GetStatus()),
		}
	}
	return st, nil
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
		if st, ok := statusOf(vm); ok {
			s.State, s.ExpiresAt = st.State, st.ExpiresAt
		}
		if vm.GetCreatedAt() > 0 {
			s.CreatedAt = time.Unix(vm.GetCreatedAt(), 0).UTC()
		}
		owned = append(owned, s)
	}
	return owned, nil
}

// ListLabelled lists every machine carrying the labels, boxd listing them in one answer
func (p *Provider) ListLabelled(
	ctx context.Context, labels map[string]string, _ string, _ int,
) (provider.ForeignPage, error) {
	owned, err := p.ListOwned(ctx, labels)
	return provider.ForeignPage{Sandboxes: owned}, err
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
	command, err := shell.Command(spec)
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

type execStream struct {
	ctx      context.Context
	stream   grpc.BidiStreamingClient[boxdpb.ExecChunk, boxdpb.ExecChunk]
	cancel   context.CancelFunc
	ending   sync.Once
	pending  []byte
	code     int
	failed   bool
	received bool
	exited   bool
	err      error
	done     bool
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
		if errors.Is(err, io.EOF) {
			s.exited = s.received
		} else {
			s.err = classify(s.ctx, err)
		}
		s.end()
		return
	}
	s.received = true
	// exit_code has no presence bit, so the first non-zero code wins and a clean close means 0
	if code := int(c.GetExitCode()); code != 0 && !s.failed {
		s.code, s.failed = code, true
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
	// The host may send more than asked, so reading stops at length and cancels the rest
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := p.api.FsRead(sctx, &boxdpb.FsReadRequest{
		VmId: vmID, Path: []byte(name), Offset: uint64(offset),
		Length: proto.Uint64(uint64(length)),
	})
	if err != nil {
		return nil, false, classify(ctx, err)
	}
	var data []byte
	size := int64(-1)
	for first := true; first || len(data) < length; first = false {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, false, fsError(ctx, err, stream.Trailer())
		}
		if e := c.GetEntry(); e != nil {
			size = int64(e.GetSize())
		}
		data = append(data, c.GetData()[:min(len(c.GetData()), length-len(data))]...)
	}
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
		return fsError(ctx, err, stream.Trailer())
	}
	if resp.GetBytesWritten() != uint64(len(data)) {
		return &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"boxd wrote %d of %d bytes to %s", resp.GetBytesWritten(), len(data), name)}
	}
	return nil
}

// ListFiles lists one directory per call and recurses into directories, never into symlinks
func (p *Provider) ListFiles(
	ctx context.Context, ref provider.Ref, name string, depth int, into *provider.Listing,
) error {
	vmID, err := machineID(ref)
	if err != nil {
		return err
	}
	if depth < 1 {
		return invalid(fmt.Errorf("depth %d is below 1", depth))
	}
	return p.list(ctx, vmID, name, depth, into)
}

func (p *Provider) list(
	ctx context.Context, vmID, dir string, depth int, into *provider.Listing,
) error {
	if !into.Visit() {
		return nil
	}
	// A full listing stops reading, and cancelling ends the server's stream
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := p.api.FsList(sctx, &boxdpb.FsListRequest{VmId: vmID, Path: []byte(dir)})
	if err != nil {
		return classify(ctx, err)
	}
	var subdirs []string
	for !into.Full() {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fsError(ctx, err, stream.Trailer())
		}
		for _, e := range c.GetEntries() {
			entry := fileEntry(dir, e)
			if !into.Add(entry) {
				break
			}
			if entry.IsDir && depth > 1 {
				subdirs = append(subdirs, entry.Path)
			}
		}
	}
	cancel()
	for _, sub := range subdirs {
		if into.Full() {
			return nil
		}
		err := p.list(ctx, vmID, sub, depth-1, into)
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
		Regular:  e.GetType() == boxdpb.FsEntryType_FS_ENTRY_TYPE_FILE,
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
		switch {
		case rateWord.MatchString(st.Message()):
			e.Code = provider.CodeRateLimited
		case machineLimitWord.MatchString(st.Message()):
			e.Code = provider.CodeQuota
		}
	case codes.Unauthenticated, codes.PermissionDenied:
		e.Code = provider.CodeAuth
	case codes.NotFound:
		e.Code, e.CanFailOver = provider.CodeNotFound, false
	case codes.FailedPrecondition:
		e.Code, e.CanFailOver = provider.CodeInvalid, false
		if goneStatus.MatchString(st.Message()) {
			e.Code = provider.CodeNotFound
		}
	case codes.InvalidArgument, codes.AlreadyExists, codes.OutOfRange:
		e.Code, e.CanFailOver = provider.CodeInvalid, false
	}
	return provider.Named(e, providerName)
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
