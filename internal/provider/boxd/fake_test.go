package boxd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd/boxdpb"
)

const (
	fakeKey            = "bxd_fake0000000000000000000000000000000000000000"
	fakeHibernateSecs  = 4 * 60 * 60
	fakeReadChunkBytes = 64 << 10
	fakeListBatch      = 2
	fakeKillGrace      = 100 * time.Millisecond
	fakeStallLimit     = 5 * time.Second
	fakeAccessDomain   = "boxd.test"
)

var (
	fakeLabelKey = regexp.MustCompile(`^[a-z0-9]([a-z0-9._/-]{0,61}[a-z0-9])?$`)
	fakeSizes    = map[string]bool{"1x4": true, "2x8": true, "4x16": true, "8x32": true}
)

// fault fails calls of one method, after applying them when after is set
type fault struct {
	err   error
	after bool
	times int
	// stall holds the call until its deadline, or fakeStallLimit without one, and then fails it
	stall bool
}

type fakeVM struct {
	id, name       string
	labels         map[string]string
	created        time.Time
	deleteAt       time.Time
	autoSuspend    *uint32
	autoHibernate  uint32
	status         string
	startingPolls  int
	agentDownExecs int
	// proxyPort is the default proxy's port, auto when boxd detects it from the guest
	proxyPort string
	sshOff    bool
	// proxies are the named proxies and their ports, beside the default one
	proxies      map[string]uint32
	accessDomain string
	botMode      string
}

// setProxyPort is one SetProxyPort call as the fake received it
type setProxyPort struct{ vm, name, port string }

// setSSH is one SetSsh call as the fake received it
type setSSH struct {
	vm      string
	enabled bool
}

// fakeBoxd serves boxd's API from memory and runs Exec commands on the host, for trusted tests
type fakeBoxd struct {
	boxdpb.UnimplementedBoxdApiServer
	t    *testing.T
	addr string

	// omitMachine answers CreateVm like a server that predates the machine and timer fields
	omitMachine bool
	// ignoreLength sends the rest of a file on FsRead whatever length was asked
	ignoreLength bool
	// refuseNetworkConfig refuses a CreateVm whose config sets network beside a spec
	refuseNetworkConfig bool
	// onCreated runs inside CreateVm once the machine exists, before the reply
	onCreated func()
	// noSpace refuses every FsWrite with boxd's no_space reason
	noSpace atomic.Bool
	// noSpaceAfter ends every FsWrite with boxd's no_space reason once it wrote this many bytes
	noSpaceAfter atomic.Int64
	// badBytesRead counts one byte too many on FsRead's eof chunk
	badBytesRead atomic.Bool
	// shortRead ends FsRead halfway with an honest eof chunk, as a file cut while it is read
	shortRead atomic.Bool
	// noEOFChunk ends FsRead after its data without the chunk that counts the bytes
	noEOFChunk atomic.Bool

	// billing is what Whoami reports, orgBilling what GetOrgBilling reports, under mu
	billing    *boxdpb.BillingInfo
	orgBilling *boxdpb.GetOrgBillingResponse
	// orgAsked is the org name of the last GetOrgBilling call
	orgAsked string
	// orgUsage is what GetOrgUsage reports and usageAsked the last window it was asked for
	orgUsage   *boxdpb.GetOrgUsageResponse
	usageAsked *boxdpb.GetOrgUsageRequest

	mu           sync.Mutex
	vms          map[string]*fakeVM
	nextID       int
	faults       map[string]*fault
	calls        map[string]int
	order        []string
	pins         []setProxyPort
	sshSets      []setSSH
	botSets      []string
	scripted     bool
	script       []*boxdpb.ExecChunk
	creates      []*boxdpb.CreateVmRequest
	commands     []string
	writeChunks  []int
	writePaths   []string
	writeHeads   []*boxdpb.FsWriteChunk
	renameCalls  [][2]string
	lengths      []uint64
	peers        map[string][]string
	cleanupPaths []string
	running      atomic.Int64
	// readHook runs with the path at the start of every FsRead, before boxd looks at it
	readHook func(name string)
}

func newFake(t *testing.T) *fakeBoxd {
	t.Helper()
	f := &fakeBoxd{
		t: t, vms: map[string]*fakeVM{}, faults: map[string]*fault{}, calls: map[string]int{},
		peers: map[string][]string{},
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.addr = lis.Addr().String()
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(f.unaryGate),
		grpc.ChainStreamInterceptor(f.streamGate),
	)
	boxdpb.RegisterBoxdApiServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, p := range f.cleanupPaths {
			_ = os.RemoveAll(p)
		}
	})
	return f
}

// provider builds an adapter talking to the fake over plaintext loopback
func (f *fakeBoxd) provider(t *testing.T, key string) *Provider {
	t.Helper()
	p, err := New(Options{
		APIKey: key, Endpoint: f.addr, Insecure: true, WakeCheck: []string{"true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.pollInterval = 20 * time.Millisecond
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func (f *fakeBoxd) inject(method string, ft fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[method] = &ft
}

func (f *fakeBoxd) take(ctx context.Context, fullMethod string) *fault {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := path.Base(fullMethod)
	if pr, ok := peer.FromContext(ctx); ok {
		f.peers[name] = append(f.peers[name], pr.Addr.String())
	}
	f.calls[name]++
	f.order = append(f.order, name)
	ft := f.faults[name]
	if ft == nil {
		return nil
	}
	if ft.times > 0 {
		ft.times--
		if ft.times == 0 {
			delete(f.faults, name)
		}
	}
	return ft
}

// peersOf lists the client address of every call of method, one per call
func (f *fakeBoxd) peersOf(method string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.peers[method])
}

func (f *fakeBoxd) callCount(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[method]
}

// callOrder lists every call the fake received, oldest first
func (f *fakeBoxd) callOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.order)
}

// replyExec answers every later Exec with these chunks and a clean end instead of a command
func (f *fakeBoxd) replyExec(chunks ...*boxdpb.ExecChunk) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scripted, f.script = true, chunks
}

func authorized(ctx context.Context) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	got := md.Get("authorization")
	return len(got) == 1 && got[0] == "Bearer "+fakeKey
}

func (f *fakeBoxd) unaryGate(
	ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	if !authorized(ctx) {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	ft := f.take(ctx, info.FullMethod)
	if ft != nil && ft.stall {
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(fakeStallLimit):
			return nil, status.Error(codes.DeadlineExceeded, "fake boxd stalled past its limit")
		}
	}
	if ft != nil && !ft.after {
		return nil, ft.err
	}
	resp, err := handler(ctx, req)
	if ft != nil {
		return nil, ft.err
	}
	return resp, err
}

func (f *fakeBoxd) streamGate(
	srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	if !authorized(ss.Context()) {
		return status.Error(codes.Unauthenticated, "invalid token")
	}
	if ft := f.take(ss.Context(), info.FullMethod); ft != nil {
		return ft.err
	}
	return handler(srv, ss)
}

/* Machines */

func (f *fakeBoxd) lookup(idOrName string) (*fakeVM, error) {
	for _, vm := range f.vms {
		if vm.id == idOrName || vm.name == idOrName {
			return vm, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "machine %q not found", idOrName)
}

func (f *fakeBoxd) vm(idOrName string) (*fakeVM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lookup(idOrName)
}

// add places a machine directly, as another client of the same account would
func (f *fakeBoxd) add(name string, labels map[string]string) *fakeVM {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	vm := &fakeVM{
		id: fmt.Sprintf("vm-%04d", f.nextID), name: name, labels: labels,
		created: time.Now(), autoHibernate: fakeHibernateSecs, status: "running",
		proxyPort: "auto", accessDomain: fakeAccessDomain, botMode: "on",
	}
	f.vms[vm.id] = vm
	return vm
}

func (f *fakeBoxd) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.vms)
}

func toMachine(vm *fakeVM) *boxdpb.GetVmResponse {
	m := &boxdpb.GetVmResponse{
		VmId: vm.id, Name: vm.name, Status: vm.status, Labels: vm.labels,
		CreatedAt: vm.created.Unix(), AutoHibernateTimeoutSecs: vm.autoHibernate,
		Isolated: true, SshDisabled: vm.sshOff, AccessDomain: vm.accessDomain,
		BotProtection: vm.botMode != "off", BotProtectionMode: vm.botMode,
	}
	if !vm.deleteAt.IsZero() {
		m.DeleteAtMs = vm.deleteAt.UnixMilli()
	}
	return m
}

func (f *fakeBoxd) CreateVm(
	_ context.Context, req *boxdpb.CreateVmRequest,
) (*boxdpb.CreateVmResponse, error) {
	spec := req.GetSpec()
	switch {
	case spec == nil:
		return nil, status.Error(codes.InvalidArgument, "fake boxd takes only spec creates")
	case req.GetImageRef() != "" || len(req.GetLabels()) > 0 || req.GetIsolated() ||
		req.GetConfig().GetVcpu() != 0 || req.GetConfig().GetMemoryBytes() != 0:
		return nil, status.Error(codes.InvalidArgument, "spec: a field the spec covers is set")
	case f.refuseNetworkConfig && req.GetConfig().GetNetwork() != nil:
		return nil, status.Error(codes.InvalidArgument,
			"spec: config.network is a field the spec covers")
	case spec.GetVersion() != 1:
		return nil, status.Error(codes.InvalidArgument, "spec.version: must be 1")
	case !fakeSizes[spec.GetSize()]:
		return nil, status.Errorf(codes.InvalidArgument, "spec.size: %q is not a size",
			spec.GetSize())
	}
	for k, v := range spec.GetLabels() {
		if !fakeLabelKey.MatchString(k) || strings.HasPrefix(k, "boxd/") || len(v) > 256 {
			return nil, status.Errorf(codes.InvalidArgument, "spec.labels: bad label %q", k)
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, req)
	if req.GetName() != "" {
		if _, err := f.lookup(req.GetName()); err == nil {
			return nil, status.Errorf(codes.FailedPrecondition, "name '%s' is already taken",
				req.GetName())
		}
	}
	f.nextID++
	vm := &fakeVM{
		id:             fmt.Sprintf("vm-%04d", f.nextID),
		name:           req.GetName(),
		labels:         spec.GetLabels(),
		created:        time.Now(),
		autoHibernate:  fakeHibernateSecs,
		status:         "starting",
		startingPolls:  1,
		agentDownExecs: 1,
		proxyPort:      "auto",
		accessDomain:   fakeAccessDomain,
		botMode:        "on",
	}
	if n := req.GetConfig().GetNetwork(); n != nil && n.Ssh != nil {
		vm.sshOff = !n.GetSsh()
	}
	if vm.name == "" {
		vm.name = "auto-" + vm.id
	}
	if s := req.GetConfig().GetSrf(); s != nil && s.AutoSuspendTimeoutSecs != nil {
		v := s.GetAutoSuspendTimeoutSecs()
		vm.autoSuspend = &v
	}
	if !f.omitMachine && req.DeleteAfterSecs != nil && req.GetDeleteAfterSecs() > 0 {
		vm.deleteAt = vm.created.Add(time.Duration(req.GetDeleteAfterSecs()) * time.Second)
	}
	f.vms[vm.id] = vm
	resp := &boxdpb.CreateVmResponse{VmId: vm.id, Name: vm.name, Status: vm.status}
	if !f.omitMachine {
		resp.Machine = toMachine(vm)
	}
	if f.onCreated != nil {
		f.onCreated()
	}
	return resp, nil
}

func (f *fakeBoxd) GetVm(
	_ context.Context, req *boxdpb.GetVmRequest,
) (*boxdpb.GetVmResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	vm, err := f.lookup(req.GetVmId())
	if err != nil {
		return nil, err
	}
	m := toMachine(vm)
	if vm.status == "starting" {
		if vm.startingPolls--; vm.startingPolls <= 0 {
			vm.status = "running"
		}
	}
	return m, nil
}

func (f *fakeBoxd) DestroyVm(
	_ context.Context, req *boxdpb.DestroyVmRequest,
) (*boxdpb.DestroyVmResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	vm, err := f.lookup(req.GetVmId())
	if err != nil {
		return nil, err
	}
	delete(f.vms, vm.id)
	return &boxdpb.DestroyVmResponse{}, nil
}

func (f *fakeBoxd) ListVms(
	_ context.Context, req *boxdpb.ListVmsRequest,
) (*boxdpb.ListVmsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &boxdpb.ListVmsResponse{}
	for _, vm := range f.vms {
		if selects(vm.labels, req.GetLabelSelector()) {
			resp.Vms = append(resp.Vms, toMachine(vm))
		}
	}
	return resp, nil
}

func selects(labels map[string]string, selector []string) bool {
	for _, s := range selector {
		k, v, hasValue := strings.Cut(s, "=")
		got, ok := labels[k]
		if !ok || (hasValue && got != v) {
			return false
		}
	}
	return true
}

func (f *fakeBoxd) SetDeleteAfter(
	_ context.Context, req *boxdpb.SetDeleteAfterRequest,
) (*boxdpb.SetDeleteAfterResponse, error) {
	if req.GetDeleteAfterCron() != "" {
		return nil, status.Error(codes.InvalidArgument, "fake boxd takes no cron timers")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	vm, err := f.lookup(req.GetVmId())
	if err != nil {
		return nil, err
	}
	if req.GetDeleteAfterSecs() == 0 {
		vm.deleteAt = time.Time{}
		return &boxdpb.SetDeleteAfterResponse{}, nil
	}
	vm.deleteAt = time.Now().Add(time.Duration(req.GetDeleteAfterSecs()) * time.Second)
	return &boxdpb.SetDeleteAfterResponse{DeleteAtMs: vm.deleteAt.UnixMilli()}, nil
}

func (f *fakeBoxd) HibernateVm(
	_ context.Context, req *boxdpb.HibernateVmRequest,
) (*boxdpb.HibernateVmResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	vm, err := f.lookup(req.GetVmId())
	if err != nil {
		return nil, err
	}
	if vm.status == "hibernated" {
		return nil, status.Error(codes.FailedPrecondition, "machine is already hibernated")
	}
	vm.status = "hibernated"
	return &boxdpb.HibernateVmResponse{}, nil
}

func (f *fakeBoxd) WakeVm(
	_ context.Context, req *boxdpb.WakeVmRequest,
) (*boxdpb.WakeVmResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	vm, err := f.lookup(req.GetVmId())
	if err != nil {
		return nil, err
	}
	if vm.status != "hibernated" {
		return nil, status.Error(codes.FailedPrecondition, "machine is already running")
	}
	vm.status = "running"
	return &boxdpb.WakeVmResponse{}, nil
}

func (f *fakeBoxd) SetAutoHibernateTimeout(
	_ context.Context, req *boxdpb.SetAutoHibernateTimeoutRequest,
) (*boxdpb.SetAutoHibernateTimeoutResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	vm, err := f.lookup(req.GetVmId())
	if err != nil {
		return nil, err
	}
	vm.autoHibernate = req.GetTimeoutSecs()
	return &boxdpb.SetAutoHibernateTimeoutResponse{}, nil
}

func (f *fakeBoxd) SetProxyPort(
	_ context.Context, req *boxdpb.SetProxyPortRequest,
) (*boxdpb.SetProxyPortResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pins = append(f.pins, setProxyPort{vm: req.GetVm(), name: req.GetName(), port: req.GetPort()})
	vm, err := f.lookup(req.GetVm())
	if err != nil {
		return nil, err
	}
	if _, ok := vm.proxies[req.GetName()]; req.GetName() != "" && !ok {
		return nil, status.Errorf(codes.NotFound, "proxy %q not found", req.GetName())
	}
	n, err := strconv.Atoi(req.GetPort())
	if req.GetPort() != "auto" && (err != nil || n < 1 || n > 65535) {
		return nil, status.Errorf(codes.InvalidArgument, "port: %q is not a port", req.GetPort())
	}
	if req.GetName() != "" {
		if req.GetPort() == "auto" {
			return nil, status.Error(codes.InvalidArgument, "auto is for the default proxy only")
		}
		vm.proxies[req.GetName()] = uint32(n)
		return &boxdpb.SetProxyPortResponse{}, nil
	}
	vm.proxyPort = req.GetPort()
	return &boxdpb.SetProxyPortResponse{}, nil
}

func (f *fakeBoxd) SetSsh(
	_ context.Context, req *boxdpb.SetSshRequest,
) (*boxdpb.SetSshResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sshSets = append(f.sshSets, setSSH{vm: req.GetVmId(), enabled: req.GetEnabled()})
	vm, err := f.lookup(req.GetVmId())
	if err != nil {
		return nil, err
	}
	vm.sshOff = !req.GetEnabled()
	return &boxdpb.SetSshResponse{}, nil
}

// CreateProxy adds a named proxy, refusing a name the machine already has
func (f *fakeBoxd) CreateProxy(
	_ context.Context, req *boxdpb.CreateProxyRequest,
) (*boxdpb.CreateProxyResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	vm, err := f.lookup(req.GetVm())
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" || req.GetPort() < 1 || req.GetPort() > 65535 {
		return nil, status.Error(codes.InvalidArgument, "a proxy needs a name and a port")
	}
	if _, ok := vm.proxies[req.GetName()]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "proxy '%s' already exists",
			req.GetName())
	}
	if vm.proxies == nil {
		vm.proxies = map[string]uint32{}
	}
	vm.proxies[req.GetName()] = req.GetPort()
	return &boxdpb.CreateProxyResponse{
		Name: req.GetName(), VmName: vm.name, Port: req.GetPort(),
		Domain: req.GetName() + "." + vm.name + "." + vm.accessDomain,
	}, nil
}

// SetBotProtection takes the mode, refusing one boxd does not know
func (f *fakeBoxd) SetBotProtection(
	_ context.Context, req *boxdpb.SetBotProtectionRequest,
) (*boxdpb.SetBotProtectionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	vm, err := f.lookup(req.GetVmId())
	if err != nil {
		return nil, err
	}
	switch req.GetMode() {
	case "on", "sleep_only", "off":
	default:
		return nil, status.Errorf(codes.InvalidArgument, "mode: %q", req.GetMode())
	}
	f.botSets = append(f.botSets, req.GetMode())
	vm.botMode = req.GetMode()
	return &boxdpb.SetBotProtectionResponse{}, nil
}

// proxiesOf copies the named proxies of a machine
func (f *fakeBoxd) proxiesOf(id string) map[string]uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	vm, err := f.lookup(id)
	if err != nil {
		return nil
	}
	out := map[string]uint32{}
	maps.Copy(out, vm.proxies)
	return out
}

// botModes lists every mode SetBotProtection received, oldest first
func (f *fakeBoxd) botModes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.botSets)
}

// setVM changes a machine under the fake's lock
func (f *fakeBoxd) setVM(id string, change func(*fakeVM)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if vm, err := f.lookup(id); err == nil {
		change(vm)
	}
}

func (f *fakeBoxd) proxyPins() []setProxyPort {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.pins)
}

func (f *fakeBoxd) sshCalls() []setSSH {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sshSets)
}

/* Exec */

func (f *fakeBoxd) Exec(stream grpc.BidiStreamingServer[boxdpb.ExecChunk, boxdpb.ExecChunk]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	f.mu.Lock()
	vm, err := f.lookup(first.GetVmId())
	if err == nil && vm.status == "hibernated" {
		err = status.Error(codes.Unavailable, "machine is hibernated")
	}
	if err == nil && vm.agentDownExecs > 0 {
		vm.agentDownExecs--
		err = status.Error(codes.Unavailable, "cannot connect to VM agent: connection refused")
	}
	if err == nil {
		f.commands = append(f.commands, first.GetCommand())
	}
	scripted, script := f.scripted, f.script
	f.mu.Unlock()
	if err != nil {
		return err
	}
	if scripted {
		for _, c := range script {
			if err := stream.Send(c); err != nil {
				return err
			}
		}
		return nil
	}

	cmd := exec.Command("sh", "-c", first.GetCommand())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	f.running.Add(1)
	defer f.running.Add(-1)

	exited := make(chan struct{})
	go func() {
		select {
		case <-stream.Context().Done():
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		case <-exited:
		}
	}()
	go func() {
		defer func() { _ = stdin.Close() }()
		for {
			c, err := stream.Recv()
			// boxd's agent kills the command when the caller half-closes, as the proto says
			if errors.Is(err, io.EOF) {
				terminate(cmd.Process.Pid, exited)
			}
			if err != nil {
				return
			}
			if c.GetStdin() {
				_, _ = stdin.Write(c.GetData())
			}
		}
	}()

	var sendMu sync.Mutex
	send := func(c *boxdpb.ExecChunk) {
		sendMu.Lock()
		defer sendMu.Unlock()
		_ = stream.Send(c)
	}
	var pipes sync.WaitGroup
	for _, p := range []struct {
		r        io.Reader
		isStderr bool
	}{{stdout, false}, {stderr, true}} {
		pipes.Go(func() {
			buf := make([]byte, 4096)
			for {
				n, err := p.r.Read(buf)
				if n > 0 {
					send(&boxdpb.ExecChunk{Data: append([]byte(nil), buf[:n]...),
						IsStderr: p.isStderr})
				}
				if err != nil {
					return
				}
			}
		})
	}
	pipes.Wait()
	waitErr := cmd.Wait()
	close(exited)
	code := cmd.ProcessState.ExitCode()
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		code = 128 + int(ws.Signal())
	}
	if stream.Context().Err() != nil {
		return status.FromContextError(stream.Context().Err()).Err()
	}
	if code < 0 {
		return status.Error(codes.Internal, waitErr.Error())
	}
	send(&boxdpb.ExecChunk{ExitCode: int32(code)})
	return nil
}

// terminate sends the process group SIGTERM, then SIGKILL once the grace period passes
func terminate(pid int, exited <-chan struct{}) {
	select {
	case <-exited:
		return
	default:
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	select {
	case <-exited:
	case <-time.After(fakeKillGrace):
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}
}

func (f *fakeBoxd) lastCommand() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commands) == 0 {
		return ""
	}
	return f.commands[len(f.commands)-1]
}

/* Files */

// fsErr answers a file call's failure with its gRPC code and boxd's reason trailer through set
func fsErr(set func(metadata.MD), err error) error {
	code, reason := codes.Internal, "io_error"
	switch {
	case errors.Is(err, fs.ErrNotExist):
		code, reason = codes.NotFound, "not_found"
	case errors.Is(err, fs.ErrExist):
		code, reason = codes.AlreadyExists, "file_exists"
	case errors.Is(err, syscall.EISDIR):
		code, reason = codes.FailedPrecondition, "is_a_directory"
	case errors.Is(err, syscall.ENOTDIR):
		code, reason = codes.FailedPrecondition, "not_a_directory"
	case errors.Is(err, syscall.ENOSPC):
		code, reason = codes.ResourceExhausted, "no_space"
	case errors.Is(err, fs.ErrPermission):
		code, reason = codes.PermissionDenied, "permission_denied"
	}
	return fsFail(set, code, reason, err.Error())
}

func fsFail(set func(metadata.MD), code codes.Code, reason, msg string) error {
	set(metadata.Pairs(reasonTrailer, reason))
	return status.Error(code, msg)
}

// unaryTrailer sets a trailer on a unary call
func unaryTrailer(ctx context.Context) func(metadata.MD) {
	return func(md metadata.MD) { _ = grpc.SetTrailer(ctx, md) }
}

func entryType(mode fs.FileMode) boxdpb.FsEntryType {
	switch {
	case mode.IsDir():
		return boxdpb.FsEntryType_FS_ENTRY_TYPE_DIRECTORY
	case mode&fs.ModeSymlink != 0:
		return boxdpb.FsEntryType_FS_ENTRY_TYPE_SYMLINK
	case !mode.IsRegular():
		return boxdpb.FsEntryType_FS_ENTRY_TYPE_OTHER
	}
	return boxdpb.FsEntryType_FS_ENTRY_TYPE_FILE
}

func entryOf(name string, info fs.FileInfo) *boxdpb.FsEntry {
	t := entryType(info.Mode())
	return &boxdpb.FsEntry{
		Name: []byte(info.Name()), Path: []byte(name), Type: t, TargetType: t,
		Size: uint64(info.Size()), Mode: uint32(info.Mode().Perm()),
		MtimeNs: info.ModTime().UnixNano(),
	}
}

// lstatEntry describes name itself like boxd, a symlink typed as one with its target's size
func lstatEntry(name string) (*boxdpb.FsEntry, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	e := entryOf(name, info)
	if info.Mode()&fs.ModeSymlink != 0 {
		e.TargetType = boxdpb.FsEntryType_FS_ENTRY_TYPE_UNSPECIFIED
		if target, err := os.Stat(name); err == nil {
			e.TargetType, e.Size = entryType(target.Mode()), uint64(target.Size())
		}
	}
	return e, nil
}

func (f *fakeBoxd) FsStat(
	ctx context.Context, req *boxdpb.FsStatRequest,
) (*boxdpb.FsStatResponse, error) {
	if _, err := f.vm(req.GetVmId()); err != nil {
		return nil, err
	}
	e, err := lstatEntry(string(req.GetPath()))
	if err != nil {
		return nil, fsErr(unaryTrailer(ctx), err)
	}
	return &boxdpb.FsStatResponse{Entry: e}, nil
}

func (f *fakeBoxd) FsRead(
	req *boxdpb.FsReadRequest, stream grpc.ServerStreamingServer[boxdpb.FsReadChunk],
) error {
	if _, err := f.vm(req.GetVmId()); err != nil {
		return err
	}
	name := string(req.GetPath())
	f.mu.Lock()
	f.lengths = append(f.lengths, req.GetLength())
	hook := f.readHook
	f.mu.Unlock()
	if hook != nil {
		hook(name)
	}
	entry, err := lstatEntry(name)
	if err != nil {
		return fsErr(stream.SetTrailer, err)
	}
	if entry.GetTargetType() != boxdpb.FsEntryType_FS_ENTRY_TYPE_FILE {
		return fsFail(stream.SetTrailer, codes.FailedPrecondition, "not_a_regular_file", name)
	}
	file, err := os.Open(name)
	if err != nil {
		return fsErr(stream.SetTrailer, err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return fsErr(stream.SetTrailer, err)
	}
	left := info.Size() - int64(req.GetOffset())
	if req.Length != nil && !f.ignoreLength {
		left = min(left, int64(req.GetLength()))
	}
	if f.shortRead.Load() {
		left /= 2
	}
	r := io.NewSectionReader(file, int64(req.GetOffset()), max(left, 0))
	first := &boxdpb.FsReadChunk{Entry: entry}
	var sent uint64
	buf := make([]byte, fakeReadChunkBytes)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			c := &boxdpb.FsReadChunk{Data: append([]byte(nil), buf[:n]...)}
			if first != nil {
				c.Entry, first = first.Entry, nil
			}
			if err := stream.Send(c); err != nil {
				return err
			}
			sent += uint64(n)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fsErr(stream.SetTrailer, err)
		}
	}
	if f.noEOFChunk.Load() {
		return nil
	}
	last := &boxdpb.FsReadChunk{Eof: true, BytesRead: sent}
	if f.badBytesRead.Load() {
		last.BytesRead++
	}
	if first != nil {
		last.Entry = first.Entry
	}
	return stream.Send(last)
}

// beforeRead sets the hook every later FsRead runs before it looks at its path
func (f *fakeBoxd) beforeRead(hook func(name string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readHook = hook
}

// readLengths lists the length every FsRead asked for, oldest first
func (f *fakeBoxd) readLengths() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.lengths)
}

// track remembers the topmost path a write is about to create so cleanup removes it
func (f *fakeBoxd) track(name string) {
	top := ""
	for p := name; p != "/" && p != "."; p = filepath.Dir(p) {
		if _, err := os.Lstat(p); err == nil {
			break
		}
		top = p
	}
	if top != "" {
		f.mu.Lock()
		f.cleanupPaths = append(f.cleanupPaths, top)
		f.mu.Unlock()
	}
}

// FsWrite writes each chunk into the path at once and keeps a cut write, the worst boxd could do
func (f *fakeBoxd) FsWrite(
	stream grpc.ClientStreamingServer[boxdpb.FsWriteChunk, boxdpb.FsWriteResponse],
) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if _, err := f.vm(first.GetVmId()); err != nil {
		return err
	}
	name := string(first.GetPath())
	f.mu.Lock()
	f.writePaths = append(f.writePaths, name)
	f.writeHeads = append(f.writeHeads, &boxdpb.FsWriteChunk{
		VmId: first.GetVmId(), Path: first.GetPath(), Mode: first.Mode,
		ExpectedSize: first.ExpectedSize,
	})
	f.mu.Unlock()
	if f.noSpace.Load() {
		return fsFail(stream.SetTrailer, codes.ResourceExhausted, "no_space",
			"write: no space left on device")
	}
	mode := fs.FileMode(0o644)
	if first.Mode != nil {
		mode = fs.FileMode(first.GetMode()).Perm()
	}
	f.track(name)
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return fsErr(stream.SetTrailer, err)
	}
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return fsErr(stream.SetTrailer, err)
	}
	defer func() { _ = file.Close() }()
	chunks := []int{len(first.GetData())}
	written, err := file.Write(first.GetData())
	for err == nil {
		if full := f.noSpaceAfter.Load(); full > 0 && int64(written) >= full {
			return fsFail(stream.SetTrailer, codes.ResourceExhausted, "no_space",
				"write: no space left on device")
		}
		var c *boxdpb.FsWriteChunk
		if c, err = stream.Recv(); err != nil {
			break
		}
		chunks = append(chunks, len(c.GetData()))
		var n int
		n, err = file.Write(c.GetData())
		written += n
	}
	f.mu.Lock()
	f.writeChunks = chunks
	f.mu.Unlock()
	if !errors.Is(err, io.EOF) {
		return err
	}
	if first.ExpectedSize != nil && first.GetExpectedSize() != uint64(written) {
		return fsFail(stream.SetTrailer, codes.FailedPrecondition, "size_mismatch",
			"size_mismatch")
	}
	if err := file.Chmod(mode); err != nil {
		return fsErr(stream.SetTrailer, err)
	}
	info, err := file.Stat()
	if err != nil {
		return fsErr(stream.SetTrailer, err)
	}
	return stream.SendAndClose(&boxdpb.FsWriteResponse{
		BytesWritten: uint64(written), Entry: entryOf(name, info),
	})
}

func (f *fakeBoxd) FsRename(
	ctx context.Context, req *boxdpb.FsRenameRequest,
) (*boxdpb.FsRenameResponse, error) {
	if _, err := f.vm(req.GetVmId()); err != nil {
		return nil, err
	}
	from, to := string(req.GetOldPath()), string(req.GetNewPath())
	f.mu.Lock()
	f.renameCalls = append(f.renameCalls, [2]string{from, to})
	f.mu.Unlock()
	if _, err := os.Lstat(to); err == nil && req.GetNoReplace() {
		return nil, fsErr(unaryTrailer(ctx), fs.ErrExist)
	}
	if err := os.Rename(from, to); err != nil {
		return nil, fsErr(unaryTrailer(ctx), err)
	}
	return &boxdpb.FsRenameResponse{}, nil
}

func (f *fakeBoxd) FsRemove(
	ctx context.Context, req *boxdpb.FsRemoveRequest,
) (*boxdpb.FsRemoveResponse, error) {
	if _, err := f.vm(req.GetVmId()); err != nil {
		return nil, err
	}
	name := string(req.GetPath())
	remove := os.Remove
	if req.GetRecursive() {
		remove = os.RemoveAll
	}
	if _, err := os.Lstat(name); err != nil {
		return nil, fsErr(unaryTrailer(ctx), err)
	}
	if err := remove(name); err != nil {
		return nil, fsErr(unaryTrailer(ctx), err)
	}
	return &boxdpb.FsRemoveResponse{}, nil
}

// fsWritePaths lists the path of every FsWrite, oldest first
func (f *fakeBoxd) fsWritePaths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.writePaths)
}

// lastWriteHead is the first chunk of the last FsWrite without its data
func (f *fakeBoxd) lastWriteHead() *boxdpb.FsWriteChunk {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.writeHeads) == 0 {
		return nil
	}
	return f.writeHeads[len(f.writeHeads)-1]
}

// renames lists every FsRename as its old and new path, oldest first
func (f *fakeBoxd) renames() [][2]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.renameCalls)
}

func (f *fakeBoxd) FsList(
	req *boxdpb.FsListRequest, stream grpc.ServerStreamingServer[boxdpb.FsListChunk],
) error {
	if _, err := f.vm(req.GetVmId()); err != nil {
		return err
	}
	dir := string(req.GetPath())
	des, err := os.ReadDir(dir)
	if err != nil {
		return fsErr(stream.SetTrailer, err)
	}
	var batch []*boxdpb.FsEntry
	for _, de := range des {
		info, err := de.Info()
		if err != nil {
			continue
		}
		batch = append(batch, entryOf(filepath.Join(dir, de.Name()), info))
		if len(batch) == fakeListBatch {
			if err := stream.Send(&boxdpb.FsListChunk{Entries: batch}); err != nil {
				return err
			}
			batch = nil
		}
	}
	if len(batch) > 0 {
		return stream.Send(&boxdpb.FsListChunk{Entries: batch})
	}
	return nil
}

func (f *fakeBoxd) lastWriteChunks() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeChunks
}

func (f *fakeBoxd) lastCreate() *boxdpb.CreateVmRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creates) == 0 {
		return nil
	}
	return f.creates[len(f.creates)-1]
}

func (f *fakeBoxd) setBilling(b *boxdpb.BillingInfo, org *boxdpb.GetOrgBillingResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.billing, f.orgBilling = b, org
}

func (f *fakeBoxd) Whoami(context.Context, *boxdpb.WhoamiRequest) (*boxdpb.WhoamiResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &boxdpb.WhoamiResponse{UserId: "u1", Billing: f.billing}, nil
}

func (f *fakeBoxd) GetOrgMachineLimit(
	context.Context, *boxdpb.GetOrgMachineLimitRequest,
) (*boxdpb.GetOrgMachineLimitResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := uint32(0)
	for _, vm := range f.vms {
		if vm.status != "destroyed" {
			n++
		}
	}
	return &boxdpb.GetOrgMachineLimitResponse{
		OrgId: "o1", OrgName: "acme", MaxVms: 25, VmCount: n,
	}, nil
}

func (f *fakeBoxd) GetOrgUsage(
	_ context.Context, req *boxdpb.GetOrgUsageRequest,
) (*boxdpb.GetOrgUsageResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.usageAsked = req
	if f.orgUsage == nil {
		return &boxdpb.GetOrgUsageResponse{Currency: "usd"}, nil
	}
	return f.orgUsage, nil
}

func (f *fakeBoxd) GetOrgBilling(
	_ context.Context, req *boxdpb.GetOrgBillingRequest,
) (*boxdpb.GetOrgBillingResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.orgAsked = req.GetOrg()
	if f.orgBilling == nil {
		return &boxdpb.GetOrgBillingResponse{BillingMode: "stripe"}, nil
	}
	return f.orgBilling, nil
}
