package saga

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// ErrSSHPreparing means the placement's sshd is not ready yet
var ErrSSHPreparing = errors.New("saga: the sandbox's SSH server is being prepared")

const (
	sshSpecPath = "/tmp/bugnet/ssh-spec.json"
	// sshdPath is where openssh-server puts sshd on Debian and Ubuntu
	sshdPath = "/usr/sbin/sshd"
	// defaultInstall is ssh.install_timeout's default
	defaultInstall = 120 * time.Second
	// sshUpSlack covers ssh-up's other steps beside the install, sshd's 10 s start among them
	sshUpSlack = time.Minute
	// aliasTries bounds the fresh aliases tried at ready
	aliasTries = 5
	// metaSSHAddress holds the address of Boat's own sshd in the placement's ref
	metaSSHAddress = "ssh_address"
)

// SSHSettings are what the ssh-up spec carries beyond the sandbox's own values
type SSHSettings struct {
	// Ports are ssh.sshd_port and ssh.sshd_fallback_port, 22 and 2022 when empty
	Ports []int
	// InstallTimeout bounds ssh-up's apt-get, 120 s when zero
	InstallTimeout time.Duration
	// LocalSSHDPath is the sshd a local sandbox runs, /usr/sbin/sshd when empty
	LocalSSHDPath string
}

// sshSpec is what bugnet-run ssh-up reads from sshSpecPath
type sshSpec struct {
	V               int      `json:"v"`
	User            string   `json:"user"`
	Ports           []int    `json:"ports"`
	SSHDPath        string   `json:"sshd_path"`
	Install         bool     `json:"install"`
	InstallTimeoutS int      `json:"install_timeout_s"`
	Authorize       []string `json:"authorize"`
}

// execUser is the user Exec runs as, which our sshd lets in alone
func execUser(prov provider.Provider) string {
	return cmp.Or(prov.Capabilities().ExecUser, "root")
}

func (s SSHSettings) installTimeout() time.Duration {
	return cmp.Or(s.InstallTimeout, defaultInstall)
}

// specFor builds the ssh-up spec, a local sandbox naming its one port and never installing
func (s SSHSettings) specFor(prov provider.Provider, ref provider.Ref, authorize []string) sshSpec {
	spec := sshSpec{V: 1, User: execUser(prov), Authorize: slices.Clone(authorize)}
	if spec.Authorize == nil {
		spec.Authorize = []string{}
	}
	if port := metaPort(ref, "sshd_port"); port > 0 {
		spec.Ports, spec.SSHDPath = []int{port}, cmp.Or(s.LocalSSHDPath, sshdPath)
		return spec
	}
	spec.Ports = slices.Clone(s.Ports)
	if len(spec.Ports) == 0 {
		spec.Ports = []int{22, 2022}
	}
	spec.SSHDPath, spec.Install = sshdPath, true
	spec.InstallTimeoutS = int(s.installTimeout().Seconds())
	return spec
}

// sshUp runs ssh-up and answers the host it reports, with the exec user and never its own
func (s *Saga) sshUp(
	ctx context.Context, prov provider.Provider, ref provider.Ref, id uuid.UUID,
	authorize []string,
) (store.SSHHost, error) {
	ctx, cancel := context.WithTimeout(ctx, s.d.SSH.installTimeout()+sshUpSlack)
	defer cancel()
	spec := s.d.SSH.specFor(prov, ref, authorize)
	ans, err := rootCommand(ctx, prov, ref, "ssh-up", sshSpecPath, spec)
	if err != nil {
		return store.SSHHost{}, err
	}
	if !slices.Contains(spec.Ports, ans.Port) {
		return store.SSHHost{}, errors.New("ssh-up reported a port the spec did not offer")
	}
	key, err := sshkeys.HostKey(ans.HostKey)
	if err != nil {
		return store.SSHHost{}, errors.New("ssh-up reported no usable host key")
	}
	return store.SSHHost{
		PlacementID: id, Transport: store.SSHTransportGate, SSHDPort: ans.Port,
		SSHUser: execUser(prov), HostKey: key,
	}, nil
}

// sshLock names the advisory lock one placement's SSH steps run under
func sshLock(id uuid.UUID) string {
	return "ssh:" + id.String()
}

// mintAlias gives a ready placement an alias inside its ready transaction, never failing it
func (s *Saga) mintAlias(ctx context.Context, ts *store.Store, id uuid.UUID) {
	for range aliasTries {
		var ok bool
		// A savepoint, so a racing holder of the same alias costs one try and not the ready step
		err := ts.WithTx(ctx, func(tx pgx.Tx) error {
			var err error
			ok, err = s.d.Store.Tx(tx).SetSSHAlias(ctx, id, sshkeys.NewAlias())
			return err
		})
		if err == nil && ok {
			return
		}
		if err != nil {
			s.d.Log.Warn("mint ssh alias", "placement", id, "err", err)
		}
	}
	s.d.Log.Error("no ssh alias after every try", "placement", id)
}

// EnqueuePrepareSSH asks the worker to make the placement's sshd ready, once while one is pending
func (s *Saga) EnqueuePrepareSSH(ctx context.Context, id uuid.UUID) error {
	if _, err := s.d.River.Insert(ctx, jobargs.PrepareSSH{PlacementID: id}, nil); err != nil {
		return fmt.Errorf("insert prepare_ssh job: %w", err)
	}
	return nil
}

// PrepareSSH runs ssh-up for a ready placement that has no SSH server record yet
func (s *Saga) PrepareSSH(ctx context.Context, id uuid.UUID) error {
	return s.d.Store.WithLock(ctx, sshLock(id), func(ts *store.Store) error {
		cur, err := readyRow(ctx, ts, id)
		if errors.Is(err, ErrNotReady) || errors.Is(err, store.ErrNotFound) {
			s.d.Log.Info("prepare ssh skipped, the placement is not ready", "placement", id)
			return nil
		}
		if err != nil {
			return err
		}
		switch _, err := ts.SSHHost(ctx, id); {
		case err == nil:
			return nil
		case !errors.Is(err, store.ErrNotFound):
			return err
		}
		prov, ref, err := s.home(cur)
		if err != nil {
			return err
		}
		if s.d.Registry.NativeSSH(prov) {
			return nil
		}
		host, err := s.sshUp(ctx, prov, ref, id, nil)
		if err != nil {
			return fmt.Errorf("ssh-up: %w", err)
		}
		if err := ts.PutSSHHost(ctx, host); err != nil {
			return err
		}
		s.d.Log.Info("ssh prepared", "placement", id, "provider", prov.Name(),
			"port", host.SSHDPort)
		return nil
	})
}

// SSHUpstream is how the gateway reaches one placement's SSH server
type SSHUpstream struct {
	Host store.SSHHost
	// Address is Boat's own sshd for the native transport, empty for the gate
	Address string
}

// AuthorizeGateway puts the gateway's key for a placement in its SSH server once
func (s *Saga) AuthorizeGateway(
	ctx context.Context, id uuid.UUID, line, fingerprint string,
) (SSHUpstream, error) {
	var out SSHUpstream
	err := s.d.Store.WithLock(ctx, sshLock(id), func(ts *store.Store) error {
		cur, err := readyRow(ctx, ts, id)
		if err != nil {
			return err
		}
		prov, ref, err := s.home(cur)
		if err != nil {
			return err
		}
		if s.d.Registry.NativeSSH(prov) {
			out, err = s.authorizeNative(ctx, ts, cur, prov, ref, line, fingerprint)
			return err
		}
		host, err := ts.SSHHost(ctx, id)
		if errors.Is(err, store.ErrNotFound) {
			return ErrSSHPreparing
		}
		if err != nil {
			return err
		}
		has, err := ts.HasGatewayKey(ctx, id, fingerprint)
		if err != nil || has {
			out.Host = host
			return err
		}
		if host, err = s.sshUp(ctx, prov, ref, id, []string{line}); err != nil {
			return fmt.Errorf("%w: ssh-up: %w", ErrSSHPreparing, err)
		}
		if err := ts.PutSSHHost(ctx, host); err != nil {
			return err
		}
		out.Host = host
		return ts.AddGatewayKey(ctx, id, fingerprint)
	})
	return out, err
}

// authorizeNative sends the gateway's key to the provider's own sshd once and keeps its address
func (s *Saga) authorizeNative(
	ctx context.Context, ts *store.Store, p store.Placement, prov provider.Provider,
	ref provider.Ref, line, fingerprint string,
) (SSHUpstream, error) {
	host, herr := ts.SSHHost(ctx, p.ID)
	has, err := ts.HasGatewayKey(ctx, p.ID, fingerprint)
	if err != nil {
		return SSHUpstream{}, err
	}
	if has && herr == nil && ref.Meta[metaSSHAddress] != "" {
		return SSHUpstream{Host: host, Address: ref.Meta[metaSSHAddress]}, nil
	}
	n, ok := prov.(provider.NativeSSHer)
	if !ok {
		return SSHUpstream{}, fmt.Errorf("provider %s has no SSH server of its own", prov.Name())
	}
	ns, err := n.AuthorizeNativeSSH(ctx, ref, line)
	if err != nil {
		return SSHUpstream{}, fmt.Errorf("%w: authorize on the provider's sshd: %w",
			ErrSSHPreparing, err)
	}
	key, err := sshkeys.HostKey(ns.HostKey)
	if err != nil {
		return SSHUpstream{}, fmt.Errorf("%w: the provider reported no usable host key",
			ErrSSHPreparing)
	}
	_, portText, err := net.SplitHostPort(ns.Address)
	port, perr := strconv.Atoi(portText)
	if err != nil || perr != nil || port < 1 || port > 65535 || ns.User == "" {
		return SSHUpstream{}, fmt.Errorf("%w: the provider reported no usable address",
			ErrSSHPreparing)
	}
	host = store.SSHHost{
		PlacementID: p.ID, Transport: store.SSHTransportBoatNative, SSHDPort: port,
		SSHUser: ns.User, HostKey: key,
	}
	if err := ts.PutSSHHost(ctx, host); err != nil {
		return SSHUpstream{}, err
	}
	if err := s.addRouteMeta(ctx, p.ID, map[string]string{metaSSHAddress: ns.Address}); err != nil {
		return SSHUpstream{}, err
	}
	if err := ts.AddGatewayKey(ctx, p.ID, fingerprint); err != nil {
		return SSHUpstream{}, err
	}
	return SSHUpstream{Host: host, Address: ns.Address}, nil
}

// addRouteMeta adds entries to a ready placement's ref under the gate lock that guards it
func (s *Saga) addRouteMeta(ctx context.Context, id uuid.UUID, meta map[string]string) error {
	return s.d.Store.WithLock(ctx, gateLock(id), func(ts *store.Store) error {
		cur, err := readyRow(ctx, ts, id)
		if err != nil {
			return err
		}
		if cur.ProviderRef == nil {
			return fmt.Errorf("placement %s has no sandbox", id)
		}
		return setRoute(ctx, ts, id, withMeta(*cur.ProviderRef, meta), nil)
	})
}
