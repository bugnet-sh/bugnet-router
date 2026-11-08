package api

import (
	"cmp"
	"context"
	"errors"
	"regexp"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// defaultMaxKeys is ssh.max_keys_per_placement's default
	defaultMaxKeys = 4
	// defaultKnownHosts is where the hand-off keeps the gateway's host keys by default
	defaultKnownHosts = "~/.ssh/bugnet_known_hosts"
)

var (
	errNoSSH        = errors.New("api: the placement's provider serves no SSH")
	errNoSSHGateway = errors.New("api: this router serves no SSH gateway")
	errSSHPath      = errors.New("api: a key or known_hosts path is not accepted")
	errFingerprint  = errors.New("api: the fingerprint is not a SHA256 fingerprint")
)

// fingerprintShape is what ssh-keygen -l prints for SHA256, with room for any spelling of it
var fingerprintShape = regexp.MustCompile(`^SHA256:[A-Za-z0-9+/=]{1,64}$`)

// AddSshKey registers a public key on a ready placement and answers how to connect with it
func (s *Server) AddSshKey(
	ctx context.Context, req *connect.Request[routerv1.AddSshKeyRequest],
) (*connect.Response[routerv1.AddSshKeyResponse], error) {
	m := req.Msg
	p, err := s.placement(ctx, m.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	prov, err := s.home(p)
	if err != nil {
		return nil, s.status(err)
	}
	if err := s.sshServes(prov, p); err != nil {
		return nil, s.status(err)
	}
	if err := s.spend(ctx, p); err != nil {
		return nil, s.status(err)
	}
	h := sshkeys.HandOff{
		Alias: p.SSHAlias, Host: s.d.SSHHost, Port: cmp.Or(s.d.SSHPort, 22),
		IdentityFile:   cmp.Or(m.GetIdentityFile(), "~/.ssh/bugnet_"+p.SSHAlias),
		KnownHostsFile: cmp.Or(m.GetKnownHostsFile(), defaultKnownHosts),
	}
	if !sshkeys.ValidPath(h.IdentityFile) || !sshkeys.ValidPath(h.KnownHostsFile) {
		return nil, s.status(errSSHPath)
	}
	key, err := sshkeys.Parse(m.GetPublicKey())
	if err != nil {
		return nil, s.status(err)
	}
	most := cmp.Or(s.d.SSH.MaxKeysPerPlacement, defaultMaxKeys)
	if err := s.d.Store.AddSSHKey(ctx, p.ID, key.Fingerprint, key.Wire, most); err != nil {
		return nil, s.status(err)
	}
	// OpenSshUpstream asks again, so a lost job only delays the first connection
	if err := s.prepareSSH(ctx, p, prov); err != nil {
		s.d.Log.Warn("api: enqueue prepare_ssh", "placement", p.ID, "err", err)
	}
	return connect.NewResponse(&routerv1.AddSshKeyResponse{
		Alias: h.Alias, Host: h.Host, Port: uint32(h.Port), Fingerprint: key.Fingerprint,
		KnownHosts: h.KnownHosts(s.d.SSH.HostKeys), Command: h.Command(), Config: h.Config(),
		ExpiresAt: timestamppb.New(*p.Deadline),
	}), nil
}

// RemoveSshKey marks a key removed and tells the gateway to close its connections
func (s *Server) RemoveSshKey(
	ctx context.Context, req *connect.Request[routerv1.RemoveSshKeyRequest],
) (*connect.Response[routerv1.RemoveSshKeyResponse], error) {
	m := req.Msg
	p, err := s.placement(ctx, m.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	if !fingerprintShape.MatchString(m.GetFingerprint()) {
		return nil, s.status(errFingerprint)
	}
	removed, err := s.d.Store.RemoveSSHKey(ctx, p.ID, m.GetFingerprint())
	if err != nil {
		return nil, s.status(err)
	}
	if removed {
		s.events.Publish(ctx, &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_SshKeyRemoved{
			SshKeyRemoved: &routerv1.SshKeyRemoved{
				PlacementId: p.ID.String(), Fingerprint: m.GetFingerprint(),
			},
		}})
	}
	return connect.NewResponse(&routerv1.RemoveSshKeyResponse{}), nil
}

// sshServes refuses a placement whose provider has no SSH or a router without a gateway
func (s *Server) sshServes(prov provider.Provider, p store.Placement) error {
	switch {
	case !s.d.Registry.Capabilities(prov).SSH || p.SSHAlias == "":
		return errNoSSH
	case s.d.SSHHost == "" || len(s.d.SSH.HostKeys) == 0 || len(s.d.Keys.Roots()) == 0:
		return errNoSSHGateway
	}
	return nil
}

// prepareSSH enqueues prepare_ssh unless the placement's sshd is ready or is the provider's own
func (s *Server) prepareSSH(ctx context.Context, p store.Placement, prov provider.Provider) error {
	if s.d.Registry.NativeSSH(prov) {
		return nil
	}
	switch _, err := s.d.Store.SSHHost(ctx, p.ID); {
	case err == nil:
		return nil
	case !errors.Is(err, store.ErrNotFound):
		return err
	}
	return s.d.Saga.EnqueuePrepareSSH(ctx, p.ID)
}

// standing reports whether a placement is ready and inside its paid time
func standing(p store.Placement) bool {
	return p.State == store.PlacementReady && p.Deadline != nil && time.Now().Before(*p.Deadline)
}
