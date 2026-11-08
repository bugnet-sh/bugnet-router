package local

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/oynozan/bugnet-router/internal/provider"
)

const (
	metaGatePort = "gate_port"
	metaSSHDPort = "sshd_port"
)

var _ provider.Gater = (*Provider)(nil)

// PrepareGate returns the sandbox's own ports, giving them to a sandbox made without any
func (p *Provider) PrepareGate(
	_ context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	if err := provider.CheckGatePort(port); err != nil {
		return nil, err
	}
	s, err := p.open(ref)
	if err != nil {
		return nil, err
	}
	if s.meta.GatePort != 0 && s.meta.SSHDPort != 0 {
		return portsMeta(s.meta), nil
	}
	s, unlock, err := p.openLocked(ref)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if s.meta.GatePort == 0 || s.meta.SSHDPort == 0 {
		if s.meta.GatePort, s.meta.SSHDPort, err = freePorts(); err != nil {
			return nil, internalErr(err)
		}
		if err := writeMeta(s.dir, s.meta); err != nil {
			return nil, fileErr(err)
		}
	}
	return portsMeta(s.meta), nil
}

// GateRoute is the gate's loopback port, since sandboxes share the host and its port space
func (p *Provider) GateRoute(ref provider.Ref, port int) (provider.GateRoute, error) {
	if err := provider.CheckGatePort(port); err != nil {
		return provider.GateRoute{}, err
	}
	if ref.Provider != p.opts.Name || !validID(ref.ID) {
		return provider.GateRoute{}, notFound(ref)
	}
	raw := ref.Meta[metaGatePort]
	if raw == "" {
		return provider.GateRoute{}, provider.GateUnprepared(p.opts.Name, ref.ID, metaGatePort)
	}
	gate, err := strconv.Atoi(raw)
	if err != nil || gate < 1 || gate > 65535 {
		return provider.GateRoute{}, internalErr(fmt.Errorf(
			"sandbox %s/%s has a gate_port that is not a TCP port", ref.Provider, ref.ID))
	}
	return provider.GateRoute{BaseURL: "http://127.0.0.1:" + strconv.Itoa(gate)}, nil
}

// portsMeta is the Ref.Meta holding m's ports, nil when it has none
func portsMeta(m meta) map[string]string {
	if m.GatePort == 0 || m.SSHDPort == 0 {
		return nil
	}
	return map[string]string{
		metaGatePort: strconv.Itoa(m.GatePort), metaSSHDPort: strconv.Itoa(m.SSHDPort),
	}
}

// freePorts picks two loopback ports nothing listens on, both held open until both are picked
func freePorts() (int, int, error) {
	gate, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = gate.Close() }()
	sshd, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = sshd.Close() }()
	return gate.Addr().(*net.TCPAddr).Port, sshd.Addr().(*net.TCPAddr).Port, nil
}
