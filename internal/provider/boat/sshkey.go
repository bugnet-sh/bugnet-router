package boat

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat/api"
)

// nativeSSHPort is the port machineIp's sshd listens on
const nativeSSHPort = "22"

var _ provider.NativeSSHer = (*Provider)(nil)

// sshUser is an account name Boat may report, which the gateway logs in as
var sshUser = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)

// AuthorizeNativeSSH adds the gateway's key line to the sandbox's own sshd, which never drops it
func (p *Provider) AuthorizeNativeSSH(
	ctx context.Context, ref provider.Ref, gatewayKey string,
) (provider.NativeSSH, error) {
	id, err := sandboxID(ref)
	if err != nil {
		return provider.NativeSSH{}, err
	}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.SshKey(ctx, id, api.SshKeyRequest{Key: gatewayKey})
	})
	if err != nil {
		return provider.NativeSSH{}, err
	}
	var reply api.SshKeyResponse
	if err := decode(raw, &reply); err != nil {
		return provider.NativeSSH{}, internal(fmt.Errorf("boat ssh key reply for sandbox %s "+
			"is not JSON", id))
	}
	out, err := nativeSSH(reply)
	if err != nil {
		return provider.NativeSSH{}, internal(fmt.Errorf("boat ssh key reply for sandbox %s: %w",
			id, err))
	}
	return out, nil
}

// nativeSSH checks a reply's address, user and host key, its errors never quoting them
func nativeSSH(r api.SshKeyResponse) (provider.NativeSSH, error) {
	var out provider.NativeSSH
	switch {
	case r.SshEndpoint != nil && *r.SshEndpoint != "":
		ap, err := netip.ParseAddrPort(*r.SshEndpoint)
		if err != nil || ap.Port() == 0 {
			return out, errors.New("sshEndpoint is not an address and port")
		}
		out.Address = net.JoinHostPort(ap.Addr().String(), strconv.Itoa(int(ap.Port())))
	case r.MachineIp != nil:
		ip, err := netip.ParseAddr(*r.MachineIp)
		if err != nil {
			return out, errors.New("machineIp is not an address")
		}
		out.Address = net.JoinHostPort(ip.String(), nativeSSHPort)
	default:
		return out, errors.New("the reply names no address")
	}
	if r.SshUser == nil || !sshUser.MatchString(*r.SshUser) {
		return out, errors.New("sshUser is not a plain account name")
	}
	out.User = *r.SshUser
	if r.HostKey == nil || len(strings.Fields(*r.HostKey)) < 2 ||
		strings.ContainsAny(*r.HostKey, "\r\n") {
		return out, errors.New("hostKey is not a public key line")
	}
	out.HostKey = *r.HostKey
	return out, nil
}
