// Package netguard dials public hosts only, refusing the ranges no public host lives in.
package netguard

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"syscall"
	"time"
)

// ErrRefused means the address is not one a guarded dialer may reach
var ErrRefused = errors.New("netguard: the address is not one that may be reached")

// refused lists the ranges DP 10.5 step 7 closes, with the other ranges no public host lives in
var refused = prefixes(
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
	"192.0.0.0/24", "192.168.0.0/16", "198.18.0.0/15", "224.0.0.0/4", "240.0.0.0/4",
	"::/96", "2001::/32", "2002::/16", "64:ff9b::/96", "64:ff9b:1::/48", "fc00::/7", "fe80::/10",
	"ff00::/8",
)

func prefixes(list ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(list))
	for i, s := range list {
		out[i] = netip.MustParsePrefix(s)
	}
	return out
}

// Refused says whether ip may never be connected to, whatever name led to it
func Refused(ip netip.Addr) bool {
	ip = ip.Unmap().WithZone("")
	if !ip.IsValid() {
		return true
	}
	for _, p := range refused {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// Dialer connects to public hosts only, checking the address the kernel is about to use
type Dialer struct {
	// Timeout bounds one connection attempt, 10 s when zero
	Timeout time.Duration
	// Refuses replaces Refused, for tests that reach a loopback server
	Refuses func(netip.Addr) bool
}

// DialContext connects to addr, a host and port, refusing every address that is not public
func (d Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	refuses := d.Refuses
	if refuses == nil {
		refuses = Refused
	}
	dl := net.Dialer{
		Timeout: d.Timeout,
		Control: func(_, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil || refuses(ap.Addr()) {
				return ErrRefused
			}
			return nil
		},
	}
	if dl.Timeout == 0 {
		dl.Timeout = 10 * time.Second
	}
	return dl.DialContext(ctx, network, addr)
}
