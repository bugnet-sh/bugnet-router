package netguard_test

import (
	"errors"
	"net"
	"net/netip"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/netguard"
)

func TestDialerRefusesLoopbackAndPrivateRanges(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	_, err = netguard.Dialer{}.DialContext(t.Context(), "tcp", ln.Addr().String())
	if !errors.Is(err, netguard.ErrRefused) {
		t.Fatalf("dialing loopback gave %v, want a refusal", err)
	}
	for _, ip := range []string{"10.1.2.3", "169.254.169.254", "fd00::1", "::ffff:192.168.0.1"} {
		if !netguard.Refused(netip.MustParseAddr(ip)) {
			t.Errorf("%s is not refused", ip)
		}
	}
	if netguard.Refused(netip.MustParseAddr("140.82.112.3")) {
		t.Error("a public address is refused")
	}
}
