package vkclient

import (
	"testing"

	"github.com/oynozan/bugnet-router/internal/testenv"
)

func TestValkeyUnixAddress(t *testing.T) {
	path, accepted := testenv.ValkeySocket(t)
	opt, err := Options("unix://" + path)
	if err != nil {
		t.Fatal(err)
	}
	if len(opt.InitAddress) != 1 || opt.InitAddress[0] != path {
		t.Fatalf("init address %v, want the socket path alone", opt.InitAddress)
	}
	c, err := New("unix://" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := c.Do(t.Context(), c.B().Ping().Build()).ToString()
	if err != nil || got != "PONG" {
		t.Fatalf("PING over the socket gave %q, %v", got, err)
	}
	if accepted.Load() == 0 {
		t.Fatal("the client never dialled the Unix socket")
	}
}

func TestValkeyTCPAddress(t *testing.T) {
	opt, err := Options(testenv.ValkeyAddr())
	if err != nil {
		t.Fatal(err)
	}
	if opt.DialCtxFn != nil || len(opt.InitAddress) != 1 ||
		opt.InitAddress[0] != testenv.ValkeyAddr() {
		t.Fatalf("a TCP address gave %+v, want it dialled as it is", opt)
	}
	c, err := New(testenv.ValkeyAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Do(t.Context(), c.B().Ping().Build()).Error(); err != nil {
		t.Fatal(err)
	}
}

func TestValkeyAddressRefusesARelativeSocket(t *testing.T) {
	for _, addr := range []string{"unix://", "unix://run/valkey.sock", ""} {
		if _, err := Options(addr); err == nil {
			t.Errorf("%q gave no error", addr)
		}
	}
}
