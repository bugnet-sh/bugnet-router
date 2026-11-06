// Package vkclient connects to Valkey at a host and port or at a Unix socket path
package vkclient

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"path/filepath"
	"strings"

	"github.com/valkey-io/valkey-go"
)

// Options builds the client options for addr, dialling a Unix socket for the unix scheme
func Options(addr string) (valkey.ClientOption, error) {
	socket, unix := strings.CutPrefix(addr, "unix://")
	switch {
	case unix && filepath.IsAbs(socket):
		return valkey.ClientOption{
			InitAddress: []string{socket},
			DialCtxFn: func(
				ctx context.Context, dst string, d *net.Dialer, _ *tls.Config,
			) (net.Conn, error) {
				return d.DialContext(ctx, "unix", dst)
			},
		}, nil
	case unix, addr == "":
		return valkey.ClientOption{}, errors.New(
			"vkclient: the address must be host:port, or unix:// and an absolute socket path")
	}
	return valkey.ClientOption{InitAddress: []string{addr}}, nil
}

// New connects to the Valkey at addr
func New(addr string) (valkey.Client, error) {
	opt, err := Options(addr)
	if err != nil {
		return nil, err
	}
	return valkey.NewClient(opt)
}
