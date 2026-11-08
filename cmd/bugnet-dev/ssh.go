package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
)

// maxPublicKeyFile bounds what ssh-key reads, a public key line being far smaller
const maxPublicKeyFile = 16 << 10

// sshKey registers a public key on a placement and prints how to connect with it
func (c *cli) sshKey(args []string) error {
	fs := newFlags("ssh-key")
	h := fs.String("handle", "", "sandbox handle")
	file := fs.String("public-key", "", "public key file, such as ~/.ssh/id_ed25519.pub")
	identity := fs.String("identity-file", "", "private key path the command names")
	knownHosts := fs.String("known-hosts-file", "", "known_hosts path the command names")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("handle", *h); err != nil {
		return err
	}
	if err := required("public-key", *file); err != nil {
		return err
	}
	line, err := readPublicKey(*file)
	if err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.AddSshKey(c.ctx, connect.NewRequest(&routerv1.AddSshKeyRequest{
		Handle: *h, PublicKey: line, IdentityFile: *identity, KnownHostsFile: *knownHosts,
	}))
	if err != nil {
		return err
	}
	m := res.Msg
	kv := [][2]string{
		{"alias", m.GetAlias()}, {"host", m.GetHost()},
		{"port", strconv.FormatUint(uint64(m.GetPort()), 10)},
		{"fingerprint", m.GetFingerprint()},
	}
	for _, k := range m.GetKnownHosts() {
		kv = append(kv, [2]string{"known_hosts", k})
	}
	kv = append(kv, [2]string{"command", m.GetCommand()},
		[2]string{"expires_at", stamp(m.GetExpiresAt())})
	for _, l := range strings.Split(strings.TrimSuffix(m.GetConfig(), "\n"), "\n") {
		kv = append(kv, [2]string{"config", l})
	}
	return c.print(kv)
}

// readPublicKey reads one public key line, refusing a private key so it never leaves the machine
func readPublicKey(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, maxPublicKeyFile+1))
	if err != nil {
		return "", err
	}
	switch {
	case len(raw) > maxPublicKeyFile:
		return "", fmt.Errorf("%w: --public-key names a file too large for a public key", errUsage)
	case bytes.Contains(raw, []byte("PRIVATE KEY")):
		return "", fmt.Errorf("%w: --public-key names a private key, give the .pub file", errUsage)
	}
	return strings.TrimSpace(string(raw)), nil
}
