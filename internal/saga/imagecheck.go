package saga

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys"
)

// Ref.Meta keys a check boot leaves for the base image check to compare
const (
	// MetaSSHHostKey is the host key ssh-up made, an authorized_keys line without comment
	MetaSSHHostKey = "check_ssh_host_key"
	// MetaReseed is how setup says it mixed in the seed, ioctl, write or failed
	MetaReseed = "check_reseed"
)

// CheckBoot builds the start steps a base image check runs on a fresh sandbox of the image
//
// They are the manifest read, the baked runner's ping, setup with a throwaway gate key, no CA and
// a fresh seed, and ssh-up with a throwaway key. They use no router secret and no database.
func CheckBoot(
	b BootSettings, ssh SSHSettings,
) func(context.Context, provider.Provider, provider.Ref) (provider.Ref, error) {
	// The check trusts no CA, so it never needs the PEM and never installs one
	b.CAPEM, b.CAVersion = nil, 0
	return func(
		ctx context.Context, prov provider.Provider, ref provider.Ref,
	) (provider.Ref, error) {
		m, ok, err := image.Read(ctx, prov, ref)
		if err != nil {
			return ref, fmt.Errorf("read the image manifest: %w", err)
		}
		if !ok {
			return ref, errors.New("the sandbox holds no image manifest")
		}
		pctx, cancel := context.WithTimeout(ctx, uploadTimeout)
		err = pingRunner(pctx, prov, ref, image.RunnerPath)
		cancel()
		if err != nil {
			return ref, fmt.Errorf("the baked runner: %w", err)
		}
		ref = withMeta(ref, map[string]string{"runner": image.RunnerPath})
		key := make([]byte, 32)
		_, _ = rand.Read(key)
		sctx, cancel := context.WithTimeout(ctx, setupTimeout)
		ans, err := rootCommand(sctx, prov, ref, "setup", setupSpecPath,
			b.setupSpecFor(prov.Name(), ref, base64.RawURLEncoding.EncodeToString(key), false, &m))
		cancel()
		if err != nil {
			return ref, err
		}
		line, err := sshkeys.Throwaway()
		if err != nil {
			return ref, err
		}
		spec := ssh.specFor(prov, ref, []string{line})
		uctx, cancel := context.WithTimeout(ctx, ssh.installTimeout()+sshUpSlack)
		up, err := rootCommand(uctx, prov, ref, "ssh-up", sshSpecPath, spec)
		cancel()
		if err != nil {
			return ref, err
		}
		host, err := sshkeys.HostKey(up.HostKey)
		if err != nil {
			return ref, errors.New("ssh-up reported no usable host key")
		}
		return withMeta(ref, map[string]string{MetaSSHHostKey: host, MetaReseed: ans.Reseed}), nil
	}
}
