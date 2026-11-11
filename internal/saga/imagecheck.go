package saga

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"

	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys"
)

// Ref.Meta keys a check boot leaves for the base image check to compare
const (
	// MetaSSHHostKey is the sandbox sshd's host key, an authorized_keys line without comment
	MetaSSHHostKey = "check_ssh_host_key"
	// MetaReseed is how setup says it mixed in the seed, ioctl, write or failed
	MetaReseed = "check_reseed"
)

// CheckBoot builds the start steps of a base image check, which use no router secret or database
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
		var host string
		if pinsHostKey(ssh, prov) {
			host, err = pinnedHostKey(ref)
		} else {
			host, err = checkSSHUp(ctx, ssh, prov, ref)
		}
		if err != nil {
			return ref, err
		}
		return withMeta(ref, map[string]string{MetaSSHHostKey: host, MetaReseed: ans.Reseed}), nil
	}
}

// pinsHostKey reports whether prov's own sshd serves SSH and its ReadyMeta pinned that sshd's key
func pinsHostKey(ssh SSHSettings, prov provider.Provider) bool {
	// Boat's own sshd pins no host key, so its check starts ours beside it
	_, ready := provider.As[provider.ReadyReporter](prov)
	return ready && slices.Contains(ssh.NativeSSH, prov.Name())
}

// pinnedHostKey is the host key of the provider's own sshd, which its ReadyMeta pinned in Meta
func pinnedHostKey(ref provider.Ref) (string, error) {
	line, ok := ref.Meta[provider.MetaHostKey]
	if !ok {
		return "", errors.New("the provider's own sshd has no host_key in the sandbox's Meta")
	}
	host, err := sshkeys.HostKey(line)
	if err != nil {
		return "", errors.New("the host_key in the sandbox's Meta is not a usable host key")
	}
	return host, nil
}

// checkSSHUp starts our sshd with a throwaway key and answers the host key it made
func checkSSHUp(
	ctx context.Context, ssh SSHSettings, prov provider.Provider, ref provider.Ref,
) (string, error) {
	line, err := sshkeys.Throwaway()
	if err != nil {
		return "", err
	}
	spec := ssh.specFor(prov, ref, []string{line})
	uctx, cancel := context.WithTimeout(ctx, ssh.installTimeout()+sshUpSlack)
	up, err := rootCommand(uctx, prov, ref, "ssh-up", sshSpecPath, spec)
	cancel()
	if err != nil {
		return "", err
	}
	host, err := sshkeys.HostKey(up.HostKey)
	if err != nil {
		return "", errors.New("ssh-up reported no usable host key")
	}
	return host, nil
}
