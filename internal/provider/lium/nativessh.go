package lium

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var _ provider.NativeSSHer = (*Provider)(nil)

const (
	// podAuthorizedKeys is root's key file in the pod, where Lium writes the router's key
	podAuthorizedKeys = "/root/.ssh/authorized_keys"
	// maxKeyLine bounds a gateway key line, an ed25519 key with its options being about 200 bytes
	maxKeyLine = 8 << 10
	// appendKeyScript adds $2 to the key file $1 on a line of its own unless a line equals it
	appendKeyScript = `umask 077; f=$1
mkdir -p -- "${f%/*}" && touch -- "$f" || exit 1
grep -qxF -e "$2" -- "$f" && exit 0
if [ -s "$f" ] && [ -n "$(tail -c 1 -- "$f")" ]; then printf '\n' >> "$f" || exit 1; fi
printf '%s\n' "$2" >> "$f"`
)

// AuthorizeNativeSSH adds the gateway's key line to root's keys in the pod and answers its sshd
func (p *Provider) AuthorizeNativeSSH(
	ctx context.Context, ref provider.Ref, gatewayKey string,
) (provider.NativeSSH, error) {
	id, ok := podID(ref)
	if !ok {
		return provider.NativeSSH{}, notLium(ref)
	}
	if err := keyLine(gatewayKey); err != nil {
		return provider.NativeSSH{}, invalid(err, false)
	}
	d, err := p.getPod(ctx, id)
	if err != nil {
		return provider.NativeSSH{}, err
	}
	// The address is read again, so a pod Lium now places on a private network gets no key
	if addr, err := netip.ParseAddr(d.Executor.Address); err != nil || p.refuses(addr) {
		return provider.NativeSSH{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium pod %s is at %q, an address no public host has", id,
				d.Executor.Address)}
	}
	name, addr, err := p.nativeTarget(ref, d)
	if err != nil {
		return provider.NativeSSH{}, err
	}
	if err := p.appendKey(ctx, ref, gatewayKey); err != nil {
		return provider.NativeSSH{}, err
	}
	pin, err := p.knownPin(ref, name, addr)
	if err != nil {
		return provider.NativeSSH{}, err
	}
	if pin == nil {
		return provider.NativeSSH{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium pod %s has no pinned host key", id)}
	}
	return provider.NativeSSH{Address: addr, User: sshUser, HostKey: authorizedKey(pin)}, nil
}

// keyLine refuses anything but one authorized_keys line, so the pod's file gains one key alone
func keyLine(line string) error {
	if len(line) > maxKeyLine || strings.ContainsAny(line, "\r\n\x00") {
		return errors.New("the gateway key is not one line")
	}
	_, _, _, rest, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil || len(bytes.TrimSpace(rest)) > 0 {
		return errors.New("the gateway key is not one authorized_keys line")
	}
	return nil
}

// nativeTarget is the pod name and the sshd's address from Ref.Meta, else from the pod read
func (p *Provider) nativeTarget(ref provider.Ref, d podDetail) (string, string, error) {
	name, addr, err := p.metaTarget(ref)
	if err != nil {
		return "", "", err
	}
	name = cmp.Or(name, d.Name)
	if addr != "" {
		return name, addr, nil
	}
	port, ok := d.sshPortOf()
	if !ok {
		return "", "", &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium pod %s has no SSH port", ref.ID)}
	}
	if addr, err = p.sshAddr(d.Executor.Address, strconv.Itoa(port)); err != nil {
		return "", "", &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium pod %s: %w", ref.ID, err)}
	}
	return name, addr, nil
}

// appendKey runs appendKeyScript on the pod as root
func (p *Provider) appendKey(ctx context.Context, ref provider.Ref, line string) error {
	s, err := p.Exec(ctx, ref, provider.ExecSpec{
		Argv: []string{"sh", "-c", appendKeyScript, "sh", p.authorizedKeys, line},
	})
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(s, maxErrorBytes))
	code, err := s.Wait()
	if err != nil {
		return err
	}
	if code != 0 {
		return &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"adding the gateway key to %s in lium pod %s exited %d", p.authorizedKeys, ref.ID,
			code)}
	}
	return nil
}
