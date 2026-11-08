package boat

import (
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestBoatAuthorizeNativeSSH(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	s := f.add(nil)
	ref := newRef(s.id, s.created)
	const line = "restrict ssh-ed25519 AAAA bn-gw"
	got, err := p.AuthorizeNativeSSH(t.Context(), ref, line)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got, provider.NativeSSH{
		Address: "203.0.113.10:22", User: "user", HostKey: fakeHostKey,
	})
	eq(t, f.sentSSHKeys(), []string{line})

	f.setSSHReply(map[string]any{
		"ok": true, "machineIp": nil, "sshEndpoint": "198.51.100.7:22001", "sshUser": "user",
		"hostKey": fakeHostKey,
	})
	got, err = p.AuthorizeNativeSSH(t.Context(), ref, line)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.Address, "198.51.100.7:22001")
}

func TestBoatAuthorizeNativeSSHRefusesOddReplies(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	s := f.add(nil)
	ref := newRef(s.id, s.created)
	for name, reply := range map[string]map[string]any{
		"no address":   {"ok": true, "sshUser": "user", "hostKey": fakeHostKey},
		"bad address":  {"ok": true, "machineIp": "secret.example", "sshUser": "user"},
		"bad endpoint": {"ok": true, "sshEndpoint": "198.51.100.7", "sshUser": "user"},
		"odd user":     {"ok": true, "machineIp": "203.0.113.10", "sshUser": "-oProxy"},
		"no host key":  {"ok": true, "machineIp": "203.0.113.10", "sshUser": "user"},
		"two host keys": {"ok": true, "machineIp": "203.0.113.10", "sshUser": "user",
			"hostKey": fakeHostKey + "\n" + fakeHostKey},
	} {
		t.Run(name, func(t *testing.T) {
			f.setSSHReply(reply)
			_, err := p.AuthorizeNativeSSH(t.Context(), ref, "k bn-gw")
			if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInternal {
				t.Fatalf("reply gave %v, want internal", err)
			}
			if strings.Contains(err.Error(), "secret.example") ||
				strings.Contains(err.Error(), "-oProxy") {
				t.Fatalf("the error quotes the reply: %v", err)
			}
		})
	}
}
