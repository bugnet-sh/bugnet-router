package hostrules

import (
	"errors"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/config"
)

func settings() (config.X402, config.Names) {
	return config.X402{
		SharedHostingSuffixes: []string{"workers.dev", "github.io"},
		AllowedSharedHosts:    []string{"api.workers.dev"},
		BlockedHosts:          []string{"bad.example.com"},
	}, config.Names{
		PreviewDomain: "preview.bugnet.sh", TransferHost: "transfer.bugnet.sh",
		SecretsHost: "secrets.bugnet.sh", EgressHost: "egress.dev.bugnet.sh",
	}
}

func TestNewTakesEveryInput(t *testing.T) {
	x, n := settings()
	r := New(x, n, "e2b.example.org")
	eq(t, r.BugnetNames, []string{"bugnet.sh"})
	eq(t, r.ProviderDomains, []string{
		"e2b.app", "e2b.dev", "e2b-juliett.dev", "boxd.sh", "koyeb.app", "boat.dev",
		"ascii.dev", "e2b.example.org",
	})
	eq(t, r.SharedSuffixes, x.SharedHostingSuffixes)
	eq(t, r.AllowedShared, x.AllowedSharedHosts)
	eq(t, r.Blocked, x.BlockedHosts)
	eq(t, New(x, n, "").ProviderDomains, New(x, n, "e2b.app").ProviderDomains)
}

func TestEveryBugnetNameIsRefused(t *testing.T) {
	x, n := settings()
	r := New(x, n, "")
	for _, host := range []string{
		"bugnet.sh", "mcp.bugnet.sh", "preview.bugnet.sh", "a.preview.bugnet.sh",
		"x.e2b-juliett.dev",
	} {
		refused(t, host, r)
	}
	eq(t, New(x, config.Names{}, "").BugnetNames, []string(nil))
}

func TestSecretAndX402HostsGetTheSameAnswer(t *testing.T) {
	x, n := settings()
	paid := New(x, n, "")
	secret := paid.ForSecrets()
	eq(t, secret.AllowedShared, []string(nil))
	eq(t, secret.Blocked, []string(nil))
	for _, host := range []string{
		"huggingface.co", "api.example.com", "co.uk", "Upper.com", "1.2.3.4", "e2b.dev",
		"preview.bugnet.sh", "other.workers.dev", "a.github.io", "a.b.example.com",
	} {
		a, b := Check(host, paid), Check(host, secret)
		if errors.Is(a, ErrRefused) != errors.Is(b, ErrRefused) {
			t.Errorf("%q: x402 %v, secret %v", host, a, b)
		}
	}
	// Only the allowed shared host and the blocked host differ
	eq(t, Check("api.workers.dev", paid), nil)
	refused(t, "api.workers.dev", secret)
	refused(t, "bad.example.com", paid)
	eq(t, Check("bad.example.com", secret), nil)
}
