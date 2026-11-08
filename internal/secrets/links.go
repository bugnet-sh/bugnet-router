package secrets

import (
	"slices"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
)

// maxKey bounds a request id, submission id or idempotency key
const maxKey = 128

// Suggest keeps a name and only its preset's hosts, so an agent never prefills a custom host
func Suggest(name string, hosts []string) *routerv1.SecretSpec {
	out := &routerv1.SecretSpec{Name: name}
	p, ok := PresetOf(name)
	if !ok {
		return out
	}
	for _, h := range hosts {
		if slices.Contains(p.Hosts, h) && !slices.Contains(out.Hosts, h) {
			out.Hosts = append(out.Hosts, h)
		}
	}
	// A preset name given no hosts suggests its whole preset
	if len(hosts) == 0 {
		out.Hosts = slices.Clone(p.Hosts)
	}
	if len(out.Hosts) > 0 {
		out.HeaderNames = slices.Clone(p.Headers)
	}
	return out
}

// LinkURL is the page a link token opens on the secrets host
func LinkURL(host, token string) string {
	return "https://" + host + "/l/" + token
}

// ValidKey accepts a request id, submission id or idempotency key of 1 to 128 printable ASCII
func ValidKey(key string) bool {
	if key == "" || len(key) > maxKey {
		return false
	}
	for i := range len(key) {
		if key[i] < 0x20 || key[i] > 0x7e {
			return false
		}
	}
	return true
}
