package secrets

import (
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/config"
)

func TestNameRules(t *testing.T) {
	for _, n := range []string{"PATH", "ld_preload", "BUGNET_X", "ssl_cert_file", "http_proxy",
		"GIT_ASKPASS", "pip_index_url", "UV_X", "DENO_X", "npm_config_x", "PYTHONPATH",
		"BASH_ENV", "ENV", "__VITE_X", "HOME", "shell", "User", "NODE_OPTIONS",
		"REQUESTS_CA_BUNDLE", "curl_ca_bundle", "AWS_CA_BUNDLE", "HTTPS_PROXY", "all_proxy",
		"NO_PROXY", "LD_LIBRARY_PATH"} {
		mustCode(t, validate(t, name(n)), connect.CodeInvalidArgument, "NAME_RESERVED")
	}
	for _, n := range []string{"", "1ABC", "A-B", "A B", "É", strings.Repeat("A", 65)} {
		mustCode(t, validate(t, name(n)), connect.CodeInvalidArgument, "NAME_RESERVED")
	}
	for _, n := range []string{"HF_TOKEN", "_X", "a", "ENVIRONMENT", "PYTHO", "GITHUB_TOKEN",
		"NPM_TOKEN", strings.Repeat("A", 64)} {
		eq(t, validate(t, name(n)), nil)
	}
}

func TestHostRules(t *testing.T) {
	for _, h := range []string{"HuggingFace.co", "a.example.", "1.2.3.4", "*.example.com",
		"example.com:443", "co.uk", "e2b.app", "x.boxd.sh", "preview.bugnet.sh",
		"mcp.bugnet.sh", "https://example.com", "example.com/a", "[::1]", "x.workers.dev", ""} {
		mustCode(t, validate(t, hosts(h)), connect.CodeInvalidArgument, "HOST_REFUSED")
	}
	mustCode(t, validate(t, hosts()), connect.CodeInvalidArgument, "HOST_REFUSED")
	mustCode(t, validate(t, hosts("a.example.com", "a.example.com")),
		connect.CodeInvalidArgument, "HOST_REFUSED")
	err := validate(t, hosts("api.example.com"), customHostsOff)
	mustCode(t, err, connect.CodeFailedPrecondition, "HOST_NOT_ALLOWED")
	eq(t, validate(t, hosts("huggingface.co"), customHostsOff), nil)
	eq(t, validate(t, hosts("api.example.com", "huggingface.co")), nil)
	eq(t, validate(t, hosts("xn--bcher-kva.example")), nil)
}

// with egress.allow on the package, a secret host outside it is refused (X12)
func TestRestrictedPackageHostMustBeAllowed(t *testing.T) {
	err := validate(t, hosts("huggingface.co"), allow("api.github.com"))
	mustCode(t, err, connect.CodeFailedPrecondition, "HOST_NOT_ALLOWED")
	eq(t, validate(t, hosts("huggingface.co"), allow("api.github.com", "huggingface.co")), nil)
	eq(t, validate(t, hosts("huggingface.co"), allow()), nil)
	// A refused host is HOST_REFUSED before the allowlist is read
	mustCode(t, validate(t, hosts("e2b.app"), allow("api.github.com")),
		connect.CodeInvalidArgument, "HOST_REFUSED")
}

func TestHeaderRules(t *testing.T) {
	for _, h := range []string{"cookie", "x-forwarded-for", "X-Api-Key", "host",
		"content-length", "transfer-encoding", "connection", "upgrade", "te", "trailer",
		"proxy-authorization", "set-cookie", "referer", "user-agent", "origin", "forwarded",
		"x-forwarded-host", "via", ":authority", "", "x api", "x-api-key\n"} {
		mustCode(t, validate(t, headers(h)), connect.CodeInvalidArgument, "HEADER_REFUSED")
	}
	mustCode(t, validate(t, headers("x-api-key", "x-api-key")), connect.CodeInvalidArgument,
		"HEADER_REFUSED")
	eq(t, validate(t, headers("x-api-key", "authorization", "private-token")), nil)
}

func TestSizeRules(t *testing.T) {
	ten := make([]string, 11)
	for i := range ten {
		ten[i] = fmt.Sprintf("h%d.example.com", i)
	}
	mustCode(t, validate(t, hosts(ten...)), connect.CodeResourceExhausted, "TOO_MANY_SECRETS")
	eq(t, validate(t, hosts(ten[:10]...)), nil)
	nine := []string{"a-1", "a-2", "a-3", "a-4", "a-5", "a-6", "a-7", "a-8", "a-9"}
	mustCode(t, validate(t, headers(nine...)), connect.CodeResourceExhausted,
		"TOO_MANY_SECRETS")
	eq(t, validate(t, headers(nine[:8]...)), nil)

	keys := []config.BoxKey{{ID: "k2", Key: make([]byte, 32)}, {ID: "k1", Key: make([]byte, 32)}}
	value := func(id string, n int) *routerv1.SealedValue {
		return &routerv1.SealedValue{KeyId: id, Box: make([]byte, n)}
	}
	eq(t, CheckValue(value("k1", 70<<10), keys), nil)
	mustCode(t, CheckValue(value("k1", 70<<10+1), keys), connect.CodeInvalidArgument,
		"SEALED_TOO_LARGE")
	mustCode(t, CheckValue(value("k1", 0), keys), connect.CodeInvalidArgument,
		"SEALED_TOO_LARGE")
	mustCode(t, CheckValue(nil, keys), connect.CodeInvalidArgument, "SEALED_TOO_LARGE")
	// a key id not in secrets.box_public_keys -> KEY_UNKNOWN
	mustCode(t, CheckValue(value("k3", 100), keys), connect.CodeInvalidArgument, "KEY_UNKNOWN")
	mustCode(t, CheckValue(value("k1", 100), nil), connect.CodeInvalidArgument, "KEY_UNKNOWN")
}

func TestPlacementLimits(t *testing.T) {
	var held []Held
	for i := range 20 {
		held = append(held, Held{Name: fmt.Sprintf("S%d", i),
			Hosts: []string{fmt.Sprintf("h%d.example.com", i%16)}})
	}
	spec := &routerv1.SecretSpec{Name: "NEW", Hosts: []string{"h1.example.com"}}
	// 21 secrets
	mustCode(t, Fit(spec, held, nil, 20), connect.CodeResourceExhausted, "TOO_MANY_SECRETS")
	// Replacing one of the 20 keeps 20
	spec.Name = "S3"
	eq(t, Fit(spec, held, nil, 20), nil)
	// The others hold 16 hosts, so 17 new ones make 33 in all
	spec.Hosts = []string{}
	for i := 16; i < 33; i++ {
		spec.Hosts = append(spec.Hosts, fmt.Sprintf("h%d.example.com", i))
	}
	mustCode(t, Fit(spec, held, nil, 20), connect.CodeResourceExhausted, "TOO_MANY_SECRETS")
	spec.Hosts = spec.Hosts[:16]
	eq(t, Fit(spec, held, nil, 20), nil)
	// A host the others hold counts once
	spec.Hosts = append(spec.Hosts, "h0.example.com")
	eq(t, Fit(spec, held, nil, 20), nil)
}

func TestX402HostsCountTowardThirtyTwo(t *testing.T) {
	var held []Held
	for i := range 27 {
		held = append(held, Held{Name: fmt.Sprintf("S%d", i),
			Hosts: []string{fmt.Sprintf("h%d.example.com", i)}})
	}
	live := []string{"a1.example.com", "a2.example.com", "a3.example.com", "a4.example.com",
		"a5.example.com"}
	spec := &routerv1.SecretSpec{Name: "S0", Hosts: []string{"h0.example.com"}}
	eq(t, Fit(spec, held, live, 30), nil)
	spec.Hosts = []string{"h0.example.com", "extra.example.com"}
	mustCode(t, Fit(spec, held, live, 30), connect.CodeResourceExhausted, "TOO_MANY_SECRETS")
}

func TestCheckInUse(t *testing.T) {
	live := []string{"api.example.com"}
	spec := &routerv1.SecretSpec{Name: "K", Hosts: []string{"b.example.com", "api.example.com"}}
	mustCode(t, CheckInUse(spec, live), connect.CodeFailedPrecondition, "HOST_IN_USE")
	spec.Hosts = []string{"b.example.com"}
	eq(t, CheckInUse(spec, live), nil)
}
