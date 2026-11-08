// Package secrets holds DP 10.3's rules for sealed secrets, which are validation, the presets,
// what a link may suggest and how a stored put is compared and shown.
//
// The router never sees a value, so Validate checks the spec alone and CheckValue only the sealed
// box's size and key id. Hosts and header names must already be canonical, and are refused
// rather than rewritten, so the sealed payload and the record always agree. Every refusal is a
// Refusal naming its Connect code and DP 11.3's reason, and never quotes a host or a name.
package secrets

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/hostrules"
)

// Limits of DP 10.3
const (
	MaxSealedBytes     = 70 << 10
	MaxHostsPerSecret  = 10
	MaxHostsPerSandbox = 32
	MaxHeaderNames     = 8
)

// Reasons of DP 11.3 that the rules answer
const (
	ReasonNameReserved   = "NAME_RESERVED"
	ReasonHostRefused    = "HOST_REFUSED"
	ReasonHostNotAllowed = "HOST_NOT_ALLOWED"
	ReasonHeaderRefused  = "HEADER_REFUSED"
	ReasonSealedTooLarge = "SEALED_TOO_LARGE"
	ReasonKeyUnknown     = "KEY_UNKNOWN"
	ReasonTooMany        = "TOO_MANY_SECRETS"
	ReasonHostInUse      = "HOST_IN_USE"
)

// Refusal is a refused secret, its Code and Reason as the API answers them
type Refusal struct {
	Code   connect.Code
	Reason string
	text   string
}

// Error is the refusal's message, which never quotes the spec
func (r *Refusal) Error() string { return r.text }

func refuse(code connect.Code, reason, format string, args ...any) *Refusal {
	return &Refusal{Code: code, Reason: reason, text: fmt.Sprintf(format, args...)}
}

// Context is what Validate needs beside the spec
type Context struct {
	// Hosts are the secret form of the host rules, hostrules.New's ForSecrets
	Hosts hostrules.Rules
	// CustomHosts is secrets.custom_hosts, false refusing hosts outside every preset
	CustomHosts bool
	// EgressAllow is the package's egress.allow, the package unrestricted when empty
	EgressAllow []string
}

var (
	namePattern   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
	headerPattern = regexp.MustCompile("^[a-z0-9!#$%&'*+.^_`|~-]+$")
)

// reservedNames are refused in any case
var reservedNames = []string{
	"PATH", "HOME", "SHELL", "USER", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "AWS_CA_BUNDLE",
	"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "BASH_ENV", "ENV",
}

// reservedPrefixes are refused in any case
var reservedPrefixes = []string{
	"LD_", "BUGNET_", "SSL_", "NODE_", "GIT_", "PIP_", "UV_", "DENO_", "NPM_CONFIG_", "PYTHON",
	"__VITE_",
}

// refusedHeaders are the header names a placeholder may never ride in
var refusedHeaders = []string{
	"host", "content-length", "transfer-encoding", "connection", "upgrade", "te", "trailer",
	"cookie", "set-cookie", "referer", "user-agent", "origin", "forwarded", "via",
}

// refusedHeaderPrefixes cover proxy-* and x-forwarded-*
var refusedHeaderPrefixes = []string{"proxy-", "x-forwarded-"}

// Validate checks a spec against DP 10.3's rules, the per-placement counts left to Fit
func Validate(spec *routerv1.SecretSpec, c Context) error {
	if err := CheckName(spec.GetName()); err != nil {
		return err
	}
	if err := checkHosts(spec.GetHosts(), c); err != nil {
		return err
	}
	return checkHeaders(spec.GetHeaderNames())
}

// CheckName refuses a name that is malformed or reserved
func CheckName(name string) error {
	if !namePattern.MatchString(name) {
		return refuse(connect.CodeInvalidArgument, ReasonNameReserved,
			"a secret name is a letter or _ then up to 63 letters, digits or _")
	}
	upper := strings.ToUpper(name)
	if slices.Contains(reservedNames, upper) ||
		slices.ContainsFunc(reservedPrefixes, func(p string) bool {
			return strings.HasPrefix(upper, p)
		}) {
		return refuse(connect.CodeInvalidArgument, ReasonNameReserved,
			"that secret name is reserved, since the sandbox's tools read it")
	}
	return nil
}

func checkHosts(hosts []string, c Context) error {
	switch {
	case len(hosts) == 0:
		return refuse(connect.CodeInvalidArgument, ReasonHostRefused,
			"a secret needs at least one host")
	case len(hosts) > MaxHostsPerSecret:
		return refuse(connect.CodeResourceExhausted, ReasonTooMany,
			"a secret has at most %d hosts", MaxHostsPerSecret)
	}
	for i, h := range hosts {
		if slices.Contains(hosts[:i], h) {
			return refuse(connect.CodeInvalidArgument, ReasonHostRefused,
				"host %d repeats an earlier host", i+1)
		}
		if hostrules.Check(h, c.Hosts) != nil {
			return refuse(connect.CodeInvalidArgument, ReasonHostRefused,
				"host %d is refused: hosts are exact lowercase names, never an IP address, "+
					"a public suffix, a shared-hosting name, a provider's or Bugnet's own", i+1)
		}
	}
	for i, h := range hosts {
		if Custom(h) && !c.CustomHosts {
			return refuse(connect.CodeFailedPrecondition, ReasonHostNotAllowed,
				"host %d is outside every preset, and this router allows no other hosts", i+1)
		}
		if len(c.EgressAllow) > 0 && !slices.Contains(c.EgressAllow, h) {
			return refuse(connect.CodeFailedPrecondition, ReasonHostNotAllowed,
				"host %d is outside the hosts this package may reach", i+1)
		}
	}
	return nil
}

func checkHeaders(names []string) error {
	if len(names) > MaxHeaderNames {
		return refuse(connect.CodeResourceExhausted, ReasonTooMany,
			"a secret has at most %d header names", MaxHeaderNames)
	}
	for i, h := range names {
		refused := !headerPattern.MatchString(h) || slices.Contains(names[:i], h) ||
			slices.Contains(refusedHeaders, h) ||
			slices.ContainsFunc(refusedHeaderPrefixes, func(p string) bool {
				return strings.HasPrefix(h, p)
			})
		if refused {
			return refuse(connect.CodeInvalidArgument, ReasonHeaderRefused,
				"header name %d is refused: names are lowercase, each once, and never one "+
					"the proxy or the browser sets", i+1)
		}
	}
	return nil
}

// CheckValue refuses an empty or oversized box, or one sealed to a key the proxy does not hold
func CheckValue(v *routerv1.SealedValue, keys []config.BoxKey) error {
	if n := len(v.GetBox()); n == 0 || n > MaxSealedBytes {
		return refuse(connect.CodeInvalidArgument, ReasonSealedTooLarge,
			"the sealed value must hold 1 byte to 70 KiB")
	}
	if !slices.ContainsFunc(keys, func(k config.BoxKey) bool { return k.ID == v.GetKeyId() }) {
		return refuse(connect.CodeInvalidArgument, ReasonKeyUnknown,
			"the value is sealed to a key that is not current, fetch the sealing keys again")
	}
	return nil
}

// Held is a secret the placement already holds, for Fit
type Held struct {
	Name  string
	Hosts []string
}

// CheckInUse refuses a spec with a host that a live x402 service of the placement uses
func CheckInUse(spec *routerv1.SecretSpec, x402Hosts []string) error {
	if slices.ContainsFunc(spec.GetHosts(), func(h string) bool {
		return slices.Contains(x402Hosts, h)
	}) {
		return refuse(connect.CodeFailedPrecondition, ReasonHostInUse,
			"x402 pays for a host of this secret in this sandbox, stop paying it first")
	}
	return nil
}

// Fit refuses a spec that would take the placement past max secrets or 32 hosts in all,
// x402Hosts being the hosts of the placement's live x402 services
func Fit(spec *routerv1.SecretSpec, held []Held, x402Hosts []string, most int) error {
	count := 1
	all := slices.Concat(spec.GetHosts(), x402Hosts)
	for _, h := range held {
		if h.Name == spec.GetName() {
			continue
		}
		count++
		all = append(all, h.Hosts...)
	}
	slices.Sort(all)
	switch {
	case count > most:
		return refuse(connect.CodeResourceExhausted, ReasonTooMany,
			"a sandbox holds at most %d secrets", most)
	case len(slices.Compact(all)) > MaxHostsPerSandbox:
		return refuse(connect.CodeResourceExhausted, ReasonTooMany,
			"a sandbox's secrets reach at most %d hosts in all", MaxHostsPerSandbox)
	}
	return nil
}
