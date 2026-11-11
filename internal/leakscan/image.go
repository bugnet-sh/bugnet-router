package leakscan

import (
	"fmt"
	"io"
	"slices"
	"strings"
)

// imageAllow is the allowlist of an image scan, the provider's own variables and nothing of ours
func imageAllow(provider string) Allowlist {
	a := For(provider, false)
	a.Own = nil
	a.Allow = slices.DeleteFunc(a.Allow, func(x Allow) bool { return x.Shape == "private-key" })
	// Boat keeps its own sshd and its stock host keys until the live row IMG-BOAT3 decides
	if provider == "boat" {
		a.Allow = append(a.Allow, Allow{Shape: "private-key", Path: "etc/ssh/ssh_host_*"})
	}
	// E2B's MMDS answers in a fresh sandbox, and harden blocks it in every placement (DP N2)
	a.Metadata = provider == "e2b"
	// systemd hands its services this path to their own credentials, a directory and no secret
	a.Names = append(a.Names, "CREDENTIALS_DIRECTORY")
	return a
}

// ScanImage scans an image's archive, allowing only the provider rows and the CA certificate
//
// No Bugnet runtime file is allowed, any private key is a finding, and so is a variable whose
// name holds KEY, TOKEN, SECRET, PASSWORD or CREDENTIAL. A finding never holds a value.
func ScanImage(archive io.Reader, n Needles, provider string) ([]Finding, error) {
	res, err := ScanFull(archive, n, imageAllow(provider))
	if err != nil {
		return nil, err
	}
	out := res.Findings
	for _, name := range res.Names {
		out = append(out, Finding{Rule: "secret-like variable " + name, Path: "environ"})
	}
	slices.SortFunc(out, func(x, y Finding) int { return strings.Compare(x.String(), y.String()) })
	return out, nil
}

// FormatFindings prints one line per finding, rules and paths and never a value
func FormatFindings(f []Finding) string {
	var b strings.Builder
	for _, x := range f {
		fmt.Fprintln(&b, x.String())
	}
	return b.String()
}
