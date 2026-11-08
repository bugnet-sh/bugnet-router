// Package hostrules holds DP 10.3's host rules, the one set secret hosts and x402 hosts share
package hostrules

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"golang.org/x/net/publicsuffix"

	"github.com/bugnet-sh/bugnet-router/internal/hostname"
)

// ErrRefused is the error Check answers for every host it refuses
var ErrRefused = errors.New("hostrules: host refused")

// Rules are DP 10.3's host rules, shared by secret hosts and x402 hosts
type Rules struct {
	BugnetNames     []string
	ProviderDomains []string
	SharedSuffixes  []string
	// AllowedShared and Blocked are empty for secret hosts
	AllowedShared []string
	Blocked       []string
}

// Check answers ErrRefused when host fails a rule, never quoting the host
func Check(host string, r Rules) error {
	switch {
	case !hostname.Valid(host):
		return refuse("not an exact lowercase host name")
	case isPublicSuffix(host):
		return refuse("a public suffix")
	case UnderSuffix(host, r.BugnetNames):
		return refuse("a Bugnet name")
	case UnderSuffix(host, r.ProviderDomains):
		return refuse("under a provider domain")
	case slices.Contains(r.Blocked, host):
		return refuse("blocked")
	case UnderSuffix(host, r.SharedSuffixes) && !slices.Contains(r.AllowedShared, host):
		return refuse("under a shared-hosting suffix")
	}
	return nil
}

// UnderSuffix says whether host equals or sits under one of suffixes
func UnderSuffix(host string, suffixes []string) bool {
	for _, s := range suffixes {
		if host == s || strings.HasSuffix(host, "."+s) {
			return true
		}
	}
	return false
}

// isPublicSuffix says whether host is itself a public suffix, ICANN or private
func isPublicSuffix(host string) bool {
	suffix, _ := publicsuffix.PublicSuffix(host)
	return suffix == host
}

func refuse(rule string) error {
	return fmt.Errorf("%w: %s", ErrRefused, rule)
}
