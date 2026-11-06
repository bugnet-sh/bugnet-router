// Package hostname holds the one rule for an exact host name in the settings and the catalog
package hostname

import (
	"net/netip"
	"regexp"
)

// labels is a lowercase DNS name of letters, digits and inner hyphens, with no final dot
var labels = regexp.MustCompile(
	`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$`)

// Valid says whether s is a lowercase DNS name of at most 253 bytes and not an IP address
func Valid(s string) bool {
	if _, err := netip.ParseAddr(s); err == nil {
		return false
	}
	return len(s) <= 253 && labels.MatchString(s)
}
