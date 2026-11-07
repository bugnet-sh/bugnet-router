// Package x402 holds the router's per-service rules for x402 hosts and the event names
package x402

import (
	"crypto/sha256"
	"encoding/base32"
	"slices"
	"strings"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/hostrules"
	"github.com/oynozan/bugnet-router/internal/solana"
)

const (
	maxPrefixLen   = 200
	maxPaidPathLen = 400
)

// Spec is one service as X402ServiceSpec carries it
type Spec struct {
	Host, Prefix, Method string
	MaxPriceMicros       int64
	PayTo                string
	Indexed              bool
	PaidPath             string
}

// Refusal is a refused check, its Reason a name of XS 6.3
type Refusal struct{ Reason string }

// Error answers the reason, so a Refusal travels as an error
func (r Refusal) Error() string { return r.Reason }

// ServiceID is the id the gateway and Payments compute for the same method, host and prefix
func ServiceID(method, host, prefix string) string {
	sum := sha256.Sum256([]byte(method + "\x00" + host + "\x00" + prefix))
	b32 := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])
	return "xs_" + strings.ToLower(b32[:16])
}

// CheckSpec runs every per-service check of XS 6.3 that needs no placement
func CheckSpec(s Spec, maxPriceMicros int64, hosts hostrules.Rules) error {
	switch {
	case hostrules.Check(s.Host, hosts) != nil:
		return Refusal{"HOST_REFUSED"}
	case !prefixOK(s.Prefix), !paidPathOK(s):
		return Refusal{"PREFIX_REFUSED"}
	case s.Method != "GET" && s.Method != "POST":
		return Refusal{"METHOD_REFUSED"}
	case s.MaxPriceMicros <= 0 || s.MaxPriceMicros > maxPriceMicros:
		return Refusal{"PRICE_REFUSED"}
	}
	if _, ok := solana.PublicKey(s.PayTo); !ok {
		return Refusal{"PAYTO_REFUSED"}
	}
	return nil
}

// prefixOK is ^/.+/$ within maxPrefixLen and XS 6.7's path rules
func prefixOK(p string) bool {
	return len(p) >= 3 && len(p) <= maxPrefixLen && strings.HasSuffix(p, "/") && PathOK(p)
}

// paidPathOK says whether a paid path is set only when unindexed, valid and under its prefix
func paidPathOK(s Spec) bool {
	if s.Indexed {
		return s.PaidPath == ""
	}
	return s.PaidPath != "" && len(s.PaidPath) <= maxPaidPathLen && PathOK(s.PaidPath) &&
		strings.HasPrefix(s.PaidPath+"/", s.Prefix)
}

// Blocked says whether ResolveTunnel drops host and the answers show HOST_BLOCKED
func Blocked(host string, s config.X402) bool {
	return slices.Contains(s.BlockedHosts, host) ||
		(hostrules.UnderSuffix(host, s.SharedHostingSuffixes) &&
			!slices.Contains(s.AllowedSharedHosts, host))
}
