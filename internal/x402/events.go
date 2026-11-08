package x402

import (
	"errors"
	"regexp"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/solana"
)

// Codes holds XS 6.13's 37 codes, each a reason an x402 event may carry
var Codes = set(
	"path_refused", "path_not_enabled", "unpaid_cap", "upgrade_refused", "host_rate",
	"buffer_busy", "payments_unavailable", "upstream_unreachable", "x402_disabled",
	"account_closed", "service_not_enabled", "ref_conflict", "method_not_enabled",
	"path_not_paid", "body_too_large", "body_slow", "first_answer_slow", "not_x402",
	"version_unsupported", "network_unsupported", "splits_unsupported", "flow_unsupported",
	"ata_creation_unsupported", "fee_payer_refused", "payee_refused", "payee_changed",
	"payee_unreliable", "payto_blocked", "price_above_max", "allowance_spent", "rate_cap",
	"too_many_open_payments", "payto_daily_cap", "outflow_cap", "outflow_paused",
	"payment_rejected", "paid_no_answer",
)

// Statuses holds XS 8.2's statuses of an x402 event
var Statuses = set(
	"paid", "uncertain", "payment_failed", "free", "self_paid", "refused_cap",
	"refused_price", "refused_payee", "refused_network", "refused_terms", "refused_request",
	"refused_host", "not_paid", "upstream_error",
)

// moneyStatuses are the statuses whose rows always land, one row per call
var moneyStatuses = set("paid", "uncertain", "payment_failed")

// Foldable says whether a row can never involve money, so its count may pass 1
func Foldable(status string, amountMicros int64) bool {
	return Statuses[status] && !moneyStatuses[status] && amountMicros == 0
}

func set(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

var (
	refShape     = regexp.MustCompile(`^[a-z2-7]{16}$`)
	methodShape  = regexp.MustCompile(`^([A-Z]{1,10}|OTHER)$`)
	serviceShape = regexp.MustCompile(`^xs_[a-z2-7]{16}$`)
)

// The one asset, network, scheme and version an event may name
const (
	usdcMint     = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	solanaMain   = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
	exactScheme  = "exact"
	x402Version  = 2
	maxHostChars = 253
)

// emptyReason are the statuses whose reason may be empty
var emptyReason = set("paid", "free", "self_paid", "refused_host", "uncertain")

// CheckEvent answers nil when ev has XS 6.14's shape, never quoting a field
func CheckEvent(ev *routerv1.X402Event) error {
	status, reason := ev.GetStatus(), ev.GetReason()
	rules := []struct {
		name string
		ok   bool
	}{
		{"ref", refShape.MatchString(ev.GetRef())},
		{"payment_ref", ev.GetPaymentRef() == "" || refShape.MatchString(ev.GetPaymentRef())},
		{"status", Statuses[status]},
		{"host", len(ev.GetHost()) <= maxHostChars &&
			(ev.GetHost() != "" || status == "refused_host")},
		{"method", methodShape.MatchString(ev.GetMethod()) ||
			(ev.GetMethod() == "" && status == "refused_host")},
		{"path", ev.GetPath() != "" || status == "refused_host" ||
			reason == "path_refused" || reason == "upgrade_refused"},
		{"reason", reasonOK(status, reason)},
		{"asked_micros", ev.GetAskedMicros() >= 0},
		{"amount_micros", ev.GetAmountMicros() >= 0},
		{"asset", ev.GetAsset() == "" || ev.GetAsset() == usdcMint},
		{"network", ev.GetNetwork() == "" || ev.GetNetwork() == solanaMain},
		{"scheme", ev.GetScheme() == "" || ev.GetScheme() == exactScheme},
		{"x402_version", ev.GetX402Version() == 0 || ev.GetX402Version() == x402Version},
		{"pay_to", payToOK(ev.GetPayTo())},
		{"service_id", ev.GetServiceId() == "" || serviceShape.MatchString(ev.GetServiceId())},
		{"count", ev.GetCount() == 1 ||
			(ev.GetCount() > 1 && Foldable(status, ev.GetAmountMicros()))},
	}
	for _, r := range rules {
		if !r.ok {
			return errors.New(r.name)
		}
	}
	return nil
}

func reasonOK(status, reason string) bool {
	switch {
	case reason == "":
		return emptyReason[status]
	case status == "uncertain":
		return reason == "paid_no_answer"
	}
	return Codes[reason]
}

func payToOK(s string) bool {
	if s == "" {
		return true
	}
	_, ok := solana.PublicKey(s)
	return ok
}

// RefusalKey names the x402_refusals row an event adds to, false when it is not a refusal
func RefusalKey(status, reason string) (string, bool) {
	switch status {
	case "paid", "uncertain", "payment_failed", "free", "self_paid":
		return "", false
	case "refused_host":
		return "refused_host", true
	}
	return reason, true
}
