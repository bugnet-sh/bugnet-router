package x402

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
