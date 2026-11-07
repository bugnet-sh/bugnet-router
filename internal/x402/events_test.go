package x402

import (
	"regexp"
	"slices"
	"testing"
)

// xsCodes is XS 6.13's table, in its order
var xsCodes = []string{
	"path_refused", "path_not_enabled", "unpaid_cap", "upgrade_refused", "host_rate",
	"buffer_busy", "payments_unavailable", "upstream_unreachable", "x402_disabled",
	"account_closed", "service_not_enabled", "ref_conflict", "method_not_enabled",
	"path_not_paid", "body_too_large", "body_slow", "first_answer_slow", "not_x402",
	"version_unsupported", "network_unsupported", "splits_unsupported", "flow_unsupported",
	"ata_creation_unsupported", "fee_payer_refused", "payee_refused", "payee_changed",
	"payee_unreliable", "payto_blocked", "price_above_max", "allowance_spent", "rate_cap",
	"too_many_open_payments", "payto_daily_cap", "outflow_cap", "outflow_paused",
	"payment_rejected", "paid_no_answer",
}

// xsStatuses is XS 8.2's table, in its order
var xsStatuses = []string{
	"paid", "uncertain", "payment_failed", "free", "self_paid", "refused_cap", "refused_price",
	"refused_payee", "refused_network", "refused_terms", "refused_request", "refused_host",
	"not_paid", "upstream_error",
}

// keys is the sorted keys of m whose value is true
func keys(m map[string]bool) []string {
	var out []string
	for k, v := range m {
		if v {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

func TestCodesAndStatuses(t *testing.T) {
	eq(t, len(xsCodes), 37)
	eq(t, len(Codes), 37)
	eq(t, keys(Codes), slices.Sorted(slices.Values(xsCodes)))
	eq(t, len(Statuses), 14)
	eq(t, keys(Statuses), slices.Sorted(slices.Values(xsStatuses)))
	short := regexp.MustCompile(`^[a-z0-9_]{1,32}$`)
	for c := range Codes {
		if !short.MatchString(c) {
			t.Errorf("code %q is not a short lower-case name", c)
		}
	}
	eq(t, Codes["resolved"] || Statuses["resolved"] || Statuses[""] || Codes[""], false)
}

func TestFoldable(t *testing.T) {
	for _, s := range []string{"paid", "uncertain", "payment_failed"} {
		eq(t, Foldable(s, 0), false)
		eq(t, Foldable(s, 10_000), false)
	}
	for _, s := range xsStatuses[3:] {
		eq(t, Foldable(s, 0), true)
		eq(t, Foldable(s, 1), false)
		eq(t, Foldable(s, 10_000), false)
		eq(t, Foldable(s, -1), false)
	}
	eq(t, Foldable("", 0), false)
	eq(t, Foldable("resolved", 0), false)
	eq(t, Foldable("PAID", 0), false)
}
