package saga

import (
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
)

// A start token of 1053000 with allowance 1000000 writes 53000 (x402 plan Task X19's test)
func TestPaymentEventCarriesPackageShare(t *testing.T) {
	if got := packageShare(1_053_000, 1_000_000); got != 53_000 {
		t.Fatalf("package share %d, want 53000", got)
	}
	// Until Task 28's claims land, the whole amount is the package's and the tx is empty
	amount, tx := paymentClaims(ordertoken.Claims{AmountMicros: 50_000})
	if amount != 50_000 || tx != "" {
		t.Fatalf("claims gave %d and %q, want 50000 and none", amount, tx)
	}
}
