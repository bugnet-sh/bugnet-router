package saga

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
)

// A start token of 1053000 with allowance 1000000 writes 53000 (x402 plan Task X19's test)
func TestPaymentEventCarriesPackageShare(t *testing.T) {
	if got := packageShare(1_053_000, 1_000_000); got != 53_000 {
		t.Fatalf("package share %d, want 53000", got)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	const sig = "51puwr7wSkmx6bQAxKheZ4WLBtEsBoCNypUgB3RMh5NDHqj2e" +
		"BpsvvNbWDPwnNgfsGaxFp7vKAY9wjLa4pSgEzF2"
	token, err := ordertoken.Sign(priv, ordertoken.Claims{
		OrderID: "o1", Kind: ordertoken.KindStart, PackageID: "dev-small-10m",
		AmountMicros: 1_053_000, AllowanceMicros: 1_000_000, Tx: sig,
		ExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := ordertoken.Verify(pub, token, now)
	if err != nil {
		t.Fatal(err)
	}
	amount, tx := paymentClaims(c)
	if amount != 53_000 || tx != sig {
		t.Fatalf("claims gave %d and %q, want 53000 and %q", amount, tx, sig)
	}
}
