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

// A start token of 1053000 with allowance 1000000 and fee 3000 writes 50000
func TestPaymentEventLeavesTheFeeOut(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	token, err := ordertoken.Sign(priv, ordertoken.Claims{
		OrderID: "o1", Kind: ordertoken.KindStart, PackageID: "dev-small-10m",
		AmountMicros: 1_053_000, AllowanceMicros: 1_000_000, FeeMicros: 3_000,
		ExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := ordertoken.Verify(pub, token, now)
	if err != nil {
		t.Fatal(err)
	}
	if amount, _ := paymentClaims(c); amount != 50_000 {
		t.Fatalf("payment event amount %d, want 50000", amount)
	}
}
