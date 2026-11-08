package ordertoken

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

func keys(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func start() Claims {
	return Claims{
		OrderID: "o1", Kind: KindStart, PackageID: "cpu-small-10m",
		AmountMicros: 50000, ExpiresAt: now.Add(time.Minute),
	}
}

func TestTokenRoundTrip(t *testing.T) {
	pub, priv := keys(t)
	c := start()
	tok, err := Sign(priv, c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Verify(pub, tok, now)
	if err != nil {
		t.Fatal(err)
	}
	if got != c {
		t.Fatalf("got %+v want %+v", got, c)
	}
	e := start()
	e.Kind, e.HandleSHA256 = KindExtend, "ab"
	tok, _ = Sign(priv, e)
	if got, err = Verify(pub, tok, now); err != nil || got != e {
		t.Fatalf("extend: %+v %v", got, err)
	}
}

func TestTokenExpired(t *testing.T) {
	pub, priv := keys(t)
	tok, _ := Sign(priv, start())
	if _, err := Verify(pub, tok, now.Add(time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("at expiry: %v", err)
	}
	if _, err := Verify(pub, tok, now.Add(time.Minute-time.Nanosecond)); err != nil {
		t.Fatalf("before expiry: %v", err)
	}
}

func TestTokenTampered(t *testing.T) {
	pub, priv := keys(t)
	tok, _ := Sign(priv, start())
	parts := strings.Split(tok, ".")
	p := []byte(parts[1])
	if p[3] == 'A' {
		p[3] = 'B'
	} else {
		p[3] = 'A'
	}
	parts[1] = string(p)
	if _, err := Verify(pub, strings.Join(parts, "."), now); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("got %v", err)
	}
}

func TestTokenWrongKey(t *testing.T) {
	_, priv := keys(t)
	other, _ := keys(t)
	tok, _ := Sign(priv, start())
	if _, err := Verify(other, tok, now); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("got %v", err)
	}
}

func TestTokenMalformed(t *testing.T) {
	pub, _ := keys(t)
	for _, tok := range []string{"", "v2.a.b", "v1.a", "v1.a.b.c", "v1.a.!!!"} {
		if _, err := Verify(pub, tok, now); !errors.Is(err, ErrMalformed) {
			t.Errorf("%q: got %v", tok, err)
		}
	}
}

func signRaw(priv ed25519.PrivateKey, payload string) string {
	seg := base64.RawURLEncoding.EncodeToString([]byte(payload))
	sig := ed25519.Sign(priv, []byte("bugnet-order-v1."+seg))
	return "v1." + seg + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestTokenClaimChecks(t *testing.T) {
	pub, priv := keys(t)
	exp := `"expires_at":"2026-11-04T12:01:00Z"`
	cases := []struct {
		name, payload string
		want          error
	}{
		{"bad kind", `{"kind":"refund",` + exp + `}`, ErrBadKind},
		{"empty kind", `{` + exp + `}`, ErrBadKind},
		{"extend without handle", `{"kind":"extend",` + exp + `}`, ErrMalformed},
		{"unknown field", `{"kind":"start","extra":1,` + exp + `}`, ErrMalformed},
		{"not json", `nope`, ErrMalformed},
	}
	for _, c := range cases {
		if _, err := Verify(pub, signRaw(priv, c.payload), now); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v want %v", c.name, err, c.want)
		}
	}
}

const (
	payerAddr = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	feeAddr   = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	// txSig is base58 of 64 bytes, 88 characters long
	txSig = "51puwr7wSkmx6bQAxKheZ4WLBtEsBoCNypUgB3RMh5NDHqj2e" +
		"BpsvvNbWDPwnNgfsGaxFp7vKAY9wjLa4pSgEzF2"
	// shortTx is base58 of 64 bytes, two of them leading zeros, 86 characters long
	shortTx = "114b6fqHc3c1Tj3tAi3m9QXPNFDkk3uGJ8QYEMYTfPzFaKTsK" +
		"5yAfAd2HzL4BzrrHD1uudP4zC8XwTnPqWztkV"
)

func TestVerifyPayerClaims(t *testing.T) {
	pub, priv := keys(t)
	c := start()
	tok, _ := Sign(priv, c)
	if got, err := Verify(pub, tok, now); err != nil || got.Payer != "" {
		t.Fatalf("without a payer: %+v %v", got, err)
	}
	c.Payer, c.FeePayer, c.Tx = payerAddr, feeAddr, txSig
	tok, _ = Sign(priv, c)
	if got, err := Verify(pub, tok, now); err != nil || got != c {
		t.Fatalf("with a payer: %+v %v", got, err)
	}
	for name, bad := range map[string]func(*Claims){
		"payer too short":  func(c *Claims) { c.Payer = payerAddr[:31] },
		"payer with 0":     func(c *Claims) { c.Payer = "0" + payerAddr[1:] },
		"fee payer with l": func(c *Claims) { c.FeePayer = "l" + feeAddr[1:] },
		"tx of 63 bytes":   func(c *Claims) { c.Tx = txSig[:86] },
		"tx not base58":    func(c *Claims) { c.Tx = "O" + txSig[1:] },
		"tx is an address": func(c *Claims) { c.Tx = payerAddr },
	} {
		c := start()
		c.Payer, c.FeePayer, c.Tx = payerAddr, feeAddr, txSig
		bad(&c)
		tok, _ := Sign(priv, c)
		if _, err := Verify(pub, tok, now); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: got %v, want ErrMalformed", name, err)
		}
	}
}

func TestTxClaimByDecodingNotLength(t *testing.T) {
	pub, priv := keys(t)
	c := start()
	c.Tx = shortTx
	tok, _ := Sign(priv, c)
	if len(shortTx) != 86 {
		t.Fatal("shortTx must be 86 characters")
	}
	if _, err := Verify(pub, tok, now); err != nil {
		t.Fatalf("an 86-character signature: %v", err)
	}
}

func TestAllowanceClaimStartOnly(t *testing.T) {
	pub, priv := keys(t)
	e := start()
	e.Kind, e.HandleSHA256, e.AllowanceMicros = KindExtend, "ab", 1000
	tok, _ := Sign(priv, e)
	if _, err := Verify(pub, tok, now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("extend with an allowance: %v", err)
	}
	s := start()
	s.AllowanceMicros = 1000
	tok, _ = Sign(priv, s)
	if got, err := Verify(pub, tok, now); err != nil || got.AllowanceMicros != 1000 {
		t.Fatalf("start with an allowance: %+v %v", got, err)
	}
}

func TestAllowanceClaimBounds(t *testing.T) {
	pub, priv := keys(t)
	for _, c := range []struct {
		allowance int64
		ok        bool
	}{{-1, false}, {50001, false}, {0, true}, {50000, true}} {
		s := start()
		s.AllowanceMicros = c.allowance
		tok, _ := Sign(priv, s)
		_, err := Verify(pub, tok, now)
		if c.ok != (err == nil) || !c.ok && !errors.Is(err, ErrMalformed) {
			t.Errorf("allowance %d: %v", c.allowance, err)
		}
	}
}

func TestUnknownClaimsStillRefused(t *testing.T) {
	pub, priv := keys(t)
	exp := `"expires_at":"2026-11-04T12:01:00Z"`
	for _, payload := range []string{
		`{"kind":"start","issued_at":"2026-11-04T12:00:00Z",` + exp + `}`,
		`{"kind":"start","payer":"` + payerAddr + `","allowance":1,` + exp + `}`,
		`{"kind":"start","fee_micros":1,` + exp + `}`,
	} {
		if _, err := Verify(pub, signRaw(priv, payload), now); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: got %v, want ErrMalformed", payload, err)
		}
	}
}
