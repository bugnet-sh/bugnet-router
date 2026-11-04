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
