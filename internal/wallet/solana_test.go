package wallet

import (
	"errors"
	"math/big"
	"reflect"
	"strings"
	"testing"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

const alphabetRef = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// base58 encodes n bytes whose first is nonzero, through math/big
func base58(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(7*i + 1)
	}
	return encode(b)
}

func encode(b []byte) string {
	zeros := 0
	for zeros < len(b) && b[zeros] == 0 {
		zeros++
	}
	v := new(big.Int).SetBytes(b)
	base, mod := big.NewInt(58), new(big.Int)
	var out []byte
	for v.Sign() > 0 {
		v.DivMod(v, base, mod)
		out = append(out, alphabetRef[mod.Int64()])
	}
	out = append(out, strings.Repeat("1", zeros)...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

func ok(_ string, err error) bool { return err == nil }

// nonCanonical is 44 characters whose value needs more than 32 bytes
var nonCanonical = strings.Repeat("z", 44)

func TestParseSolana(t *testing.T) {
	eq(t, ok(ParseSolana("11111111111111111111111111111111")), true) // 32 zero bytes
	eq(t, ok(ParseSolana(base58(32))), true)
	lead := make([]byte, 32)
	lead[3] = 9
	withOnes := encode(lead)
	eq(t, strings.HasPrefix(withOnes, "111"), true)
	eq(t, ok(ParseSolana(withOnes)), true)
	eq(t, ok(ParseSolana("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")), true)
	for _, s := range []string{base58(31), base58(33), nonCanonical, "0" + base58(32)[1:],
		"O" + base58(32)[1:], "I" + base58(32)[1:], "l" + base58(32)[1:], "",
		"1" + withOnes, withOnes[1:]} {
		_, err := ParseSolana(s)
		if !errors.Is(err, ErrBadAddress) {
			t.Errorf("%q: got %v, want ErrBadAddress", s, err)
		}
	}
}

func TestPayerDecision(t *testing.T) {
	user, fee, listed, seen := base58(32), encode(append([]byte{2}, make([]byte, 31)...)),
		encode(append([]byte{3}, make([]byte, 31)...)),
		encode(append([]byte{4}, make([]byte, 31)...))
	on := Guard{UsePayer: true, Facilitators: []string{listed}}
	cases := []struct {
		name  string
		g     Guard
		c     Claim
		payer string
		why   Reason
	}{
		{"1 use_payer off", Guard{Facilitators: []string{listed}},
			Claim{Payer: user, FeePayer: fee}, "", AbsentOff},
		{"no payer claimed", on, Claim{FeePayer: fee}, "", AbsentNoClaim},
		{"2 payer is the fee payer", on, Claim{Payer: fee, FeePayer: fee}, "",
			AbsentIsFeePayer},
		{"3 payer is listed", on, Claim{Payer: listed, FeePayer: fee}, "", AbsentFacilitator},
		{"3 payer came as a fee payer before", on, Claim{Payer: seen, FeePayer: fee}, "",
			AbsentFacilitator},
		{"4 no fee payer and no list", Guard{UsePayer: true}, Claim{Payer: user}, "",
			AbsentUnguarded},
		{"5 no fee payer with a list", on, Claim{Payer: user}, user, Present},
		{"5 fee payer and no list", Guard{UsePayer: true}, Claim{Payer: user, FeePayer: fee},
			user, Present},
		{"5 both", on, Claim{Payer: user, FeePayer: fee}, user, Present},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			payer, why, err := Decide(c.g, c.c, func(addr string) (bool, error) {
				return addr == seen, nil
			})
			eq(t, err, nil)
			eq(t, payer, c.payer)
			eq(t, why, c.why)
		})
	}
	boom := errors.New("down")
	_, _, err := Decide(on, Claim{Payer: user, FeePayer: fee}, func(string) (bool, error) {
		return false, boom
	})
	eq(t, errors.Is(err, boom), true)
}
