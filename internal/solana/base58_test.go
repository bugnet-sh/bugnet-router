package solana

import (
	"bytes"
	"math/big"
	"math/rand/v2"
	"strings"
	"testing"
)

const alphabetRef = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// encode is a reference base58 encoder through math/big, independent of the decoder
func encode(b []byte) string {
	zeros := 0
	for zeros < len(b) && b[zeros] == 0 {
		zeros++
	}
	n := new(big.Int).SetBytes(b)
	base, mod := big.NewInt(58), new(big.Int)
	var out []byte
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		out = append(out, alphabetRef[mod.Int64()])
	}
	for range zeros {
		out = append(out, '1')
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

// value is n bytes, the first zeros of them 0 and the next one nonzero
func value(r *rand.Rand, n, zeros int) []byte {
	b := make([]byte, n)
	for i := zeros; i < n; i++ {
		b[i] = byte(r.IntN(256))
	}
	if zeros < n {
		b[zeros] = byte(1 + r.IntN(255))
	}
	return b
}

func TestKnownKeys(t *testing.T) {
	zero, ok := PublicKey("11111111111111111111111111111111")
	eq(t, ok, true)
	eq(t, zero, [32]byte{})
	for _, s := range []string{
		"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
		"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
		"ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
	} {
		k, ok := PublicKey(s)
		eq(t, ok, true)
		eq(t, encode(k[:]), s)
	}
}

func TestSignatureDecodesNotLength(t *testing.T) {
	sig := make([]byte, 64)
	sig[1] = 1
	for i := 2; i < 64; i++ {
		sig[i] = 0xff
	}
	short := encode(sig)
	eq(t, len(short), 86)
	got, ok := Signature(short)
	eq(t, ok, true)
	eq(t, got[:], sig)

	full := bytes.Repeat([]byte{0xff}, 64)
	got, ok = Signature(encode(full))
	eq(t, ok, true)
	eq(t, got[:], full)
	eq(t, len(encode(full)), 88)

	for _, n := range []int{63, 65, 32} {
		_, ok := Signature(encode(bytes.Repeat([]byte{0xab}, n)))
		eq(t, ok, false)
	}
	for _, n := range []int{31, 33, 64} {
		_, ok := PublicKey(encode(bytes.Repeat([]byte{0xab}, n)))
		eq(t, ok, false)
	}
}

func TestPublicKeyRefusesANonCanonicalLeadingOne(t *testing.T) {
	key := bytes.Repeat([]byte{0x9c}, 32)
	s := encode(key)
	got, ok := PublicKey(s)
	eq(t, ok, true)
	eq(t, got[:], key)
	_, ok = PublicKey("1" + s)
	eq(t, ok, false)

	padded := append([]byte{0}, bytes.Repeat([]byte{0x9c}, 31)...)
	s = encode(padded)
	eq(t, s[0], byte('1'))
	got, ok = PublicKey(s)
	eq(t, ok, true)
	eq(t, got[:], padded)
	_, ok = PublicKey(s[1:])
	eq(t, ok, false)
	_, ok = PublicKey("1" + s)
	eq(t, ok, false)
}

func TestRoundTrips(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 400 {
		zeros := r.IntN(4)
		k := value(r, 32, zeros)
		s := encode(k)
		got, ok := PublicKey(s)
		eq(t, ok, true)
		eq(t, got[:], k)
		_, ok = PublicKey("1" + s)
		eq(t, ok, false)
		if zeros > 0 {
			_, ok = PublicKey(s[1:])
			eq(t, ok, false)
		}

		sig := value(r, 64, zeros)
		s = encode(sig)
		gotSig, ok := Signature(s)
		eq(t, ok, true)
		eq(t, gotSig[:], sig)
		_, ok = Signature("1" + s)
		eq(t, ok, false)
	}
}

func TestRefusesOtherText(t *testing.T) {
	good := encode(bytes.Repeat([]byte{0x42}, 32))
	for _, s := range []string{
		"",
		" " + good,
		good + " ",
		good + "\n",
		strings.Replace(good, good[5:6], "0", 1),
		strings.Replace(good, good[5:6], "O", 1),
		strings.Replace(good, good[5:6], "I", 1),
		strings.Replace(good, good[5:6], "l", 1),
		strings.Replace(good, good[5:6], "+", 1),
		strings.Replace(good, good[5:6], "é", 1),
		strings.Repeat("1", 33),
		strings.Repeat("z", 4096),
		strings.Repeat("1", 4096),
	} {
		_, ok := PublicKey(s)
		eq(t, ok, false)
		_, ok = Signature(s)
		eq(t, ok, false)
	}
}
