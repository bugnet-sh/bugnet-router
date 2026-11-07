// Package solana holds the router's one base58 decoder for Solana keys and signatures
package solana

const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// digits maps a byte to its base58 value, -1 when it is not in the alphabet
var digits = func() (d [256]int8) {
	for i := range d {
		d[i] = -1
	}
	for i := range len(alphabet) {
		d[alphabet[i]] = int8(i)
	}
	return d
}()

// PublicKey decodes canonical base58 of exactly 32 bytes
func PublicKey(s string) ([32]byte, bool) {
	var k [32]byte
	return k, decode(s, k[:])
}

// Signature decodes canonical base58 of exactly 64 bytes
func Signature(s string) ([64]byte, bool) {
	var sig [64]byte
	return sig, decode(s, sig[:])
}

// decode fills out from s, true only when s is the one base58 text of exactly len(out) bytes
func decode(s string, out []byte) bool {
	n := len(out)
	if s == "" {
		return false
	}
	// Each leading 1 is one leading zero byte, so a text with one more or one fewer is refused
	zeros := 0
	for zeros < len(s) && s[zeros] == '1' {
		zeros++
	}
	if zeros > n {
		return false
	}
	// num holds the value after the leading zeros, least significant byte first
	num := make([]byte, 0, n)
	for i := zeros; i < len(s); i++ {
		d := digits[s[i]]
		if d < 0 {
			return false
		}
		carry := int(d)
		for j := range num {
			carry += int(num[j]) * 58
			num[j] = byte(carry)
			carry >>= 8
		}
		for ; carry > 0; carry >>= 8 {
			num = append(num, byte(carry))
		}
		if zeros+len(num) > n {
			return false
		}
	}
	if zeros+len(num) != n {
		return false
	}
	for i, b := range num {
		out[n-1-i] = b
	}
	return true
}
