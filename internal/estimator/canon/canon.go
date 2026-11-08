// Package canon writes RFC 8785 canonical JSON and hashes it
package canon

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
)

// maxExact is the largest integer below which every integer has an exact double
const maxExact = 1<<53 - 1

// Marshal encodes v with encoding/json and rewrites the result in RFC 8785 form
func Marshal(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("canon: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var tree any
	if err := dec.Decode(&tree); err != nil {
		return nil, fmt.Errorf("canon: %w", err)
	}
	var out bytes.Buffer
	if err := write(&out, tree); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// Sum is the hex SHA-256 of b
func Sum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func write(out *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		out.WriteString(strconv.FormatBool(x))
	case string:
		writeString(out, x)
	case json.Number:
		s, err := number(string(x))
		if err != nil {
			return err
		}
		out.WriteString(s)
	case []any:
		out.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := write(out, e); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return lessUTF16(keys[i], keys[j]) })
		out.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			writeString(out, k)
			out.WriteByte(':')
			if err := write(out, x[k]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("canon: unexpected %T", v)
	}
	return nil
}

func lessUTF16(a, b string) bool {
	ua, ub := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}

func writeString(out *bytes.Buffer, s string) {
	const hexDigits = "0123456789abcdef"
	out.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if r < 0x20 {
				out.WriteString(`\u00`)
				out.WriteByte(hexDigits[r>>4])
				out.WriteByte(hexDigits[r&0xf])
				continue
			}
			out.WriteRune(r)
		}
	}
	out.WriteByte('"')
}

var errInexact = errors.New("canon: an integer with no exact double")

// number renders a JSON number literal as ECMAScript's Number.prototype.toString does
func number(lit string) (string, error) {
	if !strings.ContainsAny(lit, ".eE") {
		n, ok := new(big.Int).SetString(lit, 10)
		if !ok {
			return "", fmt.Errorf("canon: bad number")
		}
		if n.CmpAbs(big.NewInt(maxExact)) <= 0 {
			return n.String(), nil
		}
		f, _ := new(big.Float).SetInt(n).Float64()
		if back, _ := big.NewFloat(f).Int(nil); back.Cmp(n) != 0 {
			return "", errInexact
		}
		return formatES(f), nil
	}
	f, err := strconv.ParseFloat(lit, 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
		return "", fmt.Errorf("canon: bad number")
	}
	return formatES(f), nil
}

func formatES(f float64) string {
	if f == 0 {
		return "0"
	}
	sign := ""
	if f < 0 {
		sign, f = "-", -f
	}
	e := strconv.FormatFloat(f, 'e', -1, 64)
	mant, expPart, _ := strings.Cut(e, "e")
	digits := strings.Replace(mant, ".", "", 1)
	exp, _ := strconv.Atoi(expPart)
	k, n := len(digits), exp+1
	switch {
	case k <= n && n <= 21:
		return sign + digits + strings.Repeat("0", n-k)
	case 0 < n && n <= 21:
		return sign + digits[:n] + "." + digits[n:]
	case -6 < n && n <= 0:
		return sign + "0." + strings.Repeat("0", -n) + digits
	}
	m := digits[:1]
	if k > 1 {
		m += "." + digits[1:]
	}
	es := "+"
	if n-1 < 0 {
		es = "-"
	}
	return sign + m + "e" + es + strconv.Itoa(abs(n-1))
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
