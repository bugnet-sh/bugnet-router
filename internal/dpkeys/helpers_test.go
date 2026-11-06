package dpkeys

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"os"
	"reflect"
	"testing"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// b64 is standard base64 of n bytes that each hold fill
func b64(n int, fill byte) string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{fill}, n))
}

func mustParse(t *testing.T, env string) Keys {
	t.Helper()
	k, err := Parse(env)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return k
}

func sha(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
