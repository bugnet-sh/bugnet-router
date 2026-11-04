package handle

import (
	"crypto/sha256"
	"regexp"
	"testing"
)

func TestHandleShapeAndHash(t *testing.T) {
	re := regexp.MustCompile(`^sbx_[a-z2-7]{26}$`)
	seen := map[string]bool{}
	for range 1000 {
		h, sum := New()
		if !re.MatchString(h) {
			t.Fatalf("bad handle %q", h)
		}
		if seen[h] {
			t.Fatalf("duplicate handle %q", h)
		}
		seen[h] = true
		if len(sum) != 32 {
			t.Fatalf("hash length %d", len(sum))
		}
		got, err := Hash(h)
		if err != nil {
			t.Fatal(err)
		}
		want := sha256.Sum256([]byte(h))
		if string(got) != string(want[:]) || string(sum) != string(want[:]) {
			t.Fatalf("hash mismatch for %q", h)
		}
	}
	if _, err := Hash("sbx_bad"); err == nil {
		t.Fatal("expected error for malformed handle")
	}
}
