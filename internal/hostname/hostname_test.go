package hostname

import (
	"strings"
	"testing"
)

func TestValid(t *testing.T) {
	label := strings.Repeat("a", 63)
	long := strings.Repeat(label+".", 3) + strings.Repeat("b", 61)
	for _, s := range []string{
		"example.com", "a", "a-b.example.com", "1password.com", "xn--bcher-kva.example",
		label + ".com", long,
	} {
		if !Valid(s) {
			t.Errorf("%q refused, want it accepted", s)
		}
	}
	for _, s := range []string{
		"", "Example.com", "example.com.", ".example.com", "a..b", "-a.com", "a-.com",
		"*.example.com", "https://example.com", "example.com:443", "exa mple.com", "a_b.com",
		strings.Repeat("a", 64) + ".com", long + "b", "203.0.113.7", "::1", "[::1]",
		"ex\nample.com",
	} {
		if Valid(s) {
			t.Errorf("%q accepted, want it refused", s)
		}
	}
}
