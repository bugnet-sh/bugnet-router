package jobs

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestPatternWindowStartsAtALine(t *testing.T) {
	now := time.Now()
	p := progress{until: Until{Pattern: regexp.MustCompile(`^x`)}}
	// Cutting the oldest byte alone would leave the window starting at x in mid-line
	chunk := "ax\n" + strings.Repeat("b", patternWindow-2)
	if p.output(0, []byte(chunk), now) {
		t.Fatal("^x matched in the middle of a line the window cut")
	}
	if w := p.recent[0]; len(w) != patternWindow-2 || w[0] != 'b' {
		t.Fatalf("window holds %d bytes starting %q, want the line after the cut", len(w), w[:1])
	}

	p = progress{until: Until{Pattern: regexp.MustCompile(`(?m)^ready$`)}}
	if p.output(1, []byte(strings.Repeat("c", patternWindow)+"\nrea"), now) {
		t.Fatal("matched before the line was complete")
	}
	if !p.output(1, []byte("dy\n"), now) || !bytes.HasPrefix(p.recent[1], []byte("ready")) {
		t.Fatalf("no match on a whole line after the cut, window %q", p.recent[1])
	}

	p = progress{until: Until{Pattern: regexp.MustCompile(`READY`)}}
	if !p.output(0, []byte(strings.Repeat("q", patternWindow+10)+"READY\n"), now) {
		t.Fatal("no match inside a line longer than the window")
	}
	if len(p.recent[0]) != patternWindow {
		t.Fatalf("window of a long line holds %d bytes, want %d", len(p.recent[0]), patternWindow)
	}
}
