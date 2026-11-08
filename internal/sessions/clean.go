// Package sessions builds the session page's record, cleans and redacts everything it shows, and
// writes its events.
//
// Clean ports bugnet-gateway's cleanText rule for rule, in its order, and runs the gateway's own
// vectors. Redactor removes the token shapes of SP 6.8 and a placement's known values, and a
// Stream does the same to one job output frame by frame, holding back what a later frame may
// complete, so a token split across two frames is still caught. The copy job and the live view
// both read job output through a Stream, so both show the same text. Writer is the one way an
// event reaches the record, BuildRecord serves the stored rows as the page's JSON, and Snapshot
// answers the running page.
package sessions

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	// CSI, OSC, DCS and the other escape sequences, as the gateway matches them
	escapes = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)?|` +
		`\x1b[PX^_][^\x1b]*(?:\x1b\\)?|\x1b[@-Z\\-_]|\x{9b}[0-?]*[ -/]*[@-~]`)
	controls  = regexp.MustCompile(`[\x00-\x08\x0b-\x1f\x7f-\x{9f}]`)
	bidi      = regexp.MustCompile(`[\x{202a}-\x{202e}\x{2066}-\x{2069}]`)
	invisible = regexp.MustCompile(`[\x{061c}\x{200b}-\x{200f}\x{2060}-\x{2064}\x{feff}` +
		`\x{fe00}-\x{fe0d}\x{e0000}-\x{e007f}\x{e0100}-\x{e01ef}]`)
	separators = regexp.MustCompile(`[\x{2028}\x{2029}]`)
	markRuns   = regexp.MustCompile(`(\pM{4})\pM+`)
)

// Clean runs SP 6.8's cleaning and redaction on one string and cuts it to n characters
func Clean(s string, n int) string {
	return cut(shapes.Redact(cleanText(s)), n)
}

// cleanText is the gateway's cleanText, a line rewritten by carriage returns kept as its last state
func cleanText(s string) string {
	s = strings.ToValidUTF8(s, "�")
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = escapes.ReplaceAllString(s, "")
	s = separators.ReplaceAllString(s, "\n")
	return cleanLine(lastStates(s))
}

// cleanLine is the part of cleanText that runs after carriage returns are resolved
func cleanLine(s string) string {
	s = controls.ReplaceAllString(s, "")
	s = bidi.ReplaceAllString(s, "")
	s = invisible.ReplaceAllString(s, "")
	return markRuns.ReplaceAllString(s, "$1")
}

// lastStates keeps each line rewritten by carriage returns as a terminal would show it
func lastStates(s string) string {
	if !strings.Contains(s, "\r") {
		return s
	}
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		if !strings.Contains(line, "\r") {
			continue
		}
		last := ""
		for part := range strings.SplitSeq(line, "\r") {
			if part != "" {
				last = part
			}
		}
		lines[i] = last
	}
	return strings.Join(lines, "\n")
}

// cut keeps the first n characters of s, all of it when n is 0 or less
func cut(s string, n int) string {
	if n <= 0 || utf8.RuneCountInString(s) <= n {
		return s
	}
	seen := 0
	for i := range s {
		if seen == n {
			return s[:i]
		}
		seen++
	}
	return s
}

// CutBytes keeps at most n bytes of s, never splitting a character
func CutBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
