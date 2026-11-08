package sessions

import (
	"bytes"
	"cmp"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	// carryMax is the most one output holds back while waiting for a token or a key block to end
	carryMax = 16 << 10
	// minKnown keeps a short known value from redacting ordinary words
	minKnown = 8
	// maxOrdinary is the longest Bearer or Basic value that may read as an ordinary word
	maxOrdinary = 15
)

// Replacements, each [redacted <kind>]
const (
	redactedToken      = "[redacted token]"
	redactedCredential = "[redacted credential]"
	redactedPassword   = "[redacted password]"
	redactedKey        = "[redacted private key]"
	redactedSecret     = "[redacted secret]"
)

var (
	pemBlock = regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?` +
		`-----END [A-Z0-9 ]*PRIVATE KEY-----`)
	pemOpen  = regexp.MustCompile(`-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----`)
	pemClose = regexp.MustCompile(`-----END [A-Z0-9 ]*PRIVATE KEY-----`)
	// An Authorization header up to its value, the quote opening it kept, also escaped or a pair
	authz = regexp.MustCompile(`(?i)((?:\\?["'])?)(?:proxy-)?authorization` +
		`(?:\\?["']?[ \t]*[:=]|\\?["'][ \t]*,)[ \t]*`)
	authzAny = regexp.MustCompile(`(?i)(?:proxy-)?authorization`)
	scheme   = regexp.MustCompile(`(?i)\b(bearer|basic)(\s+)[A-Za-z0-9._~+/=-]{8,}`)
	// A URL password runs to the last @ before the host, an @ right after a slash starting a path
	userinfo = regexp.MustCompile("([A-Za-z][A-Za-z0-9+.-]*://[^\\s/:@]*):" +
		"[^\\s\"'<>`]*[^\\s\"'<>`/]@")
	// Bugnet shapes of DP 20.6.4 with bnk_, then the gateway's vendor shapes, where a word starts
	tokens = regexp.MustCompile(`(^|[^A-Za-z0-9])(?:(?:sbx_|bnp_|bnx_|bnsl_|bnsp_|bnt_|bnf_|` +
		`bnsec_|bxd_|e2b_|boat_|bnk_|g1\.)[A-Za-z0-9_-]{16,}|sk-ant-[A-Za-z0-9_-]{20,}|` +
		`sk-[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}|` +
		`hf_[A-Za-z0-9]{30,}|xox[abpr]-[A-Za-z0-9-]{20,}|AKIA[0-9A-Z]{16}|` +
		`glpat-[A-Za-z0-9_-]{20,}|r8_[A-Za-z0-9]{30,})`)
	// Words after which a credential follows, so a frame never ends between the two
	leadWords = regexp.MustCompile(`(?i)^(bearer|basic|(proxy-)?authorization\s*[:=]?)$`)
)

// shapes redacts the token shapes alone, for strings with no placement behind them
var shapes = NewRedactor()

// Redactor removes secrets of known shapes and a placement's known values
type Redactor struct {
	known []string
}

// NewRedactor redacts the shapes and each known value of 8 bytes or more, longest first
func NewRedactor(known ...string) *Redactor {
	var keep []string
	for _, v := range known {
		if len(v) >= minKnown && !slices.Contains(keep, v) {
			keep = append(keep, v)
		}
	}
	slices.SortFunc(keep, func(a, b string) int { return cmp.Compare(len(b), len(a)) })
	return &Redactor{known: keep}
}

// Redact writes every secret it finds in s as [redacted <kind>], a key block open to the end
func (r *Redactor) Redact(s string) string {
	s = pemBlock.ReplaceAllString(s, redactedKey)
	if loc := pemOpen.FindStringIndex(s); loc != nil {
		s = s[:loc[0]] + redactedKey
	}
	for _, v := range r.known {
		s = strings.ReplaceAll(s, v, redactedSecret)
	}
	s = redactAuthz(s)
	s = scheme.ReplaceAllStringFunc(s, schemeValue)
	s = userinfo.ReplaceAllString(s, "${1}:"+redactedPassword+"@")
	return replaceTokens(s)
}

// redactAuthz redacts each Authorization value, any scheme, to its line's end or closing quote
func redactAuthz(s string) string {
	var b strings.Builder
	last := 0
	for _, m := range authz.FindAllStringSubmatchIndex(s, -1) {
		if m[0] < last {
			continue
		}
		at, end := authzValue(s, m)
		if end == at {
			continue
		}
		b.WriteString(s[last:at])
		b.WriteString(redactedCredential)
		last = end
	}
	if last == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

// authzValue is where the value of the Authorization header authz matched at m starts and ends
func authzValue(s string, m []int) (int, int) {
	at, quote, escaped := m[1], byte(0), false
	if open := s[m[2]:m[3]]; open != "" {
		quote, escaped = open[len(open)-1], len(open) == 2
		// A JSON or dict value opens a quote of its own after the name's
		if strings.HasPrefix(s[at:], open) {
			at += len(open)
		}
	}
	return at, valueEnd(s, at, quote, escaped)
}

// valueEnd is where a value from i ends, at a line break or its closing quote, escaped or not
func valueEnd(s string, i int, quote byte, escaped bool) int {
	for ; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\r' || c == '\n':
			return i
		case quote == 0:
		case c == '\\' && escaped && i+1 < len(s) && s[i+1] == quote:
			return i
		case c == '\\':
			i++
		case c == quote:
			return i
		}
	}
	return min(i, len(s))
}

// schemeValue redacts a Bearer or Basic value unless it reads as an ordinary word
func schemeValue(m string) string {
	at := scheme.FindStringSubmatchIndex(m)[5]
	if ordinaryWord(m[at:]) {
		return m
	}
	return m[:at] + redactedCredential
}

// ordinaryWord is a short run of lower case letters after an optional capital, as in "Basic setup"
func ordinaryWord(s string) bool {
	if len(s) > maxOrdinary {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		lower := c >= 'a' && c <= 'z'
		if !lower && (i > 0 || c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

// replaceTokens redacts each token shape that starts a word, also one inside a shape that does not
func replaceTokens(s string) string {
	return tokens.ReplaceAllString(s, "${1}"+redactedToken)
}

func isAlnum(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

/* Streams */

// Frame is one piece of output as the page draws it, CR saying it replaces the last line
type Frame struct {
	S  string `json:"s"`
	T  string `json:"t"`
	CR bool   `json:"cr"`
}

// Output names a job output as frames do
const (
	Stdout = "o"
	Stderr = "e"
)

// Stream cleans and redacts one job output frame by frame
type Stream struct {
	r         *Redactor
	s         string
	buf       []byte
	inKey     bool // inside a private key block whose end has not come
	pendingCR bool // the last text ended on a carriage return, so the next frame replaces the line
	// rejoining drops text up to the next line break, as the output goes on after a gap
	rejoining bool
	dropped   int64
}

// Stream returns a Stream for the output named s, Stdout or Stderr
func (r *Redactor) Stream(s string) *Stream {
	return &Stream{r: r, s: s}
}

// Holding reports whether output waits for a later frame
func (st *Stream) Holding() bool {
	return len(st.buf) > 0 || st.inKey || st.pendingCR || st.rejoining
}

// Resume shows the held text before its last word and drops the rest through the next line break
func (st *Stream) Resume() []Frame {
	// The held word may be a token the gap cut, which no shape then matches
	n := bytes.LastIndexAny(st.buf, " \t\n\r") + 1
	st.dropped += int64(len(st.buf) - n)
	st.buf = st.buf[:n]
	out := st.Flush()
	st.rejoining = true
	return out
}

// Dropped counts the bytes Resume dropped
func (st *Stream) Dropped() int64 { return st.dropped }

// Write takes the next bytes of the output and returns the frames now safe to show
func (st *Stream) Write(data []byte) []Frame {
	st.buf = append(st.buf, data...)
	if st.rejoining && !st.rejoin() {
		return nil
	}
	var out []Frame
	for {
		if st.inKey && !st.skipKey() {
			return out
		}
		n, opensKey := st.safe()
		if n == 0 && !opensKey {
			if len(st.buf) < carryMax {
				return out
			}
			// A full carry of one run, a token's body maybe, goes with the rest of its line
			n = runeFloor(st.buf, carryMax)
			st.dropped += int64(n)
			st.buf = slices.Delete(st.buf, 0, n)
			st.rejoining = true
			if !st.rejoin() {
				return out
			}
			continue
		}
		text := string(st.buf[:n])
		st.buf = slices.Delete(st.buf, 0, n)
		if opensKey {
			text, st.inKey = text+redactedKey, true
		}
		out = append(out, st.frames(text)...)
	}
}

// Flush returns everything held back, at the output's end
func (st *Stream) Flush() []Frame {
	text := string(st.buf)
	st.buf = nil
	if st.rejoining {
		// The line a gap cut stays dropped, also when its end comes after a flush
		st.dropped += int64(len(text))
		text = ""
	}
	if st.inKey {
		text, st.inKey = "", false
	}
	out := st.frames(text)
	st.pendingCR = false
	return out
}

// rejoin drops held text through the first line break, reporting false while none has come
func (st *Stream) rejoin() bool {
	b := st.buf
	full := len(b) >= carryMax
	i := bytes.IndexAny(b, "\n\r")
	switch {
	case i >= 0 && b[i] == '\r' && i+1 == len(b) && !full:
		// A newline may follow, which belongs to the same line break
		return false
	case i >= 0 && b[i] == '\r' && i+1 < len(b) && b[i+1] == '\n':
		i++
	case i < 0 && !full:
		return false
	case i < 0:
		// A line longer than the carry goes on after its last space instead
		i = bytes.LastIndexAny(b, " \t")
	}
	// A full carry with no space at all goes, and the line it is part of stays dropped
	n, still := i+1, i < 0
	if still {
		n = len(b)
	}
	// A private key block's first line is never dropped, so its body is still redacted
	if j := bytes.Index(b[:n], []byte("-----BEGIN")); j >= 0 {
		n, still = j, false
	}
	st.dropped += int64(n)
	st.buf = slices.Delete(st.buf, 0, n)
	st.rejoining = still
	return !still
}

// skipKey drops a key block's body up to its end, reporting false while the end has not come
func (st *Stream) skipKey() bool {
	if loc := pemClose.FindIndex(st.buf); loc != nil {
		st.buf = slices.Delete(st.buf, 0, loc[1])
		st.inKey = false
		return true
	}
	// The end marker may be split, so its possible start stays
	const keep = 64
	if len(st.buf) > keep {
		st.buf = slices.Delete(st.buf, 0, len(st.buf)-keep)
	}
	return false
}

// safe returns how many held bytes can be shown now, and whether they end in an open key block
func (st *Stream) safe() (int, bool) {
	b := st.buf
	n := cutPoint(b)
	// A key block's first line may still be arriving, so its marker never splits
	line := bytes.LastIndexByte(b[:n], '\n') + 1
	if i := bytes.Index(b[line:n], []byte("-----BEGIN")); i >= 0 {
		n = line + i
	}
	if loc := pemOpen.FindIndex(b[:n]); loc != nil && pemClose.FindIndex(b[loc[1]:n]) == nil {
		n = loc[0]
	}
	n = beforeOpenEscape(b, n)
	if n > 0 || len(b) < carryMax {
		return n, false
	}
	// An open key block in a full carry is redacted to its end
	n = runeFloor(b, carryMax)
	if loc := pemOpen.FindIndex(b[:n]); loc != nil && pemClose.FindIndex(b[loc[1]:n]) == nil {
		return loc[0], true
	}
	return fullCut(b, n), false
}

// fullCut moves a full carry's end n back before the token or Authorization value it would cut
func fullCut(b []byte, n int) int {
	for n > 0 && isTokenChar(b[n-1]) {
		n--
	}
	n = beforeLeadWords(b, n)
	line := bytes.LastIndexAny(b[:n], "\r\n") + 1
	s := string(b[line:n])
	if ms := authz.FindAllStringSubmatchIndex(s, -1); ms != nil {
		m := ms[len(ms)-1]
		if _, end := authzValue(s, m); end == len(s) {
			n = line + m[0]
		}
	}
	return n
}

// isTokenChar reports whether c may be part of a token, a key or a credential
func isTokenChar(c byte) bool {
	return isAlnum(c) || strings.IndexByte("_.+/=~-", c) >= 0
}

// cutPoint is past the last space, tab or line break, before a lead word or an Authorization line
func cutPoint(b []byte) int {
	n := 0
	for i, c := range b {
		switch {
		case c == ' ' || c == '\t' || c == '\n':
			n = i + 1
		case c == '\r' && i+1 < len(b) && b[i+1] != '\n':
			n = i + 1
		}
	}
	n = beforeLeadWords(b, n)
	// Any Authorization value is a secret, so the word that names it waits for its line's end
	line := bytes.LastIndexAny(b[:n], "\r\n") + 1
	if loc := authzAny.FindIndex(b[line:n]); loc != nil {
		n = line + loc[0]
		for n > line && !isSpace(b[n-1]) {
			n--
		}
	}
	return n
}

// beforeLeadWords moves n back before the lead words that end b[:n], spaces after them aside
func beforeLeadWords(b []byte, n int) int {
	for n > 0 {
		end := n
		for end > 0 && isSpace(b[end-1]) {
			end--
		}
		start := end
		for start > 0 && !isSpace(b[start-1]) {
			start--
		}
		if start == end || !leadWord(b[start:end]) {
			return n
		}
		n = start
	}
	return n
}

// leadWord reports whether a word ends in a lead word, quotes, brackets and flags around it aside
func leadWord(w []byte) bool {
	notWord := func(r rune) bool { return r >= utf8.RuneSelf || !isAlnum(byte(r)) }
	w = bytes.TrimRightFunc(w, notWord)
	i := bytes.LastIndexFunc(w, func(r rune) bool { return notWord(r) && r != '-' })
	return leadWords.Match(w[i+1:])
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// beforeOpenEscape moves n back before a string escape sequence that has not ended by n
func beforeOpenEscape(b []byte, n int) int {
	i := bytes.LastIndexByte(b[:n], 0x1b)
	if i < 0 || i+1 >= len(b) || !bytes.ContainsRune([]byte("]PX^_"), rune(b[i+1])) {
		return n
	}
	rest := b[i+2 : n]
	if bytes.IndexByte(rest, 0x07) >= 0 || bytes.Contains(rest, []byte("\x1b\\")) {
		return n
	}
	return i
}

// runeFloor moves n back to the start of the character it falls in
func runeFloor(b []byte, n int) int {
	for n > 0 && n < len(b) && !utf8.RuneStart(b[n]) {
		n--
	}
	return n
}

// frames cleans and redacts text and splits it at each carriage return
func (st *Stream) frames(text string) []Frame {
	if text == "" {
		return nil
	}
	text = strings.ToValidUTF8(text, "�")
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = escapes.ReplaceAllString(text, "")
	text = separators.ReplaceAllString(text, "\n")
	var out []Frame
	cr := st.pendingCR
	for i, part := range strings.Split(text, "\r") {
		if i > 0 {
			cr = true
		}
		t := st.r.Redact(cleanLine(part))
		if t == "" {
			continue
		}
		out = append(out, Frame{S: st.s, T: t, CR: cr})
		cr = false
	}
	st.pendingCR = cr
	return out
}
