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
	pemEnd   = regexp.MustCompile(`^-----END [A-Z0-9 ]*PRIVATE KEY-----$`)
	// An Authorization header up to its value, the quote opening it kept, also escaped or a pair,
	// then what closes the name, a quote or a bracket, before a colon or = or before a comma
	authz = regexp.MustCompile(`(?i)((?:\\*["'])?)(?:proxy-)?authorization` +
		`(?:((?:\\*["'])?\]?)[ \t]*[:=]|(\\*["'])[ \t]*,)[ \t]*`)
	authzAny  = regexp.MustCompile(`(?i)(?:proxy-)?authorization`)
	scheme    = regexp.MustCompile(`(?i)\b(bearer|basic)(\s+)[A-Za-z0-9._~+/=-]{8,}`)
	urlScheme = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://`)
	// A host's port before a path, as in localhost:5173/@vite, where no password is
	hostPort = regexp.MustCompile(`^[0-9]+/`)
	// Bugnet shapes of DP 20.6.4 with bnk_, then the gateway's vendor shapes, where a word starts
	tokens = regexp.MustCompile(`(^|[^A-Za-z0-9])(?:(?:sbx_|bnp_|bnx_|bnsl_|bnsp_|bnt_|bnf_|` +
		`bnsec_|bxd_|e2b_|boat_|bnk_|g1\.)[A-Za-z0-9_-]{16,}|sk-ant-[A-Za-z0-9_-]{20,}|` +
		`sk-[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}|` +
		`hf_[A-Za-z0-9]{30,}|xox[abpr]-[A-Za-z0-9-]{20,}|AKIA[0-9A-Z]{16}|` +
		`glpat-[A-Za-z0-9_-]{20,}|r8_[A-Za-z0-9]{30,})`)
	// Words after which a credential follows, so a frame never ends between the two
	leadWords = regexp.MustCompile(`(?i)^(bearer|basic|(proxy-)?authorization\s*[:=]?)$`)
	// A URL still open at the end, or a scheme whose // may follow, its user info maybe cut
	openURL = regexp.MustCompile("[A-Za-z][A-Za-z0-9+.-]*:(//[^\\s\"'<>`]*)?$")
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
	s = redactPasswords(s)
	return replaceTokens(s)
}

// redactPasswords redacts the password of each URL with user info
func redactPasswords(s string) string {
	var b strings.Builder
	last, end, at, hostAt := 0, -1, -1, -1
	for _, loc := range urlScheme.FindAllStringIndex(s, -1) {
		i := loc[1]
		if loc[0] < last {
			continue
		}
		// A URL holding another, as in a query, ends where the outer one does
		if i > end {
			end, at, hostAt = urlAts(s, i)
		}
		c := strings.IndexAny(s[i:end], ":/")
		if c < 0 || s[i+c] != ':' {
			continue
		}
		colon, pw := i+c, hostAt
		// With no @ but right after a slash, which starts a path, the last @ ends the password
		if pw <= colon {
			pw = at
		}
		if pw <= colon || isPort(s[colon+1:end]) {
			continue
		}
		b.WriteString(s[last : colon+1])
		b.WriteString(redactedPassword)
		last = pw
	}
	if last == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

// isPort reports whether the text after a URL's colon starts with a host's port, digits then a
// slash, whose next path part has no @ but at its start, so user:pass@ is read first
func isPort(s string) bool {
	loc := hostPort.FindStringIndex(s)
	if loc == nil {
		return false
	}
	part := s[loc[1]:]
	if i := strings.IndexAny(part, "/?#"); i >= 0 {
		part = part[:i]
	}
	return strings.IndexByte(part, '@') <= 0
}

// urlAts is where the URL whose authority starts at i ends, its last @ and its last @ not right
// after a slash, or -1
func urlAts(s string, i int) (end, at, hostAt int) {
	end, at, hostAt = len(s), -1, -1
	if e := strings.IndexAny(s[i:], " \t\n\v\f\r\"'<>`"); e >= 0 {
		end = i + e
	}
	for j := end - 1; j >= i; j-- {
		if s[j] != '@' {
			continue
		}
		if at < 0 {
			at = j
		}
		if s[j-1] != '/' {
			hostAt = j
			break
		}
	}
	return end, at, hostAt
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
	at, quote, level := m[1], byte(0), 0
	if open := s[m[2]:m[3]]; open != "" {
		quote, level = open[len(open)-1], len(open)-1
	}
	// After a name its own quote or bracket closed, as in JSON, Ruby, PHP or Go, the value opens
	// a quote of its own, or has none and runs to the line's end
	if m[5] > m[4] || m[7] > m[6] {
		at = skipAssign(s, at)
		j := at
		for j < len(s) && s[j] == '\\' {
			j++
		}
		quote, level = 0, 0
		if j < len(s) && (s[j] == '"' || s[j] == '\'') {
			quote, level, at = s[j], j-at, j+1
		}
	}
	return at, valueEnd(s, at, quote, level)
}

// skipAssign moves i past what may join a name to its value: spaces, [, =, > and []string{
func skipAssign(s string, i int) int {
	for i < len(s) {
		switch {
		case strings.HasPrefix(s[i:], "[]string{"):
			i += len("[]string{")
		case strings.IndexByte(" \t[=>", s[i]) >= 0:
			i++
		default:
			return i
		}
	}
	return i
}

// valueEnd is where a value from i ends, at a line break or its closing quote, which has level
// backslashes before it in escaped JSON
func valueEnd(s string, i int, quote byte, level int) int {
	for ; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\r' || c == '\n':
			return i
		case quote == 0:
		case c == '\\' && level > 0:
			j := i + 1
			for j < len(s) && s[j] == '\\' {
				j++
			}
			switch {
			case j == len(s) || s[j] != quote:
				i = j - 1
			case j-i <= level:
				return i
			default:
				// More backslashes escape a quote inside the value
				i = j
			}
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
	// maybeKey drops the lines of a key block the output may have gone on inside, after a gap
	maybeKey bool
	// wholeLine drops the line through its break, also past a space, as a credential may go on
	wholeLine bool
	dropped   int64
}

// Stream returns a Stream for the output named s, Stdout or Stderr
func (r *Redactor) Stream(s string) *Stream {
	return &Stream{r: r, s: s}
}

// Holding reports whether output waits for a later frame
func (st *Stream) Holding() bool {
	return len(st.buf) > 0 || st.inKey || st.pendingCR || st.rejoining || st.maybeKey
}

// Resume shows the held text before its last word, then drops the cut line and key lines after it
func (st *Stream) Resume() []Frame {
	// The held word may be a token the gap cut, which no shape then matches
	n := bytes.LastIndexAny(st.buf, " \t\n\r") + 1
	line := bytes.LastIndexAny(st.buf, "\n\r") + 1
	st.wholeLine = st.wholeLine || authzAny.Match(st.buf[line:])
	st.dropped += int64(len(st.buf) - n)
	st.buf = st.buf[:n]
	out := st.flush()
	st.rejoining, st.maybeKey = true, true
	return out
}

// Late starts output read from past its start, where a key block's body may be under way
func (st *Stream) Late() { st.maybeKey = true }

// Dropped counts the bytes a gap or a full carry made the stream drop
func (st *Stream) Dropped() int64 { return st.dropped }

// Write takes the next bytes of the output and returns the frames now safe to show
func (st *Stream) Write(data []byte) []Frame {
	st.buf = append(st.buf, data...)
	var out []Frame
	for {
		switch {
		case st.inKey:
			if !st.skipKey() {
				return out
			}
			// A gap inside the block left nothing past its end to drop
			st.rejoining, st.maybeKey, st.wholeLine = false, false, false
			continue
		case st.rejoining:
			if !st.rejoin() {
				return out
			}
			continue
		case st.maybeKey:
			if !st.skipKeyLines() {
				return out
			}
			continue
		}
		n, opensKey := st.safe()
		if n == 0 && !opensKey {
			if len(st.buf) < carryMax {
				return out
			}
			// A full carry of one run, a token's body maybe, goes with the rest of its line, and
			// as a gap it may have cut a key block's BEGIN
			n = runeFloor(st.buf, carryMax)
			st.dropped += int64(n)
			st.buf = slices.Delete(st.buf, 0, n)
			st.rejoining, st.wholeLine, st.maybeKey = true, true, true
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
	out := st.flush()
	st.inKey = false
	return out
}

// flush shows the held text, but no line a gap cut and no part of a key block
func (st *Stream) flush() []Frame {
	text := string(st.buf)
	st.buf = nil
	switch {
	case st.inKey:
		text = ""
	case st.rejoining || st.maybeKey:
		// The line a gap cut stays dropped, also when its end comes after a flush
		st.dropped += int64(len(text))
		text = ""
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
		// A line longer than the carry goes on after its last space, which no key line has,
		// unless the space may fall inside an Authorization value or a run a full carry cut
		i = bytes.LastIndexAny(b, " \t")
		if st.wholeLine || i >= 0 && authzAny.Match(b[:i]) {
			i, st.wholeLine = -1, true
		}
		st.maybeKey = st.maybeKey && i < 0
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
	st.rejoining, st.wholeLine = still, st.wholeLine && still
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

// What a line after a gap is to a key block the gap may have cut
const (
	keyBody    = iota // a body line, a header or a blank line
	keyEnd            // the block's END line
	keyOther          // any other line, which ends the block
	keyPending        // a line still arriving that may be either
)

// skipKeyLines drops the lines a cut key block may still have, reporting false while one arrives
func (st *Stream) skipKeyLines() bool {
	for {
		b := st.buf
		i := bytes.IndexByte(b, '\n')
		raw := b
		if i >= 0 {
			raw = b[:i]
		}
		raw = bytes.TrimRight(raw, " \t\r")
		// A key in YAML or a heredoc is indented
		line := bytes.TrimLeft(raw, " \t")
		kind := keyLine(line, i >= 0)
		n := i + 1
		switch {
		case kind == keyOther:
			st.maybeKey = false
			return true
		case i < 0 && len(b) < carryMax:
			return false
		case i < 0:
			n = len(b)
		case kind == keyEnd:
			// The END line's break stays, as it does after a whole block
			n = len(raw)
			st.maybeKey = false
		}
		st.dropped += int64(n)
		st.buf = slices.Delete(st.buf, 0, n)
		if !st.maybeKey {
			return true
		}
		if i < 0 {
			return false
		}
	}
}

// keyLine says what line is to a key block, complete when its line break came
func keyLine(line []byte, complete bool) int {
	header := bytes.HasPrefix(line, []byte("Proc-Type:")) ||
		bytes.HasPrefix(line, []byte("DEK-Info:"))
	switch {
	case complete && pemEnd.Match(line):
		return keyEnd
	case header || isBase64(line):
		if complete {
			return keyBody
		}
		return keyPending
	case complete:
		return keyOther
	}
	for _, start := range []string{"Proc-Type:", "DEK-Info:", "-----END "} {
		if bytes.HasPrefix([]byte(start), line) || bytes.HasPrefix(line, []byte(start)) {
			return keyPending
		}
	}
	return keyOther
}

// isBase64 reports whether s holds base64 characters and padding alone
func isBase64(s []byte) bool {
	for _, c := range s {
		if !isAlnum(c) && c != '+' && c != '/' && c != '=' {
			return false
		}
	}
	return true
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
	// A known value may hold a colon or an @, where the run fullCut moves back over stops
	return st.r.knownCut(b, fullCut(b, st.r.knownCut(b, n))), false
}

// knownCut moves n back to the start of a known value b[:n] would cut, or that may start there
func (r *Redactor) knownCut(b []byte, n int) int {
	for moved := true; moved; {
		moved = false
		for _, v := range r.known {
			for at := max(n-len(v)+1, 0); at < n; at++ {
				w := b[at:min(at+len(v), len(b))]
				if string(w) == v[:len(w)] {
					n, moved = at, true
					break
				}
			}
		}
	}
	return n
}

// fullCut moves a full carry's end n back before the token or Authorization value it would cut
func fullCut(b []byte, n int) int {
	for n > 0 && isTokenChar(b[n-1]) {
		n--
	}
	// A password stops the run at a colon or an @, and no part of it shows without its scheme
	if loc := openURL.FindSubmatchIndex(b[:n]); loc != nil &&
		(loc[2] >= 0 || n == len(b) || b[n] == '/') {
		n = loc[0]
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
		// The word glued to it may follow a Bearer or Basic
		n = beforeLeadWords(b, n)
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
