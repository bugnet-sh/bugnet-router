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
	authz    = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)[^\r\n"',;]+`)
	scheme   = regexp.MustCompile(`(?i)\b(bearer|basic)(\s+)[A-Za-z0-9._~+/=-]{8,}`)
	userinfo = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.-]*://[^\s/:@]*):[^\s/@]+@`)
	// The Bugnet shapes of DP 20.6.4 with bnk_, then the vendor shapes the gateway redacts
	tokens = regexp.MustCompile(`(?:sbx_|bnp_|bnx_|bnsl_|bnsp_|bnt_|bnf_|bnsec_|bxd_|e2b_|` +
		`boat_|bnk_|g1\.)[A-Za-z0-9_-]{16,}|sk-ant-[A-Za-z0-9_-]{20,}|sk-[A-Za-z0-9_-]{20,}|` +
		`gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}|hf_[A-Za-z0-9]{30,}|` +
		`xox[abpr]-[A-Za-z0-9-]{20,}|AKIA[0-9A-Z]{16}|glpat-[A-Za-z0-9_-]{20,}|` +
		`r8_[A-Za-z0-9]{30,}`)
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
	s = authz.ReplaceAllString(s, "${1}"+redactedCredential)
	s = scheme.ReplaceAllStringFunc(s, schemeValue)
	s = userinfo.ReplaceAllString(s, "${1}:"+redactedPassword+"@")
	return replaceTokens(s)
}

// schemeValue redacts a Bearer or Basic value unless it reads as an ordinary word
func schemeValue(m string) string {
	at := scheme.FindStringSubmatchIndex(m)[5]
	if ordinaryWord(m[at:]) {
		return m
	}
	return m[:at] + redactedCredential
}

// ordinaryWord is lower case letters after an optional capital, as in "Basic configuration"
func ordinaryWord(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		lower := c >= 'a' && c <= 'z'
		if !lower && (i > 0 || c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

// replaceTokens redacts each token shape that starts a word
func replaceTokens(s string) string {
	locs := tokens.FindAllStringIndex(s, -1)
	if locs == nil {
		return s
	}
	var b strings.Builder
	last := 0
	for _, loc := range locs {
		if loc[0] > 0 && isAlnum(s[loc[0]-1]) {
			continue
		}
		b.WriteString(s[last:loc[0]])
		b.WriteString(redactedToken)
		last = loc[1]
	}
	b.WriteString(s[last:])
	return b.String()
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
}

// Stream returns a Stream for the output named s, Stdout or Stderr
func (r *Redactor) Stream(s string) *Stream {
	return &Stream{r: r, s: s}
}

// Holding reports whether output waits for a later frame
func (st *Stream) Holding() bool {
	return len(st.buf) > 0 || st.inKey || st.pendingCR
}

// Write takes the next bytes of the output and returns the frames now safe to show
func (st *Stream) Write(data []byte) []Frame {
	st.buf = append(st.buf, data...)
	var out []Frame
	for {
		if st.inKey && !st.skipKey() {
			return out
		}
		n, opensKey := st.safe()
		if n == 0 && !opensKey {
			return out
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
	if st.inKey {
		text, st.inKey = "", false
	}
	out := st.frames(text)
	st.pendingCR = false
	return out
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
	// A full carry goes out as it is, and an open key block is redacted to its end
	n = runeFloor(b, carryMax)
	if loc := pemOpen.FindIndex(b[:n]); loc != nil && pemClose.FindIndex(b[loc[1]:n]) == nil {
		return loc[0], true
	}
	return n, false
}

// cutPoint is just past the last space, tab, newline or lone carriage return, before a lead word
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
	for n > 0 {
		end := n
		for end > 0 && isSpace(b[end-1]) {
			end--
		}
		start := end
		for start > 0 && !isSpace(b[start-1]) {
			start--
		}
		if start == end || !leadWords.Match(b[start:end]) {
			break
		}
		n = start
	}
	return n
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
