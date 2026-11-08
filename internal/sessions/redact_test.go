package sessions

import (
	"strings"
	"testing"
)

func TestRedactEveryShape(t *testing.T) {
	body := strings.Repeat("A1b2", 12)
	r := NewRedactor()
	for _, prefix := range []string{
		"sbx_", "bnp_", "bnx_", "bnsl_", "bnsp_", "bnt_", "bnf_", "bnsec_", "g1.", "bxd_",
		"e2b_", "boat_", "bnk_", "sk-", "sk-ant-", "ghp_", "gho_", "ghu_", "ghs_", "ghr_",
		"github_pat_", "hf_", "xoxb-", "xoxp-", "glpat-", "r8_",
	} {
		eq(t, r.Redact("x "+prefix+body+" y"), "x [redacted token] y")
		eq(t, r.Redact("KEY="+prefix+body), "KEY=[redacted token]")
	}
	eq(t, r.Redact("AKIA"+"ABCDEFGHIJKLMNOP"), "[redacted token]")
	eq(t, r.Redact("Authorization: Bearer abc.def"), "Authorization: [redacted credential]")
	eq(t, r.Redact(`curl -H "authorization: token xyz123"`),
		`curl -H "authorization: [redacted credential]"`)
	eq(t, r.Redact("bearer eyJhbGciOiJIUzI1NiJ9.e30"), "bearer [redacted credential]")
	eq(t, r.Redact("Basic dXNlcjpwYXNz"), "Basic [redacted credential]")
	eq(t, r.Redact("git clone https://me:hunter2secret@github.com/a/b"),
		"git clone https://me:[redacted password]@github.com/a/b")
	key := "-----BEGIN RSA PRIVATE KEY-----\nMIIabc\n-----END RSA PRIVATE KEY-----"
	eq(t, r.Redact("a\n"+key+"\nb"), "a\n[redacted private key]\nb")
	eq(t, r.Redact("a\n-----BEGIN PRIVATE KEY-----\nMIIabc"), "a\n[redacted private key]")
}

func TestRedactLeavesOrdinaryWords(t *testing.T) {
	r := NewRedactor()
	for _, s := range []string{
		"pip install hf_transfer", "task-runner-for-everything-here", "basic configuration",
		"Basic authentication", "a bearer of news", "boat_trip", "sk-learn-tools",
		"-----BEGIN CERTIFICATE-----", "https://github.com/a/b", "AKIA is a prefix",
	} {
		eq(t, r.Redact(s), s)
	}
}

func TestRedactKnownValues(t *testing.T) {
	gate := "Zm9vYmFyYmF6cXV4cXV1eA"
	envd := "plain-envd-token-value"
	r := NewRedactor(gate, envd, "short")
	eq(t, r.Redact("gate="+gate+" envd "+envd+" short"),
		"gate=[redacted secret] envd [redacted secret] short")
}

// write feeds chunks through a fresh stream and returns every frame, flushed at the end
func write(r *Redactor, chunks ...string) []Frame {
	st := r.Stream(Stdout)
	var out []Frame
	for _, c := range chunks {
		out = append(out, st.Write([]byte(c))...)
	}
	return append(out, st.Flush()...)
}

func joined(frames []Frame) string {
	var b strings.Builder
	for _, f := range frames {
		b.WriteString(f.T)
	}
	return b.String()
}

func TestRedactAcrossFrames(t *testing.T) {
	tok := "bnsec_" + strings.Repeat("q", 26)
	r := NewRedactor()
	got := joined(write(r, "PASSWORD="+tok[:9], tok[9:20], tok[20:]+"\nnext\n"))
	eq(t, got, "PASSWORD=[redacted token]\nnext\n")
	eq(t, strings.Contains(got, "bnsec"), false)
	got = joined(write(r, "Authorization: Bearer ", "abcdef0123456789xyz\n"))
	eq(t, got, "Authorization: [redacted credential]\n")
	got = joined(write(r, "a\n-----BEGIN RSA ", "PRIVATE KEY-----\nMIIE", "abc\n-----END RSA",
		" PRIVATE KEY-----\nafter\n"))
	eq(t, got, "a\n[redacted private key]\nafter\n")
	got = joined(write(r, "\x1b]0;my ", "title\x07shown\n"))
	eq(t, got, "shown\n")
}

func TestRedactStreamHoldsAtMost16KiB(t *testing.T) {
	st := NewRedactor().Stream(Stderr)
	long := strings.Repeat("x", carryMax-1)
	eq(t, len(st.Write([]byte(long))), 0)
	eq(t, st.Holding(), true)
	frames := st.Write([]byte("yy"))
	eq(t, len(frames), 1)
	eq(t, frames[0].S, Stderr)
	eq(t, len(frames[0].T), carryMax)
	eq(t, joined(st.Flush()), "y")
	eq(t, st.Holding(), false)
	key := NewRedactor().Stream(Stdout)
	out := key.Write([]byte("-----BEGIN PRIVATE KEY-----\n" + strings.Repeat("k", carryMax)))
	eq(t, joined(out), "[redacted private key]")
	eq(t, joined(key.Write([]byte("kk\n-----END PRIVATE KEY-----\ntail\n"))), "\ntail\n")
}

func TestCarriageReturnFrames(t *testing.T) {
	r := NewRedactor()
	eq(t, write(r, "10%\r50%\r100%\ndone\n"), []Frame{
		{S: "o", T: "10%"}, {S: "o", T: "50%", CR: true}, {S: "o", T: "100%\ndone\n", CR: true},
	})
	eq(t, write(r, "step\r", "\nnext\n"), []Frame{{S: "o", T: "step\nnext\n"}})
	eq(t, write(r, "a\r", "b\r", "c\n"), []Frame{
		{S: "o", T: "a"}, {S: "o", T: "b", CR: true}, {S: "o", T: "c\n", CR: true},
	})
	eq(t, write(r, "abc\r\rxy\r"), []Frame{{S: "o", T: "abc"}, {S: "o", T: "xy", CR: true}})
	eq(t, write(r, "\x1b[2K\r\x1b[32mok\x1b[0m\n"), []Frame{{S: "o", T: "ok\n", CR: true}})
}

func TestStreamHoldsAPartialCharacter(t *testing.T) {
	st := NewRedactor().Stream(Stdout)
	b := []byte("é ")
	eq(t, joined(st.Write(b[:1])), "")
	eq(t, joined(st.Write(b[1:])), "é ")
}
