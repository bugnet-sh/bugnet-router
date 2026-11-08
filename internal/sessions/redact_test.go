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
		"no Authorization header was sent",
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
	// A full carry of one run with no break goes, and so does the rest of its line
	eq(t, len(st.Write([]byte("yy"))), 0)
	eq(t, len(st.buf) < carryMax, true)
	eq(t, joined(st.Write([]byte("y\nshown\n"))), "shown\n")
	eq(t, st.Dropped(), int64(carryMax+3))
	// A full carry shows what comes before its last run, which it holds
	st = NewRedactor().Stream(Stderr)
	frames := st.Write([]byte(strings.Repeat("a", carryMax-4) + ",abcdefgh"))
	eq(t, len(frames), 1)
	eq(t, frames[0].S, Stderr)
	eq(t, frames[0].T, strings.Repeat("a", carryMax-4)+",")
	eq(t, joined(st.Flush()), "abcdefgh")
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

func TestRedactLeadWordsInQuotesAcrossFrames(t *testing.T) {
	r := NewRedactor()
	got := joined(write(r, `{"Authorization": "Bearer Zx9`, `Q2mP7kL4tR8vB1nC6"}`+"\n"))
	eq(t, got, `{"Authorization": "[redacted credential]"}`+"\n")
	got = joined(write(r, "curl -H 'Authorization: Bearer Zx9", "Q2mP7kL4tR8vB1nC6'\n"))
	eq(t, got, "curl -H 'Authorization: [redacted credential]'\n")
	got = joined(write(r, `curl --header="Authorization: Basic `, "dXNlcjpwYXNzd29yZA==\"\n"))
	eq(t, got, `curl --header="Authorization: [redacted credential]"`+"\n")
}

func TestRedactLongLowerCaseSchemeValue(t *testing.T) {
	r := NewRedactor()
	eq(t, r.Redact("Bearer abcdefghijklmnopqrstuvwx"), "Bearer [redacted credential]")
	eq(t, r.Redact("Basic authentication"), "Basic authentication")
}

func TestStreamResumeDropsTheCutLine(t *testing.T) {
	st := NewRedactor().Stream(Stdout)
	eq(t, joined(st.Write([]byte("export T=bnt_"))), "export ")
	// The token's body comes after a gap, without the prefix that marks it
	eq(t, joined(st.Resume()), "")
	eq(t, st.Holding(), true)
	eq(t, joined(st.Write([]byte(strings.Repeat("Q", 20)))), "")
	eq(t, joined(st.Write([]byte(strings.Repeat("Q", 10)+" more\nnext line\n"))),
		"next line\n")
	eq(t, st.Dropped(), int64(42))
	eq(t, st.Holding(), false)
	// A flush keeps the cut line dropped, so its end after the flush is not shown either
	st.Resume()
	eq(t, joined(st.Write([]byte("tail"))), "")
	eq(t, joined(st.Flush()), "")
	eq(t, joined(st.Write([]byte("more tail\r\nshown here\n"))), "shown here\n")
	// A key block's first line is never dropped
	st.Resume()
	got := joined(st.Write([]byte("cut -----BEGIN PRIVATE KEY-----\nMIIabc\n" +
		"-----END PRIVATE KEY-----\nafter\n")))
	eq(t, got, "[redacted private key]\nafter\n")
	// A line longer than the carry goes on after its last space
	st.Resume()
	eq(t, joined(st.Write([]byte(strings.Repeat("y", carryMax)+" word"))), "")
	eq(t, joined(st.Write([]byte("s\n"))), "words\n")
}

func TestStreamResumeDropsAHeldPartialToken(t *testing.T) {
	st := NewRedactor().Stream(Stdout)
	body := strings.Repeat("Z", 29)
	eq(t, joined(st.Write([]byte("token ghp_"+body))), "token ")
	eq(t, joined(st.Resume()), "")
	eq(t, joined(st.Write([]byte("Z\nok then\n"))), "ok then\n")
	st = NewRedactor().Stream(Stdout)
	eq(t, joined(st.Write([]byte("curl -H 'Authorization: Bearer abcdefgh"))), "curl -H ")
	eq(t, joined(st.Resume()), "'Authorization: [redacted credential]")
}

func TestRedactAnyAuthorizationSchemeAcrossFrames(t *testing.T) {
	const secret = "s3cr3tvalue123"
	r := NewRedactor()
	for _, c := range []struct {
		chunks []string
		want   string
	}{
		{[]string{"Authorization: token ", secret + "\n"},
			"Authorization: [redacted credential]\n"},
		{[]string{"Proxy-Authorization: token ", secret + "\n"},
			"Proxy-Authorization: [redacted credential]\n"},
		{[]string{"authorization: Digest ", `username="bob", response="` + secret + `"` + "\n"},
			"authorization: [redacted credential]\n"},
		{[]string{"PROXY-AUTHORIZATION: Dig", `est username="bob", response="` + secret + "\"\n"},
			"PROXY-AUTHORIZATION: [redacted credential]\n"},
		{[]string{"a\nAuthorization: to", "ken " + secret + "\nb\n"},
			"a\nAuthorization: [redacted credential]\nb\n"},
		{[]string{"x Proxy-Author", "ization: Negotiate ", secret + "\nnext\n"},
			"x Proxy-Authorization: [redacted credential]\nnext\n"},
		{[]string{"Authorization:  ", "MyScheme ", secret + "\r\n"},
			"Authorization:  [redacted credential]\n"},
		{[]string{`curl -H 'Authorization: Digest `, `username="bob", response="` + secret +
			`"' https://x` + "\n"},
			`curl -H 'Authorization: [redacted credential]' https://x` + "\n"},
		{[]string{`{"Proxy-Authorization": "token `, secret + `", "a": 1}` + "\n"},
			`{"Proxy-Authorization": "[redacted credential]", "a": 1}` + "\n"},
		{[]string{`-d "{\"Authorization\": \"token `, secret + `\", \"a\": 1}"` + "\n"},
			`-d "{\"Authorization\": \"[redacted credential]\", \"a\": 1}"` + "\n"},
		{[]string{`[["Proxy-Authorization", "token `, secret + `"], ["a", "b"]]` + "\n"},
			`[["Proxy-Authorization", "[redacted credential]"], ["a", "b"]]` + "\n"},
	} {
		got := joined(write(r, c.chunks...))
		eq(t, got, c.want)
		// Whatever frame boundary falls in the line, the value is never shown
		whole := strings.Join(c.chunks, "")
		for i := range len(whole) {
			if got := joined(write(r, whole[:i], whole[i:])); strings.Contains(got, secret) {
				t.Fatalf("split at %d of %q shows the value: %q", i, whole, got)
			}
		}
	}
}

func TestRedactAuthorizationAfterAClosedName(t *testing.T) {
	r := NewRedactor()
	for _, c := range []struct{ in, want string }{
		{`{"Authorization":["token QJs3cr3t"]}`, `{"Authorization":["[redacted credential]"]}`},
		{`http.Header{"Authorization":[]string{"token QJs3cr3t"}}`,
			`http.Header{"Authorization":[]string{"[redacted credential]"}}`},
		{`{"Authorization"=>"token QJs3cr3t"}`, `{"Authorization"=>"[redacted credential]"}`},
		{`{"Authorization" => "token QJs3cr3t"}`,
			`{"Authorization" => "[redacted credential]"}`},
		{`'Authorization' => 'token QJs3cr3t',`, `'Authorization' => '[redacted credential]',`},
		{"[Authorization] => token QJs3cr3t\n", "[Authorization] => [redacted credential]\n"},
		{`headers["Authorization"] = "token QJs3cr3t";`,
			`headers["Authorization"] = "[redacted credential]";`},
		{`{\\\"Authorization\\\":\\\"token QJs3cr3t\\\"}`,
			`{\\\"Authorization\\\":\\\"[redacted credential]\\\"}`},
	} {
		eq(t, r.Redact(c.in), c.want)
	}
}

func TestStreamKeepsALeadWordBeforeAnAuthorizationWord(t *testing.T) {
	st := NewRedactor().Stream(Stdout)
	got := st.Write([]byte(`{"h":"Bearer QJQJ98765abcdefgh","authorization": "token `))
	got = append(got, st.Write([]byte(`abc"}`+"\n"))...)
	hidden(t, got, `"authorization": "[redacted credential]"}`+"\n")
}

func TestRedactTokenAfterASkippedMatch(t *testing.T) {
	r := NewRedactor()
	body := strings.Repeat("Ab3_", 10)
	eq(t, r.Redact("risk-sk-ant-api03-"+body), "risk-[redacted token]")
	eq(t, r.Redact("xbnt_bnt_"+body+" y"), "xbnt_[redacted token] y")
	eq(t, r.Redact("xbnt_"+body), "xbnt_"+body)
}

func TestRedactURLPasswordWithAtOrSlash(t *testing.T) {
	r := NewRedactor()
	eq(t, r.Redact("git clone https://me:p@ss/w0rd@github.com/a/b.git"),
		"git clone https://me:[redacted password]@github.com/a/b.git")
	eq(t, r.Redact("DATABASE_URL=postgres://app:x/y@z@db:5432/main ok"),
		"DATABASE_URL=postgres://app:[redacted password]@db:5432/main ok")
	eq(t, r.Redact(`"https://u:secret@host/@scope/pkg"`),
		`"https://u:[redacted password]@host/@scope/pkg"`)
	eq(t, r.Redact("GET http://localhost:5173/@vite/client"),
		"GET http://localhost:5173/@vite/client")
	// With no other @, one right after a slash ends the password
	eq(t, r.Redact("https://u:pass/@host/x"), "https://u:[redacted password]@host/x")
	// A user may hold an @, and a host's port before a path is no password
	eq(t, r.Redact("https://me@x.com:pass@host"), "https://me@x.com:[redacted password]@host")
	eq(t, r.Redact("GET http://localhost:3000/users/a@b.com 200"),
		"GET http://localhost:3000/users/a@b.com 200")
	eq(t, r.Redact("ssh://git@host:22/a@b"), "ssh://git@host:22/a@b")
	eq(t, r.Redact("redis://:pa@ss@host:6379, x=https://u:p@host'"),
		"redis://:[redacted password]@host:6379, x=https://u:[redacted password]@host'")
	// A URL inside another URL's query is read on its own
	eq(t, r.Redact("https://a/?next=http://u:secret1@b/"),
		"https://a/?next=http://u:[redacted password]@b/")
}

func TestRedactFullCarryHoldsWhatItWouldCut(t *testing.T) {
	const known = "Kn0wnSecretQQQQ123"
	r := NewRedactor(known)
	pad := strings.Repeat("x", carryMax-10)
	json := `{"a":"` + strings.Repeat("y", carryMax-40) + `","Authorization":"token `
	for _, c := range []struct{ head, tail, want string }{
		{pad, ",bnt_" + strings.Repeat("Q", 30) + "\n", ",[redacted token]\n"},
		{pad, "," + known + ",x\n", ",[redacted secret],x\n"},
		{json, strings.Repeat("Q", 20) + `"}` + "\n", `"[redacted credential]"}` + "\n"},
	} {
		for i := range len(c.tail) + 1 {
			got := ""
			for _, f := range write(r, c.head, c.tail[:i], c.tail[i:]) {
				if strings.Contains(f.T, "Q") {
					t.Fatalf("split %d of %q shows %q", i, c.tail, f.T[max(len(f.T)-60, 0):])
				}
				got += f.T
			}
			eq(t, strings.HasSuffix(got, c.want), true)
		}
	}
}

func TestStreamGapInsideAKeyShowsNoBody(t *testing.T) {
	line := func(c string) string { return strings.Repeat(c, 64) + "\n" }
	const begin, end = "-----BEGIN PRIVATE KEY-----\n", "-----END PRIVATE KEY-----\n"
	noBody := func(got string) {
		t.Helper()
		if strings.ContainsAny(got, "ABCDEFGHJ") {
			t.Fatalf("a key body shows in %q", got)
		}
	}
	// The block is held when the gap comes, its BEGIN with it
	st := NewRedactor().Stream(Stdout)
	got := joined(st.Write([]byte(begin + line("A") + line("B"))))
	got += joined(st.Resume())
	got += joined(st.Write([]byte(line("C") + line("D") + "EEEE==\n" + end + "after\n")))
	noBody(got)
	eq(t, got, "[redacted private key]\nafter\n")
	// A block longer than the carry is shown redacted before the gap, and stays so after it
	st = NewRedactor().Stream(Stdout)
	got = joined(st.Write([]byte(begin + strings.Repeat(line("F"), 300))))
	got += joined(st.Resume())
	got += joined(st.Write([]byte(line("G") + line("H") + end + "after\n")))
	noBody(got)
	eq(t, got, "[redacted private key]\nafter\n")
	// Output read from past its start may begin inside a block, with no BEGIN
	st = NewRedactor().Stream(Stdout)
	st.Late()
	got = joined(st.Write([]byte("Proc-Type: 4,ENCRYPTED\n\n" + line("J"))))
	got += joined(st.Write([]byte(end + "after\n")))
	noBody(got)
	eq(t, got, "\nafter\n")
	eq(t, st.Holding(), false)
	// A block whose END the gap took is dropped up to the first other line
	st = NewRedactor().Stream(Stdout)
	got = joined(st.Write([]byte(begin + line("A"))))
	got += joined(st.Resume())
	got += joined(st.Write([]byte(line("B") + line("C") + "BBB=\nok then\n")))
	noBody(got)
	eq(t, got, "[redacted private key]ok then\n")
	// A block the gap found open stays dropped through its END, whatever its lines look like
	st = NewRedactor().Stream(Stdout)
	got = joined(st.Write([]byte(begin + strings.Repeat(line("F"), 300))))
	got += joined(st.Resume())
	got += joined(st.Write([]byte("FF\nnot a body line\n" + end + "after\n")))
	eq(t, got, "[redacted private key]\nafter\n")
	// and its END right after the gap still ends it
	st = NewRedactor().Stream(Stdout)
	got = joined(st.Write([]byte(begin + strings.Repeat(line("F"), 300))))
	got += joined(st.Resume())
	got += joined(st.Write([]byte(end + "after\n")))
	eq(t, got, "[redacted private key]\nafter\n")
}

func TestStreamGapInsideAnIndentedKeyShowsNoBody(t *testing.T) {
	line := func(c string) string { return "    " + strings.Repeat(c, 64) + "\n" }
	const begin = "  tls.key: |\n    -----BEGIN RSA PRIVATE KEY-----\n"
	const end = "    -----END RSA PRIVATE KEY-----  \n"
	st := NewRedactor().Stream(Stdout)
	st.Late()
	got := joined(st.Write([]byte(line("Q") + "\t" + strings.Repeat("J", 40) + "==\n" + end)))
	got += joined(st.Write([]byte("after text\n")))
	eq(t, got, "  \nafter text\n")
	st = NewRedactor().Stream(Stdout)
	got = joined(st.Write([]byte(begin + line("Q") + line("J"))))
	got += joined(st.Resume())
	got += joined(st.Write([]byte(line("Q") + line("J") + "    Proc-Type: 4,ENCRYPTED\n" +
		"    QJQJ==\n" + end + "after text\n")))
	eq(t, got, "  tls.key: |\n    [redacted private key]  \nafter text\n")
}

// chunks cuts s into frames of size bytes
func chunks(s string, size int) []string {
	var out []string
	for i := 0; i < len(s); i += size {
		out = append(out, s[i:min(i+size, len(s))])
	}
	return out
}

// hidden fails when a frame shows a secret written with Q, J or bob, or the next line is lost
func hidden(t *testing.T, got []Frame, next string) {
	t.Helper()
	for _, f := range got {
		if strings.ContainsAny(f.T, "QJ") || strings.Contains(f.T, "bob") {
			t.Fatalf("shows %q", f.T[max(len(f.T)-80, 0):])
		}
	}
	if all := joined(got); !strings.HasSuffix(all, next) {
		t.Fatalf("lost the next line: %q", all[max(len(all)-80, 0):])
	}
}

func TestStreamLongLineKeepsAnAuthorizationValueWhole(t *testing.T) {
	r := NewRedactor()
	// After a gap, a line longer than the carry does not go on after a space inside the value
	st := r.Stream(Stdout)
	st.Resume()
	got := st.Write([]byte(strings.Repeat("a ", 8190) +
		`Authorization: Digest username="bob", resp`))
	got = append(got, st.Write([]byte(`onse="QJQJ98" x`+"\nafter text\n"))...)
	hidden(t, got, "after text\n")
	// The same when the gap falls inside the value
	st = r.Stream(Stdout)
	got = st.Write([]byte(`Authorization: Digest username="bob", `))
	got = append(got, st.Resume()...)
	for _, c := range chunks(strings.Repeat(`k="QJ", `, 3000)+"\nafter text\n", 4096) {
		got = append(got, st.Write([]byte(c))...)
	}
	hidden(t, got, "after text\n")
	// A value with spaces longer than two carries, after the full carry it filled was dropped
	for _, whole := range []string{
		"Authorization: Digest " + strings.Repeat("k=QJQJ, ", 6000) + "\nafter text\n",
		"curl -H 'Proxy-Authorization: Token " + strings.Repeat("QJ ", 16000) +
			"' x\nafter text\n",
		`{"Authorization": "Digest ` + strings.Repeat(`a=\"QJ\", `, 6000) + `"}` +
			"\nafter text\n",
	} {
		for _, size := range []int{100, 4096, carryMax} {
			hidden(t, write(r, chunks(whole, size)...), "after text\n")
		}
	}
}

func TestRedactFullCarryAtEveryBoundary(t *testing.T) {
	const known, other = "Kn0wnSecretQJQJ123", "Kn0wn:Secret@QJQJ!7"
	r := NewRedactor(known, other)
	for _, c := range []struct{ name, secret string }{
		{`"t":"`, "bnt_" + strings.Repeat("Q", 30)},
		{`"t":"x.`, "bnt_" + strings.Repeat("Q", 30)},
		{`"k":"pre-`, known},
		{`"k":"`, other},
		{`"k":"`, other + "and_more"},
		{`"k":"`, "sk-ant-api03-" + strings.Repeat("Q", 40)},
		{`"Authorization":"`, `Digest username=\"bob\",response=\"QJ98\"`},
		{`"authorization": "`, "token QJQJ98765"},
		{`"h":"`, "Bearer QJQJ98765abcdefgh"},
		{`"u":"`, "postgres://app:QJQJpw@db/x"},
		{`"u":"`, "https://u:QJ@QJ/QJ@db.example.com/x"},
	} {
		line := `,` + c.name + c.secret + `","b":1}`
		// The 16 KiB a full carry shows ends at every byte of the line, before its break comes
		for at := range len(line) + 1 {
			head := `{"a":"` + strings.Repeat("y", carryMax-6-at) + `"`
			whole := head + line + "\nafter text\n"
			for _, split := range []int{carryMax, len(head + line)} {
				got := write(r, whole[:split], whole[split:])
				for _, f := range got {
					if strings.ContainsAny(f.T, "QJ") || strings.Contains(f.T, "bob") ||
						strings.Contains(f.T, "Kn0wn") {
						t.Fatalf("%s cut at %d, frame %d: shows %q", c.secret, at, split,
							f.T[max(len(f.T)-60, 0):])
					}
				}
				eq(t, strings.HasSuffix(joined(got), "after text\n"), true)
			}
		}
	}
}
