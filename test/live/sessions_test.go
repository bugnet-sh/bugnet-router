package live

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pelletier/go-toml/v2"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/vkclient"
)

const (
	// sessionsKeepLive is how long a finished run's page lives in the sessions suite
	sessionsKeepLive = 3 * time.Minute
	// copyWithin is how long the copy may leave a record pending
	copyWithin = 10 * time.Minute
	// sessionsPayer is a canonical Solana address, the wallet SESS5 blocks
	sessionsPayer = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
)

// sessionsKeep is the sessions.keep every stack of the sessions suite runs with
var sessionsKeep string

// sessionsRows are SP 7.5's live rows
var sessionsRows []row

func TestLiveSessions(t *testing.T) {
	sessionsKeep = sessionsKeepLive.String()
	t.Cleanup(func() { sessionsKeep = "" })
	runRows(t, sessionsRows)
}

func init() {
	sessionsRows = append(sessionsRows,
		row{id: "SESS1", run: rowSESS1},
		row{id: "SESS2", run: rowSESS2},
		row{id: "SESS3", run: rowSESS3},
		row{id: "SESS4", run: rowSESS4},
		row{id: "SESS5", run: rowSESS5},
		row{id: "SESS6", run: rowSESS6},
	)
}

/* The page client */

// pageRequest builds the call the bundle makes for a session link, the key only in the body
func pageRequest(t *testing.T, method, link, suffix string, extra map[string]any) *http.Request {
	t.Helper()
	u, err := url.Parse(link)
	if err != nil {
		t.Fatalf("live: session link: %v", err)
	}
	frag, err := url.ParseQuery(u.Fragment)
	if err != nil || frag.Get("k") == "" || !strings.HasPrefix(u.Path, "/s/") {
		t.Fatalf("live: %q is not a session link with a key", link)
	}
	body := map[string]any{"k": frag.Get("k")}
	for k, v := range extra {
		body[k] = v
	}
	raw, _ := json.Marshal(body)
	origin := u.Scheme + "://" + u.Host
	req, err := http.NewRequestWithContext(context.Background(), method,
		origin+"/v1/s/"+path.Base(u.Path)+suffix, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", origin)
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Content-Type", "application/json")
	return req
}

// pageClient is an HTTP/1.1 client that reaches this stack's sessions role
func (s *stack) pageClient(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{Protocols: new(http.Protocols), DisableCompression: true}
	tr.Protocols.SetHTTP1(true)
	if s.mode == "local" {
		roots := x509.NewCertPool()
		if pem, err := os.ReadFile(filepath.Join(s.selfSigned(t), "fullchain.pem")); err == nil {
			roots.AppendCertsFromPEM(pem)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: roots}
		d := &net.Dialer{}
		tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return d.DialContext(ctx, network, s.sessionsAddr)
		}
	}
	return &http.Client{Transport: tr, Timeout: time.Minute}
}

// page makes one call of the page and answers the status and the body
func (s *stack) page(t *testing.T, method, link string) (int, []byte) {
	t.Helper()
	resp, err := s.pageClient(t).Do(pageRequest(t, method, link, "", nil))
	if err != nil {
		t.Fatalf("live: the page %s: %v", method, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// pageView is what the page's POST answers
type pageView struct {
	State    string `json:"state"`
	Snapshot struct {
		Jobs []struct {
			Seq     int    `json:"seq"`
			Command string `json:"command"`
			State   string `json:"state"`
		} `json:"jobs"`
	} `json:"snapshot"`
	Record struct {
		Output string `json:"output"`
		Jobs   []struct {
			Seq     int    `json:"seq"`
			Command string `json:"command"`
			Frames  []struct {
				S  string `json:"s"`
				T  string `json:"t"`
				CR bool   `json:"cr"`
			} `json:"frames"`
		} `json:"jobs"`
	} `json:"record"`
}

// view reads the page and answers its status, its decoded body and the raw body
func (s *stack) view(t *testing.T, link string) (int, pageView, string) {
	t.Helper()
	code, raw := s.page(t, "POST", link)
	var v pageView
	if code == http.StatusOK {
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatalf("live: the page answered %q: %v", raw, err)
		}
	}
	return code, v, string(raw)
}

// waitView reads the page every 5 s until ok accepts it
func (s *stack) waitView(
	t *testing.T, link string, within time.Duration, ok func(int, pageView) bool,
) (pageView, string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		code, v, raw := s.view(t, link)
		if ok(code, v) {
			return v, raw
		}
		if time.Now().After(deadline) {
			t.Fatalf("live: the page still answers %d %s after %s", code, raw, within)
		}
		time.Sleep(5 * time.Second)
	}
}

// sessionLink is the placement's session_url
func (s *stack) sessionLink(t *testing.T, h string) string {
	t.Helper()
	link := kv(s.dev(t, "get", "--handle", h))["session_url"]
	if link == "" {
		t.Fatal("live: the placement has no session_url, sessions.base_url is empty")
	}
	return link
}

// endAndCopy stops the placement and waits for its record to hold the copied output
func (s *stack) endAndCopy(t *testing.T, h, link string) (pageView, string) {
	t.Helper()
	s.dev(t, "stop", "--handle", h)
	return s.waitView(t, link, copyWithin, func(code int, v pageView) bool {
		return code == http.StatusOK && v.State == "ended" && v.Record.Output == "complete"
	})
}

/* Live streams */

// sseEvent is one server-sent event
type sseEvent struct{ name, id, data string }

// readEvents reads events until fn answers false or the stream ends
func readEvents(r io.Reader, fn func(sseEvent) bool) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var ev sseEvent
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "" && ev.name != "":
			if !fn(ev) {
				return nil
			}
			ev = sseEvent{}
		case strings.HasPrefix(line, "event: "):
			ev.name = line[len("event: "):]
		case strings.HasPrefix(line, "id: "):
			ev.id = line[len("id: "):]
		case strings.HasPrefix(line, "data: "):
			ev.data = line[len("data: "):]
		}
	}
	return sc.Err()
}

// watchLive posts to a job's live route from cursor and feeds fn the events it gets
func (s *stack) watchLive(
	t *testing.T, link string, seq int, cursor string, fn func(sseEvent) bool,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	req := pageRequest(t, "POST", link, fmt.Sprintf("/jobs/%d/live", seq),
		map[string]any{"cursor": cursor}).WithContext(ctx)
	resp, err := s.pageClient(t).Do(req)
	if err != nil {
		t.Fatalf("live: the live route: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("live: the live route answered %d", resp.StatusCode)
	}
	if err := readEvents(resp.Body, fn); err != nil && ctx.Err() == nil {
		t.Fatalf("live: reading the live route: %v", err)
	}
}

// frameLines draws frames as the page does, a cr frame replacing the last line
func frameLines(texts []string, cr []bool) []string {
	var lines []string
	cur := ""
	for i, text := range texts {
		if cr[i] {
			cur = ""
		}
		for _, c := range text {
			if c == '\n' {
				lines = append(lines, cur)
				cur = ""
			} else {
				cur += string(c)
			}
		}
	}
	if cur != "" {
		lines = append(lines, cur)
	}
	return lines
}

/* Stack helpers */

// pg opens the stack's database
func (s *stack) pg(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := config.Load(s.settings, s.lookup)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(t.Context(), cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// jobStreams counts the Valkey keys of a job's streams
func (s *stack) jobStreams(t *testing.T, jobID string) int {
	t.Helper()
	cfg, err := config.Load(s.settings, s.lookup)
	if err != nil {
		t.Fatal(err)
	}
	vk, err := vkclient.New(cfg.ValkeyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer vk.Close()
	var n int
	var cursor uint64
	for {
		cmd := vk.B().Scan().Cursor(cursor).Match(cfg.ValkeyPrefix + "job:" + jobID + ":*").
			Count(1000).Build()
		entry, err := vk.Do(t.Context(), cmd).AsScanEntry()
		if err != nil {
			t.Fatalf("live: scan valkey: %v", err)
		}
		n += len(entry.Elements)
		if cursor = entry.Cursor; cursor == 0 {
			return n
		}
	}
}

// restartWorker restarts router-worker, which runs session_expiry as it starts
func (s *stack) restartWorker(t *testing.T) {
	t.Helper()
	for i, p := range s.procs {
		if p.name != "router-worker" {
			continue
		}
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(stopGrace):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
		s.procs = append(s.procs[:i], s.procs[i+1:]...)
		break
	}
	raw, err := os.ReadFile(s.settings)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]any{}
	if err := toml.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	s.start(t, "router-worker", "-config", s.settings)
	s.waitReady(t, "http://"+table(m, "worker")["listen_addr"].(string)+"/readyz")
}

// runBackground runs a command in the sandbox and answers a channel that closes when it exits
func (s *stack) runBackground(t *testing.T, h, script string) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		cmd := exec.Command(filepath.Join(s.bin, "bugnet-dev"),
			"--config", s.settings, "exec", "--handle", h, "--", "sh", "-c", script)
		cmd.Dir, cmd.Env = s.root, s.env
		done <- cmd.Run()
	}()
	return done
}

/* Rows */

var ipv4 = regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)

// secretValues are one value of each Bugnet and vendor shape, then a header
var secretValues = []string{
	"bnk_AbCdEfGhIjKlMnOpQrStUvWx", "bnp_AbCdEfGhIjKlMnOpQrStUvWx",
	"sbx_AbCdEfGhIjKlMnOpQrStUvWx", "sk-ant-AbCdEfGhIjKlMnOpQrStUvWxYz",
	"ghp_AbCdEfGhIjKlMnOpQrStUvWxYz0123456789", "hf_AbCdEfGhIjKlMnOpQrStUvWxYz0123456789",
	"AKIAABCDEFGHIJKLMNOP", "xoxb-1234567890-AbCdEfGhIjKlMnOp",
}

const pemBody = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC"

// rowSESS1 runs two commands and reads the record the page keeps
func rowSESS1(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	link := s.sessionLink(t, h)
	args := append([]string{"%s\n", "# sess1a"}, secretValues...)
	args = append(args, "Authorization: Bearer abcdefghijklmnopqrstuvwx",
		"-----BEGIN PRIVATE KEY-----", pemBody, "-----END PRIVATE KEY-----")
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = "'" + a + "'"
	}
	s.sh(t, h, "printf "+strings.Join(quoted, " "))
	s.sh(t, h, `printf 'p 10%%\rp 50%%\rp 100%%\n' # sess1b`)
	_, raw := s.endAndCopy(t, h, link)
	var v pageView
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatal(err)
	}
	var secret, bar []string
	var secretCR, barCR []bool
	for _, j := range v.Record.Jobs {
		for _, f := range j.Frames {
			if strings.Contains(j.Command, "sess1b") {
				bar, barCR = append(bar, f.T), append(barCR, f.CR)
			} else {
				secret, secretCR = append(secret, f.T), append(secretCR, f.CR)
			}
		}
	}
	for _, val := range append(secretValues, pemBody, "abcdefghijklmnopqrstuvwx") {
		if strings.Contains(raw, val) {
			t.Fatalf("live: the page shows %q", val)
		}
	}
	text := strings.Join(frameLines(secret, secretCR), "\n")
	for _, kind := range []string{"[redacted token]", "[redacted credential]",
		"[redacted private key]"} {
		if !strings.Contains(text, kind) {
			t.Fatalf("live: the output lacks %s in %q", kind, text)
		}
	}
	eq(t, frameLines(bar, barCR), []string{"p 100%"})
	for _, word := range []string{s.provider, h} {
		if strings.Contains(strings.ToLower(raw), strings.ToLower(word)) {
			t.Fatalf("live: the page names %q", word)
		}
	}
	if ip := ipv4.FindString(raw); ip != "" {
		t.Fatalf("live: the page shows the address %s", ip)
	}
}

// rowSESS2 follows a running job live, cuts the stream and resumes it by cursor
func rowSESS2(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	link := s.sessionLink(t, h)
	done := s.runBackground(t, h, `i=1; while [ $i -le 30 ]; do echo line-$i; i=$((i+1)); `+
		`sleep 1; done # sess2`)
	seq := 0
	s.waitView(t, link, time.Minute, func(code int, v pageView) bool {
		for _, j := range v.Snapshot.Jobs {
			if strings.Contains(j.Command, "sess2") {
				seq = j.Seq
			}
		}
		return code == http.StatusOK && v.State == "running" && seq != 0
	})
	var want strings.Builder
	for i := 1; i <= 30; i++ {
		fmt.Fprintf(&want, "line-%d\n", i)
	}
	var text strings.Builder
	committed, cursor, ids := 0, "", 0
	s.watchLive(t, link, seq, "", func(ev sseEvent) bool {
		if ev.name == "out" {
			var f struct{ T string }
			_ = json.Unmarshal([]byte(ev.data), &f)
			text.WriteString(f.T)
			if ev.id != "" {
				committed, cursor = text.Len(), ev.id
				ids++
			}
		}
		return ids < 5
	})
	if cursor == "" {
		t.Fatal("live: the first stream gave no cursor")
	}
	// Text after the last cursor is sent again by the resume
	got := text.String()[:committed]
	var exit, end string
	s.watchLive(t, link, seq, cursor, func(ev sseEvent) bool {
		switch ev.name {
		case "out":
			var f struct{ T string }
			_ = json.Unmarshal([]byte(ev.data), &f)
			got += f.T
		case "exit":
			exit = ev.data
		case "end":
			end = ev.data
			return false
		}
		return true
	})
	eq(t, got, want.String())
	if !strings.Contains(exit, `"exitCode":0`) || !strings.Contains(end, "reason") {
		t.Fatalf("live: exit %q and end %q", exit, end)
	}
	<-done
}

// rowSESS3 deletes a record from the page and checks its link and its job streams
func rowSESS3(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	link := s.sessionLink(t, h)
	s.sh(t, h, "echo sess3")
	s.endAndCopy(t, h, link)
	id := path.Base(strings.SplitN(link, "#", 2)[0])
	var jobID string
	err := s.pg(t).QueryRow(t.Context(),
		`select job_id from session_jobs where record_id = $1 order by seq limit 1`, id).
		Scan(&jobID)
	if err != nil {
		t.Fatalf("live: the record's job: %v", err)
	}
	if code, _ := s.page(t, "DELETE", link); code != http.StatusNoContent {
		t.Fatalf("live: delete answered %d", code)
	}
	if code, _ := s.page(t, "POST", link); code != http.StatusNotFound {
		t.Fatalf("live: the deleted link answered %d", code)
	}
	eq(t, s.jobStreams(t, jobID), 0)
}

// rowSESS4 waits out sessions.keep and then the worker's expiry job
func rowSESS4(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	link := s.sessionLink(t, h)
	s.sh(t, h, "echo sess4")
	s.endAndCopy(t, h, link)
	s.waitView(t, link, sessionsKeepLive+time.Minute, func(code int, v pageView) bool {
		return code == http.StatusOK && v.State == "expired"
	})
	s.restartWorker(t)
	deadline := time.Now().Add(time.Minute)
	for {
		code, _ := s.page(t, "POST", link)
		if code == http.StatusNotFound {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("live: the expired link still answers %d after a worker restart", code)
		}
		time.Sleep(2 * time.Second)
	}
}

// rowSESS5 ends a run by blocking its payer and checks the hold keeps the rows
func rowSESS5(t *testing.T, s *stack) {
	out := kv(s.dev(t, "create", "--package", xferPackage, "--payer", sessionsPayer))
	h := out["handle"]
	if h == "" {
		t.Fatal("live: bugnet-dev create printed no handle")
	}
	t.Cleanup(func() {
		_, _, _ = s.devTry(t, "wallet", "unblock", sessionsPayer, "--note", "SESS5 ended")
		_, _, _ = s.devTry(t, "stop", "--handle", h)
	})
	link := s.sessionLink(t, h)
	s.sh(t, h, "echo sess5")
	placement := kv(s.dev(t, "get", "--handle", h))["placement_id"]
	s.dev(t, "wallet", "block", sessionsPayer, "--reason", "SESS5", "--stop-live")
	s.waitView(t, link, 2*time.Minute, func(code int, v pageView) bool {
		return code == http.StatusOK && v.State == "expired"
	})
	hold := func() map[string]string {
		m := kv(s.dev(t, "session", "hold", "--placement", placement))
		if !strings.Contains(m["record"], `"jobs"`) {
			t.Fatalf("live: session hold printed %v", m)
		}
		return m
	}
	hold()
	s.restartWorker(t)
	time.Sleep(10 * time.Second)
	hold()
}

// rowSESS6 rotates the link and checks the old one is gone
func rowSESS6(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	old := s.sessionLink(t, h)
	if code, _, raw := s.view(t, old); code != http.StatusOK {
		t.Fatalf("live: the first link answered %d %s", code, raw)
	}
	fresh := kv(s.dev(t, "session", "rotate", "--handle", h))["session_url"]
	if fresh == "" || fresh == old {
		t.Fatalf("live: rotate printed %q", fresh)
	}
	if code, _ := s.page(t, "POST", old); code != http.StatusNotFound {
		t.Fatalf("live: the old link answered %d", code)
	}
	if code, v, raw := s.view(t, fresh); code != http.StatusOK || v.State != "running" {
		t.Fatalf("live: the new link answered %d %s", code, raw)
	}
}

/* Without a provider */

func TestPageClientKeepsTheKeyInTheBody(t *testing.T) {
	key := "bnk_AbCdEfGhIjKlMnOpQrStUvWxYz0123456789abcdefg"
	req := pageRequest(t, "POST", "https://sessions.dev.bugnet.sh/s/abcdefghijklmnop#k="+key,
		"", nil)
	eq(t, strings.Contains(req.URL.String(), "bnk_"), false)
	eq(t, req.URL.Path, "/v1/s/abcdefghijklmnop")
	eq(t, req.Header.Get("Sec-Fetch-Site"), "same-origin")
	eq(t, req.Header.Get("Origin"), "https://sessions.dev.bugnet.sh")
	raw, _ := io.ReadAll(req.Body)
	eq(t, string(raw), `{"k":"`+key+`"}`)
}

func TestSuiteFlag(t *testing.T) {
	out, err := exec.Command("bash", filepath.Join(repo(t), "scripts", "live-test.sh"),
		"--suite", "sessions").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "the sessions suite needs at least one") {
		t.Fatalf("live-test.sh --suite sessions answered %v %s", err, out)
	}
}
