package live

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/catalog"
)

// row is one live row, run as the subtest <id>/<provider> on each provider it names
type row struct {
	id string
	// providers lists the providers the row runs on, every named provider when empty
	providers []string
	// vps means the row needs the dev front, so it fails at once in the local mode
	vps bool
	run func(t *testing.T, s *stack)
}

// dataplaneRows are DP 16.2's rows and the plan's 16.3 ids, each task adding its own
var dataplaneRows []row

func TestLiveDataplane(t *testing.T) {
	runRows(t, dataplaneRows)
}

// providerList is -providers as a list
func providerList() []string {
	var out []string
	for _, p := range strings.Split(*providersFlag, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// runRows runs each row selected by -run on each of its providers, one provider's stack at a time
func runRows(t *testing.T, rows []row) {
	providers := providerList()
	if len(providers) == 0 {
		t.Skip("live rows run through scripts/live-test.sh, which names -providers")
	}
	f, err := loadFront(filepath.Join(repo(t), "config", "dev-front.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPlan(providers, *frontFlag, f, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, p := range providers {
		var mine []row
		for _, r := range rows {
			if (len(r.providers) == 0 || slices.Contains(r.providers, p)) &&
				wanted(t.Name(), r.id, p) {
				mine = append(mine, r)
			}
		}
		if len(mine) == 0 {
			continue
		}
		var s *stack
		if needsStack(mine, *frontFlag) {
			s = startStack(t, p, *frontFlag, f)
		}
		for _, r := range mine {
			t.Run(r.id, func(t *testing.T) {
				t.Run(p, func(t *testing.T) {
					if r.vps && *frontFlag != "vps" {
						t.Fatalf("%s needs the VPS, run scripts/live-test.sh with --front vps",
							r.id)
					}
					r.run(t, s)
				})
			})
		}
		if s != nil {
			s.stop(t)
		}
	}
}

// needsStack is false when every row would fail at once for want of the dev front
func needsStack(rows []row, mode string) bool {
	return slices.ContainsFunc(rows, func(r row) bool { return !r.vps || mode == "vps" })
}

// wanted reports whether go test's -run pattern selects the subtest with these path names
func wanted(names ...string) bool {
	f := flag.Lookup("test.run")
	if f == nil || f.Value.String() == "" {
		return true
	}
	for _, alt := range split(f.Value.String(), '|') {
		if matches(split(alt, '/'), names) {
			return true
		}
	}
	return false
}

func matches(elems, names []string) bool {
	for i, e := range elems {
		if i >= len(names) {
			break
		}
		re, err := regexp.Compile(e)
		if err != nil || !re.MatchString(names[i]) {
			return false
		}
	}
	return true
}

// split cuts pattern at each sep outside parentheses and brackets, as go test reads -run
func split(pattern string, sep byte) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(pattern); i++ {
		switch c := pattern[i]; {
		case c == '\\':
			i++
		case c == '(' || c == '[':
			depth++
		case c == ')' || c == ']':
			depth--
		case c == sep && depth == 0:
			out = append(out, pattern[start:i])
			start = i + 1
		}
	}
	return append(out, pattern[start:])
}

func TestRunPatternSelection(t *testing.T) {
	for pattern, want := range map[string]bool{
		"":                                 true,
		"LiveDataplane":                    true,
		"LiveDataplane/(T[1-7]|BOAT5)":     true,
		"LiveDataplane/(T[1-7]|BOAT5)/e2b": true,
		"LiveDataplane/(T[1-7]|BOAT5)/boa": false,
		"LiveDataplane/BOAT5":              false,
		"LiveControl|LiveDataplane/T":      true,
		"LiveControl":                      false,
	} {
		elems := []string{"TestLiveDataplane", "T3", "e2b"}
		got := pattern == ""
		for _, alt := range split(pattern, '|') {
			got = got || matches(split(alt, '/'), elems)
		}
		if got != want {
			t.Errorf("-run %q selects T3 on e2b %v, want %v", pattern, got, want)
		}
	}
	eq(t, split(`a/(b|c)/d\/e|f`, '|'), []string{`a/(b|c)/d\/e`, "f"})
	eq(t, needsStack([]row{{vps: true}}, "local"), false)
	eq(t, needsStack([]row{{vps: true}, {}}, "local"), true)
}

/* Transfer rows, DP 16.2 T1 to T7 and the plan's BOAT5 */

const (
	mib = 1 << 20
	// xferPackage is a 10-minute package whose 2 GiB edge budget the small rows fit in
	xferPackage = "dev-small-10m"
	// bigPackage gives T2's 1 GiB each way 30 minutes and a 4 GiB edge budget
	bigPackage = "dev-xfer-30m"
	// asRoot sets $S to the command that runs as root, empty where the exec user is root
	asRoot = `S=; [ "$(id -u)" = 0 ] || S="sudo -n"; `
	// inUseRetries covers a released claim that the router has not freed yet
	inUseRetries = 5
)

func init() {
	dataplaneRows = append(dataplaneRows,
		row{id: "T1", run: rowT1},
		row{id: "T2", run: rowT2},
		row{id: "T3", run: rowT3},
		row{id: "T4", run: rowT4},
		row{id: "T5", run: rowT5},
		row{id: "T6", run: rowT6},
		row{id: "T7", run: rowT7},
		row{id: "BOAT5", providers: []string{"boat"}, run: rowBoat5},
	)
}

// rowT1 round-trips 0 B, 1 B, 1 MiB + 1, 5 MiB + 1 and 101 MiB, byte for byte
func rowT1(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	dir := t.TempDir()
	for _, n := range []int64{0, 1, mib + 1, 5*mib + 1, 101 * mib} {
		local := randomFile(t, dir, fmt.Sprintf("t1-%d", n), n)
		name := fmt.Sprintf("bugnet-t1-%d", n)
		got := s.upload(t, h, local, "~/"+name)
		eq(t, got.Size, n)
		eq(t, got.SHA256, fileSHA(t, local))
		eq(t, s.remoteSHA(t, h, `"$HOME/`+name+`"`), fileSHA(t, local))
		out := s.download(t, h, "~/"+name, filepath.Join(dir, "down"))
		eq(t, fileSHA(t, out), fileSHA(t, local))
	}
}

// rowT2 sends 1 GiB up and down with curl over HTTP/1.1 and times each way
func rowT2(t *testing.T, s *stack) {
	h := s.sandbox(t, bigPackage)
	dir := t.TempDir()
	local := randomFile(t, dir, "t2", 1<<30)
	start := time.Now()
	got := s.upload(t, h, local, "~/bugnet-t2")
	up := time.Since(start)
	eq(t, got.SHA256, fileSHA(t, local))
	start = time.Now()
	out := s.download(t, h, "~/bugnet-t2", filepath.Join(dir, "down"))
	down := time.Since(start)
	eq(t, fileSHA(t, out), fileSHA(t, local))
	t.Logf("T2 on %s: 1 GiB up in %s (%.1f MiB/s), down in %s (%.1f MiB/s)", s.provider,
		up.Round(time.Second), 1024/up.Seconds(), down.Round(time.Second), 1024/down.Seconds())
	if up > 30*time.Minute || down > 30*time.Minute {
		t.Fatalf("T2 took %s up and %s down, past 30 minutes", up, down)
	}
}

// rowT3 cuts a download at half its size, then resumes it with curl -C -
func rowT3(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	const size = 20 * mib
	want := s.sh(t, h, `head -c `+strconv.Itoa(size)+` /dev/urandom > "$HOME/bugnet-t3" && `+
		`sha256sum "$HOME/bugnet-t3" | cut -d' ' -f1`)
	url := s.mint(t, h, "--download", "~/bugnet-t3")["url"]
	out := filepath.Join(t.TempDir(), "t3")
	res, err := s.client().Get(url)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.StatusCode, http.StatusOK)
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(f, res.Body, size/2); err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	_ = f.Close()
	s.curlRetry(t, "-fsS", "-C", "-", "-o", out, url)
	eq(t, fileSHA(t, out), strings.TrimSpace(want))
}

// rowT4 cuts an upload at half, then sends one with a wrong sha256, and the old file stays
func rowT4(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	old := s.sh(t, h, `head -c 4194304 /dev/urandom > "$HOME/bugnet-t4" && `+
		`sha256sum "$HOME/bugnet-t4" | cut -d' ' -f1`)
	dir := t.TempDir()
	const size = 20 * mib
	local := randomFile(t, dir, "t4", size)
	url := s.mint(t, h, "--upload", local, "--path", "~/bugnet-t4")["url"]
	f, err := os.Open(local)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	body := io.MultiReader(io.LimitReader(f, size/2), cutReader{})
	req, _ := http.NewRequest(http.MethodPut, url, body)
	req.ContentLength = size
	if res, err := s.client().Do(req); err == nil {
		_ = res.Body.Close()
		t.Fatalf("an upload cut at half answered %d", res.StatusCode)
	}
	time.Sleep(2 * time.Second)
	eq(t, s.remoteSHA(t, h, `"$HOME/bugnet-t4"`), strings.TrimSpace(old))

	small := randomFile(t, dir, "t4-small", mib)
	url = s.mint(t, h, "--upload", small, "--path", "~/bugnet-t4",
		"--sha256", strings.Repeat("0", 64))["url"]
	code, reply := s.curlStatus(t, "-sS", "-T", small, url)
	eq(t, code, "422")
	if !strings.Contains(reply, "checksum_mismatch") {
		t.Fatalf("a wrong sha256 answered %s", reply)
	}
	eq(t, s.remoteSHA(t, h, `"$HOME/bugnet-t4"`), strings.TrimSpace(old))
}

// cutReader fails a request body, so the client drops the connection mid-upload
type cutReader struct{}

func (cutReader) Read([]byte) (int, error) {
	return 0, errors.New("live: the upload is cut here")
}

// rowT5 asks for /dev/urandom and a FIFO and gets NOT_REGULAR for both
func rowT5(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	s.sh(t, h, `rm -f "$HOME/bugnet-t5" && mkfifo "$HOME/bugnet-t5"`)
	for _, path := range []string{"/dev/urandom", "~/bugnet-t5"} {
		_, stderr, err := s.devTry(t, "transfer", "--handle", h, "--download", path)
		if err == nil || !strings.Contains(stderr, "not a regular file") {
			t.Fatalf("a download of %s minted, or failed otherwise: %v\n%s", path, err, stderr)
		}
	}
}

// rowT6 moves a file in /opt and a root-owned 0600 file in /tmp both ways, staged where needed
func rowT6(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	dir := t.TempDir()
	optSHA := s.sh(t, h, asRoot+`$S mkdir -p /opt/bugnet-test && `+
		`head -c 3145728 /dev/urandom | $S tee /opt/bugnet-test/f > /dev/null && `+
		`$S sha256sum /opt/bugnet-test/f | cut -d' ' -f1`)
	out := s.download(t, h, "/opt/bugnet-test/f", filepath.Join(dir, "opt"))
	eq(t, fileSHA(t, out), strings.TrimSpace(optSHA))
	local := randomFile(t, dir, "t6-opt", 2*mib+3)
	s.upload(t, h, local, "/opt/bugnet-test/up")
	eq(t, s.remoteSHA(t, h, "/opt/bugnet-test/up"), fileSHA(t, local))

	tmpSHA := s.sh(t, h, asRoot+`$S sh -c 'head -c 1048576 /dev/urandom > /tmp/bugnet-t6 && `+
		`chown root:root /tmp/bugnet-t6 && chmod 600 /tmp/bugnet-t6' && `+
		`$S sha256sum /tmp/bugnet-t6 | cut -d' ' -f1`)
	out = s.download(t, h, "/tmp/bugnet-t6", filepath.Join(dir, "tmp"))
	eq(t, fileSHA(t, out), strings.TrimSpace(tmpSHA))
	local = randomFile(t, dir, "t6-tmp", mib+7)
	s.upload(t, h, local, "/tmp/bugnet-t6")
	eq(t, s.remoteSHA(t, h, "/tmp/bugnet-t6"), fileSHA(t, local))
	t.Logf("T6 on %s: staging left %q", s.provider,
		s.sh(t, h, `ls -A "$HOME/.bugnet/xfer" 2>/dev/null; true`))
}

// rowT7 records the filesystems of /tmp and the home directory, where staging writes
func rowT7(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	got := strings.Fields(s.sh(t, h, `stat -f -c %T /tmp "$HOME"`))
	if len(got) != 2 {
		t.Fatalf("stat -f printed %q", got)
	}
	t.Logf("T7 on %s: /tmp is %s, the home directory is %s", s.provider, got[0], got[1])
	if got[1] == "tmpfs" || got[1] == "ramfs" {
		t.Fatalf("the home directory is %s, so staging would fill RAM", got[1])
	}
}

// rowBoat5 uploads to a root-owned 0600 file in /tmp and downloads from /opt, both staged
func rowBoat5(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	dir := t.TempDir()
	s.sh(t, h, asRoot+`$S sh -c 'head -c 65536 /dev/urandom > /tmp/bugnet-boat5 && `+
		`chown root:root /tmp/bugnet-boat5 && chmod 600 /tmp/bugnet-boat5' && `+
		`$S mkdir -p /opt/bugnet-test && `+
		`head -c 2097152 /dev/urandom | $S tee /opt/bugnet-test/boat5 > /dev/null && `+
		`$S chmod 600 /opt/bugnet-test/boat5`)
	// The exec user can neither write the one nor read the other, so only staging works
	eq(t, s.sh(t, h, `test -w /tmp/bugnet-boat5 || echo no; test -r /opt/bugnet-test/boat5 `+
		`|| echo no`), "no\nno\n")
	local := randomFile(t, dir, "boat5", 3*mib+1)
	s.upload(t, h, local, "/tmp/bugnet-boat5")
	eq(t, s.remoteSHA(t, h, "/tmp/bugnet-boat5"), fileSHA(t, local))
	want := s.remoteSHA(t, h, "/opt/bugnet-test/boat5")
	out := s.download(t, h, "/opt/bugnet-test/boat5", filepath.Join(dir, "opt"))
	eq(t, fileSHA(t, out), want)
}

/* Row helpers */

// sandbox creates a placement on the stack's provider and stops it when the test ends
func (s *stack) sandbox(t *testing.T, pkg string) string {
	t.Helper()
	h := kv(s.dev(t, "create", "--package", pkg))["handle"]
	if h == "" {
		t.Fatal("live: bugnet-dev create printed no handle")
	}
	t.Cleanup(func() {
		if _, stderr, err := s.devTry(t, "stop", "--handle", h); err != nil {
			t.Errorf("live: stop %s: %v\n%s", h, err, stderr)
		}
	})
	return h
}

// devTry runs bugnet-dev and answers its output and error without failing the test
func (s *stack) devTry(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command(filepath.Join(s.bin, "bugnet-dev"),
		append([]string{"--config", s.settings}, args...)...)
	cmd.Dir, cmd.Env = s.root, s.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// sh runs a shell script in the sandbox as the exec user and answers its output
func (s *stack) sh(t *testing.T, h, script string) string {
	t.Helper()
	return s.dev(t, "exec", "--handle", h, "--", "sh", "-c", script)
}

// remoteSHA is the SHA-256 in hex of a sandbox file, read as root, path a shell word
func (s *stack) remoteSHA(t *testing.T, h, path string) string {
	t.Helper()
	return strings.TrimSpace(s.sh(t, h, asRoot+`$S sha256sum `+path+` | cut -d' ' -f1`))
}

// mint makes a transfer URL with bugnet-dev transfer
func (s *stack) mint(t *testing.T, h string, args ...string) map[string]string {
	t.Helper()
	m := kv(s.dev(t, append([]string{"transfer", "--handle", h}, args...)...))
	if m["url"] == "" {
		t.Fatalf("live: bugnet-dev transfer printed %v", m)
	}
	return m
}

// uploaded is the edge's answer to an upload
type uploaded struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// upload sends a local file to path with curl -T, as DP 8.3.3 shows
func (s *stack) upload(t *testing.T, h, local, path string) uploaded {
	t.Helper()
	url := s.mint(t, h, "--upload", local, "--path", path)["url"]
	out := s.curlRetry(t, "-fsS", "-T", local, url)
	var got uploaded
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("live: the upload answered %q: %v", out, err)
	}
	return got
}

// download fetches a sandbox path to out with curl -fSo, as DP 8.3.4 shows
func (s *stack) download(t *testing.T, h, path, out string) string {
	t.Helper()
	url := s.mint(t, h, "--download", path)["url"]
	s.curlRetry(t, "-fsS", "-o", out, url)
	return out
}

// curlArgs points curl at this stack's edge and its certificate in the local mode
func (s *stack) curlArgs(args []string) []string {
	if s.mode != "local" {
		return args
	}
	return append([]string{
		"--connect-to", s.f.Names.TransferHost + ":443:" + s.edgeAddr,
		"--cacert", filepath.Join(s.dir, "tls", "fullchain.pem"),
	}, args...)
}

// curlRetry runs curl, again after a 409 while a cut stream's claim is freed
func (s *stack) curlRetry(t *testing.T, args ...string) string {
	t.Helper()
	for i := 0; ; i++ {
		var stdout, stderr bytes.Buffer
		cmd := exec.Command("curl", s.curlArgs(args)...)
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		if err == nil {
			return stdout.String()
		}
		if i == inUseRetries || !strings.Contains(stderr.String(), "409") {
			t.Fatalf("live: curl: %v\n%s", err, stderr.String())
		}
		time.Sleep(2 * time.Second)
	}
}

// curlStatus runs curl without -f and answers the status and the body
func (s *stack) curlStatus(t *testing.T, args ...string) (string, string) {
	t.Helper()
	args = append(args, "-w", "\n%{http_code}")
	out, err := exec.Command("curl", s.curlArgs(args)...).Output()
	if err != nil {
		t.Fatalf("live: curl: %v", err)
	}
	text := string(out)
	i := strings.LastIndex(text, "\n")
	if i < 0 {
		t.Fatalf("live: curl printed no status in %q", text)
	}
	return text[i+1:], text[:i]
}

// client is an HTTP/1.1 client that reaches this stack's edge for the transfer host
func (s *stack) client() *http.Client {
	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetHTTP1(true)
	if s.mode == "local" {
		roots := x509.NewCertPool()
		if pem, err := os.ReadFile(filepath.Join(s.dir, "tls", "fullchain.pem")); err == nil {
			roots.AppendCertsFromPEM(pem)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: roots}
		d := &net.Dialer{}
		tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return d.DialContext(ctx, network, s.edgeAddr)
		}
	}
	return &http.Client{Transport: tr}
}

// randomFile writes n pseudo-random bytes to dir/name
func randomFile(t *testing.T, dir, name string, n int64) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var seed [32]byte
	copy(seed[:], name)
	src := rand.NewChaCha8(seed)
	if _, err := io.CopyN(f, src, n); err != nil {
		t.Fatal(err)
	}
	return path
}

// fileSHA is a local file's SHA-256 in hex
func fileSHA(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestTransferRowsAndTheirPackages(t *testing.T) {
	var ids []string
	for _, r := range dataplaneRows {
		ids = append(ids, r.id)
		if r.id == "BOAT5" {
			eq(t, r.providers, []string{"boat"})
		}
	}
	for _, id := range []string{"T1", "T2", "T3", "T4", "T5", "T6", "T7", "BOAT5"} {
		if !slices.Contains(ids, id) {
			t.Fatalf("no row %s in %v", id, ids)
		}
	}
	c, err := catalog.Load(filepath.Join(repo(t), "config", "dev-packages.json"))
	if err != nil {
		t.Fatal(err)
	}
	small, ok := c.Get(xferPackage)
	eq(t, ok, true)
	eq(t, small.Minutes, 10)
	big, ok := c.Get(bigPackage)
	eq(t, ok, true)
	eq(t, big.Minutes, 30)
	eq(t, big.Edge.Bytes, int64(4<<30))
}
