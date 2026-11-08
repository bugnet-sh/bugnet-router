package live

import (
	"bufio"
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
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"golang.org/x/net/websocket"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/gateclient"
	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/vkclient"
	"github.com/bugnet-sh/bugnet-router/internal/x402"
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
	return s.sandboxFor(t, pkg, "")
}

// sandboxFor is sandbox for a placement that copies the source of estimateID when it is not empty
func (s *stack) sandboxFor(t *testing.T, pkg, estimateID string) string {
	t.Helper()
	args := []string{"create", "--package", pkg}
	if estimateID != "" {
		args = append(args, "--estimate-id", estimateID)
	}
	h := kv(s.dev(t, args...))["handle"]
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

/* Gate rows, DP 16.2 G1 and G2 and the gate's 16.3 rows */

func init() {
	dataplaneRows = append(dataplaneRows,
		row{id: "G1", run: rowG1},
		row{id: "G2", run: rowG2},
		row{id: "E2B1", providers: []string{"e2b"}, run: rowE2B1},
		row{id: "E2B2", providers: []string{"e2b"}, run: rowE2B2},
		row{id: "E2B5", providers: []string{"e2b"}, run: rowE2B5},
		row{id: "BOXD2", providers: []string{"boxd"}, run: rowBoxd2},
		row{id: "KOYEB1", providers: []string{"koyeb"}, run: rowKoyeb1},
		row{id: "BOAT1", providers: []string{"boat"}, run: rowBoat1},
		row{id: "BOAT2", providers: []string{"boat"}, run: rowBoat2},
	)
}

// rowG1 checks the saga recorded the gate, and /__bugnet/health through the route in 3 tries
func rowG1(t *testing.T, s *stack) {
	g := s.gated(t, s.sandbox(t, xferPackage))
	if g.p.GateReadyAt == nil {
		t.Fatal("the placement is ready with no gate_ready_at")
	}
	t.Logf("G1 on %s: the gate answered %s after the create began, %s before ready",
		s.provider, g.p.GateReadyAt.Sub(g.p.CreatedAt).Round(time.Millisecond),
		g.p.ReadyAt.Sub(*g.p.GateReadyAt).Round(time.Millisecond))
	start := time.Now()
	g.healthWithin(t, 3)
	t.Logf("G1 on %s: health through the route in %s", s.provider,
		time.Since(start).Round(time.Millisecond))
}

// rowG2 calls the route as a stranger holding the provider's credential but no gate key
func rowG2(t *testing.T, s *stack) {
	g := s.gated(t, s.sandbox(t, xferPackage))
	for _, c := range []struct {
		name    string
		path    string
		headers map[string]string
	}{
		{"no key, health", "/__bugnet/health", nil},
		{"wrong key, health", "/__bugnet/health", map[string]string{"Bugnet-Gate": "wrong"}},
		{"no key, an app port", "/", map[string]string{"Bugnet-Port": "8000"}},
	} {
		res, body := g.get(t, c.path, c.headers)
		if res.StatusCode != http.StatusNotFound || len(body) != 0 ||
			res.Header.Get("Bugnet-Gate-Response") != "1" {
			t.Fatalf("%s: %d with %d body bytes and Bugnet-Gate-Response %q, want the gate's "+
				"bare 404", c.name, res.StatusCode, len(body),
				res.Header.Get("Bugnet-Gate-Response"))
		}
	}
}

// rowE2B1 reads the traffic token from create, then from a one-second connect with endAt kept
func rowE2B1(t *testing.T, s *stack) {
	g := s.gated(t, s.sandbox(t, xferPackage))
	token := g.p.ProviderRef.Meta["traffic_token"]
	if token == "" {
		t.Fatal("the create left no traffic token in the ref")
	}
	before := s.e2bEndAt(t, g)
	adopted := *g.p.ProviderRef
	adopted.Meta = maps.Clone(adopted.Meta)
	delete(adopted.Meta, "traffic_token")
	meta, err := g.prov.(provider.Gater).PrepareGate(t.Context(), adopted, g.cfg.Previews.GatePort)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, meta["traffic_token"] == token, true)
	eq(t, s.e2bEndAt(t, g), before)
}

// rowE2B2 asks the gate for envd's port 49983 and gets the deny list's 400
func rowE2B2(t *testing.T, s *stack) {
	g := s.gated(t, s.sandbox(t, xferPackage))
	res, _ := g.get(t, "/init", map[string]string{"Bugnet-Gate": g.key, "Bugnet-Port": "49983"})
	if res.StatusCode != http.StatusBadRequest || res.Header.Get("Bugnet-Gate-Response") != "1" {
		t.Fatalf("port 49983 through the gate answered %d, want the gate's 400", res.StatusCode)
	}
}

// rowE2B5 records whether the template boots systemd, which setup's init answer follows
func rowE2B5(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	got := strings.TrimSpace(s.sh(t, h,
		`if [ -d /run/systemd/system ]; then echo systemd; else echo setsid; fi`))
	t.Logf("E2B5: the template's init is %s", got)
	units := s.sh(t, h, `ls /etc/systemd/system/bugnet-*.service 2>/dev/null; true`)
	if got == "systemd" && !strings.Contains(units, "bugnet-gate.service") {
		t.Fatalf("systemd boots but setup wrote no gate unit: %q", units)
	}
}

// rowBoxd2 times health through the route boxd.gate_route names and logs whether the other answers
func rowBoxd2(t *testing.T, s *stack) {
	g := s.gated(t, s.sandbox(t, xferPackage))
	route := g.cfg.Boxd.GateRoute
	took, err := g.timeHealth(t.Context(), 10)
	if err != nil {
		t.Fatalf("health through the %s route: %v", route, err)
	}
	if code, err := g.curlHealth(); err != nil || code != "204" {
		t.Fatalf("curl through the %s route answered %q, %v, want 204 past bot protection",
			route, code, err)
	}
	t.Logf("BOXD2: median health through the %s route %s", route, took)
	// The other route only logs, since a bn proxy made after boot can lack its certificate
	other := "subdomain"
	if route == "subdomain" {
		other = "default"
	}
	g2, err := g.boxdRoute(t, other)
	if err != nil {
		t.Logf("BOXD2: the %s route did not open: %v", other, err)
		return
	}
	if took, err = g2.timeHealth(t.Context(), 10); err != nil {
		t.Logf("BOXD2: the %s route does not answer yet: %v", other, err)
		return
	}
	code, err := g2.curlHealth()
	t.Logf("BOXD2: the %s route answers too, median health %s, curl %q, %v",
		other, took, code, err)
}

// boxdRoute opens the gate of g's machine through boxd's other route kind
func (g gated) boxdRoute(t *testing.T, kind string) (gated, error) {
	b, err := boxd.New(boxd.Options{
		APIKey: os.Getenv("BOXD_API_KEY"), Endpoint: g.cfg.Boxd.Endpoint,
		Region: g.cfg.Boxd.Region, GatePort: g.cfg.Previews.GatePort, GateRoute: kind,
	})
	if err != nil {
		return gated{}, err
	}
	t.Cleanup(func() { _ = b.Close() })
	meta, err := b.PrepareGate(t.Context(), *g.p.ProviderRef, g.cfg.Previews.GatePort)
	if err != nil {
		return gated{}, err
	}
	ref := *g.p.ProviderRef
	ref.Meta = maps.Clone(ref.Meta)
	maps.Copy(ref.Meta, meta)
	route, err := b.GateRoute(ref, g.cfg.Previews.GatePort)
	if err != nil {
		return gated{}, err
	}
	g.route = route
	return g, nil
}

// rowKoyeb1 binds the gate port, and a second PrepareGate unbinds and binds again
func rowKoyeb1(t *testing.T, s *stack) {
	g := s.gated(t, s.sandbox(t, xferPackage))
	g.healthWithin(t, 3)
	if _, err := g.prov.(provider.Gater).PrepareGate(t.Context(), *g.p.ProviderRef,
		g.cfg.Previews.GatePort); err != nil {
		t.Fatalf("a second bind_port, through unbind_port {}: %v", err)
	}
	g.healthWithin(t, 3)
}

// rowBoat1 hosts the gate port with the runtime key, whose scope goes to DP question 10
func rowBoat1(t *testing.T, s *stack) {
	g := s.gated(t, s.sandbox(t, xferPackage))
	g.healthWithin(t, 3)
	t.Log("BOAT1: POST /host worked with BOAT_API_KEY, so its scopes suffice for the gate")
}

// rowBoat2 dials the gate port on the machine's own address with no Boat key
func rowBoat2(t *testing.T, s *stack) {
	g := s.gated(t, s.sandbox(t, xferPackage))
	ip := s.boatIP(t, g)
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip,
		strconv.Itoa(g.cfg.Previews.GatePort)), 10*time.Second)
	if err != nil {
		t.Logf("BOAT2: raw TCP to the gate port is refused: %v", err)
		return
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	_, _ = io.WriteString(conn, "GET /__bugnet/health HTTP/1.0\r\n\r\n")
	line, _ := bufio.NewReader(conn).ReadString('\n')
	if !strings.HasPrefix(line, "HTTP/1.0 404") && !strings.HasPrefix(line, "HTTP/1.1 404") &&
		line != "" {
		t.Fatalf("raw TCP to the gate port answered %q, want a 404 or nothing", line)
	}
	t.Logf("BOAT2: raw TCP to the gate port answered %q", strings.TrimSpace(line))
}

/* Gate helpers */

// gated is a placement with its provider, the route to its gate and the gate's key
type gated struct {
	p     store.Placement
	cfg   config.Config
	prov  provider.Provider
	route provider.GateRoute
	key   string
}

// lookup reads a variable from the stack's environment
func (s *stack) lookup(name string) (string, bool) {
	for _, kv := range slices.Backward(s.env) {
		if k, v, ok := strings.Cut(kv, "="); ok && k == name {
			return v, true
		}
	}
	return "", false
}

// gated reads the placement of handle h and builds its gate route as the router does
func (s *stack) gated(t *testing.T, h string) gated {
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
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	p, err := store.New(pool, store.WithRefSealer(cfg.DataPlaneKeys)).PlacementByHandle(
		t.Context(), sha)
	if err != nil || p.ProviderRef == nil {
		t.Fatalf("live: the placement of %s: %v", h, err)
	}
	vk, err := vkclient.New(cfg.ValkeyAddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(vk.Close)
	reg, err := providers.Build(cfg, vk)
	if err != nil {
		t.Fatal(err)
	}
	prov, _ := reg.Get(p.Provider)
	g, ok := prov.(provider.Gater)
	if !ok {
		t.Fatalf("live: %s has no gate route", p.Provider)
	}
	route, err := g.GateRoute(*p.ProviderRef, cfg.Previews.GatePort)
	if err != nil {
		t.Fatal(err)
	}
	key, err := cfg.DataPlaneKeys.GateKey(p.KeyID, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	return gated{p: p, cfg: cfg, prov: prov, route: route, key: key}
}

// healthWithin asks the gate's health up to tries times, 2 s apart, as the saga does
func (g gated) healthWithin(t *testing.T, tries int) {
	t.Helper()
	var err error
	for i := range tries {
		if i > 0 {
			time.Sleep(2 * time.Second)
		}
		if err = gateclient.Health(t.Context(), g.route, g.key); err == nil {
			return
		}
	}
	t.Fatalf("the gate's health through the route: %v", err)
}

// timeHealth is the median time of n health calls through the route
func (g gated) timeHealth(ctx context.Context, n int) (time.Duration, error) {
	var took []time.Duration
	for range n {
		start := time.Now()
		if err := gateclient.Health(ctx, g.route, g.key); err != nil {
			return 0, err
		}
		took = append(took, time.Since(start))
	}
	slices.Sort(took)
	return took[n/2].Round(time.Millisecond), nil
}

// curlHealth asks the gate's health with curl, whose client bot protection may refuse
func (g gated) curlHealth() (string, error) {
	out, err := exec.Command("curl", "-sS", "-m", "10", "-o", "/dev/null", "-w", "%{http_code}",
		"-H", "Bugnet-Gate: "+g.key, g.route.BaseURL+"/__bugnet/health").Output()
	return string(out), err
}

// get calls the route with its provider credentials and headers, and answers status and body
func (g gated) get(t *testing.T, path string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, g.route.BaseURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range g.route.Header {
		req.Header.Set(k, v)
	}
	for k, v := range g.route.Cookies {
		req.AddCookie(&http.Cookie{Name: k, Value: v})
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	return res, body
}

// e2bEndAt reads the sandbox's endAt from E2B's API with the key in the environment
func (s *stack) e2bEndAt(t *testing.T, g gated) string {
	t.Helper()
	domain := g.cfg.E2B.Domain
	if domain == "" {
		domain = "e2b.app"
	}
	var got struct {
		EndAt string `json:"endAt"`
	}
	s.apiGet(t, "https://api."+domain+"/sandboxes/"+g.p.ProviderRef.ID,
		map[string]string{"X-API-Key": os.Getenv("E2B_API_KEY")}, &got)
	if got.EndAt == "" {
		t.Fatal("E2B's sandbox info names no endAt")
	}
	return got.EndAt
}

// boatIP reads the machine's own address from Boat's API with the key in the environment
func (s *stack) boatIP(t *testing.T, g gated) string {
	t.Helper()
	var got struct {
		Sandbox struct {
			IP *string `json:"ip"`
		} `json:"sandbox"`
	}
	s.apiGet(t, "https://boat.dev/api/v1/sandboxes/"+g.p.ProviderRef.ID,
		map[string]string{"Authorization": "Bearer " + os.Getenv("BOAT_API_KEY")}, &got)
	if got.Sandbox.IP == nil || *got.Sandbox.IP == "" {
		t.Fatal("Boat's sandbox info names no ip")
	}
	return *got.Sandbox.IP
}

// apiGet reads one JSON answer from a provider's API, quoting no header in a failure
func (s *stack) apiGet(t *testing.T, url string, headers map[string]string, into any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("live: the provider's API: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode != http.StatusOK || json.Unmarshal(body, into) != nil {
		t.Fatalf("live: the provider's API answered %d", res.StatusCode)
	}
}

func TestGateRowsAndTheirProviders(t *testing.T) {
	want := map[string][]string{
		"G1": nil, "G2": nil, "E2B1": {"e2b"}, "E2B2": {"e2b"}, "E2B5": {"e2b"},
		"BOXD2": {"boxd"}, "KOYEB1": {"koyeb"}, "BOAT1": {"boat"}, "BOAT2": {"boat"},
	}
	for _, r := range dataplaneRows {
		if p, ok := want[r.id]; ok {
			eq(t, r.providers, p)
			delete(want, r.id)
		}
	}
	eq(t, len(want), 0)
}

/* Preview rows, DP 16.2 P1 to P5, P8 and G3 and the previews' 16.3 rows */

const (
	// longPackage holds P2's 30-minute WebSocket with room to spare
	longPackage = "dev-small-40m"
	// minutePackage reaches its deadline while P8 watches
	minutePackage = "dev-small-1m"
	// appPort is where the row app listens, and listingPort python's own file server
	appPort     = 8001
	listingPort = 8000
	// closeWithin is how fast DP 16.2 P8 wants a live WebSocket closed
	closeWithin = time.Second
	// healWithin is how fast DP 16.2 G3 wants a repair to heal a killed gate
	healWithin = time.Minute
)

func init() {
	dataplaneRows = append(dataplaneRows,
		row{id: "P1", run: rowP1},
		row{id: "P2", run: rowP2},
		row{id: "P3", run: rowP3},
		row{id: "P4", run: rowP4},
		row{id: "P5", run: rowP5},
		row{id: "P8", run: rowP8},
		row{id: "G3", run: rowG3},
		row{id: "E2B4", providers: []string{"e2b"}, run: rowE2B4},
		row{id: "E2B6", providers: []string{"e2b"}, run: rowE2B6},
		row{id: "E2B7", providers: []string{"e2b"}, run: rowE2B7},
		row{id: "E2B8", providers: []string{"e2b"}, run: rowE2B8},
		row{id: "BOXD3", providers: []string{"boxd"}, run: rowBoxd3},
		row{id: "KOYEB2", providers: []string{"koyeb"}, run: rowKoyeb2},
		row{id: "KOYEB3", providers: []string{"koyeb"}, run: rowKoyeb3},
		row{id: "BOAT3", providers: []string{"boat"}, run: rowBoat3},
		row{id: "BOAT4", providers: []string{"boat"}, run: rowBoat4},
	)
}

// rowP1 serves python's file listing on 8000 by the header token, and the app sees its Host
func rowP1(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	s.sh(t, h, `mkdir -p /tmp/bugnet-p1 && echo p1 > /tmp/bugnet-p1/bugnet-p1.txt && `+
		background(`python3 -m http.server `+strconv.Itoa(listingPort)+
			` --bind 127.0.0.1 --directory /tmp/bugnet-p1`, "p1"))
	s.waitPort(t, h, listingPort)
	pv := s.preview(t, h, listingPort)
	res, body := s.previewGet(t, pv, "/", headerToken, pv.token)
	if res.StatusCode != http.StatusOK || !strings.Contains(body, "Directory listing") ||
		!strings.Contains(body, "bugnet-p1.txt") {
		t.Fatalf("the listing through the preview answered %d:\n%.400s", res.StatusCode, body)
	}
	app := s.app(t, h)
	seen := s.appHeaders(t, app)
	eq(t, seen["host"], app.host)
	t.Logf("P1 on %s: the app saw Host %s and X-Forwarded-For %q", s.provider, seen["host"],
		seen["x-forwarded-for"])
}

// rowP2 holds a WebSocket open for 30 minutes, echoing every 30 s
func rowP2(t *testing.T, s *stack) {
	h := s.sandbox(t, longPackage)
	pv := s.app(t, h)
	w := s.previewWS(t, pv, "/ws")
	start := time.Now()
	for i := 0; time.Since(start) < 30*time.Minute; i++ {
		if err := w.echo(fmt.Sprintf("p2 %d", i)); err != nil {
			t.Fatalf("the WebSocket stopped echoing after %s: %v", time.Since(start).Round(
				time.Second), err)
		}
		time.Sleep(30 * time.Second)
	}
	if err := w.echo("p2 end"); err != nil {
		t.Fatalf("the WebSocket stopped echoing at 30 minutes: %v", err)
	}
}

// rowP3 runs a 3-minute SSE stream and a 3-minute long poll at once and records any cut
func rowP3(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	pv := s.app(t, h)
	type result struct {
		what string
		took time.Duration
		err  error
	}
	got := make(chan result, 2)
	go func() {
		start := time.Now()
		events, err := s.sse(t, pv, "/sse?secs=180")
		if err == nil && events < 180 {
			err = fmt.Errorf("%d of 180 events", events)
		}
		got <- result{"SSE", time.Since(start), err}
	}()
	go func() {
		start := time.Now()
		res, body, err := s.previewTry(t, pv, "/poll?secs=180", headerToken, pv.token)
		if err == nil && (res.StatusCode != http.StatusOK || body != "done") {
			err = fmt.Errorf("answered %d %q", res.StatusCode, body)
		}
		got <- result{"long poll", time.Since(start), err}
	}()
	for range 2 {
		r := <-got
		if r.err != nil {
			t.Logf("P3 on %s: the %s was cut after %s: %v", s.provider, r.what,
				r.took.Round(time.Second), r.err)
			continue
		}
		t.Logf("P3 on %s: the %s ran its 3 minutes uncut (%s)", s.provider, r.what,
			r.took.Round(time.Second))
	}
}

// rowP4 downloads 50 MiB through the preview, which the 5 MiB/s pace holds to about 10 s
func rowP4(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	pv := s.app(t, h)
	start := time.Now()
	res, n := s.previewDrain(t, pv, "/big?n="+strconv.Itoa(50*mib))
	took := time.Since(start)
	eq(t, res.StatusCode, http.StatusOK)
	eq(t, n, int64(50*mib))
	t.Logf("P4 on %s: 50 MiB in %s, %.1f MiB/s", s.provider, took.Round(time.Millisecond),
		50/took.Seconds())
	if took < 8*time.Second {
		t.Fatalf("50 MiB took %s, faster than the 5 MiB/s pace allows", took)
	}
}

// p5Version is the Vite the row runs, the last before host and WebSocket token checks
const p5Version = "vite@5.4.11"

// rowP5 loads a Vite page of 300 modules through the preview, then sees HMR after an edit
func rowP5(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	if out := strings.TrimSpace(s.sh(t, h, `command -v npx || true`)); out == "" {
		t.Skipf("P5 needs node and npx in the %s sandbox, and found none", s.provider)
	}
	s.sh(t, h, viteProject(s.f.Names.PreviewDomain)+` && cd /tmp/bugnet-p5 && `+
		background(`npx --yes `+p5Version+` --host 127.0.0.1 --port 5173 --strictPort`, "p5"))
	s.waitPortFor(t, h, 5173, 3*time.Minute)
	pv := s.preview(t, h, 5173)
	res, body := s.previewGet(t, pv, "/", headerToken, pv.token)
	if res.StatusCode != http.StatusOK || !strings.Contains(body, "/@vite/client") {
		t.Fatalf("the Vite page answered %d:\n%.400s", res.StatusCode, body)
	}
	start := time.Now()
	codes := map[int]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range 300 {
		wg.Go(func() {
			res, _, err := s.previewTry(t, pv, fmt.Sprintf("/src/m%d.js", i), headerToken,
				pv.token)
			code := 0
			if err == nil {
				code = res.StatusCode
			}
			mu.Lock()
			codes[code]++
			mu.Unlock()
		})
	}
	wg.Wait()
	t.Logf("P5 on %s: 300 modules in %s: %v", s.provider,
		time.Since(start).Round(time.Millisecond), codes)
	eq(t, codes[http.StatusOK], 300)

	ws := s.hmr(t, pv)
	defer func() { _ = ws.Close() }()
	s.sh(t, h, `echo 'export const v0 = "edited"' > /tmp/bugnet-p5/src/m0.js`)
	_ = ws.SetReadDeadline(time.Now().Add(30 * time.Second))
	for {
		var msg string
		if err := websocket.Message.Receive(ws, &msg); err != nil {
			t.Fatalf("no HMR message after the edit: %v", err)
		}
		if strings.Contains(msg, `"update"`) || strings.Contains(msg, `"full-reload"`) {
			t.Logf("P5 on %s: HMR answered the edit with %.120s", s.provider, msg)
			return
		}
	}
}

// viteProject writes a Vite project whose page imports 300 modules into /tmp/bugnet-p5
func viteProject(previewDomain string) string {
	var b strings.Builder
	b.WriteString(`mkdir -p /tmp/bugnet-p5/src && cd /tmp/bugnet-p5 && ` +
		`printf '{"name":"p5","private":true,"type":"module"}' > package.json && ` +
		`printf '<!doctype html><script type="module" src="/src/main.js"></script>' ` +
		`> index.html && `)
	fmt.Fprintf(&b, `printf 'export default {server:{allowedHosts:[".%s"],hmr:{clientPort:443}}}' `+
		`> vite.config.js && `, previewDomain)
	b.WriteString(`i=0; : > src/main.js; while [ $i -lt 300 ]; do ` +
		`echo "export const v$i = $i" > src/m$i.js; ` +
		`echo "import { v$i } from './m$i.js'; console.log(v$i)" >> src/main.js; ` +
		`i=$((i+1)); done`)
	return b.String()
}

// rowP8 closes a live WebSocket within 1 s of RevokePreview, StopPlacement and the deadline
func rowP8(t *testing.T, s *stack) {
	t.Run("revoke", func(t *testing.T) {
		h := s.sandbox(t, xferPackage)
		pv := s.app(t, h)
		w := s.previewWS(t, pv, "/ws")
		s.dev(t, "revoke-preview", pv.id, "--reason", "live P8", "--actor", "live-test")
		w.closesWithin(t, closeWithin)
		res, _ := s.previewGet(t, pv, "/", headerToken, pv.token)
		eq(t, res.StatusCode, http.StatusGone)
	})
	t.Run("stop", func(t *testing.T) {
		h := s.sandbox(t, xferPackage)
		pv := s.app(t, h)
		w := s.previewWS(t, pv, "/ws")
		s.dev(t, "stop", "--handle", h)
		w.closesWithin(t, closeWithin)
	})
	t.Run("deadline", func(t *testing.T) {
		h := s.sandbox(t, minutePackage)
		pv := s.app(t, h)
		w := s.previewWS(t, pv, "/ws")
		wait := time.Until(pv.expires)
		if wait <= 0 {
			t.Fatal("the one-minute package ended before its preview opened")
		}
		w.closesWithin(t, wait+closeWithin)
		t.Logf("P8 on %s: closed %s after the deadline", s.provider,
			time.Since(pv.expires).Round(time.Millisecond))
	})
}

// rowG3 kills the gate from user code, then waits for a repair to heal the preview
func rowG3(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	pv := s.app(t, h)
	// The bracket keeps pgrep from matching the sh -c line that runs it
	killGate := asRoot + `for p in $(pgrep -f '[b]ugnet-run gate'); do $S kill "$p"; done`
	s.sh(t, h, killGate)
	killed := time.Now()
	saw502 := false
	for time.Since(killed) < healWithin {
		res, body, err := s.previewTry(t, pv, "/", headerToken, pv.token)
		switch {
		case err != nil:
		case res.StatusCode == http.StatusOK && body == "app /":
			took := time.Since(killed).Round(time.Second)
			if !saw502 {
				t.Skipf("G3 on %s healed %s after the kill without the edge's 502 page, so "+
					"the supervisor respawned the gate and no repair ran", s.provider, took)
			}
			t.Logf("G3 on %s: the edge's 502, then healed %s after the kill", s.provider, took)
			return
		case res.StatusCode == http.StatusBadGateway:
			if !strings.Contains(body, "preview service stopped") {
				t.Fatalf("the 502 is not the edge's page:\n%.400s", body)
			}
			saw502 = true
		default:
			t.Logf("G3 on %s: %d while the gate was down", s.provider, res.StatusCode)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("the preview did not heal within %s of killing the gate", healWithin)
}

// rowE2B4 shows the app never receives E2B's traffic token or any Bugnet header
func rowE2B4(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	pv := s.app(t, h)
	seen := s.appHeaders(t, pv)
	for name := range seen {
		if strings.HasPrefix(name, "e2b-") || strings.HasPrefix(name, "bugnet-") ||
			name == "x-routing-key" {
			t.Fatalf("the app received %s", name)
		}
	}
	if strings.Contains(seen["cookie"], "__Host-bnp") {
		t.Fatal("the app received the preview cookie")
	}
}

// rowE2B6 times 200 MiB from the app straight through E2B's route, the provider's own speed
func rowE2B6(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	s.app(t, h)
	g := s.gated(t, h)
	start := time.Now()
	n := g.drain(t, "/big?n="+strconv.Itoa(200*mib), appPort)
	took := time.Since(start)
	eq(t, n, int64(200*mib))
	t.Logf("E2B6: 200 MiB straight through E2B's route in %s, %.1f MiB/s",
		took.Round(time.Millisecond), 200/took.Seconds())
}

// rowE2B7 stops the gate and checks the visitor sees only the edge's 502 page, never E2B's
func rowE2B7(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	pv := s.app(t, h)
	// The bracket keeps pgrep from matching the sh -c line that runs it
	s.sh(t, h, asRoot+`for p in $(pgrep -f '[b]ugnet-run gate'); do $S kill -STOP "$p"; `+
		`$S kill "$p"; $S kill -CONT "$p"; done`)
	for range 5 {
		res, body := s.previewGet(t, pv, "/", headerToken, pv.token)
		if res.StatusCode == http.StatusOK {
			t.Log("E2B7: the gate answered again before E2B's own answer could be seen")
			return
		}
		lower := strings.ToLower(body)
		if res.StatusCode != http.StatusBadGateway || strings.Contains(lower, "e2b") ||
			!strings.Contains(body, "preview service stopped") {
			t.Fatalf("the visitor saw %d:\n%.400s", res.StatusCode, body)
		}
		time.Sleep(time.Second)
	}
}

// rowE2B8 sends 1,000 requests in a minute through one preview and finds no 429 from E2B
func rowE2B8(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	pv := s.app(t, h)
	codes := s.burst(t, 1000, time.Minute, func() (int, error) {
		res, _, err := s.previewTry(t, pv, "/", headerToken, pv.token)
		if err != nil {
			return 0, err
		}
		return res.StatusCode, nil
	})
	t.Logf("E2B8: 1,000 requests in a minute answered %v", codes)
	if codes[http.StatusTooManyRequests] > 0 {
		t.Fatalf("%d of 1,000 requests drew a 429", codes[http.StatusTooManyRequests])
	}
}

// rowBoxd3 records the Host and X-Forwarded-For the app sees and the gate's listening sockets
func rowBoxd3(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	pv := s.app(t, h)
	seen := s.appHeaders(t, pv)
	eq(t, seen["host"], pv.host)
	listen := s.sh(t, h, `ss -ltn 2>/dev/null | grep ':29417' || true`)
	t.Logf("BOXD3: the app saw Host %s and X-Forwarded-For %q; the gate listens on:\n%s",
		seen["host"], seen["x-forwarded-for"], listen)
}

// rowKoyeb2 holds a WebSocket through Koyeb's / route, the path SSH needs too
func rowKoyeb2(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	pv := s.app(t, h)
	w := s.previewWS(t, pv, "/ws")
	for i := range 3 {
		if err := w.echo(fmt.Sprintf("koyeb %d", i)); err != nil {
			t.Fatalf("the WebSocket through Koyeb's route: %v; Koyeb stays out of "+
				"previews.providers and ssh.providers", err)
		}
	}
}

// rowKoyeb3 records where Koyeb cuts SSE, and checks its CDN never serves the gate's answers
func rowKoyeb3(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	pv := s.app(t, h)
	start := time.Now()
	events, err := s.sse(t, pv, "/sse?secs=150")
	t.Logf("KOYEB3: SSE gave %d events in %s, %v", events,
		time.Since(start).Round(time.Second), err)
	g := s.gated(t, h)
	var second http.Header
	for range 2 {
		res, _ := g.get(t, "/static.css", map[string]string{
			"Bugnet-Gate": g.key, "Bugnet-Port": strconv.Itoa(appPort),
		})
		eq(t, res.StatusCode, http.StatusOK)
		second = res.Header
	}
	t.Logf("KOYEB3: the second fetch had Age %q and Cf-Cache-Status %q", second.Get("Age"),
		second.Get("Cf-Cache-Status"))
	if second.Get("Cf-Cache-Status") == "HIT" {
		t.Fatal("Koyeb's CDN served a gate answer from its cache")
	}
}

// rowBoat3 keeps _port_auth from the app and the visitor, and times a WebSocket on on.boat.dev
func rowBoat3(t *testing.T, s *stack) {
	h := s.sandbox(t, bigPackage)
	pv := s.app(t, h)
	seen := s.appHeaders(t, pv)
	if strings.Contains(seen["cookie"], "_port_auth") {
		t.Fatal("the app received _port_auth")
	}
	for _, path := range []string{"/", "/headers", "/static.css"} {
		res, _ := s.previewGet(t, pv, path, headerToken, pv.token)
		for _, c := range res.Header.Values("Set-Cookie") {
			if strings.Contains(c, "_port_auth") {
				t.Fatalf("a _port_auth refresh reached the visitor on %s", path)
			}
		}
	}
	w := s.previewWS(t, pv, "/ws")
	start := time.Now()
	for time.Since(start) < 20*time.Minute {
		if err := w.echo("boat"); err != nil {
			t.Logf("BOAT3: the WebSocket on on.boat.dev ended after %s: %v",
				time.Since(start).Round(time.Second), err)
			return
		}
		time.Sleep(30 * time.Second)
	}
	t.Log("BOAT3: the WebSocket on on.boat.dev lived 20 minutes")
}

// rowBoat4 sends 12,000 requests in a minute to Boat's route from this address, past its limit
func rowBoat4(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	s.app(t, h)
	g := s.gated(t, h)
	codes := s.burst(t, 12000, time.Minute, func() (int, error) {
		return g.status(t, "/", appPort)
	})
	t.Logf("BOAT4: 12,000 requests to on.boat.dev in a minute answered %v", codes)
	if codes[http.StatusTooManyRequests] == 0 {
		t.Log("BOAT4: no 429, so on.boat.dev is not under the 10,000 a minute limit and " +
			"previews.boat_requests_per_minute may drop to 0")
		return
	}
	t.Log("BOAT4: Boat answered 429, so previews.boat_requests_per_minute stays")
}

/* Preview helpers */

// headerToken is the header that carries a preview token for programs
const headerToken = "Bugnet-Token"

// previewLink is one preview bugnet-dev made
type previewLink struct {
	id      string
	host    string
	token   string
	expires time.Time
}

// preview makes the link of one port with bugnet-dev preview
func (s *stack) preview(t *testing.T, h string, port int) previewLink {
	t.Helper()
	m := kv(s.dev(t, "preview", "--handle", h, "--port", strconv.Itoa(port)))
	u, err := url.Parse(m["url"])
	if err != nil || u.Host == "" {
		t.Fatalf("live: bugnet-dev preview printed the keys %v", slices.Sorted(maps.Keys(m)))
	}
	pv := previewLink{id: m["preview_id"], host: u.Host, token: u.Query().Get("bugnet_token")}
	if pv.expires, err = time.Parse(time.RFC3339Nano, m["expires_at"]); err != nil {
		t.Fatalf("live: bugnet-dev preview printed expires_at %q", m["expires_at"])
	}
	if !strings.HasPrefix(pv.token, "bnp_") || pv.id == "" {
		t.Fatal("live: bugnet-dev preview printed no token or id")
	}
	return pv
}

// rowApp is a small Python app with /headers, /big, /sse, /poll, /static.css and an echo socket
const rowApp = `import http.server, json, socketserver, sys, time
class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def body(self, code, b, ctype="text/plain", extra=()):
        b = b if isinstance(b, bytes) else b.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        for k, v in extra: self.send_header(k, v)
        self.end_headers()
        self.wfile.write(b)
    def do_GET(self):
        path, _, query = self.path.partition("?")
        q = dict(p.split("=", 1) for p in query.split("&") if "=" in p)
        if self.headers.get("Upgrade", "").lower() == "websocket":
            self.send_response(101)
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.end_headers()
            self.wfile.flush()
            while True:
                d = self.connection.recv(65536)
                if not d: break
                self.connection.sendall(d)
            self.close_connection = True
        elif path == "/headers":
            self.body(200, json.dumps({k.lower(): v for k, v in self.headers.items()}),
                "application/json")
        elif path == "/big":
            n = int(q.get("n", "0"))
            self.send_response(200)
            self.send_header("Content-Length", str(n))
            self.end_headers()
            chunk = bytes(65536)
            while n > 0:
                self.wfile.write(chunk[:min(n, 65536)])
                n -= 65536
        elif path == "/sse":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            for i in range(int(q.get("secs", "180"))):
                self.wfile.write(("data: %d\n\n" % i).encode())
                self.wfile.flush()
                time.sleep(1)
            self.close_connection = True
        elif path == "/poll":
            time.sleep(int(q.get("secs", "180")))
            self.body(200, "done")
        elif path == "/static.css":
            self.body(200, "body{color:red}", "text/css",
                [("Cache-Control", "public, max-age=600")])
        else:
            self.body(200, "app " + path)
class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True
S(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
`

// background starts command detached from the job, its output in /tmp/bugnet-<name>.log
func background(command, name string) string {
	return `setsid nohup sh -c '` + command + `' > /tmp/bugnet-` + name + `.log 2>&1 < /dev/null &`
}

// app starts the row app on appPort and answers its preview link
func (s *stack) app(t *testing.T, h string) previewLink {
	t.Helper()
	s.sh(t, h, `command -v python3 >/dev/null || { echo "no python3"; exit 1; }; `+
		`cat > /tmp/bugnet-app.py <<'BUGNET_APP'
`+rowApp+`BUGNET_APP
`+background("python3 /tmp/bugnet-app.py "+strconv.Itoa(appPort), "app"))
	s.waitPort(t, h, appPort)
	return s.preview(t, h, appPort)
}

func (s *stack) waitPort(t *testing.T, h string, port int) {
	t.Helper()
	s.waitPortFor(t, h, port, 30*time.Second)
}

// waitPortFor waits in the sandbox until something accepts on 127.0.0.1:port
func (s *stack) waitPortFor(t *testing.T, h string, port int, wait time.Duration) {
	t.Helper()
	tries := int(wait / time.Second)
	out := s.sh(t, h, fmt.Sprintf(`i=0; while [ $i -lt %d ]; do `+
		`python3 -c 'import socket; socket.create_connection(("127.0.0.1", %d), 1)' `+
		`2>/dev/null && { echo up; exit 0; }; i=$((i+1)); sleep 1; done; echo down`, tries, port))
	if strings.TrimSpace(out) != "up" {
		t.Fatalf("live: nothing listened on port %d in the sandbox within %s", port, wait)
	}
}

// previewClient reaches the edge for the preview names, HTTP/2 offered
func (s *stack) previewClient() *http.Client {
	tr := &http.Transport{ForceAttemptHTTP2: true, MaxIdleConnsPerHost: 64}
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
	return &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

// previewTry sends one GET through the edge and answers status, body and error
func (s *stack) previewTry(t *testing.T, pv previewLink, path string, header ...string) (
	*http.Response, string, error,
) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"https://"+pv.host+path, nil)
	if err != nil {
		return nil, "", err
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	s.previewOnce.Do(func() { s.previews = s.previewClient() })
	res, err := s.previews.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return res, string(b), err
}

// previewGet is previewTry that fails the test on a transport error
func (s *stack) previewGet(t *testing.T, pv previewLink, path string, header ...string) (
	*http.Response, string,
) {
	t.Helper()
	res, body, err := s.previewTry(t, pv, path, header...)
	if err != nil {
		t.Fatalf("live: GET %s through the preview: %v", path, err)
	}
	return res, body
}

// previewDrain reads a whole answer through the preview and counts its bytes
func (s *stack) previewDrain(t *testing.T, pv previewLink, path string) (*http.Response, int64) {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+pv.host+path,
		nil)
	req.Header.Set(headerToken, pv.token)
	s.previewOnce.Do(func() { s.previews = s.previewClient() })
	res, err := s.previews.Do(req)
	if err != nil {
		t.Fatalf("live: GET %s through the preview: %v", path, err)
	}
	defer func() { _ = res.Body.Close() }()
	n, err := io.Copy(io.Discard, res.Body)
	if err != nil {
		t.Fatalf("live: reading %s through the preview after %d bytes: %v", path, n, err)
	}
	return res, n
}

// appHeaders reads the headers the row app received through the preview
func (s *stack) appHeaders(t *testing.T, pv previewLink) map[string]string {
	t.Helper()
	res, body := s.previewGet(t, pv, "/headers", headerToken, pv.token)
	eq(t, res.StatusCode, http.StatusOK)
	seen := map[string]string{}
	if err := json.Unmarshal([]byte(body), &seen); err != nil {
		t.Fatalf("live: /headers answered %q", body)
	}
	return seen
}

// sse reads server-sent events through the preview until the stream ends
func (s *stack) sse(t *testing.T, pv previewLink, path string) (int, error) {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+pv.host+path,
		nil)
	req.Header.Set(headerToken, pv.token)
	req.Header.Set("Accept", "text/event-stream")
	s.previewOnce.Do(func() { s.previews = s.previewClient() })
	res, err := s.previews.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("answered %d", res.StatusCode)
	}
	events := 0
	sc := bufio.NewScanner(res.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data: ") {
			events++
		}
	}
	return events, sc.Err()
}

// liveWS is one WebSocket through the edge, raw bytes after the upgrade
type liveWS struct {
	conn net.Conn
	br   *bufio.Reader
}

// dialEdge opens TLS to the edge for host over HTTP/1.1
func (s *stack) dialEdge(t *testing.T, host string) net.Conn {
	t.Helper()
	cfg := &tls.Config{ServerName: host, NextProtos: []string{"http/1.1"}}
	addr := net.JoinHostPort(host, "443")
	if s.mode == "local" {
		roots := x509.NewCertPool()
		if pem, err := os.ReadFile(filepath.Join(s.dir, "tls", "fullchain.pem")); err == nil {
			roots.AppendCertsFromPEM(pem)
		}
		cfg.RootCAs, addr = roots, s.edgeAddr
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		t.Fatalf("live: TLS to the edge for %s: %v", host, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// previewWS upgrades path through the preview with the header token and checks the echo
func (s *stack) previewWS(t *testing.T, pv previewLink, path string) *liveWS {
	t.Helper()
	conn := s.dialEdge(t, pv.host)
	_, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\n"+
		"Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n"+
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n%s: %s\r\n\r\n",
		path, pv.host, headerToken, pv.token)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	br := bufio.NewReader(conn)
	res, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("live: the upgrade through the preview: %v", err)
	}
	if res.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("live: the upgrade through the preview answered %d", res.StatusCode)
	}
	_ = conn.SetReadDeadline(time.Time{})
	w := &liveWS{conn: conn, br: br}
	if err := w.echo("hello"); err != nil {
		t.Fatalf("live: the first echo through the preview: %v", err)
	}
	return w
}

// echo sends msg and reads it back within 30 s
func (w *liveWS) echo(msg string) error {
	_ = w.conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer func() { _ = w.conn.SetDeadline(time.Time{}) }()
	if _, err := io.WriteString(w.conn, msg); err != nil {
		return err
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(w.br, got); err != nil {
		return err
	}
	if string(got) != msg {
		return fmt.Errorf("echoed %q for %q", got, msg)
	}
	return nil
}

// closesWithin fails the test unless the edge closes the socket within d
func (w *liveWS) closesWithin(t *testing.T, d time.Duration) {
	t.Helper()
	start := time.Now()
	_ = w.conn.SetReadDeadline(start.Add(d))
	_, err := w.br.ReadByte()
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("the WebSocket was still open %s later", d)
	}
	if err == nil {
		t.Fatal("the WebSocket sent a byte instead of closing")
	}
	t.Logf("the WebSocket closed in %s", time.Since(start).Round(time.Millisecond))
}

// hmr opens Vite's HMR socket through the preview, as the page's client does
func (s *stack) hmr(t *testing.T, pv previewLink) *websocket.Conn {
	t.Helper()
	cfg, err := websocket.NewConfig("wss://"+pv.host+"/", "https://"+pv.host)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Protocol = []string{"vite-hmr"}
	cfg.Header = http.Header{headerToken: {pv.token}}
	ws, err := websocket.NewClient(cfg, s.dialEdge(t, pv.host))
	if err != nil {
		t.Fatalf("live: Vite's HMR socket through the preview: %v", err)
	}
	return ws
}

// burst runs n calls spread over d from 32 workers and counts their answers
func (s *stack) burst(t *testing.T, n int, d time.Duration, call func() (int, error)) map[int]int {
	t.Helper()
	codes := map[int]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	gap := d / time.Duration(n)
	next := make(chan struct{})
	for range 32 {
		wg.Go(func() {
			for range next {
				code, err := call()
				if err != nil {
					code = 0
				}
				mu.Lock()
				codes[code]++
				mu.Unlock()
			}
		})
	}
	tick := time.NewTicker(gap)
	for range n {
		next <- struct{}{}
		<-tick.C
	}
	tick.Stop()
	close(next)
	wg.Wait()
	return codes
}

// drain reads an app path straight through the gate route and counts its bytes
func (g gated) drain(t *testing.T, path string, port int) int64 {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, g.route.BaseURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.credentials(req, port)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	n, err := io.Copy(io.Discard, res.Body)
	if err != nil {
		t.Fatalf("live: reading %s through the route after %d bytes: %v", path, n, err)
	}
	return n
}

// status asks an app path straight through the gate route and answers the status
func (g gated) status(t *testing.T, path string, port int) (int, error) {
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, g.route.BaseURL+path, nil)
	if err != nil {
		return 0, err
	}
	g.credentials(req, port)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	return res.StatusCode, nil
}

// credentials adds the route's provider credentials, the gate key and the app's port
func (g gated) credentials(req *http.Request, port int) {
	for k, v := range g.route.Header {
		req.Header.Set(k, v)
	}
	for k, v := range g.route.Cookies {
		req.AddCookie(&http.Cookie{Name: k, Value: v})
	}
	req.Header.Set("Bugnet-Gate", g.key)
	req.Header.Set("Bugnet-Port", strconv.Itoa(port))
}

func TestPreviewRowsAndTheirProviders(t *testing.T) {
	want := map[string][]string{
		"P1": nil, "P2": nil, "P3": nil, "P4": nil, "P5": nil, "P8": nil, "G3": nil,
		"E2B4": {"e2b"}, "E2B6": {"e2b"}, "E2B7": {"e2b"}, "E2B8": {"e2b"},
		"BOXD3": {"boxd"}, "KOYEB2": {"koyeb"}, "KOYEB3": {"koyeb"},
		"BOAT3": {"boat"}, "BOAT4": {"boat"},
	}
	for _, r := range dataplaneRows {
		if p, ok := want[r.id]; ok {
			eq(t, r.providers, p)
			eq(t, r.vps, false)
			delete(want, r.id)
		}
	}
	eq(t, len(want), 0)
	c, err := catalog.Load(filepath.Join(repo(t), "config", "dev-packages.json"))
	if err != nil {
		t.Fatal(err)
	}
	long, ok := c.Get(longPackage)
	eq(t, ok, true)
	eq(t, long.Minutes, 40)
	minute, ok := c.Get(minutePackage)
	eq(t, ok, true)
	eq(t, minute.Minutes, 1)
}

func TestRowAppScripts(t *testing.T) {
	eq(t, background("python3 a.py 1", "x"),
		`setsid nohup sh -c 'python3 a.py 1' > /tmp/bugnet-x.log 2>&1 < /dev/null &`)
	// The app goes in a quoted here-document, which only its own end line can close
	eq(t, strings.Contains(rowApp, "BUGNET_APP"), false)
	p := viteProject("preview.dev.bugnet.sh")
	for _, want := range []string{`allowedHosts:[".preview.dev.bugnet.sh"]`, "src/m$i.js",
		"-lt 300"} {
		if !strings.Contains(p, want) {
			t.Fatalf("the Vite project script lacks %q", want)
		}
	}
}

/* Secret rows, DP 16.2 X1 to X6 and the plan's KOYEB5 */

const (
	// echoHost answers with the headers it received, so a swapped value shows in its body
	echoHost = "httpbin.org"
	// otherEchoHost is a second public echo that no secret names
	otherEchoHost = "postman-echo.com"
	// secretName is the variable the rows' placeholder lands in
	secretName = "ECHO_TOKEN"
	// applyWithin is how long a secret may take to reach the sandbox's environment
	applyWithin = time.Minute
	// restartWithin is how long X4 gives systemd to bring the forwarder back
	restartWithin = 45 * time.Second
)

// secretProviders are the providers whose secret rows run, never local
var secretProviders = []string{"e2b", "boxd", "koyeb", "boat"}

func init() {
	for _, id := range []string{"X1", "X2", "X3", "X4", "X5", "X6"} {
		dataplaneRows = append(dataplaneRows, row{id: id, providers: secretProviders, vps: true,
			run: secretRows[id]})
	}
	dataplaneRows = append(dataplaneRows,
		row{id: "KOYEB5", providers: []string{"koyeb"}, vps: true, run: rowKoyeb5})
}

var secretRows = map[string]func(t *testing.T, s *stack){
	"X1": rowX1, "X2": rowX2, "X3": rowX3, "X4": rowX4, "X5": rowX5, "X6": rowX6,
}

// devStdin is dev with stdin, which bugnet-dev secret reads its value from
func (s *stack) devStdin(t *testing.T, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command(filepath.Join(s.bin, "bugnet-dev"),
		append([]string{"--config", s.settings}, args...)...)
	cmd.Dir, cmd.Env, cmd.Stdin = s.root, s.env, strings.NewReader(stdin)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("live: bugnet-dev %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return string(out)
}

// throwaway is a value that is worth nothing, since it travels to a public echo host
func throwaway() string {
	return "throwaway-" + strconv.FormatUint(rand.Uint64(), 36) +
		strconv.FormatUint(rand.Uint64(), 36)
}

// putSecret puts a secret bound to hosts and answers the placeholder the sandbox sees for it
func (s *stack) putSecret(t *testing.T, h, name, value string, hosts ...string) string {
	t.Helper()
	args := []string{"secret", "--handle", h, "--name", name}
	for _, host := range hosts {
		args = append(args, "--host", host)
	}
	put := kv(s.devStdin(t, value, args...))
	if put["state"] == "failed" {
		t.Fatalf("live: the secret %s failed: %v", name, put)
	}
	deadline := time.Now().Add(applyWithin)
	for {
		got, _, err := s.devTry(t, "exec", "--handle", h, "--", "sh", "-c", `printenv `+name)
		got = strings.TrimSpace(got)
		if err == nil && strings.HasPrefix(got, "bnsec_") {
			if strings.Contains(got, value) {
				t.Fatalf("the sandbox's %s holds the value", name)
			}
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not reach the sandbox's environment within %s, state %s",
				name, applyWithin, put["state"])
		}
		time.Sleep(2 * time.Second)
	}
}

// echoSandbox is a sandbox with a secret bound to the echo host, and the value and placeholder
func (s *stack) echoSandbox(t *testing.T) (h, value, placeholder string) {
	t.Helper()
	h = s.sandbox(t, xferPackage)
	value = throwaway()
	return h, value, s.putSecret(t, h, secretName, value, echoHost)
}

// shTry runs a shell script in the sandbox and answers its output, an error included
func (s *stack) shTry(t *testing.T, h, script string) string {
	t.Helper()
	out, stderr, _ := s.devTry(t, "exec", "--handle", h, "--", "sh", "-c", script)
	return out + stderr
}

// secretClient is one client of X1's matrix
type secretClient struct {
	name string
	// script prints SKIP when the tool is absent, else the response or the tool's own error
	script string
	// wantValue means the echo's body must show the swapped value
	wantValue bool
}

var certErrors = []string{"SSL certificate problem", "certificate verify failed",
	"CERTIFICATE_VERIFY_FAILED", "UnknownIssuer", "invalid peer certificate",
	"x509: certificate", "unable to get local issuer", "self-signed certificate",
	"self signed certificate"}

func secretClients() []secretClient {
	h := echoHost
	bearer := `"Authorization: Bearer $` + secretName + `"`
	return []secretClient{
		{"curl", `curl -sS -H ` + bearer + ` https://` + h + `/headers 2>&1`, true},
		{"requests", `python3 -c 'import requests' 2>/dev/null || pip install -q --user requests ` +
			`>/dev/null 2>&1; python3 -c "import os,requests;print(requests.get('https://` + h +
			`/headers',headers={'Authorization':'Bearer '+os.environ['` + secretName +
			`']},timeout=30).text)" 2>&1 || echo SKIP`, true},
		{"httpx", `python3 -c 'import httpx' 2>/dev/null || pip install -q --user httpx ` +
			`>/dev/null 2>&1; python3 -c "import os,httpx;print(httpx.get('https://` + h +
			`/headers',headers={'Authorization':'Bearer '+os.environ['` + secretName +
			`']},timeout=30).text)" 2>&1 || echo SKIP`, true},
		{"node", `command -v node >/dev/null || { echo SKIP; exit 0; }; node -e "fetch('https://` +
			h + `/headers',{headers:{Authorization:'Bearer '+process.env.` + secretName +
			`}}).then(r=>r.text()).then(console.log,e=>console.log(String(e.cause||e)))" ` +
			`2>&1`, true},
		{"go", `command -v go >/dev/null || { echo SKIP; exit 0; }; d=$(mktemp -d); ` +
			`cat > $d/m.go ` +
			`<<'GO'
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	req, _ := http.NewRequest("GET", "https://` + h + `/headers", nil)
	req.Header.Set("Authorization", "Bearer "+os.Getenv("` + secretName + `"))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	b, _ := io.ReadAll(res.Body)
	fmt.Println(string(b))
}
GO
cd $d && go run m.go 2>&1`, true},
		{"git", `command -v git >/dev/null || { echo SKIP; exit 0; }; ` +
			`git ls-remote https://` + h + `/ 2>&1; true`, false},
		{"pip", `command -v pip >/dev/null || { echo SKIP; exit 0; }; ` +
			`pip download -q --no-deps -d /tmp/pipx --index-url https://` + h +
			`/simple/ nothing-here 2>&1; true`, false},
		{"uv", `command -v uv >/dev/null || pip install -q --user uv >/dev/null 2>&1; ` +
			`command -v uv >/dev/null || [ -x ~/.local/bin/uv ] || { echo SKIP; exit 0; }; ` +
			`PATH=$PATH:~/.local/bin uv pip install --system --index-url https://` + h +
			`/simple/ nothing-here 2>&1; true`, false},
	}
}

// rowX1 runs the eight clients against the echo host and expects every one to trust the Bugnet CA
func rowX1(t *testing.T, s *stack) {
	h, value, _ := s.echoSandbox(t)
	ran := 0
	for _, c := range secretClients() {
		out := s.shTry(t, h, c.script)
		switch {
		case strings.Contains(out, "SKIP"):
			t.Logf("X1 on %s: %s skipped, the tool is not in the sandbox", s.provider, c.name)
			continue
		case slices.ContainsFunc(certErrors,
			func(e string) bool { return strings.Contains(out, e) }):
			t.Errorf("X1 on %s: %s does not trust the Bugnet CA: %s", s.provider, c.name,
				strings.TrimSpace(out))
			continue
		case c.wantValue && !strings.Contains(out, value):
			t.Errorf("X1 on %s: %s reached %s but the echo did not show the swapped value: %s",
				s.provider, c.name, echoHost, strings.TrimSpace(out))
			continue
		}
		ran++
		t.Logf("X1 on %s: %s trusts the Bugnet CA", s.provider, c.name)
	}
	if ran < 4 {
		t.Fatalf("X1 on %s: %d of 8 clients ran, at least curl, requests, httpx and one more "+
			"are needed", s.provider, ran)
	}
}

// rowX2 downloads a gated file and clones a private repository with throwaway tokens the
// controller names, and checks the sandbox's environment holds only placeholders
func rowX2(t *testing.T, s *stack) {
	var (
		hfURL    = os.Getenv("BUGNET_LIVE_X2_HF_URL")
		hfToken  = os.Getenv("BUGNET_LIVE_X2_HF_TOKEN")
		gitURL   = os.Getenv("BUGNET_LIVE_X2_GIT_URL")
		gitToken = os.Getenv("BUGNET_LIVE_X2_GIT_TOKEN")
	)
	if hfURL == "" || hfToken == "" || gitURL == "" || gitToken == "" {
		t.Skip("X2 needs BUGNET_LIVE_X2_HF_URL, _HF_TOKEN, _GIT_URL and _GIT_TOKEN, throwaway " +
			"tokens for one gated file and one private repository")
	}
	hfHost, gitHost := hostOf(t, hfURL), hostOf(t, gitURL)
	h := s.sandbox(t, xferPackage)
	hfPH := s.putSecret(t, h, "HF_TOKEN", hfToken, hfHost)
	gitPH := s.putSecret(t, h, "GIT_TOKEN", gitToken, gitHost)
	out := s.shTry(t, h, `curl -sS -L -o /tmp/x2.bin -w '%{http_code}' -H "Authorization: Bearer `+
		`$HF_TOKEN" `+hfURL+`; echo; wc -c < /tmp/x2.bin`)
	if !strings.HasPrefix(strings.TrimSpace(out), "200") {
		t.Fatalf("X2 on %s: the gated file answered %q", s.provider, out)
	}
	clone := s.shTry(t, h, `rm -rf /tmp/x2repo; git -c http.extraHeader="Authorization: Bearer `+
		`$GIT_TOKEN" clone --depth 1 `+gitURL+` /tmp/x2repo 2>&1 && echo CLONED`)
	if !strings.Contains(clone, "CLONED") {
		t.Fatalf("X2 on %s: the private clone failed: %s", s.provider, clone)
	}
	env := s.shTry(t, h, `env`)
	for _, secret := range []string{hfToken, gitToken} {
		if strings.Contains(env, secret) {
			t.Fatalf("X2 on %s: env shows a throwaway value", s.provider)
		}
	}
	for _, ph := range []string{hfPH, gitPH} {
		if !strings.Contains(env, ph) {
			t.Fatalf("X2 on %s: env lacks the placeholder %s", s.provider, ph)
		}
	}
}

func hostOf(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		t.Fatalf("not a URL: %q", raw)
	}
	return u.Hostname()
}

// rowX3 binds a throwaway to the echo host, which shows the value, and sends the placeholder to a
// host no secret names, which shows the placeholder
func rowX3(t *testing.T, s *stack) {
	h, value, placeholder := s.echoSandbox(t)
	auth := `-H "Authorization: Bearer $` + secretName + `"`
	bound := s.shTry(t, h, `curl -sS `+auth+` https://`+echoHost+`/headers`)
	if !strings.Contains(bound, value) || strings.Contains(bound, placeholder) {
		t.Fatalf("X3 on %s: the bound echo host did not show the value only: %s", s.provider, bound)
	}
	other := s.shTry(t, h, `curl -sS `+auth+` https://`+otherEchoHost+`/headers`)
	if !strings.Contains(other, placeholder) || strings.Contains(other, value) {
		t.Fatalf("X3 on %s: a host no secret names did not show the placeholder only: %s",
			s.provider, other)
	}
}

// rowX4 kills the forwarder by its pid file and expects secret hosts back after systemd's restart
// or at the next job
func rowX4(t *testing.T, s *stack) {
	h, value, _ := s.echoSandbox(t)
	auth := `-H "Authorization: Bearer $` + secretName + `"`
	call := `curl -sS --max-time 20 ` + auth + ` https://` + echoHost + `/headers`
	if got := s.shTry(t, h, call); !strings.Contains(got, value) {
		t.Fatalf("X4 on %s: the secret host did not work before the kill: %s", s.provider, got)
	}
	s.shTry(t, h, asRoot+`$S kill $(cat /run/bugnet/forward.pid)`)
	deadline := time.Now().Add(restartWithin)
	for {
		// Each exec is a job, and a job start respawns a dead forwarder
		if got := s.shTry(t, h, call); strings.Contains(got, value) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("X4 on %s: the forwarder was not back within %s", s.provider, restartWithin)
		}
		time.Sleep(3 * time.Second)
	}
}

// tunnelStatus asks the proxy for a tunnel with the bearer, none when empty, and answers the status
func (s *stack) tunnelStatus(t *testing.T, bearer string) string {
	t.Helper()
	args := []string{"-sS", "-o", "/dev/null", "-w", "%{http_code}", "--http1.1",
		"-H", "Connection: Upgrade", "-H", "Upgrade: websocket", "-H", "Sec-WebSocket-Version: 13",
		"-H", "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==",
		"-H", "Sec-WebSocket-Protocol: bugnet-tunnel.v1"}
	if bearer != "" {
		args = append(args, "-H", "Authorization: Bearer "+bearer)
	}
	out, err := exec.Command("curl", append(args, "--max-time", "30",
		"https://"+s.f.Names.EgressHost+"/v1/tunnel")...).Output()
	if err != nil {
		t.Fatalf("curl to the proxy: %v", err)
	}
	return string(out)
}

// rowX5 opens a tunnel from outside with no token, then with only a placeholder, and uses the
// placeholder at the echo host directly
func rowX5(t *testing.T, s *stack) {
	_, value, placeholder := s.echoSandbox(t)
	eq(t, s.tunnelStatus(t, ""), "401")
	eq(t, s.tunnelStatus(t, placeholder), "401")
	out, err := exec.Command("curl", "-sS", "--max-time", "30", "-H",
		"Authorization: Bearer "+placeholder, "https://"+echoHost+"/headers").CombinedOutput()
	if err != nil {
		t.Fatalf("curl to the echo host from outside: %v\n%s", err, out)
	}
	if strings.Contains(string(out), value) || !strings.Contains(string(out), placeholder) {
		t.Fatalf("X5 on %s: the placeholder from outside did not stay inert: %s", s.provider, out)
	}
}

// rowX6 sends TRACE to the secret host through the tunnel and expects 405
func rowX6(t *testing.T, s *stack) {
	h, _, _ := s.echoSandbox(t)
	got := strings.TrimSpace(s.shTry(t, h,
		`curl -sS -o /dev/null -w '%{http_code}' --max-time 20 -X TRACE https://`+echoHost+`/`))
	eq(t, got, "405")
}

// rowKoyeb5 checks the managed block of /etc/hosts on Koyeb, where the file may be a bind mount,
// and that a second secret rewrites it in place
func rowKoyeb5(t *testing.T, s *stack) {
	h, _, _ := s.echoSandbox(t)
	mount := strings.TrimSpace(s.shTry(t, h, `grep ' /etc/hosts ' /proc/self/mountinfo; true`))
	t.Logf("KOYEB5: /etc/hosts mount entry %q", mount)
	hosts := s.shTry(t, h, `cat /etc/hosts`)
	line := mapLine(hosts, echoHost)
	if line == "" {
		t.Fatalf("KOYEB5: /etc/hosts maps %s to nothing in the managed block: %s", echoHost, hosts)
	}
	s.putSecret(t, h, "OTHER_TOKEN", throwaway(), otherEchoHost)
	hosts = s.shTry(t, h, `cat /etc/hosts`)
	for _, host := range []string{echoHost, otherEchoHost} {
		if mapLine(hosts, host) == "" {
			t.Fatalf("KOYEB5: /etc/hosts lost %s after a second secret: %s", host, hosts)
		}
	}
	if strings.Count(hosts, "# bugnet begin") > 1 {
		t.Fatalf("KOYEB5: the managed block is written twice: %s", hosts)
	}
}

// mapLine is the /etc/hosts line that maps host to the forwarder's address
func mapLine(hosts, host string) string {
	for _, line := range strings.Split(hosts, "\n") {
		f := strings.Fields(line)
		if len(f) > 1 && f[0] == "127.77.0.1" && slices.Contains(f[1:], host) {
			return line
		}
	}
	return ""
}

func TestSecretRowsAndTheirProviders(t *testing.T) {
	want := map[string][]string{
		"X1": secretProviders, "X2": secretProviders, "X3": secretProviders,
		"X4": secretProviders, "X5": secretProviders, "X6": secretProviders,
		"KOYEB5": {"koyeb"},
	}
	for _, r := range dataplaneRows {
		if p, ok := want[r.id]; ok {
			eq(t, r.providers, p)
			eq(t, r.vps, true)
			delete(want, r.id)
		}
	}
	eq(t, len(want), 0)
	eq(t, slices.Contains(secretProviders, "local"), false)
}

func TestMapLineFindsTheForwardersLine(t *testing.T) {
	hosts := "127.0.0.1 localhost\n# bugnet begin\n127.77.0.1 a.example b.example\n# bugnet end\n"
	eq(t, mapLine(hosts, "b.example"), "127.77.0.1 a.example b.example")
	eq(t, mapLine(hosts, "c.example"), "")
	eq(t, mapLine("127.0.0.1 b.example\n", "b.example"), "")
}

func TestSecretClientsNameEightClients(t *testing.T) {
	var names []string
	for _, c := range secretClients() {
		names = append(names, c.name)
	}
	eq(t, names, []string{"curl", "requests", "httpx", "node", "go", "git", "pip", "uv"})
}

const (
	x402LiveHost   = "agent402.tools"
	x402LivePrefix = "/api/"
	// x402LivePayTo is the System Program address, a valid base58 key that holds no funds
	x402LivePayTo = "11111111111111111111111111111111"
)

func init() {
	dataplaneRows = append(dataplaneRows, row{id: "X402A", run: rowX402A})
}

// x402Change stores or disables the row of the placement behind h and queues apply_secrets
func (s *stack) x402Change(t *testing.T, h string, disable bool) {
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
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	st := store.New(pool, store.WithRefSealer(cfg.DataPlaneKeys))
	p, err := st.PlacementByHandle(t.Context(), sha)
	if err != nil {
		t.Fatalf("live: the placement of %s: %v", h, err)
	}
	rc, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	if err != nil {
		t.Fatal(err)
	}
	id := x402.ServiceID("GET", x402LiveHost, x402LivePrefix)
	if disable {
		if err = st.DisableX402Service(t.Context(), p.ID, id); err == nil {
			err = st.BumpSecretsVersion(t.Context(), p.ID, false)
		}
	} else {
		err = st.PutX402Service(t.Context(), store.X402Service{
			PlacementID: p.ID, ServiceID: id,
			Spec: x402.Spec{Host: x402LiveHost, Prefix: x402LivePrefix, Method: "GET",
				MaxPriceMicros: 10_000, PayTo: x402LivePayTo, Indexed: true},
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rc.Insert(t.Context(), jobargs.ApplySecrets{PlacementID: p.ID}, nil); err != nil {
		t.Fatal(err)
	}
}

// waitScript reruns a shell script in the sandbox until it prints want, for up to a minute
func (s *stack) waitScript(t *testing.T, h, script, want string) string {
	t.Helper()
	var out string
	for range 30 {
		if out = strings.TrimSpace(s.sh(t, h, script)); out == want {
			return out
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("live: %q printed %q, want %q", script, out, want)
	return out
}

// rowX402A maps an x402 host to the forwarder with the two env lines, and a disable removes both
func rowX402A(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	hosts := `grep -c '^127.77.0.1[[:space:]].*` + x402LiveHost + `' /etc/hosts`
	env := `printenv BUGNET_X402 BUGNET_X402_HOSTS | tr '\n' ' '`
	s.x402Change(t, h, false)
	s.waitScript(t, h, hosts, "1")
	s.waitScript(t, h, env, "1 "+x402LiveHost+" ")
	s.x402Change(t, h, true)
	s.waitScript(t, h, hosts+" || true", "0")
	s.waitScript(t, h, env+" || true", "")
}
