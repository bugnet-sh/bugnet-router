package live

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
)

const (
	// sshLongPackage lasts 75 minutes, for S4's hour-long session
	sshLongPackage = "dev-small-75m"
	// edgePackage has a 64 MiB edge budget, for B1 and B2
	edgePackage = "dev-edge-64m"
	// edgeExtension adds another 64 MiB and ten minutes to edgePackage
	edgeExtension = "dev-edge-64m-more"
	// sshRetries covers prepare_ssh, which a first connection may wait on
	sshRetries = 6
)

func init() {
	dataplaneRows = append(dataplaneRows,
		row{id: "S1", run: rowS1},
		row{id: "S2", run: rowS2},
		row{id: "S3", run: rowS3},
		row{id: "S4", run: rowS4},
		row{id: "S5", run: rowS5},
		row{id: "S6", run: rowS6},
		row{id: "S7", run: rowS7},
		row{id: "B1", run: rowB1},
		row{id: "B2", run: rowB2},
		row{id: "E2B3", providers: []string{"e2b"}, run: rowE2B3},
		row{id: "BOXD4", providers: []string{"boxd"}, run: rowBoxd4},
		row{id: "KOYEB4", providers: []string{"koyeb"}, run: rowKoyeb4},
		row{id: "KOYEB6", providers: []string{"koyeb"}, run: rowKoyeb6},
		row{id: "BOAT6", providers: []string{"boat"}, run: rowBoat6},
	)
}

/* SSH helpers */

// sshAccess is a key registered on one placement
type sshAccess struct {
	alias       string
	identity    string
	fingerprint string
}

// sshKey makes a key pair, registers it on h and answers how to connect with it
func (s *stack) sshKey(t *testing.T, h string) sshAccess {
	t.Helper()
	dir := t.TempDir()
	id := filepath.Join(dir, "id")
	if out, err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f",
		id).CombinedOutput(); err != nil {
		t.Fatalf("live: ssh-keygen: %v\n%s", err, out)
	}
	m := kv(s.dev(t, "ssh-key", "--handle", h, "--public-key", id+".pub"))
	if m["alias"] == "" || m["fingerprint"] == "" {
		t.Fatalf("live: bugnet-dev ssh-key printed %v", m)
	}
	return sshAccess{alias: m["alias"], identity: id, fingerprint: m["fingerprint"]}
}

// sshOpts are the options of DP 9.2's command, pointed at this stack's gateway
func (s *stack) sshOpts(a sshAccess) []string {
	_, port, _ := net.SplitHostPort(s.sshAddr)
	return []string{
		"-p", port, "-i", a.identity, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + s.sshKnown,
	}
}

func (s *stack) sshDest(a sshAccess) string {
	host, _, _ := net.SplitHostPort(s.sshAddr)
	return a.alias + "@" + host
}

// scpOpts are sshOpts with scp's capital P
func (s *stack) scpOpts(a sshAccess) []string {
	o := s.sshOpts(a)
	o[0] = "-P"
	return o
}

// sshRun runs ssh, again after the retry banner while the sandbox's sshd is being prepared
func (s *stack) sshRun(t *testing.T, a sshAccess, extra ...string) (string, string, error) {
	t.Helper()
	var stdout, stderr string
	var err error
	for i := range sshRetries {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Hour)
		args := append(append(s.sshOpts(a), extra[:len(extra)-1]...), s.sshDest(a),
			extra[len(extra)-1])
		cmd := exec.CommandContext(ctx, "ssh", args...)
		var so, se bytes.Buffer
		cmd.Stdout, cmd.Stderr = &so, &se
		err = cmd.Run()
		cancel()
		stdout, stderr = so.String(), se.String()
		if err == nil || !strings.Contains(stderr, "try again") &&
			!strings.Contains(stderr, "not reachable yet") || i == sshRetries-1 {
			break
		}
		time.Sleep(10 * time.Second)
	}
	return stdout, stderr, err
}

func (s *stack) sshMust(t *testing.T, a sshAccess, args ...string) string {
	t.Helper()
	out, stderr, err := s.sshRun(t, a, args...)
	if err != nil {
		t.Fatalf("live: ssh %s: %v\n%s", args[len(args)-1], err, stderr)
	}
	return out
}

// tool runs a local program and fails the test on an error
func tool(t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("live: %s %s: %v\n%s", name, strings.Join(args, " "), err, stderr.String())
	}
	return stdout.String()
}

// sshBackground starts a long ssh this test owns, ended by its PID at the end of the test
func (s *stack) sshBackground(t *testing.T, a sshAccess, extra ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("ssh", append(append(s.sshOpts(a), extra...), s.sshDest(a))...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd
}

// endsWithin reports whether cmd exits within d
func endsWithin(cmd *exec.Cmd, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// socksReply asks a SOCKS5 proxy to connect to host:port and answers the reply code
func socksReply(t *testing.T, proxy, host string, port int) byte {
	t.Helper()
	c, err := net.DialTimeout("tcp", proxy, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	_, _ = c.Write([]byte{5, 1, 0})
	if _, err := io.ReadFull(c, make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	req := append([]byte{5, 1, 0, 3, byte(len(host))}, host...)
	_, _ = c.Write(append(req, byte(port>>8), byte(port)))
	reply := make([]byte, 4)
	if _, err := io.ReadFull(c, reply); err != nil {
		return 1
	}
	return reply[1]
}

func loopbackPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	_, p, _ := net.SplitHostPort(ln.Addr().String())
	return p
}

func waitLocalPort(t *testing.T, port string) {
	t.Helper()
	for range 100 {
		if c, err := net.DialTimeout("tcp", "127.0.0.1:"+port, time.Second); err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("live: nothing listened on 127.0.0.1:%s", port)
}

/* Rows */

// rowS1 runs a command over SSH and expects the exec user
func rowS1(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	a := s.sshKey(t, h)
	want := strings.TrimSpace(s.sh(t, h, "id -un"))
	eq(t, strings.TrimSpace(s.sshMust(t, a, "id -un")), want)
}

// rowS2 runs a PTY shell, scp both ways and with -O, sftp, rsync and ssh -L
func rowS2(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	a := s.sshKey(t, h)
	dir := t.TempDir()
	s.sshMust(t, a, "true")
	if out := s.sshMust(t, a, "-tt", "tty"); !strings.Contains(out, "/dev/") {
		t.Fatalf("live: the PTY shell printed %q", out)
	}
	src := randomFile(t, dir, "s2", 5*mib+1)
	want := fileSHA(t, src)
	dest := s.sshDest(a)
	tool(t, "scp", append(s.scpOpts(a), src, dest+":bn-s2")...)
	eq(t, strings.Fields(s.sh(t, h, `sha256sum "$HOME/bn-s2"`))[0], want)
	back := filepath.Join(dir, "back")
	tool(t, "scp", append(s.scpOpts(a), dest+":bn-s2", back)...)
	eq(t, fileSHA(t, back), want)
	tool(t, "scp", append(append([]string{"-O"}, s.scpOpts(a)...), src, dest+":bn-s2o")...)
	eq(t, strings.Fields(s.sh(t, h, `sha256sum "$HOME/bn-s2o"`))[0], want)
	batch := filepath.Join(dir, "batch")
	if err := os.WriteFile(batch, []byte("put "+src+" bn-s2f\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool(t, "sftp", append(append(s.scpOpts(a), "-b", batch), dest)...)
	eq(t, strings.Fields(s.sh(t, h, `sha256sum "$HOME/bn-s2f"`))[0], want)
	if _, err := exec.LookPath("rsync"); err == nil {
		e := "ssh " + strings.Join(s.sshOpts(a), " ")
		tool(t, "rsync", "-e", e, src, dest+":bn-s2r")
		eq(t, strings.Fields(s.sh(t, h, `sha256sum "$HOME/bn-s2r"`))[0], want)
	} else {
		t.Log("S2: no rsync on this host, skipped that part")
	}
	s.sh(t, h, background("python3 -m http.server 8000 --bind 127.0.0.1", "s2http"))
	s.waitPort(t, h, 8000)
	local := loopbackPort(t)
	s.sshBackground(t, a, "-N", "-L", local+":127.0.0.1:8000")
	waitLocalPort(t, local)
	resp, err := http.Get("http://127.0.0.1:" + local + "/")
	if err != nil {
		t.Fatalf("live: ssh -L: %v", err)
	}
	_ = resp.Body.Close()
	eq(t, resp.StatusCode, http.StatusOK)
}

// rowS3 expects -D to a remote host, -R, -A and -X to be refused
func rowS3(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	a := s.sshKey(t, h)
	s.sshMust(t, a, "true")
	local := loopbackPort(t)
	s.sshBackground(t, a, "-N", "-D", local)
	waitLocalPort(t, local)
	for _, host := range []string{"example.com", "10.0.0.1"} {
		if socksReply(t, "127.0.0.1:"+local, host, 80) == 0 {
			t.Fatalf("live: ssh -D reached %s", host)
		}
	}
	_, stderr, err := s.sshRun(t, a, "-N", "-o", "ExitOnForwardFailure=yes", "-R",
		loopbackPort(t)+":127.0.0.1:22", "true")
	if err == nil || !strings.Contains(stderr, "forwarding failed") {
		t.Fatalf("live: ssh -R was not refused: %v %q", err, stderr)
	}
	sock := filepath.Join(t.TempDir(), "agent")
	ln, lerr := net.Listen("unix", sock)
	if lerr != nil {
		t.Fatal(lerr)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	cmd := exec.Command("ssh", append(append(s.sshOpts(a), "-A"), s.sshDest(a),
		`echo "[$SSH_AUTH_SOCK]"`)...)
	cmd.Env = append(os.Environ(), "SSH_AUTH_SOCK="+sock)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("live: ssh -A: %v", err)
	}
	eq(t, strings.TrimSpace(string(out)), "[]")
	t.Log("S3: -X is refused by the relay table, tested with a real client in bugnet-edges")
}

// rowS4 holds a 15-minute idle session, then a long one, and expects neither to be cut
func rowS4(t *testing.T, s *stack) {
	h := s.sandbox(t, sshLongPackage)
	a := s.sshKey(t, h)
	eq(t, strings.TrimSpace(s.sshMust(t, a, "sleep 900; echo alive")), "alive")
	long := 3000 // 50 minutes, E2B's Hobby plan caps a sandbox at 1 h
	if s.provider != "e2b" || os.Getenv("BUGNET_LIVE_E2B_PRO") == "1" {
		long = 3600
	}
	eq(t, strings.TrimSpace(s.sshMust(t, a, "sleep "+strconv.Itoa(long)+"; echo alive")), "alive")
}

// rowS5 copies 1 GiB with scp and records the rate
func rowS5(t *testing.T, s *stack) {
	h := s.sandbox(t, bigPackage)
	a := s.sshKey(t, h)
	s.sshMust(t, a, "true")
	src := randomFile(t, t.TempDir(), "s5", 1<<30)
	start := time.Now()
	tool(t, "scp", append(s.scpOpts(a), src, s.sshDest(a)+":bn-s5")...)
	took := time.Since(start)
	eq(t, strings.Fields(s.sh(t, h, `sha256sum "$HOME/bn-s5"`))[0], fileSHA(t, src))
	t.Logf("S5 on %s: 1 GiB in %s (%.1f MiB/s)", s.provider, took.Round(time.Second),
		1024/took.Seconds())
}

// rowS6 expects the SetEnv variables in a session without PAM
func rowS6(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	a := s.sshKey(t, h)
	out := s.sshMust(t, a, "env")
	for _, name := range []string{"SSL_CERT_FILE=", "REQUESTS_CA_BUNDLE=", "NODE_EXTRA_CA_CERTS="} {
		if !strings.Contains(out, name) {
			t.Fatalf("live: ssh host env shows no %s in %q", name, out)
		}
	}
}

// rowS7 expects a session to close at the deadline, at an early stop and at RemoveSshKey
func rowS7(t *testing.T, s *stack) {
	t.Run("deadline", func(t *testing.T) {
		h := s.sandbox(t, minutePackage)
		a := s.sshKey(t, h)
		s.sshMust(t, a, "true")
		cmd := s.sshBackground(t, a, "sleep 600")
		if !endsWithin(cmd, 3*time.Minute) {
			t.Fatal("live: the session outlived a one-minute package")
		}
	})
	t.Run("stop", func(t *testing.T) {
		h := s.sandbox(t, xferPackage)
		a := s.sshKey(t, h)
		s.sshMust(t, a, "true")
		cmd := s.sshBackground(t, a, "sleep 600")
		time.Sleep(3 * time.Second)
		s.dev(t, "stop", "--handle", h)
		if !endsWithin(cmd, 10*time.Second) {
			t.Fatal("live: the session survived StopPlacement")
		}
	})
	t.Run("remove key", func(t *testing.T) {
		h := s.sandbox(t, xferPackage)
		a := s.sshKey(t, h)
		s.sshMust(t, a, "true")
		cmd := s.sshBackground(t, a, "sleep 600")
		time.Sleep(3 * time.Second)
		s.dev(t, "ssh-key", "--handle", h, "--remove", a.fingerprint)
		if !endsWithin(cmd, 10*time.Second) {
			t.Fatal("live: the session survived RemoveSshKey")
		}
	})
}

// spendEdge downloads through a preview until the budget cuts it, answering the bytes it moved
func (s *stack) spendEdge(t *testing.T, pv previewLink) int64 {
	t.Helper()
	var total int64
	for range 40 {
		res, body, err := s.previewTry(t, pv, "/bn-chunk", headerToken, pv.token)
		if err != nil || res.StatusCode != http.StatusOK {
			return total
		}
		total += int64(len(body))
	}
	t.Fatal("live: 40 chunks of 8 MiB never spent the budget")
	return total
}

const chunkMiB = 8

// serveChunk starts a server in the sandbox with an 8 MiB file, answers its preview
func (s *stack) serveChunk(t *testing.T, h string) previewLink {
	t.Helper()
	s.sh(t, h, `mkdir -p /tmp/bn-chunk && head -c `+strconv.Itoa(chunkMiB<<20)+
		` /dev/urandom > /tmp/bn-chunk/bn-chunk && cd /tmp/bn-chunk && `+
		background("python3 -m http.server 8001 --bind 127.0.0.1", "chunk"))
	s.waitPort(t, h, 8001)
	return s.preview(t, h, 8001)
}

// rowB1 spends a 64 MiB budget across a preview, a transfer and scp and expects all cut by 80 MiB
func rowB1(t *testing.T, s *stack) {
	h := s.sandbox(t, edgePackage)
	a := s.sshKey(t, h)
	pv := s.serveChunk(t, h)
	s.sshMust(t, a, "true")
	dir := t.TempDir()
	var total int64
	step := int64(chunkMiB << 20)
	local := randomFile(t, dir, "b1", step)
	for i := 0; total < 64<<20; i++ {
		switch i % 3 {
		case 0:
			res, body, err := s.previewTry(t, pv, "/bn-chunk", headerToken, pv.token)
			if err != nil || res.StatusCode != http.StatusOK {
				t.Fatalf("live: the preview was cut early at %d bytes", total)
			}
			total += int64(len(body))
		case 1:
			s.download(t, h, "/tmp/bn-chunk/bn-chunk", filepath.Join(dir, "d"))
			total += step
		case 2:
			tool(t, "scp", append(s.scpOpts(a), local, s.sshDest(a)+":bn-b1")...)
			total += step
		}
	}
	// Past the budget every carrier must fail within one report threshold per carrier
	var cut [3]bool
	for range 6 {
		res, body, err := s.previewTry(t, pv, "/bn-chunk", headerToken, pv.token)
		cut[0] = cut[0] || err != nil || res.StatusCode != http.StatusOK
		if cut[0] && res != nil {
			t.Logf("B1: the preview answered %d: %.200s", res.StatusCode, body)
		}
		_, _, derr := s.devTry(t, "transfer", "--handle", h, "--download", "/tmp/bn-chunk/bn-chunk")
		cut[1] = cut[1] || derr != nil
		_, _, serr := s.sshRun(t, a, "true")
		cut[2] = cut[2] || serr != nil
	}
	if cut != [3]bool{true, true, true} {
		t.Fatalf("live: not every carrier was cut after %d MiB: preview, transfer, ssh = %v",
			total>>20, cut)
	}
}

// rowB2 spends the budget, extends, and loads the same preview link again
func rowB2(t *testing.T, s *stack) {
	h := s.sandbox(t, edgePackage)
	pv := s.serveChunk(t, h)
	spent := s.spendEdge(t, pv)
	if spent > 80<<20 {
		t.Fatalf("live: %d MiB passed a 64 MiB budget", spent>>20)
	}
	s.dev(t, "extend", "--handle", h, "--package", edgeExtension)
	res, _ := s.previewGet(t, pv, "/bn-chunk", headerToken, pv.token)
	eq(t, res.StatusCode, http.StatusOK)
}

// rowE2B3 expects no stock sshd listening beyond loopback on the rebuilt template
func rowE2B3(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	a := s.sshKey(t, h)
	s.sshMust(t, a, "true")
	out := s.sh(t, h, asRoot+`$S ss -ltnp`)
	t.Logf("E2B3: ss -ltnp\n%s", out)
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) > 3 && strings.Contains(line, "sshd") &&
			!strings.HasPrefix(f[3], "127.0.0.1:") && !strings.HasPrefix(f[3], "[::1]:") {
			t.Fatalf("live: an sshd listens beyond loopback: %s", line)
		}
	}
}

// rowBoxd4 records whether a guest sshd holds port 22 before ours
func rowBoxd4(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	t.Logf("BOXD4: listeners on port 22 before ssh-up\n%s",
		s.sh(t, h, asRoot+`$S ss -ltnp | grep -E ':22\b' || echo none`))
}

// rowKoyeb4 times the install that ssh.prepare_timeout is set from
func rowKoyeb4(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	start := time.Now()
	s.sh(t, h, asRoot+`$S apt-get update -qq`)
	update := time.Since(start)
	start = time.Now()
	s.sh(t, h, asRoot+`DEBIAN_FRONTEND=noninteractive $S apt-get install -y -qq openssh-server`)
	t.Logf("KOYEB4: apt-get update %s, install openssh-server %s", update.Round(time.Second),
		time.Since(start).Round(time.Second))
}

// rowKoyeb6 holds a session open past one minute without PAM
func rowKoyeb6(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	a := s.sshKey(t, h)
	eq(t, strings.TrimSpace(s.sshMust(t, a, "sleep 75; echo alive")), "alive")
}

// rowBoat6 expects our sshd on 2022 and the gate and sshd back after a reboot
func rowBoat6(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	a := s.sshKey(t, h)
	s.sshMust(t, a, "true")
	listeners := s.sh(t, h, asRoot+`$S ss -ltn`)
	if !strings.Contains(listeners, "127.0.0.1:2022") {
		t.Fatalf("live: no sshd on 127.0.0.1:2022 in\n%s", listeners)
	}
	_, _, _ = s.devTry(t, "exec", "--handle", h, "--", "sh", "-c", asRoot+`$S reboot`)
	var err error
	for range 36 {
		time.Sleep(5 * time.Second)
		if _, _, err = s.sshRun(t, a, "true"); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("live: no SSH session after a reboot: %v", err)
	}
}

func TestSSHRowsAndTheirPackages(t *testing.T) {
	got := map[string][]string{}
	for _, r := range dataplaneRows {
		got[r.id] = r.providers
	}
	for _, id := range []string{"S1", "S2", "S3", "S4", "S5", "S6", "S7", "B1", "B2"} {
		if _, ok := got[id]; !ok {
			t.Fatalf("no row %s", id)
		}
	}
	eq(t, got["E2B3"], []string{"e2b"})
	eq(t, got["BOXD4"], []string{"boxd"})
	eq(t, got["KOYEB4"], []string{"koyeb"})
	eq(t, got["KOYEB6"], []string{"koyeb"})
	eq(t, got["BOAT6"], []string{"boat"})
	c, err := catalog.Load(filepath.Join(repo(t), "config", "dev-packages.json"))
	if err != nil {
		t.Fatal(err)
	}
	long, ok := c.Get(sshLongPackage)
	eq(t, ok, true)
	eq(t, long.Minutes, 75)
	edge, ok := c.Get(edgePackage)
	eq(t, ok, true)
	eq(t, edge.Minutes, 10)
	eq(t, edge.Edge.Bytes, int64(64<<20))
	more, ok := c.Get(edgeExtension)
	eq(t, ok, true)
	eq(t, more.ExtensionOf, edgePackage)
}
