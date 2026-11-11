package lium

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys"
)

func TestLiumPodSSHKey(t *testing.T) {
	p, err := newProvider(testOptions(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	name := podName(testKey, placementLabels())
	mac := hmac.New(sha256.New, testSeed)
	mac.Write([]byte("bugnet/lium-ssh/v1\x00" + name))
	want := ed25519.NewKeyFromSeed(mac.Sum(nil))
	if !p.podPrivateKey(name).Equal(want) {
		t.Fatal("the pod key is not derived from the seed and the pod name")
	}
	line := p.podPublicKey(name)
	if !strings.HasPrefix(line, "ssh-ed25519 ") || !strings.HasSuffix(line, " bugnet") ||
		strings.ContainsAny(line, "\r\n") {
		t.Fatalf("authorized_keys line %q", line)
	}
	pub, comment, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	wantPub, err := ssh.NewPublicKey(want.Public())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pub.Marshal(), wantPub.Marshal()) || comment != "bugnet" {
		t.Fatalf("authorized_keys line %q names another key", line)
	}
	eq(t, p.podPublicKey(name), line)

	// Another pod and another seed each give another key
	if p.podPublicKey(podName(freshKey(), placementLabels())) == line {
		t.Fatal("two pod names gave one key")
	}
	opts := testOptions()
	opts.SSHSeed = bytes.Repeat([]byte{9}, 32)
	q, err := newProvider(opts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if q.podPublicKey(name) == line {
		t.Fatal("two seeds gave one key")
	}
}

// poolOf is the pod's pool, nil when the process holds none
func (p *Provider) poolOf(id string) *sshPool {
	p.smu.Lock()
	defer p.smu.Unlock()
	return p.pools[id]
}

// connCount is how many connections the pool holds
func (pl *sshPool) connCount() int {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	return len(pl.conns)
}

// within fails the test unless cond holds before the deadline
func within(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v", what, d)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLiumHostKeyPin(t *testing.T) {
	f, p := liveFake(t)
	ref, pod := readyRef(t, f, p)
	sshd := f.sshd(pod.id)
	eq(t, ref.Meta[provider.MetaHostKey], sshd.hostPublicKey())
	if _, err := sshkeys.HostKey(ref.Meta[provider.MetaHostKey]); err != nil {
		t.Fatalf("the pin is not a host key line CheckBoot reads: %v", err)
	}
	// The connection that pinned the key serves the first command
	if _, _, err := run(t, p, ref, sh("true")); err != nil {
		t.Fatal(err)
	}
	accepted, _, _ := sshd.stats()
	eq(t, accepted, 1)

	// Another router process checks the server against the pin the ref carries
	other := f.provider(t, testOptions(), time.Now)
	if out, _, err := run(t, other, ref, sh("echo other")); err != nil || out != "other\n" {
		t.Fatalf("another process gave %q, %v", out, err)
	}

	// A new host key fails every later connection, from either process, before any login
	sshd.rekey(t)
	sshd.dropConns()
	ran := len(f.execLines())
	for _, q := range []*Provider{p, other} {
		_, _, err := run(t, q, ref, sh("echo never"))
		wantCode(t, err, provider.CodeInternal, false)
		if !strings.Contains(err.Error(), "another host key") {
			t.Fatalf("error %v names no host key", err)
		}
	}
	eq(t, len(f.execLines()), ran)
	// ReadyMeta of a pinned ref checks the pin and never takes the new key
	if _, err := p.ReadyMeta(context.Background(), ref); err == nil {
		t.Fatal("ReadyMeta took a changed host key")
	}

	// A ref with no pin, in a process that pinned none, never logs in
	bare := provider.Ref{Provider: ref.Provider, ID: ref.ID, Meta: maps.Clone(ref.Meta)}
	delete(bare.Meta, provider.MetaHostKey)
	third := f.provider(t, testOptions(), time.Now)
	dials := f.dials()
	_, _, err := run(t, third, bare, sh("true"))
	wantCode(t, err, provider.CodeInternal, false)
	for _, line := range []string{"not a key", "ssh-ed25519 AAAA", strings.Repeat("x", 9000)} {
		bad := provider.Ref{Provider: ref.Provider, ID: ref.ID, Meta: maps.Clone(ref.Meta)}
		bad.Meta[provider.MetaHostKey] = line
		_, _, err := run(t, third, bad, sh("true"))
		wantCode(t, err, provider.CodeInternal, false)
	}
	eq(t, f.dials(), dials)
}

func TestLiumSSHFailures(t *testing.T) {
	t.Run("our key refused after ready", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		f.setPod(pod.id, func(fp *fakePod) { fp.keys = nil })
		f.sshd(pod.id).dropConns()
		_, _, err := run(t, p, ref, sh("true"))
		wantCode(t, err, provider.CodeInternal, false)
		if !strings.Contains(err.Error(), "lium_ssh_refused") {
			t.Fatalf("error %v lacks the reason", err)
		}
	})
	t.Run("sshd down on a running pod", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		f.mu.Lock()
		f.sshDown = true
		f.mu.Unlock()
		f.sshd(pod.id).dropConns()
		_, _, err := run(t, p, ref, sh("true"))
		wantCode(t, err, provider.CodeInternal, false)
	})
	t.Run("a pod Lium deleted", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		other := f.provider(t, testOptions(), time.Now)
		if err := other.Destroy(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
		_, _, err := run(t, p, ref, sh("true"))
		wantCode(t, err, provider.CodeNotFound, false)
		_ = pod
	})
	t.Run("a pod being deleted", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		f.setPod(pod.id, func(fp *fakePod) { fp.status = "DELETING" })
		f.mu.Lock()
		f.sshDown = true
		f.mu.Unlock()
		f.sshd(pod.id).dropConns()
		_, _, err := run(t, p, ref, sh("true"))
		wantCode(t, err, provider.CodeNotFound, false)
	})
	t.Run("destroyed here", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		if _, _, err := run(t, p, ref, sh("true")); err != nil {
			t.Fatal(err)
		}
		pl := p.poolOf(pod.id)
		if err := p.Destroy(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
		// Destroy closes the pod's connections and forgets its pool
		if p.poolOf(pod.id) != nil || pl.connCount() != 0 || !pl.closed {
			t.Fatal("Destroy kept the pod's connections")
		}
		_, _, err := run(t, p, ref, sh("true"))
		wantCode(t, err, provider.CodeNotFound, false)
		// A ref without the SSH entries reads the pod and finds it gone
		bare := provider.Ref{Provider: "lium", ID: ref.ID}
		_, _, err = run(t, p, bare, sh("true"))
		wantCode(t, err, provider.CodeNotFound, false)
	})
	t.Run("an address netguard refuses", func(t *testing.T) {
		f, p := liveFake(t)
		ref, _ := readyRef(t, f, p)
		bad := provider.Ref{Provider: ref.Provider, ID: ref.ID, Meta: maps.Clone(ref.Meta)}
		bad.Meta["ssh_host"] = "10.0.0.5"
		dials := f.dials()
		_, _, err := run(t, p, bad, sh("true"))
		wantCode(t, err, provider.CodeInternal, false)
		eq(t, f.dials(), dials)
	})
	t.Run("a ref of another provider", func(t *testing.T) {
		_, p := liveFake(t)
		_, _, err := run(t, p, provider.Ref{Provider: "hf", ID: "x"}, sh("true"))
		wantCode(t, err, provider.CodeNotFound, false)
	})
}

func TestLiumSSHFromOlderRef(t *testing.T) {
	f, p := liveFake(t)
	ref, pod := readyRef(t, f, p)
	// A row from before ready meta keeps only the pin, so the pod is read once for the rest
	other := f.provider(t, testOptions(), time.Now)
	older := provider.Ref{Provider: "lium", ID: ref.ID, Meta: map[string]string{
		provider.MetaHostKey: ref.Meta[provider.MetaHostKey],
	}}
	reads := len(f.requestsTo("GET", "/pods/"+pod.id))
	for range 2 {
		if out, _, err := run(t, other, older, sh("echo ok")); err != nil || out != "ok\n" {
			t.Fatalf("gave %q, %v", out, err)
		}
	}
	eq(t, len(f.requestsTo("GET", "/pods/"+pod.id)), reads+1)
}

func TestLiumReadyMetaWaitsForSSH(t *testing.T) {
	t.Run("sshd comes up late", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := rentedPod(t, f, p)
		f.setPod(pod.id, func(fp *fakePod) { fp.status = "RUNNING" })
		f.mu.Lock()
		f.sshDown = true
		f.mu.Unlock()
		go func() {
			time.Sleep(50 * time.Millisecond)
			f.mu.Lock()
			f.sshDown = false
			f.mu.Unlock()
		}()
		meta, err := p.ReadyMeta(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, meta[provider.MetaHostKey], f.sshd(pod.id).hostPublicKey())
		if f.dials() < 2 {
			t.Fatalf("%d dials", f.dials())
		}
	})
	t.Run("our key never let in", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := rentedPod(t, f, p)
		f.setPod(pod.id, func(fp *fakePod) { fp.status, fp.keys = "RUNNING", nil })
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := p.ReadyMeta(ctx, ref)
		if err == nil || !strings.Contains(err.Error(), "lium_ssh_refused") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("the pod fails meanwhile", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := rentedPod(t, f, p)
		f.setPod(pod.id, func(fp *fakePod) { fp.status = "RUNNING" })
		f.mu.Lock()
		f.sshDown = true
		f.mu.Unlock()
		go func() {
			time.Sleep(30 * time.Millisecond)
			f.setPod(pod.id, func(fp *fakePod) { fp.status = "BROKEN" })
		}()
		_, err := p.ReadyMeta(context.Background(), ref)
		wantCode(t, err, provider.CodeCapacity, true)
	})
}

// blocker is a command that runs until the file at gate exists
func blocker(gate string) provider.ExecSpec {
	return sh("while [ ! -e " + gate + " ]; do sleep 0.02; done; echo out")
}

func TestLiumSSHPool(t *testing.T) {
	t.Run("eight sessions to a connection, four connections", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		gate := filepath.Join(t.TempDir(), "go")
		const calls = 40
		results := make(chan error, calls)
		for range calls {
			go func() {
				out, code, err := run(t, p, ref, blocker(gate))
				if err == nil && (out != "out\n" || code != 0) {
					err = fmt.Errorf("gave %q, exit %d", out, code)
				}
				results <- err
			}()
		}
		sshd := f.sshd(pod.id)
		running := func() bool {
			n := 0
			for _, line := range f.execLines() {
				if strings.Contains(line, gate) {
					n++
				}
			}
			return n == 32
		}
		within(t, 10*time.Second, "32 sessions", running)
		time.Sleep(100 * time.Millisecond)
		// The pool holds 32 sessions and the rest wait for a free one
		accepted, open, peak := sshd.stats()
		if !running() || accepted != 4 || open != 4 || peak != 8 {
			t.Fatalf("accepted %d, open %d, peak %d", accepted, open, peak)
		}
		if err := os.WriteFile(gate, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		for range calls {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		accepted, _, peak = sshd.stats()
		eq(t, accepted, 4)
		eq(t, peak, 8)
	})
	t.Run("a connection opens only when the others are full", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		for range 5 {
			if _, _, err := run(t, p, ref, sh("true")); err != nil {
				t.Fatal(err)
			}
		}
		accepted, _, _ := f.sshd(pod.id).stats()
		eq(t, accepted, 1)
	})
	t.Run("a server with fewer sessions", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		sshd := f.sshd(pod.id)
		sshd.setMaxSessions(3)
		gate := filepath.Join(t.TempDir(), "go")
		results := make(chan error, 6)
		for range 6 {
			go func() {
				_, _, err := run(t, p, ref, blocker(gate))
				results <- err
			}()
		}
		within(t, 10*time.Second, "6 sessions", func() bool { return len(f.execLines()) == 6 })
		_ = os.WriteFile(gate, nil, 0o644)
		for range 6 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		accepted, _, peak := sshd.stats()
		eq(t, peak, 3)
		eq(t, accepted, 2)
	})
	t.Run("an idle connection closes", func(t *testing.T) {
		f, p := liveFake(t)
		p.ssh.idle = 50 * time.Millisecond
		ref, pod := readyRef(t, f, p)
		if _, _, err := run(t, p, ref, sh("true")); err != nil {
			t.Fatal(err)
		}
		sshd := f.sshd(pod.id)
		within(t, 5*time.Second, "the idle close", func() bool {
			_, open, _ := sshd.stats()
			return open == 0 && p.poolOf(pod.id).connCount() == 0
		})
		if _, _, err := run(t, p, ref, sh("true")); err != nil {
			t.Fatal(err)
		}
		accepted, _, _ := sshd.stats()
		eq(t, accepted, 2)
	})
	t.Run("a connection that misses a keepalive closes", func(t *testing.T) {
		f, p := liveFake(t)
		p.ssh.keepalive = 30 * time.Millisecond
		ref, pod := readyRef(t, f, p)
		sshd := f.sshd(pod.id)
		time.Sleep(100 * time.Millisecond)
		eq(t, p.poolOf(pod.id).connCount(), 1)
		sshd.setSilent(true)
		within(t, 5*time.Second, "the keepalive close", func() bool {
			return p.poolOf(pod.id).connCount() == 0
		})
		sshd.setSilent(false)
		if _, _, err := run(t, p, ref, sh("true")); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a broken connection is redialled once", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		pl := p.poolOf(pod.id)
		pl.mu.Lock()
		client := pl.conns[0].client
		pl.mu.Unlock()
		_ = client.Close()
		dials := f.dials()
		if out, _, err := run(t, p, ref, sh("echo again")); err != nil || out != "again\n" {
			t.Fatalf("gave %q, %v", out, err)
		}
		eq(t, f.dials(), dials+1)

		f.sshd(pod.id).dropConns()
		f.mu.Lock()
		f.sshDown = true
		f.mu.Unlock()
		within(t, 5*time.Second, "the drop", func() bool { return pl.connCount() == 0 })
		dials = f.dials()
		_, _, err := run(t, p, ref, sh("true"))
		wantCode(t, err, provider.CodeInternal, false)
		eq(t, f.dials(), dials+1)
	})
}
