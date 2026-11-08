package live

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/leakscan"
)

// leakRows are CP 11's rows, K1 and K2 of CP 15
var leakRows []row

// TestLiveLeak scans fresh sandboxes for every secret of the router
func TestLiveLeak(t *testing.T) {
	runRows(t, leakRows)
}

func init() {
	leakRows = append(leakRows,
		row{id: "K1", run: rowK1},
		row{id: "K2", providers: []string{"e2b"}, run: rowK2},
	)
}

const (
	// leakSecretName is the variable the leak secret's placeholder lands in
	leakSecretName = "LEAK_TOKEN"
	// leakSecretHost is the host the leak secret is bound to
	leakSecretHost = "example.com"
)

// leakBox is one sandbox of a leak run
type leakBox struct {
	handle      string
	archive     []byte
	res         leakscan.Result
	value       string
	placeholder string
}

// leakNeedles are the secrets of the stack's environment and the proxy's private keys
func (s *stack) leakNeedles(t *testing.T) leakscan.Needles {
	t.Helper()
	n, err := leakscan.FromEnv(s.env)
	if err != nil {
		t.Fatalf("live: the needles: %v", err)
	}
	entries, err := os.ReadDir(s.egress.credDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.Contains(name, "key") || strings.HasSuffix(name, ".pub") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.egress.credDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if v := bytes.TrimSpace(data); len(v) >= 8 {
			n = append(n, leakscan.Needle{Name: "credential " + name, Value: v})
		}
	}
	if len(n) == 0 {
		t.Fatal("live: the stack has no secret to search for")
	}
	return n
}

// leakCollect runs the collection script as root and brings its archive back
func (s *stack) leakCollect(t *testing.T, h string) []byte {
	t.Helper()
	script := base64.StdEncoding.EncodeToString([]byte(leakscan.Collect))
	s.sh(t, h, asRoot+`echo `+script+` | base64 -d | $S sh`)
	out := s.download(t, h, leakscan.ArchivePath, filepath.Join(t.TempDir(), "leak.tar"))
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// leakSecret puts a bnleak_ secret on the sandbox and keeps its value and placeholder
func (s *stack) leakSecret(t *testing.T, b *leakBox) {
	t.Helper()
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	b.value = "bnleak_" + hex.EncodeToString(raw)
	b.placeholder = s.putSecret(t, b.handle, leakSecretName, b.value, leakSecretHost)
}

// leakVerify scans both archives, then each with the other's own values, and fails on a finding
//
// extra lists, for each box, values of the other box that must not show in it.
func (s *stack) leakVerify(t *testing.T, boxes []*leakBox, base leakscan.Needles,
	allow leakscan.Allowlist, extra func(other *leakBox) leakscan.Needles) {
	t.Helper()
	for _, b := range boxes {
		res, err := leakscan.ScanFull(bytes.NewReader(b.archive), base, allow)
		if err != nil {
			t.Fatalf("live: scan of %s: %v", s.provider, err)
		}
		b.res = res
		hasGate := func(n leakscan.Needle) bool { return n.Name == "gate key" }
		if !slices.ContainsFunc(res.Own, hasGate) {
			t.Fatalf("live: the archive of %s holds no %s, so the collection missed /etc/bugnet",
				s.provider, leakscan.GateKeyPath)
		}
	}
	for i, a := range boxes {
		other := boxes[1-i]
		for _, o := range a.res.Own {
			for _, p := range other.res.Own {
				if o.Name == p.Name && bytes.Equal(o.Value, p.Value) {
					t.Errorf("both sandboxes hold the same %s, so it is not their own", o.Name)
				}
			}
		}
		n := append(slices.Clone(base), other.res.Own...)
		n = append(n, extra(other)...)
		findings, err := leakscan.Scan(bytes.NewReader(a.archive), n, allow)
		if err != nil {
			t.Fatalf("live: scan of %s: %v", s.provider, err)
		}
		for _, f := range findings {
			t.Errorf("sandbox %d on %s: %s", i+1, s.provider, f)
		}
		if len(a.res.Names) > 0 {
			t.Logf("sandbox %d on %s: variables named like secrets, for review: %v", i+1,
				s.provider, a.res.Names)
		}
	}
}

// rowK1 scans two fresh sandboxes, then again with a bnleak_ secret on each, as CP 11 says
func rowK1(t *testing.T, s *stack) {
	needles := s.leakNeedles(t)
	boxes := []*leakBox{{handle: s.sandbox(t, xferPackage)}, {handle: s.sandbox(t, xferPackage)}}
	for _, b := range boxes {
		b.archive = s.leakCollect(t, b.handle)
	}
	none := func(*leakBox) leakscan.Needles { return nil }
	s.leakVerify(t, boxes, needles, leakscan.For(s.provider, false), none)
	if t.Failed() {
		return
	}
	for _, b := range boxes {
		s.leakSecret(t, b)
	}
	for _, b := range boxes {
		b.archive = s.leakCollect(t, b.handle)
	}
	withValues := slices.Clone(needles)
	for i, b := range boxes {
		withValues = append(withValues, leakscan.Needle{
			Name: "leak secret " + string(rune('A'+i)), Value: []byte(b.value)})
	}
	s.leakVerify(t, boxes, withValues, leakscan.For(s.provider, true),
		func(other *leakBox) leakscan.Needles {
			return leakscan.Needles{{Name: "other placeholder", Value: []byte(other.placeholder)}}
		})
	for i, b := range boxes {
		s.leakPlaces(t, i+1, b)
	}
}

// leakPlaces checks where the placeholder sits and where nothing of ours may sit
func (s *stack) leakPlaces(t *testing.T, n int, b *leakBox) {
	t.Helper()
	h := b.handle
	a := s.sshKey(t, h)
	holds := []leakscan.Place{
		{Name: "/etc/bugnet/secrets.env", Content: []byte(s.sh(t, h,
			asRoot+`$S cat /etc/bugnet/secrets.env`))},
		{Name: "/etc/profile.d/bugnet.sh", Content: []byte(s.sh(t, h,
			`cat /etc/profile.d/bugnet.sh`))},
		{Name: "a new job's environment", Content: []byte(s.sh(t, h, `env`))},
		{Name: "a new SSH session's environment", Content: []byte(s.sshMust(t, a, "env"))},
	}
	clean := []leakscan.Place{{Name: "/etc/environment", Content: []byte(s.sh(t, h,
		`cat /etc/environment 2> /dev/null; true`))}}
	for _, bad := range leakscan.CheckPlaces(holds, clean, b.placeholder, b.value) {
		t.Errorf("sandbox %d on %s: %s", n, s.provider, bad)
	}
}

// rowK2 lists what E2B puts inside a sandbox and fails only on a secret of ours
func rowK2(t *testing.T, s *stack) {
	needles := s.leakNeedles(t)
	h := s.sandbox(t, xferPackage)
	archive := s.leakCollect(t, h)
	res, err := leakscan.ScanFull(bytes.NewReader(archive), needles, leakscan.Base(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("K2: every variable in a process environment: %v", res.Vars)
	t.Logf("K2: variables named like secrets: %v", res.Names)
	for _, f := range res.Findings {
		if strings.HasPrefix(f.Rule, "shape ") {
			t.Logf("K2: %s", f)
			continue
		}
		t.Errorf("K2: %s", f)
	}
}

func TestLeakRowsAndTheirProviders(t *testing.T) {
	var ids []string
	for _, r := range leakRows {
		ids = append(ids, r.id)
		if r.id == "K2" {
			eq(t, r.providers, []string{"e2b"})
		}
	}
	eq(t, ids, []string{"K1", "K2"})
}
