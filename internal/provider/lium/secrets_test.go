package lium

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// secretWords are the forms a secret could take in a request, raw, base64 and hex
func secretWords(secret []byte) []string {
	return []string{
		string(secret), base64.StdEncoding.EncodeToString(secret),
		base64.RawStdEncoding.EncodeToString(secret), base64.URLEncoding.EncodeToString(secret),
		base64.RawURLEncoding.EncodeToString(secret), hex.EncodeToString(secret),
		strings.ToUpper(hex.EncodeToString(secret)),
	}
}

func TestLiumSendsNoSecret(t *testing.T) {
	f, p := liveFake(t)
	ctx := context.Background()
	var refs []provider.Ref
	keyed := mustCreate(t, p, createReq(testKey))
	refs = append(refs, keyed)
	relist(t, p)
	f.inject(rentRoute, fault{drop: true, times: 1})
	check := createReq("")
	check.Labels = map[string]string{"bugnet_image_check": "1"}
	refs = append(refs, mustCreate(t, p, check))
	for i, ref := range refs {
		f.setPod(ref.ID, func(fp *fakePod) { fp.status = "RUNNING" })
		meta, err := p.ReadyMeta(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		ref.Meta = maps.Clone(ref.Meta)
		maps.Copy(ref.Meta, meta)
		refs[i] = ref
		if _, err := p.Inspect(ctx, ref); err != nil {
			t.Fatal(err)
		}
		// A command and a cancelled one, whose group the adapter kills on a second session
		if out, _, err := run(t, p, ref, provider.ExecSpec{
			Argv: []string{"sh", "-c", "echo hi"}, Cwd: "/", Env: map[string]string{"A": "1"},
		}); err != nil || out != "hi\n" {
			t.Fatalf("gave %q, %v", out, err)
		}
		cctx, cancel := context.WithCancel(ctx)
		s, pgid := startedGroup(cctx, t, f, p, ref)
		cancel()
		_, _ = s.Wait()
		waitGroupGone(t, f, pgid)
		if err := p.SetLifetime(ctx, ref, time.Now().Add(20*time.Minute)); err != nil {
			t.Fatal(err)
		}
		sftpCalls(t, p, ref)
	}
	if _, err := p.ListOwned(ctx, placementLabels()); err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		if err := p.Destroy(ctx, ref); err != nil {
			t.Fatal(err)
		}
	}

	needles := append(secretWords([]byte(fakeKey)), secretWords(testSeed)...)
	for _, ref := range refs {
		priv := p.podPrivateKey(ref.Meta["pod_name"])
		needles = append(needles, secretWords(priv.Seed())...)
		needles = append(needles, secretWords(priv)...)
	}
	needles = append(needles, "PRIVATE KEY", "bnsec_", "BUGNET_", "LIUM_")
	// A gate key of the placement under a data plane root, which no Lium pod ever gets
	env := dpkeys.NewEnv()
	keys, err := dpkeys.Parse(env)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := keys.GateKey(keys.Current().ID,
		uuid.MustParse(placementLabels()["placement"]))
	if err != nil {
		t.Fatal(err)
	}
	needles = append(needles, gate, env)

	f.mu.Lock()
	requests := append([]recorded(nil), f.requests...)
	f.mu.Unlock()
	if len(requests) < 15 {
		t.Fatalf("only %d requests recorded", len(requests))
	}
	for _, r := range requests {
		var sent bytes.Buffer
		sent.WriteString(r.method + " " + r.path + "?" + r.query.Encode() + "\n")
		sent.Write(r.body)
		for name, values := range r.header {
			if name == "X-Api-Key" {
				continue
			}
			sent.WriteString("\n" + name + ": " + strings.Join(values, ", "))
		}
		for _, n := range needles {
			if n != "" && bytes.Contains(sent.Bytes(), []byte(n)) {
				t.Errorf("%s %s carries a secret: %q", r.method, r.path, n)
			}
		}
		for _, h := range []string{"Authorization", "Cookie"} {
			if r.header.Get(h) != "" {
				t.Errorf("%s %s sent %s", r.method, r.path, h)
			}
		}
		if r.path == "/executors" && r.header.Get("X-Api-Key") != "" {
			t.Errorf("the keyless listing sent the key")
		}
	}

	// Every command line a pod ran, the kills among them
	lines := f.execLines()
	kills := 0
	for _, line := range lines {
		if strings.Contains(line, "kill -KILL") {
			kills++
		}
		for _, n := range needles {
			if n != "" && strings.Contains(line, n) {
				t.Errorf("exec line %q carries a secret: %q", line, n)
			}
		}
	}
	if len(lines) < 6 || kills < 2 {
		t.Fatalf("only %d exec lines, %d kills", len(lines), kills)
	}

	// Every byte sent over SFTP and every file written that way, reassembled
	sessions, files := f.sftpSent()
	written := 0
	for _, file := range files {
		if bytes.Equal(file, []byte(sftpFileData)) || bytes.Equal(file, []byte(sftpStreamData)) {
			written++
		}
	}
	if len(sessions) < 10 || written < 2*len(refs) {
		t.Fatalf("only %d SFTP sessions and %d of the files written recorded", len(sessions),
			written)
	}
	for _, sent := range append(sessions, files...) {
		for _, n := range needles {
			if n != "" && bytes.Contains(sent, []byte(n)) {
				t.Errorf("an SFTP session carries a secret: %q", n)
			}
		}
	}
}

// What sftpCalls writes, which the fake must find among the files it recorded
const (
	sftpFileData   = "a file written whole"
	sftpStreamData = "a file written as a stream"
)

// sftpCalls writes, reads and lists files on the pod over SFTP
func sftpCalls(t *testing.T, p *Provider, ref provider.Ref) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	if err := p.WriteFile(ctx, ref, dir+"/a/f", []byte(sftpFileData), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := p.OpenWriter(ctx, ref, dir+"/g",
		provider.WriteSpec{Size: int64(len(sftpStreamData)), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(sftpStreamData)); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(sftpStreamData))
	if _, err := w.Commit(ctx, sum[:]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.ReadFile(ctx, ref, dir+"/a/f", 0, 64); err != nil {
		t.Fatal(err)
	}
	if err := p.ListFiles(ctx, ref, dir, 2, provider.NewListing(10)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.StatFile(ctx, ref, dir+"/g"); err != nil {
		t.Fatal(err)
	}
}
