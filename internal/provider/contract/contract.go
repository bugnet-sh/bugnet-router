// Package contract checks the behaviour every provider adapter must show
package contract

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
)

const (
	labelKey     = "bugnet-contract"
	readyTimeout = 3 * time.Minute
	opTimeout    = 2 * time.Minute
	pollInterval = 2 * time.Second
	lifetime     = 15 * time.Minute
)

// Run walks one sandbox through its whole lifecycle and destroys everything it created
func Run(t *testing.T, newProvider func(t *testing.T) provider.Provider) {
	p := newProvider(t)
	labels := map[string]string{labelKey: randomHex(t, 8)}
	t.Cleanup(func() { sweep(t, p, labels) })

	size, ok := p.SizeFor(provider.Resources{})
	if !ok {
		t.Fatalf("%s offers no size", p.Name())
	}
	req := provider.CreateRequest{
		IdempotencyKey: randomHex(t, 16),
		Size:           size,
		Labels:         labels,
		Lifetime:       lifetime,
	}
	if limit := p.Capabilities().MaxLifetime; limit > 0 && limit < req.Lifetime {
		req.Lifetime = limit
	}

	ref := create(t, p, req)
	if ref.Provider != p.Name() {
		t.Fatalf("ref names provider %q, want %q", ref.Provider, p.Name())
	}
	again := create(t, p, req)
	if again.Provider != ref.Provider || again.ID != ref.ID {
		t.Fatalf("same idempotency key gave %+v and %+v", ref, again)
	}

	c := check{p: p, ref: ref, labels: labels}
	steps := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"ready", c.ready},
		{"exec", c.exec},
		{"files", c.files},
		{"list owned", c.listOwned},
		{"destroy twice", c.destroyTwice},
		{"gone from list owned", c.gone},
		{"destroyed sandbox is not found", c.notFound},
	}
	for _, s := range steps {
		if !t.Run(s.name, s.run) {
			return
		}
	}
}

type check struct {
	p      provider.Provider
	ref    provider.Ref
	labels map[string]string
}

func (c check) ready(t *testing.T) {
	if err := c.p.WaitReady(withTimeout(t, readyTimeout), c.ref); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func (c check) exec(t *testing.T) {
	spec := provider.ExecSpec{Argv: []string{"sh", "-c", "echo hi; exit 5"}}
	out, code, err := run(withTimeout(t, opTimeout), c.p, c.ref, spec)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if string(out) != "hi\n" || code != 5 {
		t.Fatalf("exec gave stdout %q and exit %d, want \"hi\\n\" and 5", out, code)
	}
}

func (c check) files(t *testing.T) {
	ctx := withTimeout(t, opTimeout)
	dir := "/tmp/bugnet-contract-" + c.labels[labelKey]
	path := dir + "/data.txt"
	data := []byte("hello, contract\n")
	if err := c.p.WriteFile(ctx, c.ref, path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	reads := []struct {
		offset int64
		length int
		want   string
		eof    bool
	}{
		{0, 1024, string(data), true},
		{7, 8, "contract", false},
		{7, 1024, "contract\n", true},
	}
	for _, r := range reads {
		got, eof, err := c.p.ReadFile(ctx, c.ref, path, r.offset, r.length)
		if err != nil {
			t.Fatalf("ReadFile at %d for %d: %v", r.offset, r.length, err)
		}
		if string(got) != r.want || eof != r.eof {
			t.Errorf("ReadFile at %d for %d gave %q eof %v, want %q eof %v",
				r.offset, r.length, got, eof, r.want, r.eof)
		}
	}

	script := []byte("#!/bin/sh\necho ran\n")
	if err := c.p.WriteFile(ctx, c.ref, dir+"/run.sh", script, 0o755); err != nil {
		t.Fatalf("WriteFile of a script: %v", err)
	}
	spec := provider.ExecSpec{Argv: []string{"sh", "-c", "./run.sh"}, Cwd: dir}
	out, code, err := run(ctx, c.p, c.ref, spec)
	if err != nil || string(out) != "ran\n" || code != 0 {
		t.Errorf("script written with mode 0755 gave %q, exit %d, %v", out, code, err)
	}

	entries, err := c.p.ListFiles(ctx, c.ref, dir, 1)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	for _, e := range entries {
		if e.Path == path {
			if e.IsDir || e.Size != int64(len(data)) {
				t.Errorf("ListFiles entry %+v, want a file of %d bytes", e, len(data))
			}
			return
		}
	}
	t.Errorf("ListFiles of %s lacks %s: %+v", dir, path, entries)
}

func (c check) listOwned(t *testing.T) {
	owned, err := c.p.ListOwned(withTimeout(t, opTimeout), c.labels)
	if err != nil {
		t.Fatalf("ListOwned: %v", err)
	}
	for _, s := range owned {
		if s.Ref.ID != c.ref.ID {
			continue
		}
		if s.Labels[labelKey] != c.labels[labelKey] || s.CreatedAt.IsZero() {
			t.Errorf("ListOwned summary %+v lacks the label or creation time", s)
		}
		return
	}
	t.Fatalf("ListOwned by %v lacks %s: %+v", c.labels, c.ref.ID, owned)
}

func (c check) destroyTwice(t *testing.T) {
	ctx := withTimeout(t, opTimeout)
	for i := range 2 {
		if err := c.p.Destroy(ctx, c.ref); err != nil {
			t.Fatalf("Destroy %d: %v", i+1, err)
		}
	}
}

func (c check) gone(t *testing.T) {
	ctx := withTimeout(t, opTimeout)
	for {
		owned, err := c.p.ListOwned(ctx, c.labels)
		if err != nil {
			t.Fatalf("ListOwned: %v", err)
		}
		if !hasID(owned, c.ref.ID) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ListOwned still lists %s after Destroy", c.ref.ID)
		case <-time.After(pollInterval):
		}
	}
}

func (c check) notFound(t *testing.T) {
	ctx := withTimeout(t, readyTimeout)
	path := "/tmp/bugnet-contract-gone.txt"

	requireNotFound(t, "WaitReady", c.p.WaitReady(ctx, c.ref))
	requireNotFound(t, "SetLifetime", c.p.SetLifetime(ctx, c.ref, time.Now().Add(time.Hour)))
	_, _, err := run(ctx, c.p, c.ref, provider.ExecSpec{Argv: []string{"sh", "-c", "true"}})
	requireNotFound(t, "Exec", err)
	_, _, err = c.p.ReadFile(ctx, c.ref, path, 0, 16)
	requireNotFound(t, "ReadFile", err)
	requireNotFound(t, "WriteFile", c.p.WriteFile(ctx, c.ref, path, []byte("x"), 0o644))
	_, err = c.p.ListFiles(ctx, c.ref, "/tmp", 1)
	requireNotFound(t, "ListFiles", err)
}

/* Helpers */

func create(t *testing.T, p provider.Provider, req provider.CreateRequest) provider.Ref {
	t.Helper()
	ref, err := p.Create(withTimeout(t, opTimeout), req)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		defer cancel()
		if err := p.Destroy(ctx, ref); err != nil {
			t.Errorf("destroy %s on cleanup: %v", ref.ID, err)
		}
	})
	return ref
}

// sweep destroys any sandbox carrying the run's label, including ones whose Create reply was lost
func sweep(t *testing.T, p provider.Provider, labels map[string]string) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	owned, err := p.ListOwned(ctx, labels)
	if err != nil {
		t.Errorf("list sandboxes left by the contract run: %v", err)
		return
	}
	for _, s := range owned {
		if err := p.Destroy(ctx, s.Ref); err != nil {
			t.Errorf("destroy leftover sandbox %s: %v", s.Ref.ID, err)
		}
	}
}

// run treats a failure reported by Wait like one reported by Exec, since remote streams fail late
func run(
	ctx context.Context, p provider.Provider, ref provider.Ref, spec provider.ExecSpec,
) ([]byte, int, error) {
	stream, err := p.Exec(ctx, ref, spec)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = stream.Close() }()
	var out bytes.Buffer
	if _, err := io.Copy(&out, stream); err != nil {
		return out.Bytes(), 0, err
	}
	code, err := stream.Wait()
	return out.Bytes(), code, err
}

func requireNotFound(t *testing.T, op string, err error) {
	t.Helper()
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeNotFound {
		t.Errorf("%s on a destroyed sandbox gave %v, want an *Error with code not_found", op, err)
	}
}

func hasID(owned []provider.Summary, id string) bool {
	for _, s := range owned {
		if s.Ref.ID == id {
			return true
		}
	}
	return false
}

func withTimeout(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return ctx
}

func randomHex(t *testing.T, n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random: %v", err)
	}
	return hex.EncodeToString(b)
}
