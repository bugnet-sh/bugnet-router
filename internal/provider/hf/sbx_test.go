package hf

import (
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/contract"
)

// sbxRequests are the calls the fake's sbx-servers got
func sbxRequests(f *fakeHF) []recorded { return f.requestsTo("GET", "/health") }

func TestHFSandboxClientRefusesOtherHosts(t *testing.T) {
	if err := checkSBXURL("https://6911abc--49983.hf.jobs/health"); err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{
		"http://6911abc--49983.hf.jobs/health",
		"https://huggingface.co/health",
		"https://huggingface.co/api/jobs/" + testNamespace,
		"https://evil.example/health",
		"https://hf.jobs/health",
		"https://.hf.jobs/health",
		"https://a.b.hf.jobs/health",
		"https://6911abc--49983.hf.jobs.evil.example/health",
		"https://6911abc--49983.hf.jobsx/health",
		"https://6911abc--49983.hf.jobs:8443/health",
		"https://user@6911abc--49983.hf.jobs/health",
		"https://6911abc--49983.HF.JOBS/health",
		"https://6911abc--49983.hf.jobs/v1/../health",
		"//6911abc--49983.hf.jobs/health",
		"https:6911abc--49983.hf.jobs/health",
		"",
	} {
		if err := checkSBXURL(u); err == nil {
			t.Errorf("the sandbox client takes %q", u)
		}
	}
	f := newFake(t)
	p := f.provider(t, testOptions())
	// A Job id is one plain part of a host name, so it cannot lead the token elsewhere
	for _, id := range []string{"", "a.b", "evil.example/x", "a b", "a?b", "../x", "a%2F"} {
		_, err := p.sbx.health(t.Context(), id, testKeyToken)
		if code, _ := codeOf(err); code != provider.CodeInvalid {
			t.Errorf("a health read of Job %q gave %v, want it refused as invalid", id, err)
		}
	}
	if len(f.all()) != 0 || len(f.dialed()) != 0 {
		t.Fatalf("refused calls reached the network: %d requests, dials %q", len(f.all()),
			f.dialed())
	}
}

func TestHFSandboxClientSendsTheSandboxTokenAlone(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	j := f.addJob("RUNNING", nil)
	f.set(j, func(j *fakeJob) { j.token = testKeyToken })
	h, err := p.sbx.health(t.Context(), j.id, testKeyToken)
	if err != nil {
		t.Fatal(err)
	}
	three := 3
	eq(t, h, health{Status: "ok", Protocol: &three, Mode: "dedicated", Auth: "required"})
	got := sbxRequests(f)
	eq(t, len(got), 1)
	eq(t, got[0].host, j.id+"--49983.hf.jobs")
	eq(t, got[0].header.Get("X-Sandbox-Token"), testKeyToken)
	for _, name := range []string{"Authorization", "Cookie", "Proxy-Authorization"} {
		if v := got[0].header.Get(name); v != "" {
			t.Fatalf("the health read sent %s %q", name, v)
		}
	}
	eq(t, f.dialed(), []string{j.id + "--49983.hf.jobs:443"})
}

func TestHFSandboxClientRefusesRedirects(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	j := f.addJob("RUNNING", nil)
	for _, to := range []string{"https://evil.example/collect",
		"https://other--49983.hf.jobs/health"} {
		f.inject(healthRoute, fault{status: 307, raw: "moved", times: 1,
			header: map[string]string{"Location": to}})
		if _, err := p.sbx.health(t.Context(), j.id, testKeyToken); err == nil {
			t.Fatalf("a redirect to %s was taken as health", to)
		}
	}
	eq(t, len(sbxRequests(f)), 2)
	eq(t, slices.Compact(f.dialed()), []string{j.id + "--49983.hf.jobs:443"})
}

func TestHFSandboxClientBoundsTheAnswer(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	j := f.addJob("RUNNING", nil)
	f.set(j, func(j *fakeJob) {
		j.health = `{"status":"ok","protocol":3,"pad":"` + strings.Repeat("x", 1<<20) + `"}`
	})
	if _, err := p.sbx.health(t.Context(), j.id, testKeyToken); err == nil {
		t.Fatal("a 1 MiB health answer was read")
	}
}

// toAgent sends every *.hf.jobs call to a plain HTTP server, and a call to any other host there
type toAgent struct{ host string }

func (a toAgent) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasSuffix(r.URL.Hostname(), sbxDomain) {
		r = r.Clone(r.Context())
		r.URL.Scheme, r.URL.Host, r.Host = "http", a.host, ""
	}
	return http.DefaultTransport.RoundTrip(r)
}

// TestHFSandboxCallsRefuseRedirects points every sandbox call at a server that only redirects
func TestHFSandboxCallsRefuseRedirects(t *testing.T) {
	agent, reached := contract.Redirector(t)
	f := newFake(t)
	p := f.provider(t, testOptions())
	u, err := url.Parse(agent)
	if err != nil {
		t.Fatal(err)
	}
	p.sbx.client.Transport = toAgent{host: u.Host}
	ref := provider.Ref{Provider: "hf", ID: "6911aaaa", Meta: map[string]string{
		"secret": testKeyToken,
	}}
	ctx := t.Context()
	if s, err := p.Exec(ctx, ref, sh("true")); err == nil {
		out, _ := io.ReadAll(s)
		t.Errorf("an exec through a redirect gave %q", out)
	}
	if _, err := p.StatFile(ctx, ref, "/etc/hostname"); err == nil {
		t.Error("a stat took a redirect as an answer")
	}
	if data, _, err := p.ReadFile(ctx, ref, "/etc/hostname", 0, 64); err == nil {
		t.Errorf("a read through a redirect gave %q", data)
	}
	if err := p.WriteFile(ctx, ref, "/tmp/f", []byte("tenant bytes"), 0o644); err == nil {
		t.Error("a write through a redirect succeeded")
	}
	if err := p.ListFiles(ctx, ref, "/tmp", 1, provider.NewListing(10)); err == nil {
		t.Error("a listing took a redirect as an answer")
	}
	if r, err := p.OpenReader(ctx, ref, "/etc/hostname", 0); err == nil {
		_ = r.Close()
		t.Error("a stream through a redirect opened")
	}
	if w, err := p.OpenWriter(ctx, ref, "/tmp/f", provider.WriteSpec{Size: 1,
		Mode: 0o644}); err == nil {
		_ = w.Abort()
		t.Error("a write stream through a redirect opened")
	}
	if reached.Load() {
		t.Fatal("the sandbox client followed a redirect")
	}
}
