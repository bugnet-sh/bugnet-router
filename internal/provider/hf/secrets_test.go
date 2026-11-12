package hf

import (
	"context"
	"encoding/base64"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// tokenWords are the forms the token could take beside its own header, whole, encoded or cut
func tokenWords() []string {
	return []string{
		fakeToken, base64.StdEncoding.EncodeToString([]byte(fakeToken)),
		base64.RawURLEncoding.EncodeToString([]byte(fakeToken)), strings.ToLower(fakeToken),
		fakeToken[:8],
	}
}

// TestHFJobsTokenOnlyToHub runs every Jobs call and holds the token to huggingface.co's routes
func TestHFJobsTokenOnlyToHub(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	f.pageSize = 1
	logs := &syncBuffer{}
	opts := testOptions()
	opts.Log = slog.New(slog.NewTextHandler(logs, nil))
	p := f.provider(t, opts)
	labels := map[string]string{"bugnet": "1"}
	j := f.addJob("RUNNING", labels)
	f.addJob("SCHEDULING", labels)
	var errs []error
	keep := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	_, err := p.jobs.getJob(ctx, j.id)
	keep(err)
	_, err = p.jobs.listJobs(ctx, labels, []string{"SCHEDULING", "RUNNING"})
	keep(err)
	keep(p.loadHardware(ctx))
	// Answers that point elsewhere
	f.inject(jobRoute, fault{status: 307, raw: "moved", header: map[string]string{
		"Location": "https://evil.example/collect",
	}, times: 1})
	_, err = p.jobs.getJob(ctx, j.id)
	keep(err)
	f.inject("GET /api/jobs/hardware", fault{status: 302, raw: "moved", header: map[string]string{
		"Location": "https://evil.example/hardware",
	}, times: 1})
	keep(p.loadHardware(ctx))
	f.mu.Lock()
	f.nextLink = "https://evil.example/api/jobs/" + testNamespace + "?cursor=1"
	f.mu.Unlock()
	_, err = p.jobs.listJobs(ctx, labels, nil)
	keep(err)
	// Error answers, one of which echoes the token it got
	f.inject(jobRoute, fault{status: 401, message: "Invalid credentials", echo: true, times: 1})
	_, err = p.jobs.getJob(ctx, j.id)
	keep(err)
	f.inject(jobRoute, fault{status: 401, message: "Invalid credentials", times: 1,
		header: map[string]string{"X-Error-Code": fakeToken}})
	_, err = p.jobs.getJob(ctx, j.id)
	keep(err)
	// A status the table lacks is logged with its request id
	f.inject(jobRoute, fault{status: 418, message: "I'm a teapot", times: 1,
		header: map[string]string{"X-Request-Id": "req-" + fakeToken}})
	_, err = p.jobs.getJob(ctx, j.id)
	keep(err)
	// A token across the cut of a long message
	f.inject(jobRoute, fault{status: 403, times: 1,
		message: strings.Repeat("x", maxMessageBytes-10) + fakeToken})
	_, err = p.jobs.getJob(ctx, j.id)
	keep(err)
	f.inject(jobRoute, fault{status: 429, message: "slow down", times: 1})
	_, err = p.jobs.getJob(ctx, j.id)
	keep(err)
	p.jobs.mu.Lock()
	p.jobs.heldUntil = time.Time{}
	p.jobs.mu.Unlock()
	_, err = p.jobs.getJob(ctx, "missing")
	keep(err)
	// Calls the client refuses before sending
	_, err = p.jobs.do(ctx, call{method: http.MethodGet, url: "https://evil.example/api/jobs"}, nil)
	keep(err)
	_, err = p.jobs.getJob(ctx, "../../whoami-v2")
	keep(err)

	reqs := f.all()
	if len(reqs) < 8 {
		t.Fatalf("only %d requests were made", len(reqs))
	}
	for _, r := range reqs {
		where := r.method + " " + r.host + r.path
		if r.host != jobsHost {
			t.Errorf("%s went to a host other than %s", where, jobsHost)
		}
		auth := r.header.Get("Authorization")
		switch {
		case r.path == "/api/jobs/hardware" && auth != "":
			t.Errorf("%s, a public route, carried the token", where)
		case r.path != "/api/jobs/hardware" && auth != "Bearer "+fakeToken:
			t.Errorf("%s carried Authorization %q", where, auth)
		}
		r.header.Del("Authorization")
		var rest strings.Builder
		rest.WriteString(r.path + "?" + r.query.Encode() + string(r.body))
		for k, vs := range r.header {
			rest.WriteString(k + ": " + strings.Join(vs, ",") + "\n")
		}
		for _, w := range tokenWords() {
			if strings.Contains(rest.String(), w) {
				t.Errorf("%s carried the token outside its Authorization header", where)
			}
		}
	}
	for _, d := range f.dialed() {
		if d != jobsHost+":443" {
			t.Errorf("the client dialled %s", d)
		}
	}
	for _, err := range errs {
		if _, ok := provider.AsError(err); !ok {
			t.Errorf("%v is not a provider error", err)
		}
		for _, w := range tokenWords() {
			if strings.Contains(err.Error(), w) {
				t.Errorf("the error %q carries the token", err)
			}
		}
	}
	if len(errs) < 11 {
		t.Fatalf("only %d calls failed, the refused ones did not run", len(errs))
	}
	if !strings.Contains(logs.String(), "status=418") {
		t.Fatal("the status the table lacks was not logged")
	}
	for _, w := range tokenWords() {
		if strings.Contains(logs.String(), w) {
			t.Errorf("the log carries the token: %s", logs.String())
		}
	}
}
