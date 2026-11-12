package hf

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
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

// seedWords are the forms the sandbox seed could take in a request, a log or an error
func seedWords() []string {
	return []string{
		string(testSeed), hex.EncodeToString(testSeed),
		base64.StdEncoding.EncodeToString(testSeed), base64.RawURLEncoding.EncodeToString(testSeed),
	}
}

// TestHFSandboxSecretsStayInPlace holds each sandbox token to its create and its own sbx-server
func TestHFSandboxSecretsStayInPlace(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	logs := &syncBuffer{}
	opts := testOptions()
	opts.Log = slog.New(slog.NewTextHandler(logs, nil))
	p := f.provider(t, opts)
	var refs []provider.Ref
	var errs []error
	keep := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	for _, key := range []string{testKey, ""} {
		req := keyed()
		req.IdempotencyKey = key
		ref, err := p.Create(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
		keep(p.WaitReady(ctx, ref))
	}
	// A create whose answer was lost, found again by its key
	lost := keyed()
	lost.IdempotencyKey = "fedcba9876543210fedcba9876543210"
	f.inject(createRoute, fault{status: 500, message: "Internal Error", served: true, times: 1})
	ref, err := p.Create(ctx, lost)
	if err != nil {
		t.Fatal(err)
	}
	refs = append(refs, ref)
	// Creates whose error answers quote the sandbox token their body carried
	var failed []string
	for i, c := range []struct {
		fault    fault
		unlisted bool
		relist   *fault
	}{
		{fault: fault{status: 422}},
		{fault: fault{status: 500, served: true}, unlisted: true},
		{fault: fault{status: 500, served: true},
			relist: &fault{status: 500, message: "Internal Error", after: 1, times: 1}},
		{fault: fault{status: 418}},
	} {
		req := keyed()
		req.IdempotencyKey = strings.Repeat(string(rune('a'+i)), 32)
		token := p.sandboxToken(req.IdempotencyKey)
		failed = append(failed, token)
		c.fault.message, c.fault.times = "Invalid body: secrets.SBX_TOKEN "+token, 1
		f.mu.Lock()
		f.unlistNew = c.unlisted
		f.mu.Unlock()
		f.inject(createRoute, c.fault)
		if c.relist != nil {
			f.inject(listRoute, *c.relist)
		}
		_, err := p.Create(ctx, req)
		if err == nil {
			t.Fatalf("a create answered %d gave no error", c.fault.status)
		}
		keep(err)
	}
	f.mu.Lock()
	f.unlistNew = false
	f.mu.Unlock()
	// Waits that end in errors, one sbx-server refusing the token and one echoing it
	wrong := cloneRef(ref)
	wrong.Meta["secret"] = refs[0].Meta["secret"]
	keep(p.WaitReady(ctx, wrong))
	f.set(f.job(ref.ID), func(j *fakeJob) {
		j.health = `{"status":"ok","protocol":2,"echo":"` + j.token + `"}`
	})
	keep(p.WaitReady(ctx, ref))
	f.inject(healthRoute, fault{status: 401, message: "bad token", echo: true, times: 1})
	keep(p.WaitReady(ctx, refs[0]))
	// An sbx-server the Job's root replaced may echo the token it got
	f.inject(healthRoute, fault{status: 500, raw: "got " + refs[0].Meta["secret"], times: 1})
	_, err = p.sbx.health(ctx, refs[0].ID, refs[0].Meta["secret"])
	keep(err)
	f.inject(healthRoute, fault{status: 200, times: 1, raw: `{"status":"ok","protocol":3,` +
		`"mode":"` + refs[0].Meta["secret"] + `","auth":"required"}`})
	keep(p.WaitReady(ctx, refs[0]))
	_, err = p.Inspect(ctx, ref)
	keep(err)
	owned, err := p.ListOwned(ctx, map[string]string{"bugnet": "1"})
	keep(err)
	for _, s := range owned {
		if _, ok := s.Ref.Meta["secret"]; ok {
			t.Errorf("ListOwned's ref of %s carries a token", s.Ref.ID)
		}
	}
	keep(p.SetLifetime(ctx, ref, time.Now().Add(time.Hour)))
	keep(p.Destroy(ctx, ref))

	var tokens []string
	for _, r := range refs {
		if r.Meta["secret"] == "" {
			t.Fatalf("%s has no token", r.ID)
		}
		tokens = append(tokens, r.Meta["secret"])
	}
	tokens = append(tokens, failed...)
	carries := func(s string, words []string) bool {
		return slices.ContainsFunc(words, func(w string) bool { return strings.Contains(s, w) })
	}
	tokenParts := slices.Clone(tokens)
	for _, tk := range tokens {
		tokenParts = append(tokenParts, tk[:16], strings.ToUpper(tk))
	}
	var created, health int
	for _, r := range f.all() {
		where := r.method + " " + r.host + r.path
		sandboxToken := r.header.Get("X-Sandbox-Token")
		rest := r.header.Clone()
		rest.Del("X-Sandbox-Token")
		rest.Del("Authorization")
		body := string(r.body)
		if r.method == "POST" && r.path == "/api/jobs/"+testNamespace {
			// The create carries the token once, as the Job secret HF encrypts
			var spec map[string]any
			if err := json.Unmarshal(r.body, &spec); err != nil {
				t.Fatal(err)
			}
			secret, _ := spec["secrets"].(map[string]any)["SBX_TOKEN"].(string)
			if !slices.Contains(tokens, secret) {
				t.Errorf("%s carried secret %q", where, secret)
			}
			delete(spec, "secrets")
			plain, _ := json.Marshal(spec)
			body = string(plain)
			created++
		}
		var other strings.Builder
		other.WriteString(r.path + "?" + r.query.Encode() + body)
		for k, vs := range rest {
			other.WriteString(k + ": " + strings.Join(vs, ",") + "\n")
		}
		switch {
		case r.host == jobsHost:
			if sandboxToken != "" {
				t.Errorf("%s carried a sandbox token", where)
			}
		case strings.HasSuffix(r.host, "--49983.hf.jobs"):
			health++
			if r.header.Get("Authorization") != "" || carries(other.String(), tokenWords()) {
				t.Errorf("%s carried the Jobs token", where)
			}
			if !slices.Contains(tokens, sandboxToken) {
				t.Errorf("%s carried X-Sandbox-Token %q", where, sandboxToken)
			}
		default:
			t.Errorf("%s went to a host neither HF's API nor a Job's", where)
		}
		if carries(other.String(), tokenParts) {
			t.Errorf("%s carried a sandbox token outside its place", where)
		}
		if carries(other.String()+sandboxToken+r.header.Get("Authorization"), seedWords()) {
			t.Errorf("%s carried the seed", where)
		}
	}
	// Both fresh and keyed creates, the lost and failed ones and the waits all ran
	if created != 3+len(failed) || health < 4 {
		t.Fatalf("%d creates and %d health reads ran", created, health)
	}
	if len(errs) < 3 {
		t.Fatalf("only %d calls failed, the failing waits and reads did not run", len(errs))
	}
	for _, err := range errs {
		if carries(err.Error(), append(append(tokenParts, tokenWords()...), seedWords()...)) {
			t.Errorf("the error %q carries a secret", err)
		}
	}
	if carries(logs.String(), append(append(tokenParts, tokenWords()...), seedWords()...)) {
		t.Errorf("the log carries a secret: %s", logs.String())
	}
}
