package hf

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
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
	// The limits and the account check, refusals echoing the token they got
	_, err = p.ReadLimits(ctx)
	keep(err)
	f.inject(listRoute, fault{status: 402, message: "No credits", echo: true, times: 1})
	_, err = p.ReadLimits(ctx)
	keep(err)
	// A next page HF names by the token, refused for credit and then dropped
	linkPath := "/api/jobs/" + testNamespace + "/" + fakeToken
	f.mu.Lock()
	f.nextLink = "https://" + jobsHost + linkPath + "?cursor=1"
	f.mu.Unlock()
	f.inject(jobRoute, fault{status: 402, message: "No credits", times: 1})
	_, err = p.jobs.listJobs(ctx, labels, nil)
	keep(err)
	f.inject(jobRoute, fault{drop: true})
	_, err = p.jobs.listJobs(ctx, labels, nil)
	keep(err)
	f.clear(jobRoute)
	// Jobs HF names by the token, ended for credit and at a limit
	credit, limit := f.addJob("CANCELED", labels), f.addJob("CANCELED", labels)
	f.set(credit, func(j *fakeJob) { j.cancelReason = "NO_CREDITS" })
	f.set(limit, func(j *fakeJob) { j.cancelReason = "SPEND_LIMIT" })
	f.mu.Lock()
	f.nextLink = ""
	f.show = func(job map[string]any) {
		switch job["id"] {
		case credit.id:
			job["id"] = fakeToken
		case limit.id:
			job["id"] = strings.ToLower(fakeToken)
		}
	}
	f.mu.Unlock()
	_, err = p.jobs.listJobs(ctx, labels, nil)
	keep(err)
	f.mu.Lock()
	f.show = nil
	f.mu.Unlock()
	var details []string
	for _, want := range []provider.Block{provider.BlockUnfunded, provider.BlockBanned} {
		acc, err := p.CheckAccount(ctx)
		keep(err)
		if acc.Block != want || !strings.Contains(acc.Detail, "[token]") {
			t.Fatalf("the account check gave %+v, want %s", acc, want)
		}
		details = append(details, acc.Detail)
		f.inject(authCheckRoute, fault{status: 403, message: "Org suspended", echo: true,
			times: 1})
	}
	// Answers naming a Job by the token, as written and in lowercase, on each path quoting it
	answerAs := func(id string, status map[string]any) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.show = func(job map[string]any) {
			job["id"] = id
			if status != nil {
				job["status"] = status
			}
		}
	}
	for _, id := range []string{fakeToken, strings.ToLower(fakeToken)} {
		answerAs(id, nil)
		keep(p.WaitReady(ctx, refTo(j)))
		_, err = p.StatFile(ctx, provider.Ref{Provider: "hf", ID: j.id}, "/tmp/a")
		keep(err)
		answerAs(id, map[string]any{"stage": "PAUSED"})
		_, err = p.Inspect(ctx, refTo(j))
		keep(err)
		answerAs(id+"/x", nil)
		_, err = p.Create(ctx, keyed())
		keep(err)
	}
	f.mu.Lock()
	f.show = nil
	f.mu.Unlock()
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
		path := r.path
		if path == linkPath {
			// HF's own Link named this path, so the call sends it back to HF as HF wrote it
			path = ""
		}
		var rest strings.Builder
		rest.WriteString(path + "?" + r.query.Encode() + string(r.body))
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
	if len(errs) < 19 {
		t.Fatalf("only %d calls failed, the refused ones did not run", len(errs))
	}
	for _, d := range details {
		for _, w := range tokenWords() {
			if strings.Contains(d, w) {
				t.Errorf("the account detail %q carries the token", d)
			}
		}
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

// TestHFSightingsKeepTheTokenOutOfValkey holds every sighting kept in Valkey free of the token
func TestHFSightingsKeepTheTokenOutOfValkey(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	f.pageSize = 1
	vk, prefix := testenv.Valkey(t)
	p := sharedProvider(t, f, vk, prefix)
	labels := map[string]string{"bugnet": "1"}
	// Jobs HF names by the token, ended for credit and at a limit
	credit := endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
	limit := endedJob(f, "CANCELED", "SPEND_LIMIT", time.Time{})
	f.mu.Lock()
	f.show = func(job map[string]any) {
		switch job["id"] {
		case credit.id:
			job["id"] = fakeToken
		case limit.id:
			job["id"] = "job-" + strings.ToLower(fakeToken)
		}
	}
	f.mu.Unlock()
	if _, err := p.jobs.listJobs(ctx, labels, nil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.show = nil
	f.mu.Unlock()
	// Refusals for credit echoing the token as written and in lowercase
	f.inject(listRoute, fault{status: 402, message: "No credits", echo: true, times: 1})
	if _, err := p.ReadLimits(ctx); err != nil {
		t.Fatal(err)
	}
	f.inject(jobRoute, fault{status: 402, times: 1, header: map[string]string{
		"X-Error-Message": "No credits for " + strings.ToLower(fakeToken),
		"X-Request-Id":    "req-" + strings.ToLower(fakeToken),
	}})
	if _, err := p.jobs.getJob(ctx, credit.id); !isCode(err, provider.CodeUnfunded) {
		t.Fatalf("getJob = %v", err)
	}
	// A next page HF names by the token, refused for credit
	f.mu.Lock()
	f.nextLink = "https://" + jobsHost + "/api/jobs/" + testNamespace + "/" + fakeToken +
		"?cursor=1"
	f.mu.Unlock()
	f.inject(jobRoute, fault{status: 402, message: "No credits", times: 1})
	if _, err := p.jobs.listJobs(ctx, labels, nil); !isCode(err, provider.CodeUnfunded) {
		t.Fatalf("listJobs = %v", err)
	}

	var keys []string
	for cursor := uint64(0); ; {
		scan, err := vk.Do(ctx, vk.B().Scan().Cursor(cursor).Match(prefix+"*").Count(1000).
			Build()).AsScanEntry()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, scan.Elements...)
		if cursor = scan.Cursor; cursor == 0 {
			break
		}
	}
	slices.Sort(keys)
	eq(t, keys, []string{prefix + "hf:sightings", prefix + "hf:sightings:words"})
	var kept strings.Builder
	for _, key := range keys {
		kept.WriteString(key + "\n")
		for _, cmd := range []valkey.Completed{
			vk.B().Zrange().Key(key).Min("0").Max("-1").Withscores().Build(),
			vk.B().Hgetall().Key(key).Build(),
		} {
			// Each key answers one of the two, the other a wrong type error
			if all, err := vk.Do(ctx, cmd).ToAny(); err == nil {
				kept.WriteString(fmt.Sprintf("%v\n", all))
			}
		}
	}
	// The last list's first page reads the Job ended for credit once more, by its own id
	if n := zcard(t, vk, prefix+"hf:sightings"); n != 6 {
		t.Fatalf("Valkey keeps %d sightings, want 6: %s", n, kept.String())
	}
	if !strings.Contains(kept.String(), "[token]") {
		t.Fatalf("no sighting names the token's place: %s", kept.String())
	}
	for _, w := range tokenWords() {
		if strings.Contains(kept.String(), w) {
			t.Errorf("Valkey keeps the token: %s", kept.String())
		}
	}
}

// TestHFJobNamedByTheTokenStaysOutOfErrors keeps a ref's token-named Job out of errors and logs
func TestHFJobNamedByTheTokenStaysOutOfErrors(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	logs := &syncBuffer{}
	opts := testOptions()
	opts.Log = slog.New(slog.NewTextHandler(logs, nil))
	p := f.provider(t, opts)
	var errs []error
	keep := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	var reasons []string
	waitBriefly := func(ref provider.Ref) {
		wctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		keep(p.WaitReady(wctx, ref))
	}
	for i, id := range []string{fakeToken, strings.ToLower(fakeToken)} {
		j := f.addJob("SCHEDULING", nil)
		f.set(j, func(j *fakeJob) { j.id, j.token = id, testKeyToken })
		ref := refTo(j)
		waitBriefly(ref)
		// Health reads that fail, drop, answer no protocol, or come from a server too old
		f.set(j, func(j *fakeJob) { j.stage = "RUNNING" })
		f.inject(healthRoute, fault{status: 500, message: "down"})
		waitBriefly(ref)
		f.inject(healthRoute, fault{drop: true})
		waitBriefly(ref)
		f.clear(healthRoute)
		f.set(j, func(j *fakeJob) { j.health = `{"status":"ok"}` })
		waitBriefly(ref)
		f.set(j, func(j *fakeJob) { j.health = `{"status":"ok","protocol":2}` })
		keep(p.WaitReady(ctx, ref))
		f.set(j, func(j *fakeJob) { j.health = "" })
		// Sandbox calls the server refuses or the network drops on every try
		f.inject(sbxStatRoute, fault{status: 502, times: 1})
		_, err := p.StatFile(ctx, ref, "/tmp/a")
		keep(err)
		f.inject(sbxStatRoute, fault{drop: true})
		_, err = p.StatFile(ctx, ref, "/tmp/a")
		keep(err)
		f.clear(sbxStatRoute)
		dir := t.TempDir()
		f.inject(sbxWriteRoute, fault{drop: true})
		_, err = p.OpenWriter(ctx, ref, dir+"/w", provider.WriteSpec{Size: 1, Mode: 0o644})
		keep(err)
		f.clear(sbxWriteRoute)
		mustWrite(t, dir+"/r", "data", 0o644)
		// A file reader reads a dropped read as a short file, so the read gets bytes not HTTP
		f.inject(sbxReadRoute, fault{serve: func(w http.ResponseWriter, _ *http.Request) {
			if conn, buf, err := w.(http.Hijacker).Hijack(); err == nil {
				_, _ = buf.WriteString("not http\r\n\r\n")
				_ = buf.Flush()
				_ = conn.Close()
			}
		}})
		r, err := p.OpenReader(ctx, ref, dir+"/r", 0)
		if err == nil {
			_, err = io.ReadAll(r)
			_ = r.Close()
		}
		keep(err)
		f.clear(sbxReadRoute)
		// A stat answer that is not JSON, and a command sbx-server stopped at its timeout
		f.inject(sbxStatRoute, fault{status: 200, raw: "not json", times: 1})
		_, err = p.StatFile(ctx, ref, "/tmp/a")
		keep(err)
		f.inject(sbxExecRoute, fault{status: 200, times: 1, raw: ndjson(
			`{"event":"start","pid":7}`, `{"event":"exit","exit_code":null,"timed_out":true}`)})
		s, err := p.Exec(ctx, ref, sh("true"))
		if err == nil {
			_, _ = io.ReadAll(s)
			_, err = s.Wait()
			_ = s.Close()
		}
		keep(err)
		// A dropped exec, its kill dropped too
		f.inject(sbxExecRoute, fault{drop: true, times: 2})
		_, err = p.Exec(ctx, ref, sh("true"))
		keep(err)
		within(t, 5*time.Second, "the failed kill's log", func() bool {
			return strings.Count(logs.String(), "was not killed") == i+1
		})
		f.clear(sbxExecRoute)
		// sbx-server, which the Job's root may replace, echoes the host that names the Job
		host := id + "--49983.hf.jobs"
		f.inject(sbxStatRoute, fault{status: 500, times: 1, raw: "bad host " + host})
		_, err = p.StatFile(ctx, ref, "/tmp/a")
		keep(err)
		f.inject(healthRoute, fault{status: 500, raw: "bad host " + host})
		waitBriefly(ref)
		f.clear(healthRoute)
		f.set(j, func(j *fakeJob) {
			j.health = `{"status":"ok","protocol":3,"mode":"` + host + `","auth":"required"}`
		})
		keep(p.WaitReady(ctx, ref))
		f.set(j, func(j *fakeJob) { j.health = "" })
		// A ref with no token for a Job with no key to derive one from, and one with a bad token
		_, err = p.StatFile(ctx, provider.Ref{Provider: "hf", ID: id}, "/tmp/a")
		keep(err)
		_, err = p.StatFile(ctx, provider.Ref{Provider: "hf", ID: id,
			Meta: map[string]string{metaSecret: "not-hex"}}, "/tmp/a")
		keep(err)
		// A stage the adapter does not know, then a Job HF ended
		f.set(j, func(j *fakeJob) { j.stage = "PAUSED" })
		_, err = p.Inspect(ctx, ref)
		keep(err)
		f.set(j, func(j *fakeJob) { j.stage = "COMPLETED" })
		keep(p.SetLifetime(ctx, provider.Ref{Provider: "hf", ID: id}, time.Now().Add(time.Hour)))
		// HF answers the token as the stage, read by a check, a wait, a failed call and a renewal
		f.set(j, func(j *fakeJob) { j.stage = id })
		_, err = p.Inspect(ctx, ref)
		keep(err)
		keep(p.WaitReady(ctx, ref))
		_, err = p.StatFile(ctx, ref, "/tmp/a")
		keep(err)
		keep(p.SetLifetime(ctx, provider.Ref{Provider: "hf", ID: id}, time.Now().Add(time.Hour)))
		// A Job that ended its start with a message naming the token
		f.set(j, func(j *fakeJob) { j.stage, j.message = "ERROR", "boom "+id })
		keep(p.WaitReady(ctx, ref))
		st, err := p.Inspect(ctx, ref)
		keep(err)
		reasons = append(reasons, st.Reason)
		// Ids that are not one plain segment
		_, err = p.GateRoute(provider.Ref{Provider: "hf", ID: id + ".x"},
			provider.DefaultGatePort)
		keep(err)
		_, err = p.Inspect(ctx, provider.Ref{Provider: "hf", ID: id + "/x"})
		keep(err)
	}
	if len(errs) < 52 {
		t.Fatalf("only %d calls failed, the failing paths did not run", len(errs))
	}
	// Each path that names the Job from a transport error or an answer ran
	for _, path := range []string{
		"sbx-server GET /v1/files/stat: ", "sbx-server health: ",
		"sbx-server PUT /v1/files/write: ", "moved no bytes", "stat of /tmp/a: ",
		"not 64 lowercase hex", "at its timeout", "answered 500: bad host [token]",
		`health answered 500: "bad host [token]`, `runs with mode "[token]`,
		`has stage "[token]"`, "hf shows the Job [token]", "ended as [token]",
		"ended its start as ERROR: boom [token]",
	} {
		if !slices.ContainsFunc(errs, func(err error) bool {
			return strings.Contains(err.Error(), path)
		}) {
			t.Errorf("no error came from %q, so its path did not run", path)
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
	for _, w := range tokenWords() {
		if strings.Contains(logs.String(), w) {
			t.Errorf("the log carries the token: %s", logs.String())
		}
		if slices.ContainsFunc(reasons, func(r string) bool { return strings.Contains(r, w) }) {
			t.Errorf("a status reason carries the token: %q", reasons)
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
	_, err = p.health(ctx, sandboxOf(refs[0].ID, refs[0].Meta["secret"]))
	keep(err)
	f.inject(healthRoute, fault{status: 200, times: 1, raw: `{"status":"ok","protocol":3,` +
		`"mode":"` + refs[0].Meta["secret"] + `","auth":"required"}`})
	keep(p.WaitReady(ctx, refs[0]))
	// Hex reads the same in capitals, so an echo of the token in capitals is the token
	f.inject(healthRoute, fault{status: 200, times: 1, raw: `{"status":"ok","protocol":3,` +
		`"mode":"dedicated","auth":"` + strings.ToUpper(refs[0].Meta["secret"]) + `"}`})
	keep(p.WaitReady(ctx, refs[0]))
	f.inject(healthRoute, fault{status: 500, times: 1,
		raw: "got " + strings.ToUpper(refs[0].Meta["secret"])})
	_, err = p.health(ctx, sandboxOf(refs[0].ID, refs[0].Meta["secret"]))
	keep(err)
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
		tokenParts = append(tokenParts, tk[:16], strings.ToUpper(tk), strings.ToUpper(tk[:16]))
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

// TestHFSandboxCallsCarryTheSandboxTokenAlone holds every sandbox call to its Job and its token
func TestHFSandboxCallsCarryTheSandboxTokenAlone(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	logs := &syncBuffer{}
	opts := testOptions()
	opts.Log = slog.New(slog.NewTextHandler(logs, nil))
	p := f.provider(t, opts)
	p.partBytes = 1 << 10
	ref := readyJob(t, f, p)
	token := ref.Meta["secret"]
	dir := t.TempDir()
	var errs []error
	keep := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	data := []byte(strings.Repeat("tenant bytes ", 300))
	keep(p.WriteFile(ctx, ref, dir+"/a", data, 0o644))
	_, _, err := p.ReadFile(ctx, ref, dir+"/a", 0, 64)
	keep(err)
	_, err = p.StatFile(ctx, ref, dir+"/a")
	keep(err)
	keep(p.ListFiles(ctx, ref, dir, 2, provider.NewListing(100)))
	keep(streamUp(t, p, ref, dir+"/b", data))
	_, err = streamDown(t, p, ref, dir+"/b", 3)
	keep(err)
	_, _, err = run(t, p, ref, sh("echo hi"))
	keep(err)
	// A cancel sends its kill, and a ref without its token derives it from the Job's key
	cctx, cancel := context.WithCancel(ctx)
	s, err := p.Exec(cctx, ref, sh("echo started; sleep 60"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Read(make([]byte, 16))
	cancel()
	_, err = s.Wait()
	keep(err)
	within(t, 5*time.Second, "the kill", func() bool { return killExecs(f) == 1 })
	_, _, err = run(t, p, provider.Ref{Provider: "hf", ID: ref.ID}, sh("true"))
	keep(err)
	// Error answers that echo the token they got, from sbx-server or what replaced it
	for _, route := range []string{sbxStatRoute, sbxReadRoute, sbxWriteRoute, sbxListRoute,
		sbxExecRoute, sbxDeleteRoute} {
		for _, status := range []int{400, 403, 404, 500, 502, 503} {
			f.inject(route, fault{status: status, times: 1,
				message: "got X-Sandbox-Token " + token + " and " + strings.ToUpper(token)})
			_, err = p.StatFile(ctx, ref, dir+"/a")
			keep(err)
			_, _, err = p.ReadFile(ctx, ref, dir+"/a", 0, 8)
			keep(err)
			keep(p.WriteFile(ctx, ref, dir+"/c", []byte("x"), 0o644))
			keep(p.ListFiles(ctx, ref, dir, 1, provider.NewListing(10)))
			_, _, err = run(t, p, ref, sh("true"))
			keep(err)
			f.clear(route)
		}
	}
	f.inject(sbxExecRoute, fault{status: 200, times: 1, raw: token + "\n"})
	_, _, err = run(t, p, ref, sh("true"))
	keep(err)

	var sandbox, hub int
	for _, r := range f.all() {
		where := r.method + " " + r.host + r.path
		rest := r.header.Clone()
		got := rest.Get("X-Sandbox-Token")
		rest.Del("X-Sandbox-Token")
		body := string(r.body)
		if r.method == "POST" && r.path == "/api/jobs/"+testNamespace {
			// The create carries the token once, as the Job secret HF encrypts
			var spec map[string]any
			if err := json.Unmarshal(r.body, &spec); err != nil {
				t.Fatal(err)
			}
			if spec["secrets"].(map[string]any)["SBX_TOKEN"] != token {
				t.Errorf("%s carried secrets %v", where, spec["secrets"])
			}
			delete(spec, "secrets")
			plain, _ := json.Marshal(spec)
			body = string(plain)
		}
		var other strings.Builder
		other.WriteString(r.path + "?" + r.query.Encode() + body)
		for k, vs := range rest {
			other.WriteString(k + ": " + strings.Join(vs, ",") + "\n")
		}
		switch {
		case r.host == ref.ID+"--49983.hf.jobs":
			sandbox++
			for _, name := range []string{"Authorization", "Cookie", "Proxy-Authorization"} {
				if v := r.header.Get(name); v != "" {
					t.Errorf("%s sent %s %q", where, name, v)
				}
			}
			if got != token {
				t.Errorf("%s sent X-Sandbox-Token %q", where, got)
			}
			if slices.ContainsFunc(append(tokenWords(), token, token[:16]), func(w string) bool {
				return strings.Contains(other.String(), w)
			}) {
				t.Errorf("%s carried a token outside its header", where)
			}
		case r.host == jobsHost:
			hub++
			if got != "" || strings.Contains(other.String(), token) {
				t.Errorf("%s carried the sandbox token", where)
			}
		default:
			t.Errorf("%s went to a host neither HF's API nor the Job's", where)
		}
	}
	if sandbox < 40 || hub < 3 {
		t.Fatalf("%d sandbox and %d Jobs calls ran", sandbox, hub)
	}
	for _, d := range f.dialed() {
		if d != jobsHost+":443" && d != ref.ID+"--49983.hf.jobs:443" {
			t.Errorf("the adapter dialled %s", d)
		}
	}
	if len(errs) < 25 {
		t.Fatalf("only %d calls failed, the error answers did not run", len(errs))
	}
	secrets := append(tokenWords(), token, token[:16], strings.ToUpper(token),
		strings.ToUpper(token[:16]))
	for _, err := range errs {
		for _, w := range secrets {
			if strings.Contains(err.Error(), w) {
				t.Errorf("the error %q carries a secret", err)
			}
		}
	}
	for _, w := range secrets {
		if strings.Contains(logs.String(), w) {
			t.Errorf("the log carries a secret: %s", logs.String())
		}
	}
}
