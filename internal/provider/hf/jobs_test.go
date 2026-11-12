package hf

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	jobRoute  = "GET /api/jobs/{ns}/{id}"
	listRoute = "GET /api/jobs/{ns}"
)

// listOnce reads the namespace's first page of Jobs, a call on the namespace and not on a Job
func listOnce(ctx context.Context, p *Provider) error {
	_, err := p.jobs.do(ctx, call{method: http.MethodGet, url: jobsURL(nil, testNamespace)}, nil)
	return err
}

// createOnce sends one call shaped as a Job's create, which may start a Job when sent
func createOnce(ctx context.Context, p *Provider) error {
	_, err := p.jobs.do(ctx, call{
		method: http.MethodGet, url: jobsURL(nil, testNamespace), create: true,
	}, &[]job{})
	return err
}

func TestHFErrorMapping(t *testing.T) {
	type row struct {
		name     string
		route    string
		fault    fault
		code     provider.Code
		failOver bool
		// message is a part of the error the caller reads
		message string
	}
	ft := func(status int, message string) fault {
		return fault{status: status, message: message}
	}
	rows := []row{
		{name: "bad request", route: listRoute, fault: ft(400, "Invalid label filter"),
			code: provider.CodeInvalid, failOver: true, message: "Invalid label filter"},
		{name: "unprocessable", route: listRoute, fault: ft(422, "flavor must be one of"),
			code: provider.CodeInvalid, failOver: true},
		{name: "bad token", route: listRoute, fault: ft(401, "Invalid credentials"),
			code: provider.CodeAuth, failOver: true},
		{name: "payment required", route: listRoute, fault: ft(402, "Payment required"),
			code: provider.CodeUnfunded, failOver: true},
		{name: "forbidden", route: listRoute, fault: ft(403, "You don't have the rights"),
			code: provider.CodeAuth, failOver: true},
		{name: "suspended", route: listRoute, fault: ft(403, "Your account has been suspended"),
			code: provider.CodeAbuseLock, failOver: true},
		{name: "banned", route: jobRoute, fault: ft(403, "This user is Banned from the Hub"),
			code: provider.CodeAbuseLock, failOver: true},
		{name: "suspended in a header alone", route: listRoute, fault: fault{
			status: 403, raw: " ", header: map[string]string{"X-Error-Message": "Org suspended"},
		}, code: provider.CodeAbuseLock, failOver: true, message: "Org suspended"},
		// A word that merely holds one of the lock words is no lock
		{name: "unbanned", route: listRoute, fault: ft(403, "unbanned members only"),
			code: provider.CodeAuth, failOver: true},
		{name: "a Job gone", route: jobRoute, fault: ft(404, "Job not found"),
			code: provider.CodeNotFound},
		{name: "a namespace wrong", route: listRoute, fault: ft(404, "Namespace not found"),
			code: provider.CodeInvalid, failOver: true},
		{name: "server error", route: listRoute, fault: ft(500, "Internal Error"),
			code: provider.CodeInternal, failOver: true},
		{name: "bad gateway", route: jobRoute, fault: fault{status: 502, raw: "<html>502</html>"},
			code: provider.CodeInternal, failOver: true, message: "<html>502</html>"},
		{name: "unavailable", route: listRoute, fault: ft(503, "Service Unavailable"),
			code: provider.CodeInternal, failOver: true},
		{name: "a status the table lacks", route: listRoute, fault: ft(409, "Conflict"),
			code: provider.CodeInvalid, failOver: true},
		{name: "a redirect", route: listRoute, fault: fault{
			status: 302, raw: "moved", header: map[string]string{"Location": "/elsewhere"},
		}, code: provider.CodeInternal, failOver: true},
		{name: "an empty answer", route: jobRoute, fault: fault{status: 404, raw: " "},
			code: provider.CodeNotFound, message: "Not Found"},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, testOptions())
			j := f.addJob("RUNNING", nil)
			f.inject(tc.route, tc.fault)
			var err error
			if tc.route == jobRoute {
				_, err = p.jobs.getJob(context.Background(), j.id)
			} else {
				err = listOnce(context.Background(), p)
			}
			e, ok := provider.AsError(err)
			if !ok || e.Code != tc.code || e.CanFailOver != tc.failOver {
				t.Fatalf("got %v, want code %s fail over %v", err, tc.code, tc.failOver)
			}
			named := provider.AccountBlock(tc.code) != provider.BlockNone
			if named && e.Provider != "hf" || !named && e.Provider != "" {
				t.Fatalf("an error of code %s names provider %q", tc.code, e.Provider)
			}
			if tc.message != "" && !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("the error %q does not carry %q", err, tc.message)
			}
		})
	}
}

func TestHFTransportErrors(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	p := f.provider(t, testOptions())
	f.inject(listRoute, fault{drop: true, times: 1})
	e, ok := provider.AsError(listOnce(ctx, p))
	if !ok || e.Code != provider.CodeInternal || e.CanFailOver {
		t.Fatalf("a read sent with no answer gave %v, want internal and no fail over", e)
	}
	// A create sent with no answer may have started a Job
	f.inject(listRoute, fault{drop: true, times: 1})
	e, ok = provider.AsError(createOnce(ctx, p))
	if !ok || e.Code != provider.CodeOutcomeUnknown || e.CanFailOver {
		t.Fatalf("a create sent with no answer gave %v, want outcome_unknown", e)
	}
	f.inject(listRoute, fault{status: 200, raw: "{not json", times: 1})
	e, ok = provider.AsError(createOnce(ctx, p))
	if !ok || e.Code != provider.CodeOutcomeUnknown {
		t.Fatalf("a create whose 200 could not be read gave %v, want outcome_unknown", e)
	}
	_, err := p.jobs.getJob(ctx, "missing")
	if e, ok = provider.AsError(err); !ok || e.Code != provider.CodeNotFound {
		t.Fatalf("a read of a missing Job gave %v", err)
	}
	f.inject(jobRoute, fault{status: 200, raw: `{"id": 12}`, times: 1})
	_, err = p.jobs.getJob(ctx, "6911")
	if e, ok = provider.AsError(err); !ok || e.Code != provider.CodeInternal || e.CanFailOver {
		t.Fatalf("a read whose 200 could not be read gave %v", err)
	}
	// A call that never reached HF started nothing, so the order may go elsewhere
	f.srv.Close()
	// A kept connection would take the write before the dial is refused
	p.jobs.client.CloseIdleConnections()
	e, ok = provider.AsError(createOnce(ctx, p))
	if !ok || e.Code != provider.CodeInternal || !e.CanFailOver {
		t.Fatalf("a create that never connected gave %v, want internal with fail over", e)
	}
}

func TestHFJobsRefusesRedirects(t *testing.T) {
	for _, to := range []string{
		"https://huggingface.co/api/jobs/elsewhere", "https://evil.example/api/jobs/x",
	} {
		f := newFake(t)
		p := f.provider(t, testOptions())
		j := f.addJob("RUNNING", nil)
		f.inject(jobRoute, fault{status: 307, raw: "moved", header: map[string]string{
			"Location": to,
		}})
		_, err := p.jobs.getJob(context.Background(), j.id)
		if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInternal {
			t.Fatalf("a redirect to %s gave %v", to, err)
		}
		if got := len(f.all()); got != 1 {
			t.Fatalf("a redirect to %s was followed, %d requests", to, got)
		}
		for _, d := range f.dialed() {
			if d != "huggingface.co:443" {
				t.Fatalf("a redirect to %s dialled %s", to, d)
			}
		}
	}
}

func TestHFJobsRefusesURLsOffTheJobsAPI(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	for _, u := range []string{
		"http://huggingface.co/api/jobs/" + testNamespace,
		"https://evil.example/api/jobs/" + testNamespace,
		"https://huggingface.co.evil.example/api/jobs/" + testNamespace,
		"https://huggingface.co:8443/api/jobs/" + testNamespace,
		"https://user@huggingface.co/api/jobs/" + testNamespace,
		"https://huggingface.co/api/whoami-v2",
		"https://huggingface.co/api/jobsx",
		"https://huggingface.co/api/jobs/../whoami-v2",
		"https://huggingface.co/api/jobs/%2e%2e/whoami-v2",
		"//huggingface.co/api/jobs/" + testNamespace,
		"https:huggingface.co/api/jobs",
		"",
	} {
		_, err := p.jobs.do(context.Background(), call{method: http.MethodGet, url: u}, nil)
		e, ok := provider.AsError(err)
		if !ok || e.Code != provider.CodeInternal {
			t.Errorf("a call to %q gave %v, want it refused", u, err)
		}
	}
	// A Job id is one plain path segment, so it cannot lead the token off its route
	for _, id := range []string{"", ".", "..", "a/../../whoami-v2", "a?b=c", "a%2F..", "a b"} {
		_, err := p.jobs.getJob(context.Background(), id)
		if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
			t.Errorf("a read of Job %q gave %v, want it refused as invalid", id, err)
		}
	}
	if got := f.all(); len(got) != 0 || len(f.dialed()) != 0 {
		t.Fatalf("refused calls reached the network: %d requests, dials %q", len(got), f.dialed())
	}
}

func TestHFPacingGivesUpWithTheCaller(t *testing.T) {
	f := newFake(t)
	opts := testOptions()
	opts.OpsPerSecond = 1
	p := f.provider(t, opts)
	if err := listOnce(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := listOnce(ctx, p)
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeInternal || !e.CanFailOver ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a call its caller gave up on while paced gave %v", err)
	}
	eq(t, len(f.all()), 1)
}

// idsOf lists the Jobs' ids in order
func idsOf(js []job) []string {
	var out []string
	for _, j := range js {
		out = append(out, j.ID)
	}
	return out
}

func TestHFListJobsFollowsPages(t *testing.T) {
	f := newFake(t)
	f.pageSize = 2
	p := f.provider(t, testOptions())
	ours := map[string]string{"bugnet": "1", "placement": "p1"}
	var want []string
	for _, stage := range []string{"RUNNING", "SCHEDULING", "CANCELED", "RUNNING", "RUNNING"} {
		j := f.addJob(stage, ours)
		if stage != "CANCELED" {
			want = append(want, j.id)
		}
	}
	f.addJob("RUNNING", map[string]string{"bugnet": "1"})
	got, err := p.jobs.listJobs(context.Background(), ours, []string{"SCHEDULING", "RUNNING"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, idsOf(got), want)
	pages := f.requestsTo("GET", "/api/jobs/"+testNamespace)
	eq(t, len(pages), 2)
	for _, r := range pages {
		eq(t, r.query["label"], []string{"bugnet=1", "placement=p1"})
		eq(t, r.query["stage"], []string{"SCHEDULING", "RUNNING"})
	}
	eq(t, pages[1].query.Get("cursor"), "2")
	eq(t, got[0].Labels, ours)
	eq(t, got[0].Status.Stage, "RUNNING")

	// A relative link is read against the page that gave it
	f.mu.Lock()
	f.nextLink = "?cursor=2&label=bugnet%3D1&label=placement%3Dp1&stage=SCHEDULING&stage=RUNNING"
	f.mu.Unlock()
	got, err = p.jobs.listJobs(context.Background(), ours, []string{"SCHEDULING", "RUNNING"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, idsOf(got), want)
}

func TestHFListJobsRefusesPagesOffTheJobsAPI(t *testing.T) {
	for _, next := range []string{
		"https://evil.example/api/jobs/" + testNamespace + "?cursor=2",
		"http://huggingface.co/api/jobs/" + testNamespace + "?cursor=2",
		"/api/whoami-v2?cursor=2",
		"//evil.example/api/jobs/x",
		"../../whoami-v2",
	} {
		f := newFake(t)
		f.pageSize = 1
		f.nextLink = next
		p := f.provider(t, testOptions())
		f.addJob("RUNNING", map[string]string{"bugnet": "1"})
		f.addJob("RUNNING", map[string]string{"bugnet": "1"})
		_, err := p.jobs.listJobs(context.Background(), map[string]string{"bugnet": "1"}, nil)
		if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInternal {
			t.Errorf("a next page at %q gave %v, want it refused", next, err)
		}
		if got := len(f.all()); got != 1 {
			t.Errorf("a next page at %q was read, %d requests", next, got)
		}
		eq(t, slices.Compact(f.dialed()), []string{"huggingface.co:443"})
	}
}

func TestHFListJobsStopsAnEndlessList(t *testing.T) {
	f := newFake(t)
	f.pageSize = 1
	p := f.provider(t, testOptions())
	f.addJob("RUNNING", nil)
	f.addJob("RUNNING", nil)
	f.nextLink = jobsURL(nil, testNamespace) + "?cursor=0"
	_, err := p.jobs.listJobs(context.Background(), nil, nil)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInternal {
		t.Fatalf("an endless list gave %v", err)
	}
	eq(t, len(f.all()), maxPages)
}

func TestHFNextLink(t *testing.T) {
	base := jobsURL(nil, testNamespace) + "?label=a%3D1"
	cases := []struct{ link, want string }{
		{"", ""},
		{`<https://huggingface.co/api/jobs/x?cursor=2>; rel="next"`,
			"https://huggingface.co/api/jobs/x?cursor=2"},
		{`<https://huggingface.co/api/jobs/x?cursor=1>; rel="prev", ` +
			`<https://huggingface.co/api/jobs/x?cursor=3>; rel="next"`,
			"https://huggingface.co/api/jobs/x?cursor=3"},
		{`<?cursor=4>; rel=next`, jobsURL(nil, testNamespace) + "?cursor=4"},
		{`<https://huggingface.co/api/jobs/x?cursor=5>; rel="last next"`,
			"https://huggingface.co/api/jobs/x?cursor=5"},
		{`<https://huggingface.co/api/jobs/x?cursor=6>; rel="prev"`, ""},
		{`<https://huggingface.co/api/jobs/x?cursor=7>; title="next"`, ""},
	}
	for _, tc := range cases {
		h := http.Header{}
		if tc.link != "" {
			h.Set("Link", tc.link)
		}
		got, err := nextLink(base, h)
		if err != nil || got != tc.want {
			t.Errorf("nextLink(%q) = %q, %v, want %q", tc.link, got, err, tc.want)
		}
	}
}
