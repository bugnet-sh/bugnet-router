package hf

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const cancelRoute = "POST /api/jobs/{ns}/{id}/cancel"

var _ provider.MaxUntiler = (*Provider)(nil)

// ownedJobs adds Jobs in every stage, ours and another deployment's, and returns our live ids
func ownedJobs(f *fakeHF, ours map[string]string) []string {
	var live []string
	for i, stage := range []string{"RUNNING", "SCHEDULING", "CANCELED", "ERROR", "RUNNING",
		"COMPLETED", "DELETED", "SCHEDULING"} {
		labels := maps.Clone(ours)
		labels["placement"] = "p" + string(rune('a'+i))
		labels["bugnet_key"] = freshKey()
		j := f.addJob(stage, labels)
		if liveStage(stage) {
			live = append(live, j.id)
		}
	}
	f.addJob("RUNNING", map[string]string{"bugnet": "1", "bugnet_deployment": "other"})
	f.addJob("RUNNING", nil)
	return live
}

func TestHFListOwned(t *testing.T) {
	f := newFake(t)
	f.pageSize = 2
	p := f.provider(t, testOptions())
	ours := map[string]string{"bugnet": "1", "bugnet_deployment": "main"}
	want := ownedJobs(f, ours)
	got, err := p.ListOwned(t.Context(), ours)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, s := range got {
		ids = append(ids, s.Ref.ID)
		j := f.job(s.Ref.ID)
		state := provider.StateRunning
		if j.stage == "SCHEDULING" {
			state = provider.StateStarting
		}
		// A listed Job's ref carries no token, which only its create derives
		eq(t, s, provider.Summary{
			Ref: provider.Ref{Provider: "hf", ID: j.id, Meta: map[string]string{
				"job_id": j.id, "created_at": j.createdAt.Truncate(time.Second).Format(
					time.RFC3339), "timeout": "5400", "arch": "amd64",
			}},
			Labels: j.labels, CreatedAt: j.createdAt.Round(0), State: state,
		})
	}
	eq(t, ids, want)
	pages := lists(f)
	if len(pages) < 2 {
		t.Fatalf("the listing read %d pages", len(pages))
	}
	for _, r := range pages {
		eq(t, r.query["label"], []string{"bugnet=1", "bugnet_deployment=main"})
		eq(t, r.query["stage"], []string{"SCHEDULING", "RUNNING"})
	}
}

// TestHFListOwnedFiltersItself lists against a fake that ignores every filter
func TestHFListOwnedFiltersItself(t *testing.T) {
	f := newFake(t)
	f.unfiltered = true
	f.pageSize = 3
	p := f.provider(t, testOptions())
	ours := map[string]string{"bugnet": "1", "bugnet_deployment": "main"}
	want := ownedJobs(f, ours)
	// A Job whose id is not one plain path segment is never swept
	f.set(f.addJob("RUNNING", maps.Clone(ours)), func(j *fakeJob) { j.id = "6911/../x" })
	// A Job listed twice across pages is one sandbox
	f.mu.Lock()
	f.jobs = append(f.jobs, f.jobs[0])
	f.mu.Unlock()
	got, err := p.ListOwned(t.Context(), ours)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, s := range got {
		ids = append(ids, s.Ref.ID)
	}
	eq(t, ids, want)
}

func TestHFListOwnedRefuses(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	_, err := p.ListOwned(t.Context(), map[string]string{"bugnet": "1", "a.b": "c"})
	if code, _ := codeOf(err); code != provider.CodeInvalid {
		t.Fatalf("a label HF takes no filter for gave %v", err)
	}
	eq(t, len(f.all()), 0)
	f.addJob("RUNNING", map[string]string{"bugnet": "1"})
	f.inject(listRoute, fault{status: 500, message: "Internal Error", times: 1})
	got, err := p.ListOwned(t.Context(), map[string]string{"bugnet": "1"})
	if err == nil || got != nil {
		t.Fatalf("a failed listing gave %+v, %v", got, err)
	}
}

func TestHFDestroy(t *testing.T) {
	ctx := t.Context()
	f := newFake(t)
	p := f.provider(t, testOptions())
	j := f.addJob("RUNNING", map[string]string{"bugnet": "1"})
	if err := p.Destroy(ctx, refTo(j)); err != nil {
		t.Fatal(err)
	}
	eq(t, f.stageOf(j.id), "CANCELED")
	cancels := f.requestsTo("POST", "/api/jobs/"+testNamespace+"/"+j.id+"/cancel")
	eq(t, len(cancels), 1)
	eq(t, len(cancels[0].body), 0)
	// HF answers a cancel of an ended Job with the Job, or an error the Job's read explains
	if err := p.Destroy(ctx, refTo(j)); err != nil {
		t.Fatal(err)
	}
	f.cancelEnded = 400
	if err := p.Destroy(ctx, refTo(j)); err != nil {
		t.Fatalf("a cancel refused for an ended Job gave %v", err)
	}
	for _, stage := range []string{"COMPLETED", "ERROR", "DELETED"} {
		ended := f.addJob(stage, nil)
		if err := p.Destroy(ctx, refTo(ended)); err != nil {
			t.Fatalf("a destroy of a %s Job gave %v", stage, err)
		}
	}
	if err := p.Destroy(ctx, provider.Ref{Provider: "hf", ID: "6911deadbeef"}); err != nil {
		t.Fatalf("a destroy of a Job HF no longer has gave %v", err)
	}

	// A cancel that failed while the Job lives is the caller's to retry
	live := f.addJob("SCHEDULING", nil)
	f.inject(cancelRoute, fault{status: 500, message: "Internal Error", times: 1})
	err := p.Destroy(ctx, refTo(live))
	if code, _ := codeOf(err); code != provider.CodeInternal {
		t.Fatalf("a failed cancel of a live Job gave %v", err)
	}
	f.inject(cancelRoute, fault{status: 500, message: "Internal Error", times: 1})
	f.inject(jobRoute, fault{status: 503, message: "Unavailable", times: 1})
	err = p.Destroy(ctx, refTo(live))
	if code, _ := codeOf(err); code != provider.CodeInternal || err == nil {
		t.Fatalf("a failed cancel and read gave %v", err)
	}
	f.inject(cancelRoute, fault{status: 401, message: "Invalid credentials", times: 1})
	err = p.Destroy(ctx, refTo(live))
	if code, _ := codeOf(err); code != provider.CodeAuth {
		t.Fatalf("a refused cancel gave %v", err)
	}
	eq(t, f.stageOf(live.id), "SCHEDULING")

	calls := len(f.all())
	for _, ref := range []provider.Ref{{Provider: "lium", ID: live.id}, {Provider: "hf"}} {
		if err := p.Destroy(ctx, ref); err != nil {
			t.Fatalf("Destroy of %+v gave %v", ref, err)
		}
	}
	eq(t, len(f.all()), calls)
}

func TestHFMaxUntil(t *testing.T) {
	p, err := newProvider(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	ref := func(created, timeout string) provider.Ref {
		return provider.Ref{Provider: "hf", ID: "6911", Meta: map[string]string{
			"created_at": created, "timeout": timeout,
		}}
	}
	until, ok := p.MaxUntil(ref("2026-11-12T10:00:00Z", "5400"))
	eq(t, ok, true)
	// 90 minutes of timeout less the 10 minute margin
	eq(t, until, time.Date(2026, 11, 12, 11, 20, 0, 0, time.UTC))
	for _, r := range []provider.Ref{
		ref("", "5400"), ref("2026-11-12T10:00:00Z", ""), ref("soon", "5400"),
		ref("2026-11-12T10:00:00Z", "0"), ref("2026-11-12T10:00:00Z", "-60"),
		ref("2026-11-12T10:00:00Z", "5400.5"), ref("2026-11-12T10:00:00Z", "99999999999999"),
		{Provider: "lium", ID: "6911", Meta: ref("2026-11-12T10:00:00Z", "5400").Meta},
	} {
		if until, ok := p.MaxUntil(r); ok {
			t.Errorf("MaxUntil of %+v gave %v", r, until)
		}
	}
}

func TestHFSetLifetime(t *testing.T) {
	ctx := t.Context()
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref, err := p.Create(ctx, keyed())
	if err != nil {
		t.Fatal(err)
	}
	created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
	if err != nil {
		t.Fatal(err)
	}
	// The 20 minute lifetime with the hour of room
	end := created.Add(80 * time.Minute)
	until, ok := p.MaxUntil(ref)
	eq(t, ok, true)
	eq(t, until, end)
	if err := p.SetLifetime(ctx, ref, end); err != nil {
		t.Fatal(err)
	}
	calls := len(f.all())
	err = p.SetLifetime(ctx, ref, end.Add(time.Second))
	if code, failOver := codeOf(err); code != provider.CodeInvalid || failOver {
		t.Fatalf("an until past the Job's end gave %v, want invalid", err)
	}
	eq(t, len(f.all()), calls)

	// A ref without its end reads the end from the Job
	bare := provider.Ref{Provider: "hf", ID: ref.ID}
	if _, ok := p.MaxUntil(bare); ok {
		t.Fatal("a ref without its end gave one")
	}
	if err := p.SetLifetime(ctx, bare, end); err != nil {
		t.Fatal(err)
	}
	err = p.SetLifetime(ctx, bare, end.Add(time.Minute))
	if code, _ := codeOf(err); code != provider.CodeInvalid {
		t.Fatalf("an until past the end the Job shows gave %v, want invalid", err)
	}

	// An ended Job, or one HF no longer has, is not found
	f.set(f.job(ref.ID), func(j *fakeJob) { j.stage = "CANCELED" })
	for _, r := range []provider.Ref{ref, {Provider: "hf", ID: "6911deadbeef"},
		{Provider: "lium", ID: ref.ID}} {
		err := p.SetLifetime(ctx, r, time.Now().Add(time.Minute))
		if code, _ := codeOf(err); code != provider.CodeNotFound {
			t.Fatalf("SetLifetime of %+v gave %v, want not_found", r, err)
		}
	}
}

// TestHFSetLifetimeReadsOnlyForAnEnd lets an extension the ref's end allows pass whatever the read
func TestHFSetLifetimeReadsOnlyForAnEnd(t *testing.T) {
	ctx := t.Context()
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref, err := p.Create(ctx, keyed())
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Minute)
	f.inject(jobRoute, fault{status: 503, message: "Unavailable", times: 1})
	if err := p.SetLifetime(ctx, ref, until); err != nil {
		t.Fatalf("a failed read of a Job whose end the ref holds gave %v", err)
	}
	// A ref without its end has only the read to check it by
	f.inject(jobRoute, fault{status: 503, message: "Unavailable", times: 1})
	err = p.SetLifetime(ctx, provider.Ref{Provider: "hf", ID: ref.ID}, until)
	if code, _ := codeOf(err); code != provider.CodeInternal {
		t.Fatalf("a failed read of a Job with no end in its ref gave %v", err)
	}
	gone := cloneRef(ref)
	gone.ID = "6911deadbeef"
	err = p.SetLifetime(ctx, gone, until)
	if code, _ := codeOf(err); code != provider.CodeNotFound {
		t.Fatalf("a Job HF no longer has gave %v, want not_found", err)
	}
	// An extension holds the payer's wallet lock, so a 429's hold never delays one
	reads := len(jobReads(f, ref.ID))
	p.jobs.hold(time.Minute)
	hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := p.SetLifetime(hctx, ref, until); err != nil || time.Since(start) > time.Second {
		t.Fatalf("an extension under a 429's hold gave %v after %s", err, time.Since(start))
	}
	eq(t, len(jobReads(f, ref.ID)), reads)
}
