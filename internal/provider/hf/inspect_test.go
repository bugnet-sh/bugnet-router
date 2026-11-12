package hf

import (
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// refTo is the ref Create would give a Job of the fake
func refTo(j *fakeJob) provider.Ref {
	return provider.Ref{Provider: "hf", ID: j.id, Meta: map[string]string{
		"job_id": j.id, "created_at": j.createdAt.Format("2006-01-02T15:04:05Z"),
		"timeout": "5400", "secret": j.token, "arch": "amd64",
	}}
}

func TestHFInspect(t *testing.T) {
	const (
		starting = provider.StateStarting
		running  = provider.StateRunning
		failed   = provider.StateFailed
		gone     = provider.StateGone
	)
	cases := []struct {
		name, stage, message, cancelReason string
		state                              provider.State
		kill                               string
	}{
		{"scheduling", "SCHEDULING", "", "", starting, ""},
		{"running", "RUNNING", "", "", running, ""},
		{"completed", "COMPLETED", "", "", gone, "completed"},
		{"canceled", "CANCELED", "", "", gone, "canceled"},
		{"deleted", "DELETED", "", "", gone, "deleted"},
		{"an error", "ERROR", "exit code 137", "", failed, "error"},
		{"timed out", "CANCELED", "Job timeout", "", gone, "timeout"},
		{"timed out on an error", "ERROR", "Job timeout", "", failed, "timeout"},
		{"timed out while running", "RUNNING", "Job timeout", "", running, "timeout"},
		{"a message that only holds the timeout's words", "ERROR", "echo Job timeout", "",
			failed, "error"},
		{"no credits", "CANCELED", "", "NO_CREDITS", gone, "no_credits"},
		{"no credits on an error", "ERROR", "", "NO_CREDITS", gone, "no_credits"},
		{"no subscription on an error", "ERROR", "", "NO_SUBSCRIPTION", gone, "no_subscription"},
		{"the spend limit", "CANCELED", "Resource group spend limit reached", "SPEND_LIMIT",
			gone, "spend_limit"},
		{"the spend limit on an error", "ERROR", "", "SPEND_LIMIT", gone, "spend_limit"},
		{"the resource group limit on an error", "ERROR", "", "RESOURCE_GROUP_LIMIT", gone,
			"resource_group_limit"},
		{"a cancel reason HF adds later", "CANCELED", "", "MAINTENANCE", gone, "maintenance"},
		{"another reason on an error", "ERROR", "", "OOM_KILLED", failed, "oom_killed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, testOptions())
			j := f.addJob(tc.stage, map[string]string{"bugnet": "1"})
			f.set(j, func(j *fakeJob) { j.message, j.cancelReason = tc.message, tc.cancelReason })
			st, err := p.Inspect(t.Context(), refTo(j))
			if err != nil {
				t.Fatal(err)
			}
			eq(t, st, provider.Status{State: tc.state, Reason: tc.message, KillReason: tc.kill})
			eq(t, f.requestsTo("GET", "/api/jobs/"+testNamespace+"/"+j.id)[0].method, "GET")
		})
	}
}

func TestHFInspectGoneAndUnknown(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	// HF keeps ended Jobs, so a Job it no longer has is HF's doing
	st, err := p.Inspect(t.Context(), provider.Ref{Provider: "hf", ID: "6911deadbeef"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, st, provider.Status{State: provider.StateGone, KillReason: "not_found"})

	j := f.addJob("SUSPENDED", nil)
	_, err = p.Inspect(t.Context(), refTo(j))
	if code, _ := codeOf(err); code != provider.CodeInternal ||
		!strings.Contains(err.Error(), `"SUSPENDED"`) {
		t.Fatalf("an unknown stage gave %v, want internal naming it", err)
	}

	f.inject(jobRoute, fault{status: 500, message: "Internal Error", times: 1})
	if _, err = p.Inspect(t.Context(), refTo(j)); err == nil {
		t.Fatal("a failed read gave a status")
	}
	f.inject(jobRoute, fault{status: 401, message: "Invalid credentials", times: 1})
	_, err = p.Inspect(t.Context(), refTo(j))
	if code, _ := codeOf(err); code != provider.CodeAuth {
		t.Fatalf("a refused read gave %v", err)
	}

	// A ref this adapter never made names no Job of the namespace
	calls := len(f.all())
	for _, ref := range []provider.Ref{{Provider: "lium", ID: j.id}, {Provider: "hf"}} {
		st, err := p.Inspect(t.Context(), ref)
		if err != nil || st.State != provider.StateGone {
			t.Fatalf("Inspect of %+v gave %+v, %v", ref, st, err)
		}
	}
	eq(t, len(f.all()), calls)
}

func TestHFInspectBoundsTheMessage(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	j := f.addJob("ERROR", nil)
	f.set(j, func(j *fakeJob) {
		j.message = fakeToken + strings.Repeat("m", 4096)
		j.cancelReason = strings.Repeat("R", 4096)
	})
	st, err := p.Inspect(t.Context(), refTo(j))
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Reason) > maxMessageBytes+3 || len(st.KillReason) > maxMessageBytes+3 {
		t.Fatalf("a long message gave a %d byte reason and a %d byte kill reason",
			len(st.Reason), len(st.KillReason))
	}
	for _, w := range tokenWords() {
		if strings.Contains(st.Reason, w) || strings.Contains(st.KillReason, w) {
			t.Fatal("the status carries the Jobs token")
		}
	}
}
