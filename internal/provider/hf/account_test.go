package hf

import (
	"context"
	"log/slog"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// accountFake is an adapter on an empty fake namespace, its error log in logs
func accountFake(t *testing.T) (*fakeHF, *Provider, *syncBuffer) {
	t.Helper()
	f := newFake(t)
	logs := &syncBuffer{}
	opts := testOptions()
	opts.Log = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelError}))
	return f, f.provider(t, opts), logs
}

func TestHFReadLimits(t *testing.T) {
	ctx := context.Background()
	listPath := "/api/jobs/" + testNamespace
	stages := []string{"SCHEDULING", "RUNNING", "COMPLETED", "CANCELED", "DELETED", "ERROR"}
	t.Run("live Jobs of every label", func(t *testing.T) {
		f, p, _ := accountFake(t)
		var _ provider.LimitsReader = p
		got, err := p.ReadLimits(ctx)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, got, provider.Limits{})
		for _, stage := range stages {
			f.addJob(stage, map[string]string{"bugnet": "1"})
		}
		// Jobs the router did not label are the account's too
		f.addJob("RUNNING", nil)
		f.addJob("SCHEDULING", map[string]string{"team": "research"})
		got, err = p.ReadLimits(ctx)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, got, provider.Limits{Active: 4})
		reqs := f.requestsTo("GET", listPath)
		q := reqs[len(reqs)-1].query
		eq(t, q["stage"], []string{"SCHEDULING", "RUNNING"})
		eq(t, len(q["label"]), 0)
	})
	t.Run("paged lists", func(t *testing.T) {
		f, p, _ := accountFake(t)
		f.pageSize = 2
		for range 5 {
			f.addJob("RUNNING", nil)
		}
		for _, stage := range stages[2:] {
			f.addJob(stage, nil)
		}
		got, err := p.ReadLimits(ctx)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, got, provider.Limits{Active: 5})
		eq(t, len(f.requestsTo("GET", listPath)), 3)
		// A list whose filters HF ignores still counts the live Jobs alone
		f.unfiltered = true
		if got, err = p.ReadLimits(ctx); err != nil {
			t.Fatal(err)
		}
		eq(t, got, provider.Limits{Active: 5})
		eq(t, len(f.requestsTo("GET", listPath)), 3+5)
		// A Job listed on two pages counts once
		f.unfiltered = false
		f.mu.Lock()
		f.nextLink = "https://huggingface.co" + listPath + "?stage=RUNNING&cursor=1"
		f.mu.Unlock()
		f.inject(listRoute, fault{after: 2, status: 200, raw: "[]"})
		if got, err = p.ReadLimits(ctx); err != nil {
			t.Fatal(err)
		}
		eq(t, got, provider.Limits{Active: 3})
	})
	t.Run("a budget refusal reads as no Jobs", func(t *testing.T) {
		f, p, _ := accountFake(t)
		f.addJob("RUNNING", nil)
		f.inject(listRoute, fault{status: 402, message: "Payment required", times: 1})
		got, err := p.ReadLimits(ctx)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, got, provider.Limits{})
	})
	t.Run("failed lists give no limits", func(t *testing.T) {
		for _, ft := range []fault{
			{status: 401, message: "Invalid credentials"},
			{status: 403, message: "This organization is suspended"},
			{status: 429, message: "slow down"},
			{status: 500, message: "Internal Error"},
			{drop: true},
			{status: 200, raw: "not json"},
			// A later page failing gives no count from the pages before it
			{status: 503, message: "Service Unavailable", after: 1},
		} {
			f, p, _ := accountFake(t)
			f.pageSize = 1
			f.addJob("RUNNING", nil)
			f.addJob("RUNNING", nil)
			f.inject(listRoute, ft)
			if got, err := p.ReadLimits(ctx); err == nil || got != (provider.Limits{}) {
				t.Fatalf("%+v gave %+v, %v", ft, got, err)
			}
		}
	})
}
