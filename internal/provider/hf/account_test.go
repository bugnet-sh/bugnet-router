package hf

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

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

// endedJob adds a Job HF ended in stage for reason, finished at end unless end is zero
func endedJob(f *fakeHF, stage, reason string, end time.Time) *fakeJob {
	j := f.addJob(stage, map[string]string{"bugnet": "1"})
	f.set(j, func(j *fakeJob) { j.cancelReason, j.finishedAt = reason, end })
	return j
}

// clock is a time the test moves, read by the adapter's Jobs client
type clock struct{ now time.Time }

func (c *clock) read() time.Time { return c.now }

// stopClock gives p's Jobs client a clock the test moves
func stopClock(p *Provider) *clock {
	c := &clock{now: time.Now()}
	p.jobs.now = c.read
	return c
}

// wantAccount checks the account check's answer, its Detail naming every part
func wantAccount(t *testing.T, p *Provider, block provider.Block, parts ...string) {
	t.Helper()
	acc, err := p.CheckAccount(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if acc.Block != block {
		t.Fatalf("got %+v, want block %q", acc, block)
	}
	for _, part := range parts {
		if !strings.Contains(acc.Detail, part) {
			t.Fatalf("detail %q does not name %q", acc.Detail, part)
		}
	}
	if len(parts) == 0 && acc.Detail != "" {
		t.Fatalf("detail %q, want none", acc.Detail)
	}
}

func TestHFCheckAccount(t *testing.T) {
	ctx := context.Background()
	authPath := "/api/jobs/" + testNamespace + "/auth-check/write"
	t.Run("a token that may write Jobs passes", func(t *testing.T) {
		f, p, logs := accountFake(t)
		var _ provider.AccountChecker = p
		wantAccount(t, p, provider.BlockNone)
		eq(t, logs.String(), "")
		reqs := f.all()
		eq(t, len(reqs), 1)
		eq(t, reqs[0].method+" "+reqs[0].host+reqs[0].path, "POST "+jobsHost+authPath)
		eq(t, reqs[0].header.Get("Authorization"), "Bearer "+fakeToken)
		eq(t, len(reqs[0].body), 0)
	})
	t.Run("a Job HF ended for credit is unfunded", func(t *testing.T) {
		for _, stage := range []string{"CANCELED", "ERROR"} {
			for _, reason := range []string{"NO_CREDITS", "NO_SUBSCRIPTION"} {
				f, p, _ := accountFake(t)
				j := endedJob(f, stage, reason, time.Time{})
				if _, err := p.Inspect(ctx, refTo(j)); err != nil {
					t.Fatal(err)
				}
				wantAccount(t, p, provider.BlockUnfunded, j.id, strings.ToLower(reason))
			}
		}
	})
	t.Run("every Jobs read is seen", func(t *testing.T) {
		f, p, _ := accountFake(t)
		j := endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
		if err := p.WaitReady(ctx, refTo(j)); !isCode(err, provider.CodeUnfunded) {
			t.Fatalf("WaitReady = %v", err)
		}
		wantAccount(t, p, provider.BlockUnfunded, j.id)

		f, p, _ = accountFake(t)
		j = endedJob(f, "ERROR", "NO_SUBSCRIPTION", time.Time{})
		// A list whose filters HF ignores shows the ended Job too
		f.unfiltered = true
		if _, err := p.ListOwned(ctx, map[string]string{"bugnet": "1"}); err != nil {
			t.Fatal(err)
		}
		wantAccount(t, p, provider.BlockUnfunded, j.id, "no_subscription")

		// A Job HF names oddly still speaks for the account, its name quoted and cut
		f, p, _ = accountFake(t)
		j = endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
		f.show = func(job map[string]any) { job["id"] = strings.Repeat("a\nb", 100) }
		f.unfiltered = true
		if _, err := p.ListOwned(ctx, nil); err != nil {
			t.Fatal(err)
		}
		acc, err := p.CheckAccount(ctx)
		if err != nil || acc.Block != provider.BlockUnfunded ||
			acc.Detail != `hf ended Job "`+strings.Repeat(`a\nb`, 13)+`a" for no_credits` {
			t.Fatalf("got %+v, %v", acc, err)
		}
	})
	t.Run("a Job is held from when HF ended it for 10 minutes", func(t *testing.T) {
		f, p, _ := accountFake(t)
		c := stopClock(p)
		old := endedJob(f, "CANCELED", "NO_CREDITS", c.now.Add(-10*time.Minute-time.Second))
		if _, err := p.Inspect(ctx, refTo(old)); err != nil {
			t.Fatal(err)
		}
		wantAccount(t, p, provider.BlockNone)
		j := endedJob(f, "CANCELED", "NO_CREDITS", c.now.Add(-9*time.Minute))
		if _, err := p.Inspect(ctx, refTo(j)); err != nil {
			t.Fatal(err)
		}
		wantAccount(t, p, provider.BlockUnfunded, j.id)
		c.now = c.now.Add(time.Minute)
		wantAccount(t, p, provider.BlockUnfunded, j.id)
		c.now = c.now.Add(time.Second)
		wantAccount(t, p, provider.BlockNone)
	})
	t.Run("a Job with no end is held from when the adapter first saw it", func(t *testing.T) {
		f, p, _ := accountFake(t)
		c := stopClock(p)
		j := endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
		f.show = func(job map[string]any) {
			// An end HF writes in a shape the adapter cannot read is no end
			if job["id"] == j.id {
				job["finishedAt"] = "None"
			}
		}
		if _, err := p.Inspect(ctx, refTo(j)); err != nil {
			t.Fatal(err)
		}
		c.now = c.now.Add(9 * time.Minute)
		// Reading the Job again does not make it new
		if _, err := p.Inspect(ctx, refTo(j)); err != nil {
			t.Fatal(err)
		}
		c.now = c.now.Add(time.Minute)
		wantAccount(t, p, provider.BlockUnfunded, j.id)
		c.now = c.now.Add(time.Second)
		wantAccount(t, p, provider.BlockNone)
		// An end past the adapter's clock counts from the read
		late := endedJob(f, "CANCELED", "NO_CREDITS", c.now.Add(time.Hour))
		if _, err := p.Inspect(ctx, refTo(late)); err != nil {
			t.Fatal(err)
		}
		c.now = c.now.Add(10 * time.Minute)
		wantAccount(t, p, provider.BlockUnfunded, late.id)
		c.now = c.now.Add(time.Second)
		wantAccount(t, p, provider.BlockNone)
	})
	t.Run("limits HF enforces are logged and shown, never a block", func(t *testing.T) {
		for _, reason := range []string{"SPEND_LIMIT", "RESOURCE_GROUP_LIMIT"} {
			f, p, logs := accountFake(t)
			j := endedJob(f, "CANCELED", reason, time.Time{})
			if _, err := p.Inspect(ctx, refTo(j)); err != nil {
				t.Fatal(err)
			}
			wantAccount(t, p, provider.BlockNone, j.id, strings.ToLower(reason))
			if !strings.Contains(logs.String(), "level=ERROR") ||
				!strings.Contains(logs.String(), strings.ToLower(reason)) {
				t.Fatalf("log %q", logs.String())
			}
		}
		// Beside a Job ended for credit, the limit is shown with the block
		f, p, logs := accountFake(t)
		limit := endedJob(f, "CANCELED", "SPEND_LIMIT", time.Time{})
		credit := endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
		f.unfiltered = true
		if _, err := p.ListOwned(ctx, nil); err != nil {
			t.Fatal(err)
		}
		wantAccount(t, p, provider.BlockUnfunded, limit.id, credit.id, "spend_limit",
			"no_credits")
		if !strings.Contains(logs.String(), "spend_limit") {
			t.Fatalf("log %q", logs.String())
		}
	})
	t.Run("other ends say nothing of the account", func(t *testing.T) {
		f, p, logs := accountFake(t)
		for _, j := range []*fakeJob{
			endedJob(f, "CANCELED", "", time.Time{}),
			endedJob(f, "CANCELED", "MAINTENANCE", time.Time{}),
			endedJob(f, "ERROR", "OOM_KILLED", time.Time{}),
			endedJob(f, "COMPLETED", "", time.Time{}),
		} {
			if _, err := p.Inspect(ctx, refTo(j)); err != nil {
				t.Fatal(err)
			}
		}
		timedOut := endedJob(f, "CANCELED", "", time.Time{})
		f.set(timedOut, func(j *fakeJob) { j.message = jobTimeout })
		if _, err := p.Inspect(ctx, refTo(timedOut)); err != nil {
			t.Fatal(err)
		}
		wantAccount(t, p, provider.BlockNone)
		eq(t, logs.String(), "")
	})
	t.Run("the detail names three Jobs and counts the rest", func(t *testing.T) {
		f, p, _ := accountFake(t)
		for range 5 {
			endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
		}
		f.unfiltered = true
		if _, err := p.ListOwned(ctx, nil); err != nil {
			t.Fatal(err)
		}
		acc, err := p.CheckAccount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, acc.Block, provider.BlockUnfunded)
		if n := strings.Count(acc.Detail, "no_credits"); n != 3 ||
			!strings.HasSuffix(acc.Detail, "and 2 more") {
			t.Fatalf("detail %q", acc.Detail)
		}
	})
	t.Run("refused calls", func(t *testing.T) {
		for _, tc := range []struct {
			ft    fault
			block provider.Block
			word  string
		}{
			{fault{status: 401, message: "Invalid credentials"}, provider.BlockAuth,
				"Invalid credentials"},
			{fault{status: 403, message: "You don't have the rights to run Jobs"},
				provider.BlockAuth, "rights"},
			{fault{status: 403, message: "This organization is suspended"},
				provider.BlockBanned, "suspended"},
			{fault{status: 403, message: "Your account has been banned"}, provider.BlockBanned,
				"banned"},
			{fault{status: 402, message: "Payment required"}, provider.BlockUnfunded,
				"Payment required"},
			// An answer that echoes the token never carries it into the Detail
			{fault{status: 401, message: "Invalid credentials", echo: true}, provider.BlockAuth,
				"[token]"},
		} {
			f, p, _ := accountFake(t)
			f.inject(authCheckRoute, tc.ft)
			acc, err := p.CheckAccount(ctx)
			if err != nil || acc.Block != tc.block || !strings.Contains(acc.Detail, tc.word) {
				t.Fatalf("%d %s gave %+v, %v, want %s", tc.ft.status, tc.ft.message, acc, err,
					tc.block)
			}
			for _, w := range tokenWords() {
				if strings.Contains(acc.Detail, w) {
					t.Fatalf("detail %q carries the token", acc.Detail)
				}
			}
		}
	})
	t.Run("a 402 to any Jobs call is unfunded for 10 minutes", func(t *testing.T) {
		f, p, _ := accountFake(t)
		c := stopClock(p)
		f.inject(listRoute, fault{status: 402, message: "Payment required", times: 1})
		if _, err := p.ListOwned(ctx, nil); !isCode(err, provider.CodeUnfunded) {
			t.Fatalf("ListOwned = %v", err)
		}
		wantAccount(t, p, provider.BlockUnfunded, "/api/jobs/"+testNamespace,
			"Payment required")
		c.now = c.now.Add(10*time.Minute + time.Second)
		wantAccount(t, p, provider.BlockNone)
		// Each refusal of a route counts from its own answer
		f.inject(listRoute, fault{status: 402, message: "Payment required", times: 2})
		for range 2 {
			if _, err := p.ReadLimits(ctx); err != nil {
				t.Fatal(err)
			}
			c.now = c.now.Add(9 * time.Minute)
		}
		wantAccount(t, p, provider.BlockUnfunded, "Payment required")
		c.now = c.now.Add(time.Minute + time.Second)
		wantAccount(t, p, provider.BlockNone)
		// The hardware list goes without the token, so its refusal is not the account's
		f.inject("GET /api/jobs/hardware", fault{status: 402, message: "Payment required"})
		if err := p.loadHardware(ctx); !isCode(err, provider.CodeUnfunded) {
			t.Fatalf("loadHardware = %v", err)
		}
		wantAccount(t, p, provider.BlockNone)
	})
	t.Run("the sightings kept are bounded", func(t *testing.T) {
		f, p, _ := accountFake(t)
		for range maxSightings + 44 {
			endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
		}
		f.unfiltered = true
		if _, err := p.ListOwned(ctx, nil); err != nil {
			t.Fatal(err)
		}
		wantAccount(t, p, provider.BlockUnfunded, fmt.Sprintf("and %d more", maxSightings-3))
	})
	t.Run("failed calls prove nothing", func(t *testing.T) {
		for _, ft := range []fault{
			{status: 429, message: "slow down"},
			{status: 500, message: "Internal Error"},
			{status: 503, raw: "<html>503</html>"},
			{drop: true},
			{status: 307, raw: "moved", header: map[string]string{
				"Location": "https://evil.example/auth-check"}},
			{status: 400, message: "Invalid permission"},
		} {
			f, p, _ := accountFake(t)
			f.inject(authCheckRoute, ft)
			if acc, err := p.CheckAccount(ctx); err == nil || acc != (provider.Account{}) {
				t.Fatalf("%+v gave %+v, %v", ft, acc, err)
			}
		}
		// A namespace HF does not know is the settings' fault, not the account's
		f := newFake(t)
		opts := testOptions()
		opts.Namespace = "not-ours"
		acc, err := f.provider(t, opts).CheckAccount(ctx)
		if !isCode(err, provider.CodeInvalid) || acc != (provider.Account{}) {
			t.Fatalf("a wrong namespace gave %+v, %v", acc, err)
		}
	})
	t.Run("an unreadable answer", func(t *testing.T) {
		// HF documents no body for the check, so a 200 passes whatever it holds
		f, p, _ := accountFake(t)
		f.inject(authCheckRoute, fault{status: 200, raw: "not json"})
		wantAccount(t, p, provider.BlockNone)
		// A Job read that cannot be read says nothing of the account
		j := endedJob(f, "CANCELED", "NO_CREDITS", time.Time{})
		f.inject(jobRoute, fault{status: 200, raw: `{"id": "` + j.id + `", "status": 3`})
		if _, err := p.Inspect(ctx, refTo(j)); !isCode(err, provider.CodeInternal) {
			t.Fatalf("Inspect = %v", err)
		}
		wantAccount(t, p, provider.BlockNone)
	})
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
