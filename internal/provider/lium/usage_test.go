package lium

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// usageDay is the window every usage test reads, the day the fake's pods are made in
var usageDay = time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)

// deploymentName is the pod name of a router placement of deployment d
func deploymentName(d string) string {
	return podName(freshKey(), map[string]string{
		"bugnet": "1", "bugnet_deployment": d, "placement": "0b8d1c2e-3f4a-4b5c-8d6e-7f8091a2b3c4",
	})
}

// liumRefs are refs of the pod ids
func liumRefs(ids ...string) []provider.Ref {
	out := make([]provider.Ref, 0, len(ids))
	for _, id := range ids {
		out = append(out, provider.Ref{Provider: "lium", ID: id})
	}
	return out
}

func TestLiumReadUsage(t *testing.T) {
	ctx := context.Background()
	from, to := usageDay, usageDay.AddDate(0, 0, 1)
	t.Run("each ref's record, then our pods not in refs", func(t *testing.T) {
		f := newFake(t)
		p := f.provider(t, testOptions(), time.Now)
		var _ provider.UsageReader = p
		ended := f.addPod(fakePod{name: deploymentName("main")})
		live := f.addPod(fakePod{name: deploymentName("main")})
		untotalled := f.addPod(fakePod{name: "renamed-by-hand"})
		f.setRecord(ended.id, map[string]any{
			"id": ended.id, "pod_name": ended.name, "removed": true,
			"removed_at": "2026-11-11T12:20:00Z", "close_reason": "user_initiated",
			"billed_seconds": 600, "total": 0.05, "price_segments": []any{}, "image": "x",
		})
		f.setRecord(live.id, map[string]any{
			"id": live.id, "pod_name": live.name, "removed": false, "removed_at": nil,
			"billed_seconds": "120.4", "total": "0.0125",
		})
		f.setRecord(untotalled.id, map[string]any{
			"pod_name": 7, "removed_at": "2026-11-11T12:20:00Z", "billed_seconds": 30,
			"total": nil,
		})
		// A pod Lium keeps no record of, another provider's sandbox and a repeat are skipped
		refs := append(liumRefs(ended.id, live.id, untotalled.id, "no-such-pod", ended.id),
			provider.Ref{Provider: "boat", ID: "sb-1"})
		u, err := p.ReadUsage(ctx, from, to, refs, "")
		if err != nil {
			t.Fatal(err)
		}
		eq(t, u, provider.Usage{
			Sandboxes: []provider.SandboxUsage{
				{ID: ended.id, Seconds: 600, CostMicros: 50_000, Deployment: "main"},
				{ID: live.id, Seconds: 120, CostMicros: 12_500, Deployment: "main"},
				{ID: untotalled.id, Seconds: 30, CostMicros: -1},
			},
			TotalMicros: -1, Complete: false, Attributed: true, Next: "l:",
		})
		eq(t, len(f.requestsTo("GET", "/pods/no-such-pod/record")), 1)
		eq(t, len(f.requestsTo("GET", "/pods/"+ended.id+"/record")), 1)
		eq(t, len(f.requestsTo("GET", "/pods/sb-1/record")), 0)

		// The listing names our pods made in the window that are not in refs
		day := usageDay.Add(9 * time.Hour)
		other := f.addPod(fakePod{name: deploymentName("other"), created: day})
		gone := f.addPod(fakePod{name: deploymentName("main"), created: day, status: "DELETING"})
		f.addPod(fakePod{name: deploymentName("main"), created: usageDay.Add(-time.Minute)})
		f.addPod(fakePod{name: deploymentName("main"), created: to})
		f.addPod(fakePod{name: podName(freshKey(), map[string]string{
			"bugnet_image_check": "1"}), created: day})
		f.addPod(fakePod{name: "someone-else", created: day})
		u, err = p.ReadUsage(ctx, from, to, refs, "l:")
		if err != nil {
			t.Fatal(err)
		}
		want := []provider.SandboxUsage{
			{ID: other.id, CostMicros: -1, Deployment: "other"},
			{ID: gone.id, CostMicros: -1, Deployment: "main"},
		}
		slices.SortFunc(want, func(a, b provider.SandboxUsage) int {
			return strings.Compare(a.ID, b.ID)
		})
		eq(t, u, provider.Usage{
			Sandboxes: want, TotalMicros: -1, Complete: true, Attributed: true,
		})
	})
	t.Run("every record removed is complete", func(t *testing.T) {
		f := newFake(t)
		p := f.provider(t, testOptions(), time.Now)
		pod := f.addPod(fakePod{name: deploymentName("main")})
		f.setRecord(pod.id, map[string]any{"removed": true, "billed_seconds": 1, "total": 0})
		u, err := p.ReadUsage(ctx, from, to, liumRefs(pod.id), "")
		if err != nil {
			t.Fatal(err)
		}
		eq(t, u.Complete, true)
		eq(t, u.Sandboxes, []provider.SandboxUsage{{ID: pod.id, Seconds: 1}})
		// A record that says nothing of its removal may still grow
		f.setRecord(pod.id, map[string]any{"billed_seconds": 1, "total": 0})
		if u, err = p.ReadUsage(ctx, from, to, liumRefs(pod.id), ""); err != nil {
			t.Fatal(err)
		}
		eq(t, u.Complete, false)
	})
	t.Run("refs come 25 a page", func(t *testing.T) {
		f := newFake(t)
		p := f.provider(t, testOptions(), time.Now)
		var ids []string
		for i := range 27 {
			id := fmt.Sprintf("pod-%02d", i)
			ids = append(ids, id)
			f.setRecord(id, map[string]any{"removed": true, "billed_seconds": i, "total": 0.001})
		}
		slices.Reverse(ids)
		u, err := p.ReadUsage(ctx, from, to, liumRefs(ids...), "")
		if err != nil {
			t.Fatal(err)
		}
		eq(t, len(u.Sandboxes), 25)
		eq(t, u.Sandboxes[0].ID, "pod-00")
		eq(t, u.Next, "r25")
		u, err = p.ReadUsage(ctx, from, to, liumRefs(ids...), u.Next)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, u.Sandboxes, []provider.SandboxUsage{
			{ID: "pod-25", Seconds: 25, CostMicros: 1000},
			{ID: "pod-26", Seconds: 26, CostMicros: 1000},
		})
		eq(t, u.Next, "l:")
	})
	t.Run("a failed read fails the page", func(t *testing.T) {
		f := newFake(t)
		p := f.provider(t, testOptions(), time.Now)
		pod := f.addPod(fakePod{name: deploymentName("main")})
		f.setRecord(pod.id, map[string]any{"removed": true})
		f.inject("GET /pods/{id}/record", fault{status: 500, code: "internal_error", times: 1})
		if _, err := p.ReadUsage(ctx, from, to, liumRefs(pod.id), ""); err == nil {
			t.Fatal("a failed record read gave usage")
		}
		f.inject("GET /pods/{id}/record", fault{status: 200, raw: `["a list"]`, times: 1})
		_, err := p.ReadUsage(ctx, from, to, liumRefs(pod.id), "")
		wantCode(t, err, provider.CodeInternal, false)
		f.inject("GET /pods", fault{status: 429, code: "rate_limited", times: 1})
		_, err = p.ReadUsage(ctx, from, to, nil, "l:")
		wantCode(t, err, provider.CodeRateLimited, true)
		// An account with no balance and no pods lists none
		f.inject("GET /pods", fault{status: 402, code: "BALANCE_REQUIRED", times: 1})
		u, err := p.ReadUsage(ctx, from, to, nil, "l:")
		if err != nil || len(u.Sandboxes) != 0 || u.Next != "" {
			t.Fatalf("got %+v, %v", u, err)
		}
	})
}
