package lium

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestLiumListOwned(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	other := map[string]string{"bugnet": "1", "bugnet_deployment": "main",
		"placement": "7c1d2e3f-4a5b-4c6d-8e7f-8091a2b3c4d5"}
	probe := map[string]string{"bugnet": "1", "bugnet_deployment": "main",
		"bugnet_probe": "bench", "probe": "6a0c2d1e-9f8b-4c7d-a6e5-4f3b2a1c0d9e"}
	a := f.addPod(fakePod{name: podName(freshKey(), placementLabels()), status: "RUNNING",
		removal: "2026-11-11T12:20:00Z"})
	b := f.addPod(fakePod{name: podName(freshKey(), other), status: "PENDING"})
	q := f.addPod(fakePod{name: podName(freshKey(), probe), status: "FAILED"})
	check := f.addPod(fakePod{name: podName(freshKey(),
		map[string]string{"bugnet_image_check": "1"}), status: "STOPPED"})
	foreign := f.addPod(fakePod{name: "my-notebook", status: "RUNNING"})
	f.addPod(fakePod{name: podName(freshKey(), placementLabels()), status: "DELETING"})

	ids := func(labels map[string]string) []string {
		t.Helper()
		owned, err := p.ListOwned(context.Background(), labels)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, s := range owned {
			out = append(out, s.Ref.ID)
		}
		slices.Sort(out)
		return out
	}
	sorted := func(v ...string) []string { slices.Sort(v); return v }
	eq(t, ids(map[string]string{"bugnet": "1", "bugnet_deployment": "main"}),
		sorted(a.id, b.id, q.id))
	eq(t, ids(placementLabels()), []string{a.id})
	eq(t, ids(map[string]string{"bugnet_image_check": "1"}), []string{check.id})
	eq(t, ids(map[string]string{"bugnet": "1", "bugnet_deployment": "dev"}), []string(nil))
	// No labels lists every pod the key sees but those being deleted
	eq(t, ids(nil), sorted(a.id, b.id, q.id, check.id, foreign.id))

	owned, err := p.ListOwned(context.Background(), placementLabels())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, owned, []provider.Summary{{
		Ref: provider.Ref{Provider: "lium", ID: a.id, Meta: map[string]string{
			"pod_name": a.name, "node": a.node, "arch": "amd64",
			"created_at": "2026-11-11T11:59:00Z",
		}},
		Labels: placementLabels(), CreatedAt: time.Date(2026, 11, 11, 11, 59, 0, 0, time.UTC),
		State:     provider.StateRunning,
		ExpiresAt: time.Date(2026, 11, 11, 12, 20, 0, 0, time.UTC),
	}})
	all, err := p.ListOwned(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range all {
		switch s.Ref.ID {
		case foreign.id:
			if s.Labels != nil {
				t.Fatalf("a pod of another name got labels %v", s.Labels)
			}
		case q.id:
			eq(t, s.State, provider.StateFailed)
		case b.id:
			eq(t, s.State, provider.StateStarting)
		case check.id:
			eq(t, s.State, provider.StateStopped)
		}
	}

	// An account with no balance and no pods lists none
	f.inject(podsRoute, fault{status: 402, code: "BALANCE_REQUIRED", times: 1})
	owned, err = p.ListOwned(context.Background(), nil)
	if err != nil || len(owned) != 0 {
		t.Fatalf("BALANCE_REQUIRED gave %v, %v", owned, err)
	}
	f.inject(podsRoute, fault{status: 503, code: "upstream_unavailable", times: 1})
	if _, err := p.ListOwned(context.Background(), nil); err == nil {
		t.Fatal("a failed listing gave pods")
	}
}

func TestLiumDestroy(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	pod := f.addPod(fakePod{name: podName(testKey, placementLabels())})
	ref := podRef(pod, true)
	for range 2 {
		if err := p.Destroy(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
	}
	deletes := f.requestsTo("DELETE", "/pods/"+pod.id)
	eq(t, len(deletes), 2)
	eq(t, deletes[0].query.Get("expected_name"), pod.name)
	if _, ok := f.pod(pod.id); ok {
		t.Fatal("the pod is still there")
	}

	// A ref without the pod name deletes by id alone
	bare := f.addPod(fakePod{name: "renamed"})
	if err := p.Destroy(context.Background(), provider.Ref{Provider: "lium",
		ID: bare.id}); err != nil {
		t.Fatal(err)
	}
	if f.requestsTo("DELETE", "/pods/"+bare.id)[0].query.Has("expected_name") {
		t.Fatal("a ref without a pod name sent expected_name")
	}

	// A pod renamed under the ref is never deleted
	kept := f.addPod(fakePod{name: "someone-else"})
	kref := podRef(kept, true)
	kref.Meta["pod_name"] = "bn." + testKey
	if err := p.Destroy(context.Background(), kref); err == nil {
		t.Fatal("a pod of another name was deleted")
	}
	if _, ok := f.pod(kept.id); !ok {
		t.Fatal("the pod of another name is gone")
	}
	if err := p.Destroy(context.Background(), provider.Ref{Provider: "e2b",
		ID: "x"}); err != nil {
		t.Fatal(err)
	}
}

func TestLiumSetLifetime(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	pod := f.addPod(fakePod{created: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)})
	ref := podRef(pod, true)
	until := time.Date(2026, 11, 11, 13, 35, 0, 500, time.FixedZone("CET", 3600))
	if err := p.SetLifetime(context.Background(), ref, until); err != nil {
		t.Fatal(err)
	}
	posts := f.requestsTo("POST", "/pods/"+pod.id+"/schedule-removal")
	eq(t, len(posts), 1)
	eq(t, string(posts[0].body), `{"removal_scheduled_at":"2026-11-11T12:35:00Z"}`)
	st, err := p.Inspect(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, st.ExpiresAt, until.UTC().Truncate(time.Second))

	// At most max_lifetime from creation, 2 hours here
	err = p.SetLifetime(context.Background(), ref, pod.created.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	err = p.SetLifetime(context.Background(), ref, pod.created.Add(2*time.Hour+time.Second))
	wantCode(t, err, provider.CodeInvalid, false)

	// A ref without created_at reads it from the pod
	bare := provider.Ref{Provider: "lium", ID: pod.id}
	err = p.SetLifetime(context.Background(), bare, pod.created.Add(2*time.Hour+time.Minute))
	wantCode(t, err, provider.CodeInvalid, false)
	if err := p.SetLifetime(context.Background(), bare, pod.created.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// A destroyed pod is not found
	if err := p.Destroy(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	err = p.SetLifetime(context.Background(), ref, pod.created.Add(time.Hour))
	wantCode(t, err, provider.CodeNotFound, false)
	err = p.SetLifetime(context.Background(), bare, pod.created.Add(time.Hour))
	wantCode(t, err, provider.CodeNotFound, false)
	err = p.SetLifetime(context.Background(), provider.Ref{Provider: "hf", ID: "x"},
		time.Now())
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeNotFound ||
		!strings.Contains(err.Error(), "not a lium pod") {
		t.Fatalf("a foreign ref gave %v", err)
	}
}
