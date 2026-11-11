package lium

import (
	"context"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/gpustatus"
)

// podRef is the ref of a fake pod, ready when readyAt is set
func podRef(pod *fakePod, ready bool) provider.Ref {
	ref := provider.Ref{Provider: "lium", ID: pod.id, Meta: map[string]string{
		"pod_name": pod.name, "node": pod.node, "arch": "amd64", "size": "lium-rtx3090",
		"created_at": pod.created.Format(time.RFC3339), "hourly_micros": "280000",
	}}
	if ready {
		ref.Meta["ready_at"] = "2026-11-11T12:00:30Z"
	}
	return ref
}

func TestLiumInspectMapsStates(t *testing.T) {
	type row struct {
		status string
		ready  bool
		moved  bool
		change func(*fakePod)
		state  provider.State
		reason string
		// events marks a state for which the pod's events are read
		events bool
	}
	offline := func(p *fakePod) { p.nodeOffline = true }
	outage := func(p *fakePod) { p.nodeOffline, p.outage = true, true }
	rows := map[string]row{
		"pending":        {status: "PENDING", state: "starting", reason: "PENDING"},
		"start pending":  {status: "START_PENDING", state: "starting", reason: "START_PENDING"},
		"reboot pending": {status: "REBOOT_PENDING", state: "starting", reason: "REBOOT_PENDING"},
		"pending after ready": {status: "PENDING", ready: true, state: "failed",
			reason: "PENDING lium_rebooted", events: true},
		"start after ready": {status: "START_PENDING", ready: true, state: "failed",
			reason: "START_PENDING lium_rebooted", events: true},
		"reboot after ready": {status: "REBOOT_PENDING", ready: true, state: "failed",
			reason: "REBOOT_PENDING lium_rebooted", events: true},
		"running": {status: "RUNNING", state: "running", reason: "RUNNING"},
		"running after ready": {status: "RUNNING", ready: true, state: "running",
			reason: "RUNNING"},
		"moved before ready": {status: "RUNNING", moved: true, state: "running",
			reason: "RUNNING"},
		"moved after ready": {status: "RUNNING", ready: true, moved: true, state: "failed",
			reason: "RUNNING lium_moved", events: true},
		"node offline": {status: "RUNNING", ready: true, change: offline, state: "degraded",
			reason: "RUNNING node_offline", events: true},
		"node outage": {status: "RUNNING", ready: true, change: outage, state: "degraded",
			reason: "RUNNING node_offline node_outage", events: true},
		"outage flag alone": {status: "RUNNING", ready: true,
			change: func(p *fakePod) { p.outage = true }, state: "degraded",
			reason: "RUNNING node_outage", events: true},
		"billing paused": {status: "RUNNING", ready: true,
			change: func(p *fakePod) { p.billingPaused = true }, state: "running",
			reason: "RUNNING billing_paused"},
		"disk paused": {status: "RUNNING", ready: true,
			change: func(p *fakePod) { p.diskPaused = true }, state: "running",
			reason: "RUNNING disk_paused"},
		"stopped": {status: "STOPPED", ready: true, state: "stopped", reason: "STOPPED",
			events: true},
		"stop pending": {status: "STOP_PENDING", ready: true, state: "stopped",
			reason: "STOP_PENDING", events: true},
		"failed": {status: "FAILED", ready: true, state: "failed", reason: "FAILED",
			events: true},
		"creation failed": {status: "CREATION_FAILED", state: "failed",
			reason: "CREATION_FAILED", events: true},
		"reboot failed": {status: "REBOOT_FAILED", ready: true, state: "failed",
			reason: "REBOOT_FAILED", events: true},
		"broken": {status: "BROKEN", ready: true, state: "failed", reason: "BROKEN",
			events: true},
		"broken while offline": {status: "BROKEN", ready: true, change: offline,
			state: "failed", reason: "BROKEN node_offline", events: true},
		"deleting": {status: "DELETING", ready: true, state: "gone", reason: "DELETING",
			events: true},
	}
	for name, r := range rows {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, testOptions(), time.Now)
			pod := f.addPod(fakePod{status: r.status, removal: "2026-11-11T12:20:00Z"})
			ref := podRef(pod, r.ready)
			if r.moved {
				f.setPod(pod.id, func(p *fakePod) { p.node = "node-elsewhere" })
			}
			if r.change != nil {
				f.setPod(pod.id, r.change)
			}
			f.addEvent(pod.id, fakeEvent{at: pod.created, eventType: "pod_created"})
			f.addEvent(pod.id, fakeEvent{at: pod.created.Add(time.Minute),
				eventType: "pod_lifecycle", reason: "executor_offline",
				fromStatus: "RUNNING", toStatus: "STOPPED"})
			st, err := p.Inspect(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, st.State, r.state)
			eq(t, st.Reason, r.reason)
			eq(t, st.ExpiresAt, time.Date(2026, 11, 11, 12, 20, 0, 0, time.UTC))
			read := len(f.requestsTo("GET", "/pods/"+pod.id+"/events"))
			if r.events {
				eq(t, read, 1)
				eq(t, st.KillReason, "executor_offline")
			} else {
				eq(t, read, 0)
				eq(t, st.KillReason, "")
			}
		})
	}
}

// The node_outage flag keeps reconcile's provider row from waiting out the degraded grace
func TestLiumInspectOutageIsDegraded(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	pod := f.addPod(fakePod{nodeOffline: true, outage: true})
	st, err := p.Inspect(context.Background(), podRef(pod, true))
	if err != nil {
		t.Fatal(err)
	}
	if st.State != provider.StateDegraded ||
		!gpustatus.HasWord(st.Reason, gpustatus.LiumFlagNodeOutage) {
		t.Fatalf("an outage gave %+v", st)
	}
}

func TestLiumInspectUnknownStatusIsAnError(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	pod := f.addPod(fakePod{status: "HIBERNATED"})
	_, err := p.Inspect(context.Background(), podRef(pod, true))
	wantCode(t, err, provider.CodeInternal, false)
}

func TestLiumInspectGone(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	pod := f.addPod(fakePod{})
	ref := podRef(pod, true)
	f.addEvent(pod.id, fakeEvent{at: pod.created, eventType: "pod_created"})
	if err := p.Destroy(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	f.addEvent(pod.id, fakeEvent{at: pod.created.Add(time.Hour), eventType: "pod_lifecycle",
		reason: "user_initiated", fromStatus: "RUNNING", toStatus: "DELETING"})
	st, err := p.Inspect(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, st, provider.Status{State: provider.StateGone, KillReason: "user_initiated"})

	// No event names a reason
	other := f.addPod(fakePod{})
	oref := podRef(other, true)
	if err := p.Destroy(context.Background(), oref); err != nil {
		t.Fatal(err)
	}
	st, err = p.Inspect(context.Background(), oref)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, st, provider.Status{State: provider.StateGone})

	// A ref of another provider is gone, as on E2B
	st, err = p.Inspect(context.Background(), provider.Ref{Provider: "hf", ID: "x"})
	if err != nil || st.State != provider.StateGone {
		t.Fatalf("a foreign ref gave %+v, %v", st, err)
	}
}

func TestLiumInspectNewestEvent(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	pod := f.addPod(fakePod{status: "STOPPED"})
	at := pod.created
	// Listed out of order, the newest by time wins
	f.addEvent(pod.id, fakeEvent{at: at.Add(3 * time.Minute), reason: "dispute_block"})
	f.addEvent(pod.id, fakeEvent{at: at.Add(time.Minute), reason: "insufficient_balance"})
	f.addEvent(pod.id, fakeEvent{at: at.Add(4 * time.Minute), eventType: "pod_note"})
	st, err := p.Inspect(context.Background(), podRef(pod, true))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, st.KillReason, "dispute_block")

	// A tie goes to the later entry
	f.addEvent(pod.id, fakeEvent{at: at.Add(3 * time.Minute), reason: "executor_reclaimed"})
	st, err = p.Inspect(context.Background(), podRef(pod, true))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, st.KillReason, "executor_reclaimed")
}

func TestLiumInspectEventsFail(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	pod := f.addPod(fakePod{status: "FAILED"})
	f.inject("GET /pods/{id}/events", fault{status: 503, code: "upstream_unavailable"})
	_, err := p.Inspect(context.Background(), podRef(pod, true))
	if err == nil {
		t.Fatal("a failed event read gave a status without its kill reason")
	}
}
