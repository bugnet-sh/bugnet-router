package reconcile

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	gs "github.com/bugnet-sh/bugnet-router/internal/provider/gpustatus"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

var now = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

// rules returns the default abuse table with the two class settings given
func rules(e2bAdmin, boatDegraded string) Rules {
	r := config.DefaultAbuse()
	r.E2BAdminKill, r.BoatDegraded = e2bAdmin, boatDegraded
	return r
}

func ready() Live {
	return Live{State: store.PlacementReady, Deadline: now.Add(5 * time.Minute)}
}

func status(s provider.State, reason, kill string) provider.Status {
	return provider.Status{State: s, Reason: reason, KillReason: kill}
}

// every row of CP 6.3, with both values of the two settings
func TestClassify(t *testing.T) {
	grace := config.DefaultAbuse().DegradedGrace
	spell := func(ago time.Duration) Live {
		l := ready()
		l.DegradedSince = now.Add(-ago)
		return l
	}
	terminating := ready()
	terminating.State = store.PlacementTerminating
	early := ready()
	late := Live{State: store.PlacementReady, Deadline: now.Add(-time.Minute)}
	off := ready()
	off.HibernateOff = true

	rows := []struct {
		name  string
		prov  string
		st    provider.Status
		live  Live
		rules Rules
		want  Verdict
	}{
		{"e2b admin kill is abuse", "e2b", status("gone", "", "admin"), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassAbuse}},
		{"e2b admin kill as control_lost", "e2b", status("gone", "", "admin"), ready(),
			rules("control_lost", "control_lost"), Verdict{Class: ClassControlLost}},
		{"e2b request on a ready placement", "e2b", status("gone", "", "request"), ready(),
			rules("abuse", "control_lost"),
			Verdict{Class: ClassProviderFailed, Alert: store.AlertUnaskedKill}},
		{"e2b request after our own stop", "e2b", status("gone", "", "request"), terminating,
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"e2b orphaned", "e2b", status("gone", "", "orphaned"), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"e2b base template missing", "e2b", status("gone", "", "base_template_missing"),
			ready(), rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"e2b seal failed", "e2b", status("gone", "", "seal_failed"), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"e2b resume failed", "e2b", status("gone", "", "resume_failed"), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"e2b timeout before our deadline", "e2b", status("gone", "", "timeout"), early,
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"e2b timeout after our deadline", "e2b", status("gone", "", "timeout"), late,
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"e2b unknown kill reason", "e2b", status("gone", "", "unknown"), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"e2b another kill reason", "e2b", status("gone", "", "fleet_move"), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"e2b no kill event", "e2b", status("gone", "", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"e2b paused", "e2b", status("stopped", "", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"e2b running", "e2b", status("running", "", ""), ready(),
			rules("abuse", "control_lost"), Verdict{}},

		{"boat degraded inside the grace", "boat", status("degraded", "tunnel", ""),
			spell(grace / 3), rules("abuse", "control_lost"),
			Verdict{Recheck: grace - grace/3}},
		{"boat degraded, no spell yet", "boat", status("degraded", "tunnel", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Recheck: grace}},
		{"boat degraded past the grace as control_lost", "boat",
			status("degraded", "tunnel", ""), spell(2 * grace), rules("abuse", "control_lost"),
			Verdict{Class: ClassControlLost}},
		{"boat degraded past the grace as abuse", "boat", status("degraded", "tunnel", ""),
			spell(2 * grace), rules("abuse", "abuse"), Verdict{Class: ClassAbuse}},
		{"boat degraded past the grace as provider_failed", "boat",
			status("degraded", "tunnel", ""), spell(2 * grace),
			rules("abuse", "provider_failed"), Verdict{Class: ClassProviderFailed}},
		{"boat stopped for terms", "boat", status("stopped", "Violation of the Terms", ""),
			ready(), rules("abuse", "control_lost"), Verdict{Class: ClassAbuse}},
		{"boat failed for fraud", "boat", status("failed", "fraud check", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassAbuse}},
		{"boat abuse words beat provider words", "boat",
			status("stopped", "plan limit and abuse", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassAbuse}},
		{"boat failed", "boat", status("failed", "kernel panic", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"boat stopped for its plan", "boat", status("stopped", "plan limit reached", ""),
			ready(), rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"boat stopped for maintenance", "boat", status("stopped", "host maintenance", ""),
			ready(), rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"boat gone", "boat", status("gone", "", ""), ready(), rules("abuse", "control_lost"),
			Verdict{Class: ClassProviderFailed}},
		{"boat stopped with no text", "boat", status("stopped", "", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"boat stopped with other text", "boat", status("stopped", "user asked", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},

		{"boxd failed", "boxd", status("failed", "failed", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"boxd gone", "boxd", status("gone", "", ""), ready(), rules("abuse", "control_lost"),
			Verdict{Class: ClassProviderFailed}},
		{"boxd hibernated, hibernate off", "boxd", status("stopped", "hibernated", ""), off,
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"boxd suspended, hibernate off", "boxd", status("stopped", "suspended", ""), off,
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"boxd hibernated, hibernate on", "boxd", status("stopped", "hibernated", ""),
			ready(), rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"boxd stopped", "boxd", status("stopped", "stopped", ""), off,
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},

		{"koyeb gone", "koyeb", status("gone", "", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"koyeb error naming the platform", "koyeb",
			status("failed", "instance evicted for node maintenance", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"koyeb error after our process exited", "koyeb",
			status("failed", "process exited with code 137", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"koyeb error beating a platform word", "koyeb",
			status("failed", "process killed on node 7", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"koyeb error with no event", "koyeb", status("failed", "ERROR", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"koyeb stopped", "koyeb", status("stopped", "STOPPED", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassSandboxExited}},
		{"koyeb unhealthy inside the grace", "koyeb", status("degraded", "", ""),
			spell(grace / 2), rules("abuse", "control_lost"), Verdict{Recheck: grace / 2}},
		{"koyeb unhealthy past the grace", "koyeb", status("degraded", "", ""),
			spell(2 * grace), rules("abuse", "control_lost"),
			Verdict{Class: ClassSandboxExited}},

		{"local gone", "local", status("gone", "", ""), ready(),
			rules("abuse", "control_lost"), Verdict{Class: ClassProviderFailed}},
		{"local running", "local", status("running", "", ""), ready(),
			rules("abuse", "control_lost"), Verdict{}},
		{"starting is no signal", "boat", status("starting", "", ""), ready(),
			rules("abuse", "control_lost"), Verdict{}},
	}
	for _, c := range rows {
		t.Run(c.name, func(t *testing.T) {
			eq(t, Classify(c.prov, c.st, c.live, now, c.rules), c.want)
		})
	}
}

// a changed word list changes the verdict, so the lists are settings and not constants
func TestClassifyWordListsAreSettings(t *testing.T) {
	r := rules("abuse", "control_lost")
	r.BoatAbuseWords = []string{"zorp"}
	st := status("stopped", "terms of service", "")
	eq(t, Classify("boat", st, ready(), now, r).Class, ClassSandboxExited)
	st.Reason = "a ZORP was found"
	eq(t, Classify("boat", st, ready(), now, r).Class, ClassAbuse)
}

// the placement's ref flag, and nothing else, tells boxd's auto-hibernate setting
func TestBoxdHibernationClassFollowsTheFlag(t *testing.T) {
	deadline := now.Add(time.Hour)
	for flag, want := range map[string]Class{
		"1": ClassProviderFailed, "0": ClassSandboxExited, "": ClassSandboxExited,
	} {
		ref := provider.Ref{Provider: "boxd", ID: "vm1"}
		if flag != "" {
			ref.Meta = map[string]string{"hibernate_off": flag}
		}
		p := store.Placement{State: store.PlacementReady, ProviderRef: &ref, Deadline: &deadline}
		v := Classify("boxd", status("stopped", "hibernated", ""), LiveOf(p), now,
			rules("abuse", "control_lost"))
		eq(t, v.Class, want)
	}
}

// liumStatus is a Lium reading, its newest event in KillReason and its words in Reason
func liumStatus(s provider.State, event string, words ...string) provider.Status {
	return provider.Status{State: s, KillReason: event, Reason: gs.LiumReason(words...)}
}

// every row of design 5.12 for Lium, in order, each row's own cases before the next row's
func TestClassifyLium(t *testing.T) {
	grace := config.DefaultAbuse().DegradedGrace
	spell := func(ago time.Duration) Live {
		l := ready()
		l.DegradedSince = now.Add(-ago)
		return l
	}
	terminating := ready()
	terminating.State = store.PlacementTerminating
	late := Live{State: store.PlacementReady, Deadline: now.Add(-time.Minute)}
	lateTerminating := late
	lateTerminating.State = store.PlacementTerminating
	dispute := func(class string) Rules {
		r := config.DefaultAbuse()
		r.LiumDisputeBlock = class
		return r
	}
	def := config.DefaultAbuse()
	failed := Verdict{Class: ClassProviderFailed}
	exited := Verdict{Class: ClassSandboxExited}
	funds := Verdict{Class: ClassProviderFailed, CheckAccount: true}
	blocked := func(c Class) Verdict {
		return Verdict{Class: c, Alert: store.AlertUnaskedKill, CheckAccount: true}
	}

	rows := []struct {
		name  string
		st    provider.Status
		live  Live
		rules Rules
		want  Verdict
	}{
		{"running", liumStatus("running", ""), ready(), def, Verdict{}},
		{"starting", liumStatus("starting", ""), ready(), def, Verdict{}},

		{"dispute_block at its default", liumStatus("gone", gs.LiumEventDisputeBlock), ready(),
			def, blocked(ClassProviderFailed)},
		{"dispute_block as abuse", liumStatus("gone", gs.LiumEventDisputeBlock), ready(),
			dispute("abuse"), blocked(ClassAbuse)},
		{"dispute_block as control_lost", liumStatus("gone", gs.LiumEventDisputeBlock),
			ready(), dispute("control_lost"), blocked(ClassControlLost)},
		{"dispute_block while terminating", liumStatus("gone", gs.LiumEventDisputeBlock),
			terminating, dispute("abuse"), blocked(ClassAbuse)},
		{"dispute_block beats stopped", liumStatus("stopped", gs.LiumEventDisputeBlock,
			gs.LiumStatusStopped), ready(), dispute("abuse"), blocked(ClassAbuse)},
		{"dispute_block beats stopped at its default", liumStatus("stopped",
			gs.LiumEventDisputeBlock, gs.LiumStatusStopPending), terminating, def,
			blocked(ClassProviderFailed)},
		{"dispute_block beats broken", liumStatus("failed", gs.LiumEventDisputeBlock,
			gs.LiumStatusBroken, gs.LiumFlagNodeOutage), late, def,
			blocked(ClassProviderFailed)},

		{"stopped", liumStatus("stopped", gs.LiumEventInsufficientBalance,
			gs.LiumStatusStopped), ready(), def, funds},
		{"stop pending with no event", liumStatus("stopped", "", gs.LiumStatusStopPending),
			ready(), def, funds},
		{"stopped while terminating", liumStatus("stopped", "", gs.LiumStatusStopped),
			terminating, def, funds},
		{"stopped past the deadline", liumStatus("stopped", "", gs.LiumStatusStopped), late,
			def, funds},
		{"stopped on insufficient_balance with dispute_block as abuse", liumStatus("stopped",
			gs.LiumEventInsufficientBalance, gs.LiumStatusStopped), ready(), dispute("abuse"),
			funds},
		{"failed on insufficient_balance", liumStatus("failed",
			gs.LiumEventInsufficientBalance, gs.LiumStatusFailed), ready(), def, funds},
		{"gone on insufficient_balance", liumStatus("gone", gs.LiumEventInsufficientBalance),
			lateTerminating, def, funds},
		{"degraded on insufficient_balance", liumStatus("degraded",
			gs.LiumEventInsufficientBalance, "RUNNING", gs.LiumFlagNodeOffline), spell(0), def,
			funds},

		{"broken", liumStatus("failed", "", gs.LiumStatusBroken), ready(), def, failed},
		{"broken while terminating", liumStatus("failed", "", gs.LiumStatusBroken),
			terminating, def, failed},
		{"gone on executor_offline", liumStatus("gone", gs.LiumEventExecutorOffline), ready(),
			def, failed},
		{"gone on executor_reclaimed", liumStatus("gone", gs.LiumEventExecutorReclaimed),
			late, def, failed},
		{"failed on broken_by_provider", liumStatus("failed", gs.LiumEventBrokenByProvider,
			gs.LiumStatusFailed), ready(), def, failed},
		{"failed on a node_outage", liumStatus("failed", "", gs.LiumStatusFailed,
			gs.LiumFlagNodeOutage), ready(), def, failed},
		{"degraded on a node_outage inside the grace", liumStatus("degraded", "", "RUNNING",
			gs.LiumFlagNodeOffline, gs.LiumFlagNodeOutage), spell(grace / 3), def, failed},
		{"executor_offline beats a reboot", liumStatus("failed", gs.LiumEventExecutorOffline,
			gs.LiumRebooted), ready(), def, failed},

		{"moved to another node", liumStatus("failed", "", gs.LiumMoved), ready(), def,
			failed},
		{"moved while terminating", liumStatus("failed", "", gs.LiumMoved), terminating, def,
			failed},
		{"moved past the deadline", liumStatus("failed", "", gs.LiumMoved), late, def, failed},

		{"node_offline, no spell yet", liumStatus("degraded", "", "RUNNING",
			gs.LiumFlagNodeOffline), ready(), def, Verdict{Recheck: grace}},
		{"node_offline inside the grace", liumStatus("degraded", "", "RUNNING",
			gs.LiumFlagNodeOffline), spell(grace / 3), def, Verdict{Recheck: grace - grace/3}},
		{"node_offline past the grace", liumStatus("degraded", "", "RUNNING",
			gs.LiumFlagNodeOffline), spell(2 * grace), def, failed},

		{"user_initiated on a ready placement", liumStatus("gone", gs.LiumEventUserInitiated),
			ready(), def, Verdict{Class: ClassProviderFailed, Alert: store.AlertUnaskedKill}},
		{"user_initiated past the deadline", liumStatus("gone", gs.LiumEventUserInitiated),
			late, def, Verdict{Class: ClassProviderFailed, Alert: store.AlertUnaskedKill}},
		{"user_initiated after our own stop", liumStatus("gone", gs.LiumEventUserInitiated),
			terminating, def, exited},

		{"scheduled_termination before our deadline", liumStatus("gone",
			gs.LiumEventScheduledTermination), ready(), def, failed},
		{"scheduled_termination before the deadline while terminating", liumStatus("gone",
			gs.LiumEventScheduledTermination), terminating, def, failed},
		{"scheduled_termination after our deadline", liumStatus("gone",
			gs.LiumEventScheduledTermination), late, def, exited},
		{"scheduled_termination with no deadline", liumStatus("gone",
			gs.LiumEventScheduledTermination), Live{State: store.PlacementReady}, def, exited},

		{"failed", liumStatus("failed", "", gs.LiumStatusFailed), ready(), def, exited},
		{"reboot failed", liumStatus("failed", gs.LiumEventRebootFailed,
			gs.LiumStatusRebootFailed), ready(), def, exited},
		{"creation failed", liumStatus("failed", "", gs.LiumStatusCreationFailed), ready(), def,
			exited},
		{"rebooted after ready", liumStatus("failed", "", gs.LiumRebooted, "REBOOT_PENDING"),
			ready(), def, exited},
		{"rebooted while terminating", liumStatus("failed", "", gs.LiumRebooted),
			terminating, def, exited},
		{"gone on reboot_failed", liumStatus("gone", gs.LiumEventRebootFailed), ready(), def,
			exited},
		{"gone on unknown", liumStatus("gone", gs.LiumEventUnknown), ready(), def, exited},
		{"gone on another reason", liumStatus("gone", "host_moved"), ready(), def, exited},
		{"gone with no event", liumStatus("gone", ""), ready(), def, exited},
		{"gone with no event past the deadline", liumStatus("gone", ""), late, def, exited},
	}
	for _, c := range rows {
		t.Run(c.name, func(t *testing.T) {
			eq(t, Classify("lium", c.st, c.live, now, c.rules), c.want)
		})
	}
}

// hfStatus is an HF reading, its kill reason and status message
func hfStatus(s provider.State, kill, message string) provider.Status {
	return provider.Status{State: s, KillReason: kill, Reason: message}
}

// every row of design 5.12 for HF, in order, each row's own cases before the next row's
func TestClassifyHF(t *testing.T) {
	terminating := ready()
	terminating.State = store.PlacementTerminating
	late := Live{State: store.PlacementReady, Deadline: now.Add(-time.Minute)}
	lateTerminating := late
	lateTerminating.State = store.PlacementTerminating
	def := config.DefaultAbuse()
	words := config.DefaultAbuse()
	words.HFPlatformWords = []string{"node failure", "hardware"}
	failed := Verdict{Class: ClassProviderFailed}
	exited := Verdict{Class: ClassSandboxExited}
	funds := Verdict{Class: ClassProviderFailed, CheckAccount: true}
	unasked := Verdict{Class: ClassProviderFailed, Alert: store.AlertUnaskedKill}

	rows := []struct {
		name  string
		st    provider.Status
		live  Live
		rules Rules
		want  Verdict
	}{
		{"running", hfStatus("running", "", ""), ready(), def, Verdict{}},
		{"scheduling", hfStatus("starting", "", ""), ready(), def, Verdict{}},

		{"no_credits", hfStatus("gone", gs.HFNoCredits, ""), ready(), def, funds},
		{"no_credits while terminating past the deadline", hfStatus("gone", gs.HFNoCredits,
			""), lateTerminating, def, funds},
		{"no_subscription", hfStatus("gone", gs.HFNoSubscription, ""), ready(), def, funds},
		{"no_subscription while terminating", hfStatus("gone", gs.HFNoSubscription, ""),
			terminating, def, funds},

		{"spend_limit", hfStatus("gone", gs.HFSpendLimit, ""), ready(), def, funds},
		{"resource_group_limit", hfStatus("gone", gs.HFResourceGroupLimit, ""), late, def,
			funds},
		{"spend_limit while terminating", hfStatus("gone", gs.HFSpendLimit, ""), terminating,
			def, funds},

		{"timeout before our deadline", hfStatus("gone", gs.HFTimeout, "Job timeout"),
			ready(), def, failed},
		{"timeout before the deadline while terminating", hfStatus("gone", gs.HFTimeout,
			"Job timeout"), terminating, def, failed},
		{"timeout after our deadline", hfStatus("gone", gs.HFTimeout, "Job timeout"), late,
			def, exited},
		{"timeout with no deadline", hfStatus("gone", gs.HFTimeout, "Job timeout"),
			Live{State: store.PlacementReady}, def, exited},
		{"timeout on an error stage before our deadline", hfStatus("failed", gs.HFTimeout,
			"Job timeout"), ready(), def, failed},
		{"timeout on an error stage while terminating", hfStatus("failed", gs.HFTimeout,
			"Job timeout"), terminating, def, failed},
		{"timeout on an error stage after our deadline", hfStatus("failed", gs.HFTimeout,
			"Job timeout"), late, def, exited},
		{"timeout on an error stage past the deadline naming the platform", hfStatus("failed",
			gs.HFTimeout, "node failure"), late, words, failed},

		{"canceled with no reason on a ready placement", hfStatus("gone", gs.HFCanceled, ""),
			ready(), def, unasked},
		{"deleted on a ready placement", hfStatus("gone", gs.HFDeleted, ""), late, def,
			unasked},
		{"not found on a ready placement", hfStatus("gone", gs.HFNotFound, ""), ready(), def,
			unasked},
		{"canceled after our own stop", hfStatus("gone", gs.HFCanceled, ""), terminating, def,
			exited},
		{"deleted after our own stop", hfStatus("gone", gs.HFDeleted, ""), terminating, def,
			exited},
		{"not found after our own stop", hfStatus("gone", gs.HFNotFound, ""), lateTerminating,
			def, exited},

		{"error naming the platform", hfStatus("failed", "error",
			"Hardware fault on the host"), ready(), words, failed},
		{"error naming the platform while terminating", hfStatus("failed", "error",
			"node failure"), terminating, words, failed},
		{"error with the empty default list", hfStatus("failed", "error",
			"Hardware fault on the host"), ready(), def, exited},

		{"error", hfStatus("failed", "error", "exit code 1"), ready(), words, exited},
		{"error with no message", hfStatus("failed", "error", ""), ready(), words, exited},
		{"completed", hfStatus("gone", gs.HFCompleted, ""), ready(), def, exited},
		{"completed while terminating", hfStatus("gone", gs.HFCompleted, ""), terminating,
			def, exited},
		{"gone on another reason", hfStatus("gone", "quota_exceeded", ""), ready(), def,
			exited},
		{"gone with no reason", hfStatus("gone", "", ""), ready(), def, exited},
		{"a platform word on a completed job", hfStatus("gone", gs.HFCompleted,
			"node failure"), ready(), words, exited},
	}
	for _, c := range rows {
		t.Run(c.name, func(t *testing.T) {
			eq(t, Classify("hf", c.st, c.live, now, c.rules), c.want)
		})
	}
}
