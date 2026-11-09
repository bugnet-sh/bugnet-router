package reconcile

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
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
