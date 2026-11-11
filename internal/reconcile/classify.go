// Package reconcile reads what providers say about live placements and ends the ones a
// provider lost, sorting each signal into one of five classes (CP 6.3, 6.4 and 10.2)
package reconcile

import (
	"slices"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/gpustatus"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Class is what a provider signal means for a placement and its payers
type Class string

// Signal classes
const (
	ClassNone           Class = ""
	ClassAbuse          Class = "abuse"
	ClassControlLost    Class = "control_lost"
	ClassProviderFailed Class = "provider_failed"
	ClassSandboxExited  Class = "sandbox_exited"
)

// Rules are the settings Classify reads, the abuse table
type Rules = config.Abuse

// Verdict is what one observation of a live placement calls for
type Verdict struct {
	Class Class
	// Recheck asks for another look after this long, such as the degraded grace
	Recheck time.Duration
	// Alert names a reading the operator must see, such as a kill no router asked for
	Alert string
	// CheckAccount asks for the provider's account check at once, the reading being account-wide
	CheckAccount bool
}

// Live is what Classify needs to know about the placement behind an observation
type Live struct {
	// State is the placement's state, ready or terminating
	State string
	// Deadline is the end of the paid time, zero for none
	Deadline time.Time
	// DegradedSince is when the sandbox was first seen degraded, zero when it was not
	DegradedSince time.Time
	// HibernateOff says boxd's auto-hibernate was switched off for the machine
	HibernateOff bool
}

// LiveOf reads the facts Classify needs from a placement
func LiveOf(p store.Placement) Live {
	l := Live{State: p.State, HibernateOff: p.ProviderRef != nil &&
		p.ProviderRef.Meta["hibernate_off"] == "1"}
	if p.Deadline != nil {
		l.Deadline = *p.Deadline
	}
	if p.DegradedSince != nil {
		l.DegradedSince = *p.DegradedSince
	}
	return l
}

// e2bProviderKills are the kill reasons E2B owns
var e2bProviderKills = []string{"orphaned", "base_template_missing", "seal_failed", "resume_failed"}

// Classify reads one observation of a live placement, pure so each row of 6.3 is a table test
func Classify(prov string, st provider.Status, p Live, now time.Time, r Rules) Verdict {
	switch st.State {
	case provider.StateRunning, provider.StateStarting, "":
		return Verdict{}
	}
	switch prov {
	case "e2b":
		return e2b(st, p, now, r)
	case "boat":
		return boat(st, p, now, r)
	case "boxd":
		return boxd(st, p)
	case "koyeb":
		return koyeb(st, p, now, r)
	case "lium":
		return lium(st, p, now, r)
	case "hf":
		return hf(st, p, now, r)
	}
	// local and any adapter without rows of its own
	if st.State == provider.StateGone {
		return Verdict{Class: ClassProviderFailed}
	}
	return Verdict{}
}

func e2b(st provider.Status, p Live, now time.Time, r Rules) Verdict {
	switch st.State {
	case provider.StateStopped:
		// A pause nobody asks for, which no guest can cause
		return Verdict{Class: ClassProviderFailed}
	case provider.StateGone:
	default:
		return Verdict{}
	}
	switch k := st.KillReason; {
	case k == "admin":
		return Verdict{Class: Class(r.E2BAdminKill)}
	case k == "request" && p.State == store.PlacementReady:
		// No router asked, and no guest holds an API key to ask
		return Verdict{Class: ClassProviderFailed, Alert: store.AlertUnaskedKill}
	case slices.Contains(e2bProviderKills, k):
		return Verdict{Class: ClassProviderFailed}
	case k == "timeout" && beforeDeadline(p, now):
		return Verdict{Class: ClassProviderFailed}
	}
	return Verdict{Class: ClassSandboxExited}
}

func boat(st provider.Status, p Live, now time.Time, r Rules) Verdict {
	switch st.State {
	case provider.StateDegraded:
		return degraded(p, now, r, Class(r.BoatDegraded))
	case provider.StateGone:
		return Verdict{Class: ClassProviderFailed}
	case provider.StateStopped, provider.StateFailed:
	default:
		return Verdict{}
	}
	switch {
	case hasWord(st.Reason, r.BoatAbuseWords):
		return Verdict{Class: ClassAbuse}
	case st.State == provider.StateFailed || hasWord(st.Reason, r.BoatProviderWords):
		return Verdict{Class: ClassProviderFailed}
	}
	return Verdict{Class: ClassSandboxExited}
}

func boxd(st provider.Status, p Live) Verdict {
	switch st.State {
	case provider.StateGone, provider.StateFailed:
		return Verdict{Class: ClassProviderFailed}
	case provider.StateStopped:
	default:
		return Verdict{}
	}
	// With auto-hibernate on, an idle machine hibernating is no provider failure
	if (st.Reason == "suspended" || st.Reason == "hibernated") && p.HibernateOff {
		return Verdict{Class: ClassProviderFailed}
	}
	return Verdict{Class: ClassSandboxExited}
}

func koyeb(st provider.Status, p Live, now time.Time, r Rules) Verdict {
	switch st.State {
	case provider.StateGone:
		return Verdict{Class: ClassProviderFailed}
	case provider.StateDegraded:
		return degraded(p, now, r, ClassSandboxExited)
	case provider.StateFailed:
		// An event naming our process's exit beats a platform word, since a user causes that
		if !hasWord(st.Reason, r.KoyebExitWords) && hasWord(st.Reason, r.KoyebPlatformWords) {
			return Verdict{Class: ClassProviderFailed}
		}
		return Verdict{Class: ClassSandboxExited}
	case provider.StateStopped:
		return Verdict{Class: ClassSandboxExited}
	}
	return Verdict{}
}

// liumProviderEvents are the newest pod events by which Lium names its own side as the cause
var liumProviderEvents = []string{
	gpustatus.LiumEventExecutorOffline, gpustatus.LiumEventExecutorReclaimed,
	gpustatus.LiumEventBrokenByProvider,
}

func lium(st provider.Status, p Live, now time.Time, r Rules) Verdict {
	failed, gone, k := st.State == provider.StateFailed, st.State == provider.StateGone,
		st.KillReason
	switch {
	case k == gpustatus.LiumEventDisputeBlock:
		// Whether the dispute is ours or a host's complaint about the pod is unknown (Q19)
		return Verdict{
			Class: Class(r.LiumDisputeBlock), Alert: store.AlertUnaskedKill, CheckAccount: true,
		}
	case st.State == provider.StateStopped || k == gpustatus.LiumEventInsufficientBalance:
		// Lium stops every pod of the account at zero balance
		return Verdict{Class: ClassProviderFailed, CheckAccount: true}
	case failed && gpustatus.HasWord(st.Reason, gpustatus.LiumStatusBroken),
		slices.Contains(liumProviderEvents, k),
		gpustatus.HasWord(st.Reason, gpustatus.LiumFlagNodeOutage):
		return Verdict{Class: ClassProviderFailed}
	case failed && gpustatus.HasWord(st.Reason, gpustatus.LiumMoved):
		// The container on the other node is new, so our setup and firewall are gone
		return Verdict{Class: ClassProviderFailed}
	case st.State == provider.StateDegraded:
		return degraded(p, now, r, ClassProviderFailed)
	case gone && k == gpustatus.LiumEventUserInitiated && p.State == store.PlacementReady:
		// No router asked, and no guest holds a Lium key to ask
		return Verdict{Class: ClassProviderFailed, Alert: store.AlertUnaskedKill}
	case gone && k == gpustatus.LiumEventScheduledTermination && beforeDeadline(p, now):
		return Verdict{Class: ClassProviderFailed}
	}
	// Root can fail the SSH check, kill PID 1 or break a reboot from inside
	return Verdict{Class: ClassSandboxExited}
}

// hfAccountKills are the cancel reasons that name the account's credit, plan or spend cap
var hfAccountKills = []string{
	gpustatus.HFNoCredits, gpustatus.HFNoSubscription, gpustatus.HFSpendLimit,
	gpustatus.HFResourceGroupLimit,
}

// hfUnaskedKills are the ends of a Job no guest can cause, with no reason HF gives
var hfUnaskedKills = []string{gpustatus.HFCanceled, gpustatus.HFDeleted, gpustatus.HFNotFound}

func hf(st provider.Status, p Live, now time.Time, r Rules) Verdict {
	gone, k := st.State == provider.StateGone, st.KillReason
	switch {
	case gone && slices.Contains(hfAccountKills, k):
		return Verdict{Class: ClassProviderFailed, CheckAccount: true}
	case gone && k == gpustatus.HFTimeout && beforeDeadline(p, now):
		return Verdict{Class: ClassProviderFailed}
	case gone && slices.Contains(hfUnaskedKills, k) && p.State == store.PlacementReady:
		// No router asked, and no guest holds an HF token to ask
		return Verdict{Class: ClassProviderFailed, Alert: store.AlertUnaskedKill}
	case st.State == provider.StateFailed && hasWord(st.Reason, r.HFPlatformWords):
		return Verdict{Class: ClassProviderFailed}
	}
	// PID 1 exiting, zero or not, is in root's hands
	return Verdict{Class: ClassSandboxExited}
}

// beforeDeadline reports whether the paid time had not ended at now, false with no deadline
func beforeDeadline(p Live, now time.Time) bool {
	return !p.Deadline.IsZero() && now.Before(p.Deadline)
}

// degraded waits out the grace, then gives the provider's class
func degraded(p Live, now time.Time, r Rules, class Class) Verdict {
	if p.DegradedSince.IsZero() {
		return Verdict{Recheck: r.DegradedGrace}
	}
	if left := p.DegradedSince.Add(r.DegradedGrace).Sub(now); left > 0 {
		return Verdict{Recheck: left}
	}
	return Verdict{Class: class}
}

// hasWord reports whether text holds any word, ignoring case
func hasWord(text string, words []string) bool {
	text = strings.ToLower(text)
	return slices.ContainsFunc(words, func(w string) bool { return strings.Contains(text, w) })
}
