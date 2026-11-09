package config

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Abuse holds how provider signals are sorted into classes (CP 6.13)
type Abuse struct {
	// E2BAdminKill is the class of an E2B kill with reason admin, abuse or control_lost
	E2BAdminKill string
	// BoatDegraded is the class of a Boat sandbox degraded past the grace, abuse, control_lost or
	// provider_failed
	BoatDegraded string
	// DegradedGrace is how long a degraded sandbox gets to recover
	DegradedGrace time.Duration
	// BoatAbuseWords and BoatProviderWords are matched in Boat's stop text, abuse words first
	BoatAbuseWords, BoatProviderWords []string
	// KoyebPlatformWords and KoyebExitWords are matched in Koyeb's instance events
	KoyebPlatformWords, KoyebExitWords []string
	// LostAlertPerHour is the provider_failed ends on one provider before /monitor alerts
	LostAlertPerHour int
	// CorrelatedMax is how many ends of one provider inside CorrelatedWindow count as one incident
	CorrelatedMax    int
	CorrelatedWindow time.Duration
	// LateWindow is how long after an end a provider's abuse signal still blocks the payers
	LateWindow time.Duration
}

type abuseFile struct {
	E2BAdminKill       string   `toml:"e2b_admin_kill"`
	BoatDegraded       string   `toml:"boat_degraded"`
	DegradedGrace      string   `toml:"degraded_grace"`
	BoatAbuseWords     []string `toml:"boat_abuse_words"`
	BoatProviderWords  []string `toml:"boat_provider_words"`
	KoyebPlatformWords []string `toml:"koyeb_platform_words"`
	KoyebExitWords     []string `toml:"koyeb_exit_words"`
	LostAlertPerHour   int64    `toml:"lost_alert_per_hour"`
	CorrelatedMax      int64    `toml:"correlated_max"`
	CorrelatedWindow   string   `toml:"correlated_window"`
	LateWindow         string   `toml:"late_window"`
}

func defaultAbuse() abuseFile {
	return abuseFile{
		E2BAdminKill: "abuse", BoatDegraded: "control_lost", DegradedGrace: "3m",
		BoatAbuseWords:    []string{"abuse", "terms", "violation", "fraud"},
		BoatProviderWords: []string{"boat", "plan", "limit", "capacity", "maintenance", "host"},
		KoyebPlatformWords: []string{
			"platform", "host", "maintenance", "capacity", "node", "evict",
		},
		KoyebExitWords:   []string{"exit", "exited", "code", "oom", "killed", "crash"},
		LostAlertPerHour: 3, CorrelatedMax: 3, CorrelatedWindow: "10m", LateWindow: "24h",
	}
}

// DefaultAbuse is the abuse table of an empty settings file
func DefaultAbuse() Abuse {
	a, _ := defaultAbuse().read(defaultAbuse())
	return a
}

// OrDefaults returns a with each zero value at its default, for callers built without settings
func (a Abuse) OrDefaults() Abuse {
	d := DefaultAbuse()
	a.E2BAdminKill = cmp.Or(a.E2BAdminKill, d.E2BAdminKill)
	a.BoatDegraded = cmp.Or(a.BoatDegraded, d.BoatDegraded)
	a.DegradedGrace = cmp.Or(a.DegradedGrace, d.DegradedGrace)
	if a.BoatAbuseWords == nil {
		a.BoatAbuseWords = d.BoatAbuseWords
	}
	if a.BoatProviderWords == nil {
		a.BoatProviderWords = d.BoatProviderWords
	}
	if a.KoyebPlatformWords == nil {
		a.KoyebPlatformWords = d.KoyebPlatformWords
	}
	if a.KoyebExitWords == nil {
		a.KoyebExitWords = d.KoyebExitWords
	}
	a.LostAlertPerHour = cmp.Or(a.LostAlertPerHour, d.LostAlertPerHour)
	a.CorrelatedMax = cmp.Or(a.CorrelatedMax, d.CorrelatedMax)
	a.CorrelatedWindow = cmp.Or(a.CorrelatedWindow, d.CorrelatedWindow)
	a.LateWindow = cmp.Or(a.LateWindow, d.LateWindow)
	return a
}

// read checks the abuse table against its bounds, d holding the defaults
func (f abuseFile) read(d abuseFile) (Abuse, error) {
	var a Abuse
	for _, c := range []struct {
		key, raw, def string
		allowed       []string
		into          *string
	}{
		{"e2b_admin_kill", f.E2BAdminKill, d.E2BAdminKill, []string{"abuse", "control_lost"},
			&a.E2BAdminKill},
		{"boat_degraded", f.BoatDegraded, d.BoatDegraded,
			[]string{"abuse", "control_lost", "provider_failed"}, &a.BoatDegraded},
	} {
		v := cmp.Or(c.raw, c.def)
		if !slices.Contains(c.allowed, v) {
			return Abuse{}, fmt.Errorf("abuse.%s must be one of %s", c.key,
				strings.Join(c.allowed, ", "))
		}
		*c.into = v
	}
	for _, p := range []struct {
		key, raw, def string
		into          *time.Duration
	}{
		{"degraded_grace", f.DegradedGrace, d.DegradedGrace, &a.DegradedGrace},
		{"correlated_window", f.CorrelatedWindow, d.CorrelatedWindow, &a.CorrelatedWindow},
		{"late_window", f.LateWindow, d.LateWindow, &a.LateWindow},
	} {
		v, err := time.ParseDuration(cmp.Or(p.raw, p.def))
		if err != nil || v < time.Second || v > 90*24*time.Hour {
			return Abuse{}, fmt.Errorf(
				"abuse.%s must be a Go duration from 1s to 2160h such as 10m", p.key)
		}
		*p.into = v
	}
	for _, n := range []struct {
		key  string
		v    int64
		into *int
	}{
		{"lost_alert_per_hour", f.LostAlertPerHour, &a.LostAlertPerHour},
		{"correlated_max", f.CorrelatedMax, &a.CorrelatedMax},
	} {
		if n.v < 1 || n.v > maxWalletLimit {
			return Abuse{}, fmt.Errorf("abuse.%s must be 1 to %d", n.key, maxWalletLimit)
		}
		*n.into = int(n.v)
	}
	for _, w := range []struct {
		key  string
		raw  []string
		into *[]string
	}{
		{"boat_abuse_words", f.BoatAbuseWords, &a.BoatAbuseWords},
		{"boat_provider_words", f.BoatProviderWords, &a.BoatProviderWords},
		{"koyeb_platform_words", f.KoyebPlatformWords, &a.KoyebPlatformWords},
		{"koyeb_exit_words", f.KoyebExitWords, &a.KoyebExitWords},
	} {
		words := make([]string, 0, len(w.raw))
		for _, word := range w.raw {
			word = strings.ToLower(strings.TrimSpace(word))
			if word == "" {
				return Abuse{}, fmt.Errorf("abuse.%s must hold no empty word", w.key)
			}
			words = append(words, word)
		}
		*w.into = words
	}
	return a, nil
}

// Reconcile holds the lost-placement check and lifetime drift settings (CP 10.8)
type Reconcile struct {
	// ReadyGrace is how long a fresh placement may be missing from a listing that lags
	ReadyGrace time.Duration
	// MaxPerSweep is the reconcile jobs one sweep inserts per provider
	MaxPerSweep int
	// LifetimeDrift is how far a provider-side lifetime may stray from the deadline
	LifetimeDrift time.Duration
}

type reconcileFile struct {
	ReadyGrace    string `toml:"ready_grace"`
	MaxPerSweep   int64  `toml:"max_per_sweep"`
	LifetimeDrift string `toml:"lifetime_drift"`
}

func defaultReconcile() reconcileFile {
	return reconcileFile{ReadyGrace: "30s", MaxPerSweep: 200, LifetimeDrift: "2m"}
}

// DefaultReconcile is the reconcile table of an empty settings file
func DefaultReconcile() Reconcile {
	r, _ := defaultReconcile().read(defaultReconcile())
	return r
}

// read checks the reconcile table against its bounds, d holding the defaults
func (f reconcileFile) read(d reconcileFile) (Reconcile, error) {
	var r Reconcile
	for _, p := range []struct {
		key, raw, def string
		into          *time.Duration
	}{
		{"ready_grace", f.ReadyGrace, d.ReadyGrace, &r.ReadyGrace},
		{"lifetime_drift", f.LifetimeDrift, d.LifetimeDrift, &r.LifetimeDrift},
	} {
		v, err := time.ParseDuration(cmp.Or(p.raw, p.def))
		if err != nil || v < time.Second || v > 24*time.Hour {
			return Reconcile{}, fmt.Errorf(
				"reconcile.%s must be a Go duration from 1s to 24h such as 2m", p.key)
		}
		*p.into = v
	}
	if f.MaxPerSweep < 1 || f.MaxPerSweep > 10000 {
		return Reconcile{}, fmt.Errorf("reconcile.max_per_sweep must be 1 to 10000")
	}
	r.MaxPerSweep = int(f.MaxPerSweep)
	return r, nil
}
