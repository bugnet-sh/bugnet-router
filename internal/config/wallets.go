package config

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/wallet"
)

// Wallets holds the payer claim, the per-wallet limits, blocks and strikes
type Wallets struct {
	// UsePayer false counts every order as having no payer
	UsePayer bool
	// RequirePayer refuses start orders that count as having no payer
	RequirePayer bool
	// MaxConcurrent bounds the placements placing, ready or terminating per wallet
	MaxConcurrent   int
	StartsPerHour   int
	StartsBurst     int
	StartsPerDay    int
	RefusalsPerHour int
	// AnonymousMaxConcurrent bounds every placement without a payer together
	AnonymousMaxConcurrent int
	// OpsPerSecond and OpsBurst bound user operations per wallet
	OpsPerSecond int
	OpsBurst     int
	// RefusalBlockFor is how long the refusal bucket running dry blocks a wallet
	RefusalBlockFor time.Duration
	// FacilitatorAddresses count as no payer and are never blocked
	FacilitatorAddresses []string
	// EmitEvents writes wallet.blocked and wallet.unblocked for Payments
	EmitEvents      bool
	StrikeThreshold int
	StrikeWindow    time.Duration
	StrikeBlockFor  time.Duration
	// AbuseBlockFor is how long an abuse end blocks its payers, 0 until an operator lifts it
	AbuseBlockFor time.Duration
	// StopLiveOnAbuse ends the other live placements of every payer an abuse end blocks
	StopLiveOnAbuse bool
	// RefundStoppedLive makes those wallet_blocked ends refund their unused time
	RefundStoppedLive bool
	// FailedRefundsPerDay is the refundable provider_failed ends per start payer in 24 h
	FailedRefundsPerDay int
	// AnonymousFailedRefundsPerDay is the same for every placement without a payer, together
	AnonymousFailedRefundsPerDay int
	// BytesPerDay is the edge and egress bytes one payer's placements carry together each UTC day
	BytesPerDay int64
}

type walletsFile struct {
	UsePayer               bool     `toml:"use_payer"`
	RequirePayer           bool     `toml:"require_payer"`
	MaxConcurrent          int64    `toml:"max_concurrent"`
	StartsPerHour          int64    `toml:"starts_per_hour"`
	StartsBurst            int64    `toml:"starts_burst"`
	StartsPerDay           int64    `toml:"starts_per_day"`
	AnonymousMaxConcurrent int64    `toml:"anonymous_max_concurrent"`
	OpsPerSecond           int64    `toml:"ops_per_second"`
	OpsBurst               int64    `toml:"ops_burst"`
	RefusalsPerHour        int64    `toml:"refusals_per_hour"`
	RefusalBlockFor        string   `toml:"refusal_block_for"`
	FacilitatorAddresses   []string `toml:"facilitator_addresses"`
	EmitEvents             bool     `toml:"emit_events"`
	StrikeThreshold        int64    `toml:"strike_threshold"`
	StrikeWindow           string   `toml:"strike_window"`
	StrikeBlockFor         string   `toml:"strike_block_for"`
	AbuseBlockFor          string   `toml:"abuse_block_for"`
	StopLiveOnAbuse        bool     `toml:"stop_live_on_abuse"`
	RefundStoppedLive      bool     `toml:"refund_stopped_live"`
	FailedRefundsPerDay    int64    `toml:"failed_refunds_per_day"`
	// AnonymousFailedRefunds is anonymous_failed_refunds_per_day
	AnonymousFailedRefunds int64  `toml:"anonymous_failed_refunds_per_day"`
	BytesPerDay            string `toml:"bytes_per_day"`
}

// maxWalletLimit keeps every count and rate far inside what the buckets and Postgres hold
const maxWalletLimit = 1_000_000

func defaultWallets() walletsFile {
	return walletsFile{
		MaxConcurrent: 5, StartsPerHour: 30, StartsBurst: 5, StartsPerDay: 200,
		AnonymousMaxConcurrent: 20, OpsPerSecond: 20, OpsBurst: 40, RefusalsPerHour: 10,
		RefusalBlockFor: "1h", StrikeThreshold: 2, StrikeWindow: "24h", StrikeBlockFor: "168h",
		StopLiveOnAbuse: true, FailedRefundsPerDay: 2, AnonymousFailedRefunds: 20,
		BytesPerDay: "50GiB",
	}
}

// OrDefaults returns w with each zero limit at its default, for callers built without settings,
// leaving every switch and abuse_block_for as given
func (w Wallets) OrDefaults() Wallets {
	d, _ := defaultWallets().read(defaultWallets())
	w.MaxConcurrent = cmp.Or(w.MaxConcurrent, d.MaxConcurrent)
	w.StartsPerHour = cmp.Or(w.StartsPerHour, d.StartsPerHour)
	w.StartsBurst = cmp.Or(w.StartsBurst, d.StartsBurst)
	w.StartsPerDay = cmp.Or(w.StartsPerDay, d.StartsPerDay)
	w.RefusalsPerHour = cmp.Or(w.RefusalsPerHour, d.RefusalsPerHour)
	w.AnonymousMaxConcurrent = cmp.Or(w.AnonymousMaxConcurrent, d.AnonymousMaxConcurrent)
	w.OpsPerSecond = cmp.Or(w.OpsPerSecond, d.OpsPerSecond)
	w.OpsBurst = cmp.Or(w.OpsBurst, d.OpsBurst)
	w.RefusalBlockFor = cmp.Or(w.RefusalBlockFor, d.RefusalBlockFor)
	w.StrikeThreshold = cmp.Or(w.StrikeThreshold, d.StrikeThreshold)
	w.StrikeWindow = cmp.Or(w.StrikeWindow, d.StrikeWindow)
	w.StrikeBlockFor = cmp.Or(w.StrikeBlockFor, d.StrikeBlockFor)
	w.FailedRefundsPerDay = cmp.Or(w.FailedRefundsPerDay, d.FailedRefundsPerDay)
	w.AnonymousFailedRefundsPerDay = cmp.Or(w.AnonymousFailedRefundsPerDay,
		d.AnonymousFailedRefundsPerDay)
	w.BytesPerDay = cmp.Or(w.BytesPerDay, d.BytesPerDay)
	return w
}

// read checks the wallets table against its bounds, d holding the defaults
func (f walletsFile) read(d walletsFile) (Wallets, error) {
	if f.RequirePayer && !f.UsePayer {
		// Every start would count as having no payer and be refused after Payments charged it
		return Wallets{}, errors.New("wallets.require_payer = true needs wallets.use_payer = true")
	}
	w := Wallets{
		UsePayer: f.UsePayer, RequirePayer: f.RequirePayer, EmitEvents: f.EmitEvents,
		StopLiveOnAbuse: f.StopLiveOnAbuse, RefundStoppedLive: f.RefundStoppedLive,
	}
	for _, n := range []struct {
		key  string
		raw  int64
		into *int
	}{
		{"max_concurrent", f.MaxConcurrent, &w.MaxConcurrent},
		{"starts_per_hour", f.StartsPerHour, &w.StartsPerHour},
		{"starts_burst", f.StartsBurst, &w.StartsBurst},
		{"starts_per_day", f.StartsPerDay, &w.StartsPerDay},
		{"anonymous_max_concurrent", f.AnonymousMaxConcurrent, &w.AnonymousMaxConcurrent},
		{"ops_per_second", f.OpsPerSecond, &w.OpsPerSecond},
		{"ops_burst", f.OpsBurst, &w.OpsBurst},
		{"refusals_per_hour", f.RefusalsPerHour, &w.RefusalsPerHour},
		{"strike_threshold", f.StrikeThreshold, &w.StrikeThreshold},
		{"failed_refunds_per_day", f.FailedRefundsPerDay, &w.FailedRefundsPerDay},
		{"anonymous_failed_refunds_per_day", f.AnonymousFailedRefunds,
			&w.AnonymousFailedRefundsPerDay},
	} {
		if n.raw < 1 || n.raw > maxWalletLimit {
			return Wallets{}, fmt.Errorf("wallets.%s must be 1 to %d", n.key, maxWalletLimit)
		}
		*n.into = int(n.raw)
	}
	for _, p := range []struct {
		key, raw, def string
		into          *time.Duration
	}{
		{"refusal_block_for", f.RefusalBlockFor, d.RefusalBlockFor, &w.RefusalBlockFor},
		{"strike_window", f.StrikeWindow, d.StrikeWindow, &w.StrikeWindow},
		{"strike_block_for", f.StrikeBlockFor, d.StrikeBlockFor, &w.StrikeBlockFor},
	} {
		v, err := time.ParseDuration(cmp.Or(p.raw, p.def))
		if err != nil || v <= 0 {
			return Wallets{}, fmt.Errorf("wallets.%s must be a positive Go duration such as 1h",
				p.key)
		}
		*p.into = v
	}
	if f.AbuseBlockFor != "" {
		v, err := time.ParseDuration(f.AbuseBlockFor)
		if err != nil || v <= 0 {
			return Wallets{}, errors.New("wallets.abuse_block_for must be empty or a positive " +
				"Go duration such as 720h")
		}
		w.AbuseBlockFor = v
	}
	bytes, err := byteSize("wallets.bytes_per_day", cmp.Or(f.BytesPerDay, d.BytesPerDay))
	if err != nil {
		return Wallets{}, err
	}
	w.BytesPerDay = bytes
	for i, addr := range f.FacilitatorAddresses {
		if _, err := wallet.ParseSolana(addr); err != nil {
			// An entry is never quoted, since a secret could have been pasted there
			return Wallets{}, fmt.Errorf("wallets.facilitator_addresses entry %d must be a "+
				"canonical base58 Solana address", i+1)
		}
		if first := slices.Index(f.FacilitatorAddresses[:i], addr); first >= 0 {
			return Wallets{}, fmt.Errorf("wallets.facilitator_addresses entry %d repeats "+
				"entry %d", i+1, first+1)
		}
	}
	if len(f.FacilitatorAddresses) > 0 {
		w.FacilitatorAddresses = slices.Clone(f.FacilitatorAddresses)
	}
	return w, nil
}
