package stats

import (
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/solana"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
)

// PageSize is the sessions route's rows per page
const PageSize = 50

// Window is how far back the sessions route reaches
const Window = 30 * 24 * time.Hour

// Page is GET /api/stats/sessions's body, field for field as SS 7
type Page struct {
	Items []Session `json:"items"`
	Next  *string   `json:"next"`
}

// Session is one row of the sessions route, each pointer nil when SS 7 allows null
type Session struct {
	StartedAt   Time    `json:"startedAt"`
	EndedAt     *Time   `json:"endedAt"`
	Provider    *string `json:"provider"`
	VCPU        *int64  `json:"vcpu"`
	MemoryMiB   *int64  `json:"memoryMiB"`
	GPU         *GPU    `json:"gpu"`
	Seconds     int64   `json:"seconds"`
	Commands    int64   `json:"commands"`
	PriceMicros int64   `json:"priceMicros"`
	Status      string  `json:"status"`
	EndReason   *string `json:"endReason"`
	Wallet      *string `json:"wallet"`
	Tx          *string `json:"tx"`
}

// Sessions builds a page from rows read with a limit of PageSize plus one
type Sessions struct {
	Catalog *catalog.Catalog
	// Facilitators are wallets.facilitator_addresses, never shown as a user's wallet
	Facilitators []string
	Ends         Ends
}

// Page turns up to PageSize plus one rows into a page, now ending the run time of running rows
func (s Sessions) Page(rows []store.StatsSessionRow, now time.Time) Page {
	page := Page{Items: []Session{}}
	if len(rows) > PageSize {
		last := rows[PageSize-1]
		next := EncodeCursor(last.CreatedAt, last.ID)
		page.Next = &next
		rows = rows[:PageSize]
	}
	for _, r := range rows {
		page.Items = append(page.Items, s.session(r, now))
	}
	return page
}

func (s Sessions) session(r store.StatsSessionRow, now time.Time) Session {
	out := Session{
		StartedAt: Time(r.CreatedAt), Commands: r.Commands, PriceMicros: r.PriceMicros,
		Status: status(r.State), EndReason: s.Ends.EndReason(r.State, r.TerminationReason,
			r.FailureReason),
		Wallet: Wallet(r, s.Facilitators), Tx: Tx(r.Tx),
	}
	if r.EndedAt != nil {
		end := Time(*r.EndedAt)
		out.EndedAt = &end
	}
	if r.Provider != "" {
		out.Provider = &r.Provider
	}
	if p, ok := s.Catalog.Get(r.PackageID); ok {
		res := p.Resources
		vcpu, mem := int64(res.VCPU), int64(res.MemoryMiB)
		out.VCPU, out.MemoryMiB = &vcpu, &mem
		out.GPU = gpu(res.GPUCount, res.GPUMemoryGiB, p.GPUModel)
	}
	if r.ReadyAt != nil {
		end := now
		if r.EndedAt != nil && r.EndedAt.Before(now) {
			end = *r.EndedAt
		}
		out.Seconds = max(int64(end.Sub(*r.ReadyAt)/time.Second), 0)
	}
	return out
}

// status is running for ready, ended for terminating and terminated, and failed for failed
func status(state string) string {
	switch state {
	case store.PlacementReady:
		return "running"
	case store.PlacementFailed:
		return "failed"
	}
	return "ended"
}

// Wallet returns the start order's payer when CP 7.4's steps 2 to 4 keep it and it decodes
func Wallet(r store.StatsSessionRow, facilitators []string) *string {
	payer, _, _ := wallet.Decide(wallet.Guard{UsePayer: true, Facilitators: facilitators},
		wallet.Claim{Payer: r.Payer, FeePayer: r.FeePayer},
		func(string) (bool, error) { return r.PayerWasFeePayer, nil })
	if _, ok := solana.PublicKey(payer); !ok {
		return nil
	}
	return &payer
}

// Tx returns s when it decodes to exactly 64 bytes
func Tx(s string) *string {
	if _, ok := solana.Signature(s); !ok {
		return nil
	}
	return &s
}
