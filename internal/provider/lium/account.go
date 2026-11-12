package lium

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var (
	_ provider.AccountChecker = (*Provider)(nil)
	_ provider.LimitsReader   = (*Provider)(nil)
)

const (
	// defaultAccountEvery is limits_poll.account_every's default
	defaultAccountEvery = 5 * time.Minute
	// rentRoom is the time of each pod Lium wants the balance to cover before it rents another
	rentRoom = 15 * time.Minute
	// warningPodsStopped is the balance warning of an account whose pods Lium stopped at zero
	warningPodsStopped = "pods_stopped"
	// warningUnknown names a balance warning whose kind cannot be read
	warningUnknown = "unknown"
	// maxUSD bounds an amount the adapter reads, far above any balance
	maxUSD = 1e9
)

// userMe holds the fields of GET /users/me the account check reads, their types unverified
type userMe struct {
	Balance json.RawMessage `json:"balance"`
	Warning json.RawMessage `json:"balance_warning"`
}

// CheckAccount reads the balance and its warning and holds the balance to design 3.9's headroom
func (p *Provider) CheckAccount(ctx context.Context) (provider.Account, error) {
	var me userMe
	if _, err := p.api.do(ctx, call{method: http.MethodGet, route: "/users/me"}, &me); err != nil {
		return provider.AccountReading(err)
	}
	kind := warningKind(me.Warning)
	if kind == warningPodsStopped {
		return provider.Account{Block: provider.BlockUnfunded,
			Detail: "lium warns " + kind + ", it stopped the account's pods at zero balance"}, nil
	}
	balance, ok := microsOf(me.Balance)
	if !ok {
		return provider.Account{}, &provider.Error{Code: provider.CodeInternal,
			Err: errors.New("lium GET /users/me names no balance the adapter can read")}
	}
	var pods []podItem
	// GET /pods answers BALANCE_REQUIRED on an account with no balance and no pods
	if _, err := p.api.do(ctx, call{method: http.MethodGet, route: "/pods"}, &pods); err != nil {
		return provider.AccountReading(err)
	}
	var acc provider.Account
	if kind != "" {
		p.log.Error("lium: the account has a balance warning", "kind", kind,
			"balance", usd(balance))
		acc.Detail = "lium warns " + kind
	}
	if need := p.headroom(pods); balance < need {
		detail := fmt.Sprintf("balance %s is under the headroom %s", usd(balance), usd(need))
		if acc.Detail != "" {
			detail += ", " + acc.Detail
		}
		return provider.Account{Block: provider.BlockUnfunded, Detail: detail}, nil
	}
	return acc, nil
}

// headroom is design 3.9's floor, 15 minutes of each billed pod and of every slot past a check
func (p *Provider) headroom(pods []podItem) int64 {
	dearest := p.sizes[len(p.sizes)-1].MaxHourlyMicros
	var need int64
	for _, pod := range pods {
		// A stopped pod is not charged, and a deleting one is gone
		if pod.Status == statusDeleting || stoppedStatuses[pod.Status] {
			continue
		}
		price := dearest
		if pod.Price != nil && *pod.Price >= 0 && *pod.Price < maxUSD {
			price = micros(*pod.Price)
		}
		need += ceilDiv(price*int64(rentRoom/time.Second), 3600)
	}
	window := int64((p.accountEvery + rentRoom) / time.Second)
	return need + ceilDiv(dearest*int64(p.slots)*window, 3600)
}

// warningKind reads balance_warning's kind from an object or a bare word, empty for none
func warningKind(raw json.RawMessage) string {
	if !present(raw) {
		return ""
	}
	var w struct {
		Kind string `json:"kind"`
	}
	if json.Unmarshal(raw, &w) == nil && w.Kind != "" {
		return w.Kind
	}
	var s string
	if json.Unmarshal(raw, &s) == nil && s != "" {
		return s
	}
	return warningUnknown
}

// microsOf reads USD written as a JSON number or a number in a string, in micro-USD
func microsOf(raw json.RawMessage) (int64, bool) {
	if !present(raw) {
		return 0, false
	}
	var f float64
	if json.Unmarshal(raw, &f) != nil {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return 0, false
		}
		var err error
		if f, err = strconv.ParseFloat(strings.TrimSpace(s), 64); err != nil {
			return 0, false
		}
	}
	if math.IsNaN(f) || math.Abs(f) >= maxUSD {
		return 0, false
	}
	return micros(f), true
}

// usd writes micro-USD as dollars and cents
func usd(m int64) string {
	return fmt.Sprintf("$%.2f", float64(m)/1e6)
}

// ReadLimits counts the pods the key sees that are not being deleted, as Lium names no cap
func (p *Provider) ReadLimits(ctx context.Context) (provider.Limits, error) {
	pods, err := p.listPods(ctx)
	if err != nil {
		return provider.Limits{}, err
	}
	n := 0
	for _, pod := range pods {
		if pod.Status != statusDeleting {
			n++
		}
	}
	return provider.Limits{Active: n}, nil
}
