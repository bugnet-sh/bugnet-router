package stats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// TotalsPath is Payments' totals route on its internal API
const TotalsPath = "/internal/v1/stats/totals"

// maxTotalsBytes bounds Payments' answer
const maxTotalsBytes = 1 << 20

// maxWhole is the largest whole number a double holds exactly, 2^53 - 1
const maxWhole = 1<<53 - 1

// paymentsLimits are the limit keys Payments owns, any other key it sends is dropped
var paymentsLimits = []string{
	"order.max_micros", "x402.max_allowance_micros", "x402.max_price_per_call_micros",
}

var (
	paymentKinds = []string{"sandbox", "extend", "x402_allowance", "x402_fee"}
	methodRule   = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
	dayRule      = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
)

// Payments' answers that are no outage, and the checks of CP 20.7, each naming what failed
var (
	// ErrNoToken means BUGNET_PAYMENTS_ROUTER_TOKEN is empty, so no call is made
	ErrNoToken = errors.New("BUGNET_PAYMENTS_ROUTER_TOKEN is empty")
	// ErrToken means Payments answered 404 or 401, a missing or wrong router token
	ErrToken = errors.New("payments refused the router token")

	errStatus  = errors.New("payments answered a status other than 200")
	errSize    = errors.New("payments answered over 1 MiB")
	errShape   = errors.New("payments answered a body that is not the totals shape")
	errVersion = errors.New("payments answered a version other than 1")
	errNumber  = errors.New("payments answered a number that is not whole from 0 to 2^53 - 1")
	errMissing = errors.New("payments answered without a required field")
	errDay     = errors.New("payments answered a day that is malformed, out of order or ahead")
	errKind    = errors.New("payments answered an unknown kind")
	errMethod  = errors.New("payments answered a malformed method")
)

// PaymentsPart is Payments' totals answer of CP 20.7, as checked
type PaymentsPart struct {
	Version  int              `json:"version"`
	Daily    []PaymentsDay    `json:"daily"`
	Totals   Money            `json:"totals"`
	ByKind   []KindCount      `json:"byKind"`
	Refunds  Count            `json:"refunds"`
	ByMethod []MethodCount    `json:"byMethod"`
	Limits   map[string]int64 `json:"limits"`
}

// PaymentsDay is Payments' money of one UTC day
type PaymentsDay struct {
	Day string `json:"day"`
	Money
}

// Payments reads Payments' totals call
type Payments struct {
	URL, Token string
	HTTP       *http.Client
	Timeout    time.Duration
	// Now gives the router's today for the day check, time.Now when nil
	Now func() time.Time
}

// NewPayments returns a client that never follows a redirect, so the bearer goes nowhere else
func NewPayments(url, token string, timeout time.Duration) *Payments {
	return &Payments{
		URL: strings.TrimSuffix(url, "/"), Token: token, Timeout: timeout,
		HTTP: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
}

// Totals calls the totals route and runs every check of CP 20.7
func (p *Payments) Totals(ctx context.Context) (PaymentsPart, error) {
	if p.Token == "" {
		return PaymentsPart{}, ErrNoToken
	}
	if p.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.Timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL+TotalsPath, nil)
	if err != nil {
		return PaymentsPart{}, fmt.Errorf("payments totals: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.Token)
	req.Header.Set("Accept", "application/json")
	client := p.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return PaymentsPart{}, fmt.Errorf("payments totals: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound, http.StatusUnauthorized:
		return PaymentsPart{}, fmt.Errorf("%w, status %d", ErrToken, resp.StatusCode)
	default:
		return PaymentsPart{}, fmt.Errorf("%w, status %d", errStatus, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTotalsBytes+1))
	if err != nil {
		return PaymentsPart{}, fmt.Errorf("payments totals: %w", err)
	}
	if len(body) > maxTotalsBytes {
		return PaymentsPart{}, errSize
	}
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	return ParseTotals(body, now())
}

// whole is a JSON number that must be a whole number from 0 to 2^53 - 1
type whole struct {
	v   int64
	set bool
}

// UnmarshalJSON takes a number literal alone, so a string, a fraction or null fails
func (w *whole) UnmarshalJSON(b []byte) error {
	v, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil || v < 0 || v > maxWhole {
		return errNumber
	}
	w.v, w.set = v, true
	return nil
}

type wireMoney struct {
	EarnedMicros            whole `json:"earnedMicros"`
	RefundedMicros          whole `json:"refundedMicros"`
	AllowanceReturnedMicros whole `json:"allowanceReturnedMicros"`
	X402SpentMicros         whole `json:"x402SpentMicros"`
}

type wireDay struct {
	Day *string `json:"day"`
	wireMoney
}

type wireCount struct {
	Kind   *string `json:"kind"`
	Method *string `json:"method"`
	Count  whole   `json:"count"`
	Micros whole   `json:"micros"`
}

type wirePart struct {
	Version  whole                      `json:"version"`
	Daily    *[]wireDay                 `json:"daily"`
	Totals   *wireMoney                 `json:"totals"`
	ByKind   *[]wireCount               `json:"byKind"`
	Refunds  *wireCount                 `json:"refunds"`
	ByMethod *[]wireCount               `json:"byMethod"`
	Limits   map[string]json.RawMessage `json:"limits"`
}

// ParseTotals runs CP 20.7's checks on a totals body, today being the router's UTC time
func ParseTotals(body []byte, today time.Time) (PaymentsPart, error) {
	var w wirePart
	if err := json.Unmarshal(body, &w); err != nil {
		if errors.Is(err, errNumber) {
			return PaymentsPart{}, errNumber
		}
		return PaymentsPart{}, errShape
	}
	if !w.Version.set || w.Daily == nil || w.Totals == nil || w.ByKind == nil ||
		w.Refunds == nil || w.ByMethod == nil {
		return PaymentsPart{}, errMissing
	}
	if w.Version.v != Version {
		return PaymentsPart{}, errVersion
	}
	p := PaymentsPart{Version: Version, Daily: []PaymentsDay{}, ByKind: []KindCount{},
		ByMethod: []MethodCount{}, Limits: map[string]int64{}}
	var err error
	if p.Totals, err = w.Totals.money(); err != nil {
		return PaymentsPart{}, err
	}
	if p.Daily, err = checkDays(*w.Daily, today); err != nil {
		return PaymentsPart{}, err
	}
	for _, k := range *w.ByKind {
		if k.Kind == nil || !k.Count.set || !k.Micros.set {
			return PaymentsPart{}, errMissing
		}
		if !slices.Contains(paymentKinds, *k.Kind) {
			return PaymentsPart{}, errKind
		}
		p.ByKind = append(p.ByKind, KindCount{Kind: *k.Kind, Count: k.Count.v,
			Micros: k.Micros.v})
	}
	for _, m := range *w.ByMethod {
		if m.Method == nil || !m.Count.set || !m.Micros.set {
			return PaymentsPart{}, errMissing
		}
		if !methodRule.MatchString(*m.Method) {
			return PaymentsPart{}, errMethod
		}
		p.ByMethod = append(p.ByMethod, MethodCount{Method: *m.Method, Count: m.Count.v,
			Micros: m.Micros.v})
	}
	if !w.Refunds.Count.set || !w.Refunds.Micros.set {
		return PaymentsPart{}, errMissing
	}
	p.Refunds = Count{Count: w.Refunds.Count.v, Micros: w.Refunds.Micros.v}
	for _, key := range paymentsLimits {
		raw, ok := w.Limits[key]
		if !ok {
			continue
		}
		var v whole
		if err := json.Unmarshal(raw, &v); err != nil {
			return PaymentsPart{}, errNumber
		}
		p.Limits[key] = v.v
	}
	return p, nil
}

func (w wireMoney) money() (Money, error) {
	for _, v := range []whole{w.EarnedMicros, w.RefundedMicros, w.AllowanceReturnedMicros,
		w.X402SpentMicros} {
		if !v.set {
			return Money{}, errMissing
		}
	}
	return Money{
		EarnedMicros: w.EarnedMicros.v, RefundedMicros: w.RefundedMicros.v,
		AllowanceReturnedMicros: w.AllowanceReturnedMicros.v, X402SpentMicros: w.X402SpentMicros.v,
	}, nil
}

// checkDays takes days that rise strictly and none past the router's today plus one day
func checkDays(days []wireDay, today time.Time) ([]PaymentsDay, error) {
	out := []PaymentsDay{}
	latest := today.UTC().Truncate(24*time.Hour).AddDate(0, 0, 1)
	var prev time.Time
	for _, d := range days {
		if d.Day == nil {
			return nil, errMissing
		}
		day, err := time.Parse(dayLayout, *d.Day)
		if err != nil || !dayRule.MatchString(*d.Day) || day.After(latest) ||
			!prev.IsZero() && !day.After(prev) {
			return nil, errDay
		}
		prev = day
		m, err := d.money()
		if err != nil {
			return nil, err
		}
		out = append(out, PaymentsDay{Day: *d.Day, Money: m})
	}
	return out, nil
}
