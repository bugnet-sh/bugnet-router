package lium

import (
	"context"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// wantOnePodOnNodeA fails the test unless the fake holds one pod and every rent went to node-a
func wantOnePodOnNodeA(t *testing.T, f *fakeLium, maxRents int) {
	t.Helper()
	eq(t, f.podCount(), 1)
	rents := f.rents()
	if len(rents) > maxRents {
		t.Fatalf("%d rents, at most %d", len(rents), maxRents)
	}
	for _, r := range rents {
		eq(t, rentNode(r), "node-a")
	}
}

func TestLiumAnswerAfterLostSend(t *testing.T) {
	for name, then := range map[string]fault{
		"own_rent_in_progress": {status: 400, code: "own_rent_in_progress", times: 1},
		"insufficient_balance": {status: 403, code: "insufficient_balance", times: 1},
		"unauthorized":         {status: 401, code: "unauthorized", times: 1},
		"validation_error":     {status: 422, code: "validation_error", times: 1},
		"template_not_found":   {status: 404, code: "template_not_found", times: 1},
		"an unknown 4xx":       {status: 418, code: "brand_new_code", times: 1},
		"rate limited": {status: 429, code: "rate_limited", times: 1,
			header: map[string]string{"Retry-After": "0"}},
		"node_unavailable": {status: 400, code: "node_unavailable", times: 1},
	} {
		for _, keyed := range []bool{true, false} {
			req, lists := checkReq(), 1+keylessRelists
			if keyed {
				req, lists = createReq(testKey), 2
			}
			label := name + map[bool]string{true: " with a key", false: " without a key"}[keyed]
			t.Run(label+", the pod listed", func(t *testing.T) {
				f, p := liveFake(t)
				// The first rent takes node-a and loses its answer
				f.inject(rentRoute, fault{drop: true, after: true, times: 1, then: &then})
				ref := mustCreate(t, p, req)
				wantOnePodOnNodeA(t, f, 2)
				eq(t, f.podsNamed(ref.Meta["pod_name"])[0].id, ref.ID)
				eq(t, len(f.requestsTo("POST", "/templates")), 1)
			})
			t.Run(label+", the listing lags", func(t *testing.T) {
				f, p := liveFake(t)
				f.inject(rentRoute, fault{drop: true, after: true, times: 1, then: &then})
				f.inject(podsRoute, fault{status: 200, raw: "[]"})
				_, err := p.Create(context.Background(), req)
				wantCode(t, err, provider.CodeOutcomeUnknown, false)
				wantOnePodOnNodeA(t, f, 2)
				eq(t, len(f.requestsTo("GET", "/pods")), lists)
			})
		}
	}
}

func TestLiumIdempotencyAnswersMarkUncertain(t *testing.T) {
	inProgress := fault{status: 409, code: "idempotency_in_progress", after: true, times: 1,
		header: map[string]string{"Retry-After": "0"}}
	for name, then := range map[string]fault{
		"node_unavailable": {status: 400, code: "node_unavailable", times: 1},
		"rate limited": {status: 429, code: "rate_limited", times: 1,
			header: map[string]string{"Retry-After": "0"}},
		"a server error": {status: 502, code: "upstream_unavailable", times: 2},
	} {
		t.Run("in progress, then "+name, func(t *testing.T) {
			f, p := liveFake(t)
			// Lium is still running the first rent, which takes node-a
			first := inProgress
			first.then = &then
			f.inject(rentRoute, first)
			f.inject(podsRoute, fault{status: 200, raw: "[]", skip: 1})
			_, err := p.Create(context.Background(), createReq(testKey))
			wantCode(t, err, provider.CodeOutcomeUnknown, false)
			wantOnePodOnNodeA(t, f, 3)
			r, ok := p.remembered(testKey)
			eq(t, ok && r.uncertain && r.mixed, true)
			// The key's retry adopts the pod once Lium lists it
			f.clearFaults()
			ref := mustCreate(t, p, createReq(testKey))
			eq(t, f.podsNamed(ref.Meta["pod_name"])[0].id, ref.ID)
			wantOnePodOnNodeA(t, f, 3)
		})
	}
	for name, retry := range map[string]fault{
		"rate limited": {status: 429, code: "rate_limited", times: 1,
			header: map[string]string{"Retry-After": "0"}},
		"two server errors": {status: 502, code: "upstream_unavailable", times: 2},
		"unauthorized":      {status: 401, code: "unauthorized", times: 1},
	} {
		t.Run("outcome unknown, then the key's retry is "+name, func(t *testing.T) {
			f, p := liveFake(t)
			f.inject(rentRoute, fault{status: 409, code: "idempotency_outcome_unknown",
				after: true, times: 1})
			f.inject(podsRoute, fault{status: 200, raw: "[]"})
			_, err := p.Create(context.Background(), createReq(testKey))
			wantCode(t, err, provider.CodeOutcomeUnknown, false)
			wantOnePodOnNodeA(t, f, 1)
			f.inject(rentRoute, retry)
			_, err = p.Create(context.Background(), createReq(testKey))
			wantCode(t, err, provider.CodeOutcomeUnknown, false)
			wantOnePodOnNodeA(t, f, 3)
		})
	}
}

func TestLiumMixedKeptAcrossCreates(t *testing.T) {
	f, p := liveFake(t)
	// A server error that rented node-a, then a resend whose answer is lost
	f.inject(rentRoute, fault{status: 500, code: "internal_error", after: true, times: 1,
		then: &fault{drop: true, times: 1}})
	f.inject(podsRoute, fault{status: 200, raw: "[]"})
	_, err := p.Create(context.Background(), createReq(testKey))
	wantCode(t, err, provider.CodeOutcomeUnknown, false)
	r, ok := p.remembered(testKey)
	eq(t, ok && r.uncertain && r.mixed, true)
	// The key's retry gets two server errors, which say nothing of the lost answer
	f.inject(rentRoute, fault{status: 502, code: "upstream_unavailable", times: 2})
	_, err = p.Create(context.Background(), createReq(testKey))
	wantCode(t, err, provider.CodeOutcomeUnknown, false)
	wantOnePodOnNodeA(t, f, 4)
	eq(t, len(f.rents()), 4)
}

func TestLiumKeyAnswersAfterUncertain(t *testing.T) {
	for _, code := range []string{"idempotency_key_reused", "idempotency_key_invalid"} {
		t.Run(code, func(t *testing.T) {
			f, p := liveFake(t)
			f.inject(rentRoute, fault{drop: true, after: true, times: 1,
				then: &fault{status: 422, code: code, times: 1}})
			_, err := p.Create(context.Background(), createReq(testKey))
			wantCode(t, err, provider.CodeInternal, false)
			eq(t, codeOf(err), code)
			wantOnePodOnNodeA(t, f, 2)
			eq(t, len(f.rents()), 2)
			// Only the opening listing ran
			eq(t, len(f.requestsTo("GET", "/pods")), 1)
		})
	}
}
