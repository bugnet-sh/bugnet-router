package koyeb

import (
	"net/http"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestKoyebReadLimits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		usage map[string]any
		want  provider.Limits
	}{
		{"the smaller of services and apps",
			map[string]any{"services_limit": "100", "apps_limit": 40, "services_used": "7"},
			provider.Limits{MaxConcurrent: 40, Active: 7}},
		{"services alone",
			map[string]any{"services_limit": 50, "services_used": 3},
			provider.Limits{MaxConcurrent: 50, Active: 3}},
		{"nested under usage",
			map[string]any{"usage": map[string]any{
				"services_limit": 10, "apps_limit": 20, "services_used": 4,
			}},
			provider.Limits{MaxConcurrent: 10, Active: 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.setOrg(nil, tc.usage)
			got, err := f.provider(t, fakeToken).ReadLimits(t.Context())
			eq(t, err, nil)
			eq(t, got, tc.want)
		})
	}
}

func TestKoyebReadsTheOrganizationIDOnce(t *testing.T) {
	f := newFake(t)
	f.setOrg(nil, map[string]any{"services_limit": 5})
	p := f.provider(t, fakeToken)
	for range 2 {
		if _, err := p.ReadLimits(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, f.callCount("GET /v1/account/organization"), 1)
	f.mu.Lock()
	eq(t, f.quotaOrg, "org-1")
	f.mu.Unlock()

	// A setting needs no read of the account
	g := newFake(t)
	g.setOrg(nil, map[string]any{"services_limit": 5})
	q := g.provider(t, fakeToken)
	q.orgID = "org-set"
	if _, err := q.ReadLimits(t.Context()); err != nil {
		t.Fatal(err)
	}
	eq(t, g.callCount("GET /v1/account/organization"), 0)
	g.mu.Lock()
	eq(t, g.quotaOrg, "org-set")
	g.mu.Unlock()
}

func TestKoyebAccountMappings(t *testing.T) {
	for _, tc := range []struct {
		name string
		org  map[string]any
		want provider.Block
	}{
		{"active", nil, provider.BlockNone},
		{"warning is logged and never a block", map[string]any{"status": "WARNING"},
			provider.BlockNone},
		{"locked", map[string]any{"status": "LOCKED"}, provider.BlockBanned},
		{"deactivating", map[string]any{"status": "DEACTIVATING"}, provider.BlockBanned},
		{"deactivated", map[string]any{"status": "DEACTIVATED"}, provider.BlockBanned},
		{"deleting", map[string]any{"status": "DELETING"}, provider.BlockBanned},
		{"deleted", map[string]any{"status": "DELETED"}, provider.BlockBanned},
		{"locked by admin", map[string]any{"deactivation_reason": "LOCKED_BY_ADMIN"},
			provider.BlockBanned},
		{"payment failure", map[string]any{"status_message": "PAYMENT_FAILURE"},
			provider.BlockUnfunded},
		{"billing info missing", map[string]any{"status_message": "BILLING_INFO_MISSING"},
			provider.BlockUnfunded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.setOrg(tc.org, nil)
			acc, err := f.provider(t, fakeToken).CheckAccount(t.Context())
			if err != nil || acc.Block != tc.want {
				t.Fatalf("account = %+v, %v, want block %q", acc, err, tc.want)
			}
			if tc.want != provider.BlockNone && acc.Detail == "" {
				t.Fatal("a block carries no detail")
			}
		})
	}
}

func TestKoyebAccountErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fault   fault
		want    provider.Block
		wantErr bool
	}{
		{"401 is auth", fault{status: http.StatusUnauthorized, code: "authentication_error",
			message: "bad token"}, provider.BlockAuth, false},
		{"a suspended organization", fault{status: http.StatusForbidden, code: "permission_denied",
			message: "organization suspended"}, provider.BlockBanned, false},
		{"a 5xx proves nothing", fault{status: http.StatusBadGateway, code: "api_error",
			message: "upstream"}, provider.BlockNone, true},
		{"a rate limit proves nothing", fault{status: http.StatusTooManyRequests,
			code: "rate_limit_error", message: "slow"}, provider.BlockNone, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.inject("GET /v1/account/organization", tc.fault)
			acc, err := f.provider(t, fakeToken).CheckAccount(t.Context())
			if (err != nil) != tc.wantErr || acc.Block != tc.want {
				t.Fatalf("account = %+v, %v, want block %q error %v", acc, err, tc.want, tc.wantErr)
			}
		})
	}
}
