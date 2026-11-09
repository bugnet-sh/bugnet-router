package boxd

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd/boxdpb"
)

func TestBoxdReadLimits(t *testing.T) {
	f := newFake(t)
	f.add("a", nil)
	f.add("b", nil)
	got, err := f.provider(t, fakeKey).ReadLimits(t.Context())
	eq(t, err, nil)
	eq(t, got, provider.Limits{MaxConcurrent: 25, Active: 2})
}

func TestBoxdPastDueFromWhoami(t *testing.T) {
	for _, tc := range []struct {
		name    string
		billing *boxdpb.BillingInfo
		org     *boxdpb.GetOrgBillingResponse
		want    provider.Block
	}{
		{"no billing info", nil, nil, provider.BlockNone},
		{"active subscription", &boxdpb.BillingInfo{SubscriptionStatus: "active"}, nil,
			provider.BlockNone},
		{"trialing", &boxdpb.BillingInfo{SubscriptionStatus: "trialing"}, nil,
			provider.BlockNone},
		{"past due status", &boxdpb.BillingInfo{SubscriptionStatus: "past_due"}, nil,
			provider.BlockUnfunded},
		{"canceled", &boxdpb.BillingInfo{SubscriptionStatus: "canceled"}, nil,
			provider.BlockUnfunded},
		{"past due since", &boxdpb.BillingInfo{SubscriptionStatus: "active", PastDueSince: 1}, nil,
			provider.BlockUnfunded},
		{"credits with balance", &boxdpb.BillingInfo{SubscriptionStatus: "active"},
			&boxdpb.GetOrgBillingResponse{BillingMode: "credits", BalanceMicro: 5},
			provider.BlockNone},
		{"credits spent", &boxdpb.BillingInfo{SubscriptionStatus: "active"},
			&boxdpb.GetOrgBillingResponse{BillingMode: "credits", BalanceMicro: 0},
			provider.BlockUnfunded},
		{"credits owed", &boxdpb.BillingInfo{SubscriptionStatus: "active"},
			&boxdpb.GetOrgBillingResponse{BillingMode: "credits", BalanceMicro: -3},
			provider.BlockUnfunded},
		{"a stripe org has no balance to read", &boxdpb.BillingInfo{SubscriptionStatus: "active"},
			&boxdpb.GetOrgBillingResponse{BillingMode: "stripe", BalanceMicro: 0},
			provider.BlockNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.setBilling(tc.billing, tc.org)
			acc, err := f.provider(t, fakeKey).CheckAccount(t.Context())
			if err != nil || acc.Block != tc.want {
				t.Fatalf("account = %+v, %v, want block %q", acc, err, tc.want)
			}
			if tc.want != provider.BlockNone && acc.Detail == "" {
				t.Fatal("a block carries no detail")
			}
		})
	}
}

func TestBoxdAccountNamesTheOrgFromTheMachineLimit(t *testing.T) {
	f := newFake(t)
	f.setBilling(&boxdpb.BillingInfo{SubscriptionStatus: "active"}, nil)
	if _, err := f.provider(t, fakeKey).CheckAccount(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	eq(t, f.orgAsked, "acme")
}

func TestBoxdAccountErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		method  string
		err     error
		want    provider.Block
		wantErr bool
	}{
		{"permission denied", "Whoami", status.Error(codes.PermissionDenied, "no"),
			provider.BlockAuth, false},
		{"unauthenticated", "GetOrgBilling", status.Error(codes.Unauthenticated, "no"),
			provider.BlockAuth, false},
		{"unavailable", "Whoami", status.Error(codes.Unavailable, "down"),
			provider.BlockNone, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.setBilling(&boxdpb.BillingInfo{SubscriptionStatus: "active"}, nil)
			f.inject(tc.method, fault{err: tc.err})
			acc, err := f.provider(t, fakeKey).CheckAccount(t.Context())
			if (err != nil) != tc.wantErr || acc.Block != tc.want {
				t.Fatalf("account = %+v, %v, want block %q error %v", acc, err, tc.want, tc.wantErr)
			}
		})
	}
}
