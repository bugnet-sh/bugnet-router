package e2b

import (
	"net/http"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestE2BReadLimits(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	f.add(map[string]string{"a": "1"})
	f.add(map[string]string{"a": "2"})
	got, err := p.ReadLimits(t.Context())
	// The cap is not readable with an API key, so the setting gives it
	eq(t, err, nil)
	eq(t, got, provider.Limits{Active: 2})

	f.noTotalRunning.Store(true)
	if _, err := p.ReadLimits(t.Context()); err == nil {
		t.Fatal("a listing without X-Total-Running was read as limits")
	}
}

func TestE2BAccountMappings(t *testing.T) {
	for _, tc := range []struct {
		name    string
		team    bool
		pattern string
		fault   *fault
		want    provider.Block
		wantErr bool
	}{
		{name: "healthy team", team: true, pattern: "GET /teams/{team}/metrics/max"},
		{
			name: "banned team", team: true, pattern: "GET /teams/{team}/metrics/max",
			fault: &fault{status: 403, message: "team is banned"}, want: provider.BlockBanned,
		},
		{
			name: "blocked team", team: true, pattern: "GET /teams/{team}/metrics/max",
			fault: &fault{status: 403, message: "team is blocked"}, want: provider.BlockUnfunded,
		},
		{
			name: "bad key", team: true, pattern: "GET /teams/{team}/metrics/max",
			fault: &fault{status: 401, message: "Invalid API key"}, want: provider.BlockAuth,
		},
		{name: "healthy without a team id", pattern: "GET /v2/sandboxes"},
		{
			name: "banned seen on the list call", pattern: "GET /v2/sandboxes",
			fault: &fault{status: 403, message: "team is banned"}, want: provider.BlockBanned,
		},
		{
			name: "blocked seen on the list call", pattern: "GET /v2/sandboxes",
			fault: &fault{status: 403, message: "team is blocked"}, want: provider.BlockUnfunded,
		},
		{
			name: "a server fault proves nothing", team: true,
			pattern: "GET /teams/{team}/metrics/max",
			fault:   &fault{status: http.StatusBadGateway, message: "upstream"}, wantErr: true,
		},
		{
			name: "a rate limit proves nothing", team: true,
			pattern: "GET /teams/{team}/metrics/max",
			fault:   &fault{status: http.StatusTooManyRequests, message: "slow"}, wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			if tc.team {
				p.teamID = "team-1"
			}
			if tc.fault != nil {
				f.inject(tc.pattern, *tc.fault)
			}
			acc, err := p.CheckAccount(t.Context())
			if (err != nil) != tc.wantErr || acc.Block != tc.want {
				t.Fatalf("account = %+v, %v, want block %q error %v", acc, err, tc.want, tc.wantErr)
			}
			if tc.want != provider.BlockNone && acc.Detail == "" {
				t.Fatal("a block carries no detail")
			}
		})
	}
}

func TestE2BAccountErrorsNameTheAdapter(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	f.inject("POST /v2/sandboxes", fault{status: 403, message: "team is banned"})
	_, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "k", Size: provider.Size{Name: "2x2"}, Lifetime: 10 * time.Minute,
	})
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeAbuseLock || e.Provider != "e2b" {
		t.Fatalf("create error = %+v, want an abuse lock naming e2b", e)
	}
}
