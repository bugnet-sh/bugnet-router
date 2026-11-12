package lium

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// accountFake is an adapter of 2 slots on a fake holding no pods, its error log in logs
func accountFake(t *testing.T) (*fakeLium, *Provider, *bytes.Buffer) {
	t.Helper()
	f := newFake(t)
	var logs bytes.Buffer
	opts := testOptions()
	opts.MaxConcurrent, opts.AccountEvery = 2, 5*time.Minute
	opts.Log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelError}))
	return f, f.provider(t, opts, time.Now), &logs
}

// slotsMicros is 2 slots at lium-h200's cap for 20 minutes, rounded up
const slotsMicros = 3_266_667

func TestLiumCheckAccount(t *testing.T) {
	ctx := context.Background()
	t.Run("a balance over the headroom passes", func(t *testing.T) {
		f, p, logs := accountFake(t)
		var _ provider.AccountChecker = p
		acc, err := p.CheckAccount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, acc, provider.Account{})
		eq(t, logs.String(), "")
		eq(t, len(f.requestsTo("GET", "/users/me")), 1)
		eq(t, len(f.requestsTo("GET", "/pods")), 1)
	})
	t.Run("the headroom holds 15 minutes of each pod and every slot", func(t *testing.T) {
		f, p, _ := accountFake(t)
		f.addPod(fakePod{status: "RUNNING", price: 0.25})
		f.addPod(fakePod{status: "PENDING", price: 0.30})
		f.addPod(fakePod{status: "FAILED", price: 0.40})
		// A pod with no price is held at the dearest cap
		f.addPod(fakePod{status: "START_PENDING", unpriced: true})
		// Lium bills neither a stopped pod nor one being deleted
		f.addPod(fakePod{status: "STOPPED", price: 9})
		f.addPod(fakePod{status: "STOP_PENDING", price: 9})
		f.addPod(fakePod{status: "DELETING", price: 9})
		need := int64(62_500+75_000+100_000+1_225_000) + slotsMicros
		f.setAccount(float64(need)/1e6, nil)
		acc, err := p.CheckAccount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, acc, provider.Account{})
		f.setAccount(float64(need-1)/1e6, nil)
		acc, err = p.CheckAccount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, acc.Block, provider.BlockUnfunded)
		for _, part := range []string{"balance $4.73", "headroom $4.73"} {
			if !strings.Contains(acc.Detail, part) {
				t.Fatalf("detail %q does not name %q", acc.Detail, part)
			}
		}
	})
	t.Run("the design's example", func(t *testing.T) {
		f := newFake(t)
		opts := testOptions()
		opts.Sizes = append(opts.Sizes, SizeSpec{
			Name: "lium-b200", Model: gpu.B200, Match: []string{"NVIDIA B200"}, GPUs: 1,
			GPUMemoryGiB: 180, VCPU: 16, MemoryMiB: 245760, DiskGiB: 900,
			MaxHourlyMicros: 9_500_000,
		})
		opts.MaxConcurrent, opts.AccountEvery = 10, 5*time.Minute
		p := f.provider(t, opts, time.Now)
		for range 10 {
			f.addPod(fakePod{status: "RUNNING", price: 9.5})
		}
		// 10 pods at $9.50 for 15 minutes, $23.75, and 10 slots for 20 minutes, $31.67
		f.setAccount(55.41, nil)
		acc, err := p.CheckAccount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if acc.Block != provider.BlockUnfunded || !strings.Contains(acc.Detail, "$55.42") {
			t.Fatalf("got %+v", acc)
		}
		f.setAccount(55.42, nil)
		if acc, err = p.CheckAccount(ctx); err != nil || acc.Block != provider.BlockNone {
			t.Fatalf("got %+v, %v", acc, err)
		}
	})
	t.Run("a balance written as a string", func(t *testing.T) {
		f, p, _ := accountFake(t)
		f.setAccount("3.20", nil)
		acc, err := p.CheckAccount(ctx)
		if err != nil || acc.Block != provider.BlockUnfunded {
			t.Fatalf("got %+v, %v", acc, err)
		}
		f.setAccount(" 3.30 ", nil)
		if acc, err = p.CheckAccount(ctx); err != nil || acc.Block != provider.BlockNone {
			t.Fatalf("got %+v, %v", acc, err)
		}
	})
	t.Run("an unreadable balance proves nothing", func(t *testing.T) {
		for _, balance := range []any{nil, "lots", true, map[string]any{"usd": 3}, "NaN"} {
			f, p, _ := accountFake(t)
			f.setAccount(balance, nil)
			_, err := p.CheckAccount(ctx)
			wantCode(t, err, provider.CodeInternal, false)
		}
	})
	t.Run("pods stopped at zero is unfunded", func(t *testing.T) {
		f, p, _ := accountFake(t)
		f.setAccount(500.0, map[string]any{"kind": "pods_stopped", "message": "pods stopped"})
		acc, err := p.CheckAccount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, acc.Block, provider.BlockUnfunded)
		if !strings.Contains(acc.Detail, "pods_stopped") {
			t.Fatalf("detail %q", acc.Detail)
		}
	})
	t.Run("other warnings are logged and shown, never a block", func(t *testing.T) {
		for _, kind := range []string{"runway", "auto_top_up_failed", "a_kind_lium_adds"} {
			f, p, logs := accountFake(t)
			f.setAccount(42.5, map[string]any{"kind": kind})
			acc, err := p.CheckAccount(ctx)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, acc.Block, provider.BlockNone)
			if !strings.Contains(acc.Detail, kind) {
				t.Fatalf("detail %q does not name %s", acc.Detail, kind)
			}
			if !strings.Contains(logs.String(), "level=ERROR") ||
				!strings.Contains(logs.String(), kind) {
				t.Fatalf("log %q", logs.String())
			}
		}
		// A warning written as a bare kind is read too
		f, p, _ := accountFake(t)
		f.setAccount(42.5, "pods_stopped")
		acc, err := p.CheckAccount(ctx)
		if err != nil || acc.Block != provider.BlockUnfunded {
			t.Fatalf("got %+v, %v", acc, err)
		}
	})
	t.Run("an empty warning is none", func(t *testing.T) {
		for _, warning := range []any{"", false, map[string]any{}} {
			f, p, logs := accountFake(t)
			f.setAccount(42.5, warning)
			acc, err := p.CheckAccount(ctx)
			if err != nil || acc.Block != provider.BlockNone ||
				strings.Contains(acc.Detail, warningUnknown) ||
				strings.Contains(logs.String(), "level=ERROR") {
				t.Fatalf("warning %#v gave %+v, %v, log %q", warning, acc, err, logs.String())
			}
		}
	})
	t.Run("refused calls", func(t *testing.T) {
		for _, tc := range []struct {
			route string
			ft    fault
			block provider.Block
		}{
			{"GET /users/me", fault{status: 401, code: "unauthorized"}, provider.BlockAuth},
			{"GET /users/me", fault{status: 401, code: "api_key_inactive"}, provider.BlockAuth},
			{"GET /users/me", fault{status: 403, code: "forbidden"}, provider.BlockAuth},
			{"GET /users/me", fault{status: 403, code: "email_not_verified"},
				provider.BlockAuth},
			{"GET /users/me", fault{status: 403, code: "insufficient_balance"},
				provider.BlockUnfunded},
			{"GET /users/me", fault{status: 403, code: "account_balance_invalid"},
				provider.BlockAuth},
			{"GET /pods", fault{status: 402, code: "BALANCE_REQUIRED"}, provider.BlockUnfunded},
			{"GET /pods", fault{status: 402, code: "API_KEY_BUDGET_EXCEEDED"},
				provider.BlockUnfunded},
			{"GET /pods", fault{status: 401, code: "api_key_inactive"}, provider.BlockAuth},
		} {
			f, p, _ := accountFake(t)
			f.inject(tc.route, tc.ft)
			acc, err := p.CheckAccount(ctx)
			if err != nil || acc.Block != tc.block || !strings.Contains(acc.Detail, tc.ft.code) {
				t.Fatalf("%s %s gave %+v, %v, want %s", tc.route, tc.ft.code, acc, err, tc.block)
			}
		}
	})
	t.Run("failed calls prove nothing", func(t *testing.T) {
		for _, tc := range []struct {
			route string
			ft    fault
		}{
			{"GET /users/me", fault{status: 429, code: "rate_limited"}},
			{"GET /users/me", fault{status: 500, code: "internal_error"}},
			{"GET /users/me", fault{drop: true}},
			{"GET /users/me", fault{status: 200, raw: "not json"}},
			{"GET /pods", fault{status: 503, code: "upstream_unavailable"}},
		} {
			f, p, _ := accountFake(t)
			f.inject(tc.route, tc.ft)
			if _, err := p.CheckAccount(ctx); err == nil {
				t.Fatalf("%s %+v gave no error", tc.route, tc.ft)
			}
		}
	})
}

func TestLiumReadLimits(t *testing.T) {
	ctx := context.Background()
	f, p, _ := accountFake(t)
	var _ provider.LimitsReader = p
	got, err := p.ReadLimits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got, provider.Limits{})
	for _, status := range []string{
		"PENDING", "START_PENDING", "RUNNING", "STOPPED", "FAILED", "BROKEN", "DELETING",
	} {
		f.addPod(fakePod{status: status})
	}
	// A pod of a name the router did not write is the account's too
	f.addPod(fakePod{status: "RUNNING", name: "someone-else"})
	got, err = p.ReadLimits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got, provider.Limits{Active: 7})

	f.inject("GET /pods", fault{status: 402, code: "BALANCE_REQUIRED", times: 1})
	got, err = p.ReadLimits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got, provider.Limits{})
	f.inject("GET /pods", fault{status: 500, code: "internal_error", times: 1})
	if _, err := p.ReadLimits(ctx); err == nil {
		t.Fatal("a failed listing gave limits")
	}
}
