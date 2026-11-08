package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testPayTo = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

// x402Router answers CheckX402, refusing the host bad.example.com, and ListX402
type x402Router struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	checks chan *connect.Request[routerv1.CheckX402Request]
	lists  chan *connect.Request[routerv1.ListX402Request]
}

func (r *x402Router) CheckX402(
	_ context.Context, req *connect.Request[routerv1.CheckX402Request],
) (*connect.Response[routerv1.CheckX402Response], error) {
	r.checks <- req
	for _, s := range req.Msg.GetAdd() {
		if s.GetHost() == "bad.example.com" {
			ce := connect.NewError(connect.CodeInvalidArgument,
				errors.New("an x402 service's host is refused"))
			d, _ := connect.NewErrorDetail(&errdetails.ErrorInfo{
				Reason: "HOST_REFUSED", Domain: "bugnet.router",
				Metadata: map[string]string{
					"service_id": "xs_aaaaaaaaaaaaaaaa", "host": s.GetHost(),
				},
			})
			ce.AddDetail(d)
			return nil, ce
		}
	}
	return connect.NewResponse(&routerv1.CheckX402Response{}), nil
}

func (r *x402Router) ListX402(
	_ context.Context, req *connect.Request[routerv1.ListX402Request],
) (*connect.Response[routerv1.ListX402Response], error) {
	r.lists <- req
	return connect.NewResponse(&routerv1.ListX402Response{
		Enabled: true,
		Services: []*routerv1.X402ServiceInfo{{
			ServiceId: "xs_w5a4htopmpzmmhos",
			State:     routerv1.X402ServiceState_X402_SERVICE_STATE_ACTIVE,
			Spec: &routerv1.X402ServiceSpec{
				Host: "agent402.tools", Prefix: "/v1/auto/", Method: "POST",
				MaxPriceMicros: 10000, PayTo: testPayTo, Indexed: true,
			},
			Requests: 4, Paid: 2, Uncertain: 1,
		}, {
			ServiceId: "xs_bbbbbbbbbbbbbbbb",
			State:     routerv1.X402ServiceState_X402_SERVICE_STATE_FAILED,
			Spec: &routerv1.X402ServiceSpec{
				Host: "api.example.com", Prefix: "/search/", Method: "GET",
				MaxPriceMicros: 500, PayTo: testPayTo, PaidPath: "/search",
			},
			FailureReason: "HOST_BLOCKED",
		}},
		Refusals: []*routerv1.X402Refusal{{
			Reason: "path_refused", Count: 3,
			LastAt: timestamppb.New(time.Date(2026, 11, 8, 9, 5, 0, 0, time.UTC)),
		}},
	}), nil
}

func newX402Router() *x402Router {
	return &x402Router{
		checks: make(chan *connect.Request[routerv1.CheckX402Request], 1),
		lists:  make(chan *connect.Request[routerv1.ListX402Request], 1),
	}
}

func TestX402Check(t *testing.T) {
	r := newX402Router()
	settings := serve(t, r)
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": strings.Repeat("i", 32)})
	out, stderr, code := runCLI(t, vars, "--config", settings, "x402", "check", "--handle",
		"sbx_x", "--service", "GET,api.example.com,/search/,10000,"+testPayTo+",/search,a",
		"--service", "POST,agent402.tools,/v1/auto/,500,"+testPayTo, "--disable", "xs_one")
	if code != 0 || out != "passed=true\n" {
		t.Fatalf("x402 check exited %d with %q and %q", code, out, stderr)
	}
	got := (<-r.checks).Msg
	want := &routerv1.CheckX402Request{
		Handle: "sbx_x", Disable: []string{"xs_one"},
		Add: []*routerv1.X402ServiceSpec{{
			Method: "GET", Host: "api.example.com", Prefix: "/search/", MaxPriceMicros: 10000,
			PayTo: testPayTo, PaidPath: "/search,a",
		}, {
			Method: "POST", Host: "agent402.tools", Prefix: "/v1/auto/", MaxPriceMicros: 500,
			PayTo: testPayTo, Indexed: true,
		}},
	}
	if !proto.Equal(got, want) {
		t.Fatalf("CheckX402 got %v, want %v", got, want)
	}

	_, stderr, code = runCLI(t, vars, "--config", settings, "x402", "check", "--service",
		"GET,bad.example.com,/v1/,1,"+testPayTo)
	want2 := "bugnet-dev x402: invalid_argument HOST_REFUSED host=bad.example.com " +
		"service_id=xs_aaaaaaaaaaaaaaaa: an x402 service's host is refused\n"
	if code != 1 || stderr != want2 {
		t.Fatalf("refused check exited %d with %q, want 1 and %q", code, stderr, want2)
	}
	eq(t, (<-r.checks).Msg.GetHandle(), "")

	for name, args := range map[string][]string{
		"four parts":     {"--service", "GET,a.example.com,/v1/,1"},
		"price not int":  {"--service", "GET,a.example.com,/v1/,cheap," + testPayTo},
		"an argument":    {"more"},
		"no subcommand":  nil,
		"bad subcommand": nil,
	} {
		full := []string{"--config", settings, "x402", "check"}
		switch name {
		case "no subcommand":
			full = full[:3]
		case "bad subcommand":
			full = append(full[:3], "enable")
		}
		if _, _, code := runCLI(t, vars, append(full, args...)...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
}

func TestX402List(t *testing.T) {
	r := newX402Router()
	settings := serve(t, r)
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": strings.Repeat("i", 32)})
	out, stderr, code := runCLI(t, vars, "--config", settings, "x402", "list", "--handle",
		"sbx_x")
	want := "enabled=true\n" +
		"service=xs_w5a4htopmpzmmhos state=active method=POST host=agent402.tools " +
		"prefix=/v1/auto/ indexed=true max_price_micros=10000 pay_to=" + testPayTo +
		" requests=4 paid=2 uncertain=1\n" +
		"service=xs_bbbbbbbbbbbbbbbb state=failed method=GET host=api.example.com " +
		"prefix=/search/ indexed=false paid_path=/search max_price_micros=500 pay_to=" +
		testPayTo + " requests=0 paid=0 uncertain=0 failure_reason=HOST_BLOCKED\n" +
		"refusal=path_refused count=3 last_at=2026-11-08T09:05:00Z\n"
	if code != 0 || out != want {
		t.Fatalf("x402 list exited %d with %q and %q, want %q", code, out, stderr, want)
	}
	eq(t, (<-r.lists).Msg.GetHandle(), "sbx_x")
	if _, _, code := runCLI(t, vars, "--config", settings, "x402", "list"); code != 2 {
		t.Errorf("list without a handle exited %d, want 2", code)
	}
}
