package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// reserveRouter records the Reserve and CreatePlacement calls and answers ready at once
type reserveRouter struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	mu       sync.Mutex
	reserves []*routerv1.ReserveRequest
	creates  []*routerv1.CreatePlacementRequest
	skip     routerv1.ReserveSkip
	created  atomic.Int32
}

func (r *reserveRouter) Reserve(
	_ context.Context, req *connect.Request[routerv1.ReserveRequest],
) (*connect.Response[routerv1.ReserveResponse], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reserves = append(r.reserves, req.Msg)
	if r.skip != routerv1.ReserveSkip_RESERVE_SKIP_UNSPECIFIED {
		return connect.NewResponse(&routerv1.ReserveResponse{
			Outcome: routerv1.ReserveOutcome_RESERVE_OUTCOME_SKIPPED, Skip: r.skip,
		}), nil
	}
	return connect.NewResponse(&routerv1.ReserveResponse{
		Outcome:   routerv1.ReserveOutcome_RESERVE_OUTCOME_HELD,
		ExpiresAt: timestamppb.New(time.Date(2026, 11, 10, 12, 2, 0, 0, time.UTC)),
	}), nil
}

func (r *reserveRouter) CreatePlacement(
	_ context.Context, req *connect.Request[routerv1.CreatePlacementRequest],
) (*connect.Response[routerv1.CreatePlacementResponse], error) {
	r.mu.Lock()
	r.creates = append(r.creates, req.Msg)
	r.mu.Unlock()
	n := r.created.Add(1)
	return connect.NewResponse(&routerv1.CreatePlacementResponse{
		Handle: "sbx_" + strconv.Itoa(int(n)),
		Placement: &routerv1.Placement{
			PlacementId: "pl-" + strconv.Itoa(int(n)),
			State:       routerv1.PlacementState_PLACEMENT_STATE_READY,
		},
	}), nil
}

func (r *reserveRouter) Exec(
	context.Context, *connect.Request[routerv1.ExecRequest],
) (*connect.Response[routerv1.ExecResponse], error) {
	code := int32(0)
	return connect.NewResponse(&routerv1.ExecResponse{
		State: routerv1.JobState_JOB_STATE_EXITED, ExitCode: &code,
	}), nil
}

func (r *reserveRouter) StopPlacement(
	context.Context, *connect.Request[routerv1.StopPlacementRequest],
) (*connect.Response[routerv1.StopPlacementResponse], error) {
	return connect.NewResponse(&routerv1.StopPlacementResponse{}), nil
}

// pathOperator answers every placement's start path with path
type pathOperator struct {
	routerv1connect.UnimplementedOperatorServiceHandler
	path  string
	found atomic.Int32
}

func (o *pathOperator) FindPlacement(
	_ context.Context, req *connect.Request[routerv1.FindPlacementRequest],
) (*connect.Response[routerv1.FindPlacementResponse], error) {
	o.found.Add(1)
	return connect.NewResponse(&routerv1.FindPlacementResponse{
		Placement: &routerv1.OperatorPlacement{
			PlacementId: req.Msg.GetTarget().GetPlacementId(), StartPath: o.path,
		},
	}), nil
}

// serveBoth serves the router and the operator service from one address
func serveBoth(
	t *testing.T, r routerv1connect.SandboxRouterServiceHandler,
	o routerv1connect.OperatorServiceHandler,
) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(routerv1connect.NewSandboxRouterServiceHandler(r))
	mux.Handle(routerv1connect.NewOperatorServiceHandler(o))
	srv := httptest.NewUnstartedServer(mux)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)
	return writeSettings(t, "[api]\nlisten_addr = \""+srv.Listener.Addr().String()+"\"\n")
}

// claimsOf reads the payload of an order token
func claimsOf(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(token, "v1."), ".")
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func catalogFile(t *testing.T) string {
	t.Helper()
	packages := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(packages, []byte(catalogJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	return packages
}

func TestTokenSignsIssuedAtUnlessAskedNot(t *testing.T) {
	_, priv := newKey(t)
	settings := appendTo(t, writeSettings(t, ""), "\npackages_file = \""+catalogFile(t)+"\"\n")
	vars := env(map[string]string{"BUGNET_ORDER_PRIVATE_KEY": priv})
	before := time.Now().Add(-time.Second)
	out, stderr, code := runCLI(t, vars, "--config", settings, "token", "--package", "small-10m")
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	issued, ok := claimsOf(t, strings.TrimSpace(out))["issued_at"].(string)
	eq(t, ok, true)
	at, err := time.Parse(time.RFC3339, issued)
	eq(t, err, nil)
	eq(t, at.After(before) && at.Before(time.Now().Add(time.Second)), true)
	out, _, code = runCLI(t, vars, "--config", settings, "token", "--package", "small-10m",
		"--no-issued-at")
	eq(t, code, 0)
	_, has := claimsOf(t, strings.TrimSpace(out))["issued_at"]
	eq(t, has, false)
}

func TestReserveThenCreateWithTheQuote(t *testing.T) {
	r := &reserveRouter{}
	settings := appendTo(t, serveBoth(t, r, &pathOperator{}),
		"\npackages_file = \""+catalogFile(t)+"\"\n")
	_, priv := newKey(t)
	vars := env(map[string]string{
		"BUGNET_INTERNAL_TOKEN": strings.Repeat("i", 32), "BUGNET_ORDER_PRIVATE_KEY": priv,
	})
	out, stderr, code := runCLI(t, vars, "--config", settings, "reserve", "--quote", "q2.dev",
		"--package", "small-10m", "--ssh", "--client-ip", "203.0.113.9")
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	for _, want := range []string{"outcome=held", "expires_at=2026-11-10T12:02:00Z"} {
		if !strings.Contains(out, want) {
			t.Errorf("reserve printed %q, want %q", out, want)
		}
	}
	got := r.reserves[0]
	eq(t, [3]string{got.GetQuoteId(), got.GetPackageId(), got.GetClientIp()},
		[3]string{"q2.dev", "small-10m", "203.0.113.9"})
	eq(t, got.GetRequirements().GetSsh(), true)

	_, stderr, code = runCLI(t, vars, "--config", settings, "create", "--package", "small-10m",
		"--quote", "q2.dev")
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	eq(t, r.creates[0].GetQuoteId(), "q2.dev")
	_, has := claimsOf(t, r.creates[0].GetOrderToken())["issued_at"]
	eq(t, has, true)
	_, stderr, code = runCLI(t, vars, "--config", settings, "create", "--package", "small-10m",
		"--no-issued-at")
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	eq(t, r.creates[1].GetQuoteId(), "")
	_, has = claimsOf(t, r.creates[1].GetOrderToken())["issued_at"]
	eq(t, has, false)

	// A skip prints its reason
	r.skip = routerv1.ReserveSkip_RESERVE_SKIP_NO_WARM
	out, _, code = runCLI(t, vars, "--config", settings, "reserve", "--quote", "q2.dev2",
		"--package", "small-10m")
	eq(t, code, 0)
	eq(t, strings.Contains(out, "outcome=skipped") && strings.Contains(out, "skip=no_warm"), true)
	_, _, code = runCLI(t, vars, "--config", settings, "reserve", "--package", "small-10m")
	eq(t, code, 2)
}

func TestBenchRouterReservedCallsReserveFirst(t *testing.T) {
	r := &reserveRouter{}
	o := &pathOperator{path: "reserved"}
	settings := appendTo(t, serveBoth(t, r, o), "\npackages_file = \""+catalogFile(t)+"\"\n")
	_, priv := newKey(t)
	vars := env(map[string]string{
		"BUGNET_INTERNAL_TOKEN": strings.Repeat("i", 32), "BUGNET_ORDER_PRIVATE_KEY": priv,
		"BUGNET_OPERATOR_TOKEN": strings.Repeat("o", 32),
	})
	out, stderr, code := runCLI(t, vars, "--config", settings, "bench", "router",
		"--package", "small-10m", "--runs", "4", "--concurrency", "2", "--path", "reserved",
		"--pay-delay", "30ms")
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	eq(t, len(r.reserves), 4)
	eq(t, len(r.creates), 4)
	quotes := map[string]bool{}
	for _, c := range r.creates {
		quotes[c.GetQuoteId()] = true
		_, has := claimsOf(t, c.GetOrderToken())["issued_at"]
		eq(t, has, true)
	}
	eq(t, len(quotes), 4)
	for _, rs := range r.reserves {
		eq(t, quotes[rs.GetQuoteId()], true)
	}
	// The placement's own start path names the table, which the operator reads per run
	eq(t, o.found.Load(), int32(4))
	eq(t, strings.Contains(out, "start_path reserved  4 runs"), true)
	eq(t, strings.Contains(out, "total"), true)

	// A plain run calls no Reserve
	r.mu.Lock()
	r.reserves = nil
	r.mu.Unlock()
	_, _, code = runCLI(t, vars, "--config", settings, "bench", "router", "--package", "small-10m",
		"--runs", "2")
	eq(t, code, 0)
	eq(t, len(r.reserves), 0)
	_, _, code = runCLI(t, vars, "--config", settings, "bench", "router", "--package", "small-10m",
		"--path", "fast")
	eq(t, code, 2)
}
