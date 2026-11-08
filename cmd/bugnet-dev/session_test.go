package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	oldLink = "https://sessions.test.example/s/abcdefghijklmnop#k=bnk_old"
	newLink = "https://sessions.test.example/s/abcdefghijklmnop#k=bnk_new"
)

// sessionRouter answers GetPlacement with oldLink and rotates it, recording each rotation
type sessionRouter struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	rotated chan *routerv1.RotateSessionLinkRequest
}

func (r *sessionRouter) GetPlacement(
	context.Context, *connect.Request[routerv1.GetPlacementRequest],
) (*connect.Response[routerv1.GetPlacementResponse], error) {
	return connect.NewResponse(&routerv1.GetPlacementResponse{Placement: &routerv1.Placement{
		PlacementId: "p", SessionUrl: oldLink, CreatedAt: timestamppb.Now(),
	}}), nil
}

func (r *sessionRouter) RotateSessionLink(
	_ context.Context, req *connect.Request[routerv1.RotateSessionLinkRequest],
) (*connect.Response[routerv1.RotateSessionLinkResponse], error) {
	r.rotated <- req.Msg
	return connect.NewResponse(&routerv1.RotateSessionLinkResponse{SessionUrl: newLink}), nil
}

// holdOperator answers GetSessionHold for placement p only
type holdOperator struct {
	routerv1connect.UnimplementedOperatorServiceHandler
}

func (holdOperator) GetSessionHold(
	_ context.Context, req *connect.Request[routerv1.GetSessionHoldRequest],
) (*connect.Response[routerv1.GetSessionHoldResponse], error) {
	if req.Header().Get("Authorization") != "Bearer "+strings.Repeat("o", 32) {
		return nil, connect.NewError(connect.CodeUnauthenticated, nil)
	}
	return connect.NewResponse(&routerv1.GetSessionHoldResponse{
		RecordJson: []byte(`{"v":1}`), ClosedBy: "abuse",
		HoldUntil: timestamppb.New(time.Date(2026, 12, 8, 10, 0, 0, 0, time.UTC)),
	}), nil
}

func serveSessions(t *testing.T, r *sessionRouter) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(routerv1connect.NewSandboxRouterServiceHandler(r))
	mux.Handle(routerv1connect.NewOperatorServiceHandler(holdOperator{}))
	srv := httptest.NewUnstartedServer(mux)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)
	return writeSettings(t, "[api]\nlisten_addr = \""+srv.Listener.Addr().String()+"\"\n")
}

func TestSessionRotate(t *testing.T) {
	r := &sessionRouter{rotated: make(chan *routerv1.RotateSessionLinkRequest, 1)}
	settings := serveSessions(t, r)
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": strings.Repeat("i", 32)})
	out, stderr, code := runCLI(t, vars, "--config", settings, "session", "rotate", "--handle",
		"sbx_x")
	if code != 0 || out != "session_url="+newLink+"\n" {
		t.Fatalf("session rotate exited %d with %q and %q", code, out, stderr)
	}
	got := <-r.rotated
	eq(t, got.GetHandle(), "sbx_x")
	eq(t, got.GetSessionUrl(), oldLink)
}

func TestSessionHold(t *testing.T) {
	settings := serveSessions(t, &sessionRouter{})
	vars := env(map[string]string{"BUGNET_OPERATOR_TOKEN": strings.Repeat("o", 32)})
	out, stderr, code := runCLI(t, vars, "--config", settings, "session", "hold",
		"--placement", "p")
	want := "closed_by=abuse\nhold_until=2026-12-08T10:00:00Z\nrecord={\"v\":1}\n"
	if code != 0 || out != want {
		t.Fatalf("session hold exited %d with %q and %q, want %q", code, out, stderr, want)
	}
	for _, args := range [][]string{{"session"}, {"session", "drop"}, {"session", "hold"},
		{"session", "rotate"}} {
		args = append([]string{"--config", settings}, args...)
		if _, _, code := runCLI(t, vars, args...); code != 2 {
			t.Errorf("%v exited %d, want 2", args, code)
		}
	}
}
