package main

import (
	"context"
	"errors"
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

// previewRouter answers CreatePreview and records each request and its bearer
type previewRouter struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	got chan *connect.Request[routerv1.CreatePreviewRequest]
}

func (r *previewRouter) CreatePreview(
	_ context.Context, req *connect.Request[routerv1.CreatePreviewRequest],
) (*connect.Response[routerv1.CreatePreviewResponse], error) {
	r.got <- req
	return connect.NewResponse(&routerv1.CreatePreviewResponse{
		Url:       "https://abcdefghijklmn27.preview.test.example/?bugnet_token=bnp_t",
		Token:     "bnp_t",
		ExpiresAt: timestamppb.New(time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)),
		Listening: true, PreviewId: "abcdefghijklmn27",
	}), nil
}

// operator answers RevokePreview, refusing an unknown id, and records each request
type operator struct {
	routerv1connect.UnimplementedOperatorServiceHandler
	got chan *connect.Request[routerv1.RevokePreviewRequest]
}

func (o *operator) RevokePreview(
	_ context.Context, req *connect.Request[routerv1.RevokePreviewRequest],
) (*connect.Response[routerv1.RevokePreviewResponse], error) {
	o.got <- req
	if req.Msg.GetPreviewId() != "abcdefghijklmn27" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("no such preview"))
	}
	return connect.NewResponse(&routerv1.RevokePreviewResponse{}), nil
}

// servePreviews runs both services over h2c and returns a settings file pointing at them
func servePreviews(t *testing.T, r *previewRouter, o *operator) string {
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

func TestPreview(t *testing.T) {
	r := &previewRouter{got: make(chan *connect.Request[routerv1.CreatePreviewRequest], 1)}
	settings := servePreviews(t, r, &operator{})
	internal := strings.Repeat("i", 32)
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": internal})
	out, stderr, code := runCLI(t, vars, "--config", settings, "preview", "--handle", "sbx_x",
		"--port", "5173")
	want := "url=https://abcdefghijklmn27.preview.test.example/?bugnet_token=bnp_t\n" +
		"preview_id=abcdefghijklmn27\nexpires_at=2026-11-08T10:00:00Z\nlistening=true\n"
	if code != 0 || out != want {
		t.Fatalf("preview exited %d with %q and %q, want %q", code, out, stderr, want)
	}
	req := <-r.got
	eq(t, req.Msg.GetHandle(), "sbx_x")
	eq(t, req.Msg.GetPort(), uint32(5173))
	eq(t, req.Header().Get("Authorization"), "Bearer "+internal)

	for name, args := range map[string][]string{
		"no handle":    {"--port", "80"},
		"no port":      {"--handle", "h"},
		"port 0":       {"--handle", "h", "--port", "0"},
		"port 65536":   {"--handle", "h", "--port", "65536"},
		"not a number": {"--handle", "h", "--port", "http"},
		"an argument":  {"--handle", "h", "--port", "80", "more"},
	} {
		args := append([]string{"--config", settings, "preview"}, args...)
		if _, _, code := runCLI(t, vars, args...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
	_, stderr, code = runCLI(t, env(nil), "--config", settings, "preview", "--handle", "h",
		"--port", "80")
	if code != 1 || !strings.Contains(stderr, "BUGNET_INTERNAL_TOKEN is not set") {
		t.Errorf("preview without a token exited %d with %q", code, stderr)
	}
}

func TestRevokePreview(t *testing.T) {
	o := &operator{got: make(chan *connect.Request[routerv1.RevokePreviewRequest], 1)}
	settings := servePreviews(t, &previewRouter{}, o)
	op := strings.Repeat("o", 32)
	vars := env(map[string]string{"BUGNET_OPERATOR_TOKEN": op, "USER": "founder",
		"BUGNET_INTERNAL_TOKEN": strings.Repeat("i", 32)})
	for _, args := range [][]string{
		{"revoke-preview", "abcdefghijklmn27", "--reason", "phishing page"},
		{"revoke-preview", "--reason", "phishing page", "abcdefghijklmn27"},
	} {
		out, stderr, code := runCLI(t, vars, append([]string{"--config", settings}, args...)...)
		if code != 0 || out != "preview_id=abcdefghijklmn27\nrevoked=true\n" {
			t.Fatalf("%v exited %d with %q and %q", args, code, out, stderr)
		}
		req := <-o.got
		eq(t, req.Msg.GetPreviewId(), "abcdefghijklmn27")
		eq(t, req.Msg.GetReason(), "phishing page")
		eq(t, req.Msg.GetActor(), "founder")
		eq(t, req.Header().Get("Authorization"), "Bearer "+op)
	}
	_, _, code := runCLI(t, vars, "--config", settings, "revoke-preview", "abcdefghijklmn27",
		"--reason", "r", "--actor", "oncall")
	eq(t, code, 0)
	eq(t, (<-o.got).Msg.GetActor(), "oncall")

	_, stderr, code := runCLI(t, vars, "--config", settings, "revoke-preview",
		"aaaaaaaaaaaaaaaa", "--reason", "r")
	<-o.got
	if code != 1 || !strings.Contains(stderr, "not_found") {
		t.Errorf("an unknown preview exited %d with %q", code, stderr)
	}
	for name, args := range map[string][]string{
		"no id":     {"--reason", "r"},
		"two ids":   {"a", "b", "--reason", "r"},
		"no reason": {"abcdefghijklmn27"},
	} {
		args := append([]string{"--config", settings, "revoke-preview"}, args...)
		if _, _, code := runCLI(t, vars, args...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
	noUser := env(map[string]string{"BUGNET_OPERATOR_TOKEN": op})
	if _, _, code := runCLI(t, noUser, "--config", settings, "revoke-preview",
		"abcdefghijklmn27", "--reason", "r"); code != 2 {
		t.Errorf("no --actor and no $USER exited %d, want 2", code)
	}
	internalOnly := env(map[string]string{"BUGNET_INTERNAL_TOKEN": op, "USER": "founder"})
	_, stderr, code = runCLI(t, internalOnly, "--config", settings, "revoke-preview",
		"abcdefghijklmn27", "--reason", "r")
	if code != 1 || !strings.Contains(stderr, "BUGNET_OPERATOR_TOKEN is not set") {
		t.Errorf("revoke-preview without the operator token exited %d with %q", code, stderr)
	}
}
