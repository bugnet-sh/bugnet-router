package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/jackc/pgx/v5"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

// previewStack is a stack whose settings list the local provider for previews
func previewStack(t *testing.T) *stack {
	t.Helper()
	return startStackWith(t, stackOptions{extra: "\n[previews]\nproviders = [\"local\"]\n"})
}

func (s *stack) edge(t *testing.T, p *proc) routerv1connect.EdgeServiceClient {
	return routerv1connect.NewEdgeServiceClient(bearer(t, s.keys["BUGNET_EDGE_TOKEN"]), p.url)
}

// sql runs one statement on the stack's database
func (s *stack) sql(t *testing.T, statement string, args ...any) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), s.db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(t.Context(), statement, args...); err != nil {
		t.Fatal(err)
	}
}

// logged counts the lines of msg about placement in the replicas' logs
func logged(t *testing.T, msg, placement string, ps ...*proc) int {
	t.Helper()
	n := 0
	for _, p := range ps {
		raw, err := os.ReadFile(p.log)
		if err != nil {
			t.Fatal(err)
		}
		for line := range strings.Lines(string(raw)) {
			var l struct {
				Msg       string `json:"msg"`
				Placement string `json:"placement"`
			}
			if json.Unmarshal([]byte(line), &l) == nil && l.Msg == msg &&
				l.Placement == placement {
				n++
			}
		}
	}
	return n
}

// reason is the ErrorInfo reason err carries, empty when it carries none
func reason(err error) string {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return ""
	}
	for _, d := range ce.Details() {
		if v, derr := d.Value(); derr == nil {
			if info, ok := v.(*errdetails.ErrorInfo); ok {
				return info.GetReason()
			}
		}
	}
	return ""
}

func createPreview(
	ctx context.Context, c routerv1connect.SandboxRouterServiceClient, h string, port uint32,
) (*routerv1.CreatePreviewResponse, error) {
	res, err := c.CreatePreview(ctx, connect.NewRequest(&routerv1.CreatePreviewRequest{
		Handle: h, Port: port,
	}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func TestE2ERacingCreatePreviewOneGateSetup(t *testing.T) {
	t.Parallel()
	s := previewStack(t)
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	a, b := s.client(t, s.a), s.client(t, s.b)
	h, p := s.place(t, a, smallPkg, smallPrice)
	s.sql(t, `update placements set gate_ready_at = null where id = $1`, p.GetPlacementId())

	var wg sync.WaitGroup
	clients := []routerv1connect.SandboxRouterServiceClient{a, b, a, b}
	urls := make([]string, len(clients))
	errs := make([]error, len(clients))
	for i, c := range clients {
		wg.Go(func() {
			r, err := createPreview(ctx, c, h, 5173)
			if r != nil {
				urls[i] = r.GetUrl()
			}
			errs[i] = err
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("racing CreatePreview %d: %v", i, err)
		}
		if urls[i] != urls[0] || !strings.Contains(urls[0], ".preview.e2e.example/") {
			t.Fatalf("racing CreatePreview gave %q and %q", urls[i], urls[0])
		}
	}
	if n := logged(t, "gate opened", p.GetPlacementId(), s.a, s.b); n != 1 {
		t.Fatalf("the gate steps ran %d times across both replicas, want once", n)
	}

	for port := uint32(3001); port <= 3006; port++ {
		if _, err := createPreview(ctx, a, h, port); err != nil {
			t.Fatalf("preview of port %d: %v", port, err)
		}
	}
	errs = make([]error, 2)
	for i, c := range []routerv1connect.SandboxRouterServiceClient{a, b} {
		wg.Go(func() {
			_, errs[i] = createPreview(ctx, c, h, 4001+uint32(i))
		})
	}
	wg.Wait()
	made, refused := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			made++
		case reason(err) == "TOO_MANY_PREVIEWS":
			refused++
		default:
			t.Fatalf("a racing eighth or ninth preview: %v", err)
		}
	}
	if made != 1 || refused != 1 {
		t.Fatalf("the eighth and ninth previews: %d made and %d refused, want 1 and 1", made,
			refused)
	}
}

func TestE2ERacingRepairsOneRuns(t *testing.T) {
	t.Parallel()
	s := previewStack(t)
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	h, p := s.place(t, s.client(t, s.a), smallPkg, smallPrice)
	r, err := createPreview(ctx, s.client(t, s.a), h, 5173)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ran := make([]bool, 2)
	errs := make([]error, 2)
	for i, rp := range []*proc{s.a, s.b} {
		wg.Go(func() {
			res, err := s.edge(t, rp).RepairPreview(ctx, connect.NewRequest(
				&routerv1.RepairPreviewRequest{PreviewId: r.GetPreviewId(), Token: r.GetToken()}))
			if err == nil {
				ran[i] = res.Msg.GetRepaired()
			}
			errs[i] = err
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("RepairPreview: %v", err)
		}
	}
	if ran[0] == ran[1] {
		t.Fatalf("racing repairs answered %v and %v, want one true", ran[0], ran[1])
	}
	if n := logged(t, "gate repaired", p.GetPlacementId(), s.a, s.b); n != 1 {
		t.Fatalf("%d repairs ran across both replicas, want 1", n)
	}
	if _, err := s.edge(t, s.b).ResolvePreview(ctx, connect.NewRequest(
		&routerv1.ResolvePreviewRequest{PreviewId: r.GetPreviewId(), Token: r.GetToken()},
	)); err != nil {
		t.Fatalf("ResolvePreview after the repair: %v", err)
	}
}

func TestE2EPreviewAndRevokeThroughBugnetDev(t *testing.T) {
	t.Parallel()
	s := previewStack(t)
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	h, p := s.place(t, s.client(t, s.a), smallPkg, smallPrice)
	got := parseKV(s.devOK(t, s.a, "preview", "--handle", h, "--port", "5173"))
	if got["listening"] != "false" || !strings.HasPrefix(got["url"],
		"https://"+got["preview_id"]+".preview.e2e.example/?bugnet_token=bnp_") {
		t.Fatalf("bugnet-dev preview printed %v", got)
	}
	tok := got["url"][strings.Index(got["url"], "=")+1:]
	edge := s.edge(t, s.b)
	resolve := func() error {
		_, err := edge.ResolvePreview(ctx, connect.NewRequest(&routerv1.ResolvePreviewRequest{
			PreviewId: got["preview_id"], Token: tok,
		}))
		return err
	}
	if err := resolve(); err != nil {
		t.Fatalf("ResolvePreview on the other replica: %v", err)
	}
	out := parseKV(s.devOK(t, s.b, "revoke-preview", got["preview_id"], "--reason",
		"e2e check", "--actor", "e2e"))
	if out["revoked"] != "true" {
		t.Fatalf("bugnet-dev revoke-preview printed %v", out)
	}
	if err := resolve(); reason(err) != "PREVIEW_REVOKED" {
		t.Fatalf("ResolvePreview after the revoke: %v", err)
	}
	if n := logged(t, "api: preview revoked", p.GetPlacementId(), s.b); n != 1 {
		t.Fatalf("%d revoke lines in api-b's log, want 1", n)
	}
	var found bool
	for _, ev := range s.edgeEvents(t) {
		if r := ev.GetPreviewRevoked(); r.GetPreviewId() == got["preview_id"] &&
			r.GetPlacementId() == p.GetPlacementId() {
			found = true
		}
	}
	if !found {
		t.Fatal("no PreviewRevoked event for the preview")
	}
	_, stderr, code := s.dev(t, s.a, "preview", "--handle", h, "--port", "5173")
	if code != 1 || !strings.Contains(stderr, "failed_precondition") {
		t.Fatalf("bugnet-dev preview of a revoked port exited %d with %q", code, stderr)
	}
}
