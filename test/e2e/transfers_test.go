package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
)

// upstream mints an upload on replica a, claims it on replica b and opens it there with first
func (s *stack) upstream(
	ctx context.Context, t *testing.T, h, path string, size uint64, first string,
) *connect.ClientStreamForClient[routerv1.WriteTransferRequest, routerv1.WriteTransferResponse] {
	t.Helper()
	m, err := s.client(t, s.a).CreateTransfer(ctx, connect.NewRequest(
		&routerv1.CreateTransferRequest{
			Handle: h, Direction: routerv1.TransferDirection_TRANSFER_DIRECTION_UPLOAD,
			Path: path, SizeBytes: &size,
		}))
	if err != nil {
		t.Fatalf("CreateTransfer: %v", err)
	}
	tok, ok := strings.CutPrefix(m.Msg.GetUrl(), "https://transfer.e2e.example/")
	if !ok {
		t.Fatalf("transfer URL %q is not on the stack's transfer host", m.Msg.GetUrl())
	}
	edge := routerv1connect.NewEdgeServiceClient(bearer(t, s.keys["BUGNET_EDGE_TOKEN"]), s.b.url)
	o, err := edge.OpenTransfer(ctx, connect.NewRequest(&routerv1.OpenTransferRequest{
		Token: tok, Direction: routerv1.TransferDirection_TRANSFER_DIRECTION_UPLOAD,
		EdgeId: "e2e",
	}))
	if err != nil {
		t.Fatalf("OpenTransfer: %v", err)
	}
	stream := edge.WriteTransfer(ctx)
	if err := stream.Send(&routerv1.WriteTransferRequest{
		TransferId: o.Msg.GetTransferId(), Lease: o.Msg.GetLease(), Data: []byte(first),
	}); err != nil {
		t.Fatalf("first message of the upload: %v", err)
	}
	return stream
}

// feed sends one byte every gap until until passes or the stream ends, counting what it sent
func feed(
	stream *connect.ClientStreamForClient[routerv1.WriteTransferRequest,
		routerv1.WriteTransferResponse],
	gap time.Duration, until time.Time,
) (sent int, ended bool, at time.Time) {
	for time.Now().Before(until) {
		if stream.Send(&routerv1.WriteTransferRequest{Data: []byte("x")}) != nil {
			return sent, true, time.Now()
		}
		sent++
		time.Sleep(gap)
	}
	return sent, false, time.Time{}
}

func TestE2EStopCutsTransferWithin1s(t *testing.T) {
	t.Parallel()
	s := startStack(t)
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()
	h, _ := s.place(t, s.client(t, s.a), smallPkg, smallPrice)
	stream := s.upstream(ctx, t, h, "/root/cut", 1<<20, "first")
	if _, err := s.client(t, s.a).StopPlacement(ctx, connect.NewRequest(
		&routerv1.StopPlacementRequest{Handle: h})); err != nil {
		t.Fatalf("StopPlacement: %v", err)
	}
	stopped := time.Now()
	_, ended, at := feed(stream, 20*time.Millisecond, stopped.Add(5*time.Second))
	if !ended || at.Sub(stopped) > time.Second {
		t.Fatalf("the upload on the other replica ran on %v past the stop", time.Since(stopped))
	}
	_, err := stream.CloseAndReceive()
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("the cut upload ended with %v, want NOT_READY", err)
	}
}

func TestE2EExtensionMovesHardStop(t *testing.T) {
	t.Parallel()
	s := startStack(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	a := s.client(t, s.a)
	h, p := s.place(t, a, shortPkg, 10000)
	deadline := p.GetDeadline().AsTime()
	const size = 200
	stream := s.upstream(ctx, t, h, "/root/kept", size, "")
	token := strings.TrimSpace(s.devOK(t, s.a, "token", "--kind", "extend", "--package",
		shortMore, "--handle", h))
	ext, err := a.ExtendPlacement(ctx, connect.NewRequest(&routerv1.ExtendPlacementRequest{
		Handle: h, OrderToken: token,
	}))
	if err != nil {
		t.Fatalf("ExtendPlacement: %v", err)
	}
	if !ext.Msg.GetPlacement().GetDeadline().AsTime().After(deadline) {
		t.Fatal("the extension did not move the deadline")
	}
	sent, ended, _ := feed(stream, 2*time.Second, deadline.Add(5*time.Second))
	if ended {
		t.Fatalf("the upload ended after %d bytes, before the extended deadline", sent)
	}
	if err := stream.Send(&routerv1.WriteTransferRequest{
		Data: []byte(strings.Repeat("y", size-sent)),
	}); err != nil {
		t.Fatalf("the rest of the upload: %v", err)
	}
	resp, err := stream.CloseAndReceive()
	if err != nil || resp.Msg.GetSize() != size {
		t.Fatalf("the upload past the old deadline ended with %v, %v", resp, err)
	}
	read, err := a.ReadFile(ctx, connect.NewRequest(&routerv1.ReadFileRequest{
		Handle: h, Path: "/root/kept",
	}))
	if err != nil || len(read.Msg.GetData()) != size {
		t.Fatalf("the committed file reads %v, %v", read, err)
	}
}
