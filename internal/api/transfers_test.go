package api_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/proto"

	"github.com/oynozan/bugnet-router/internal/api"
	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/handle"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/local"
	"github.com/oynozan/bugnet-router/internal/transfer"
	"github.com/oynozan/bugnet-router/internal/transfer/transfertest"
)

const transferHost = "transfer.test.example"

const (
	toSandbox   = routerv1.TransferDirection_TRANSFER_DIRECTION_UPLOAD
	fromSandbox = routerv1.TransferDirection_TRANSFER_DIRECTION_DOWNLOAD
)

func upload(h, p string, size uint64) *routerv1.CreateTransferRequest {
	return &routerv1.CreateTransferRequest{
		Handle: h, Direction: toSandbox, Path: p, SizeBytes: &size,
	}
}

func download(h, p string) *routerv1.CreateTransferRequest {
	return &routerv1.CreateTransferRequest{Handle: h, Direction: fromSandbox, Path: p}
}

func (e *env) mint(
	t *testing.T, req *routerv1.CreateTransferRequest,
) (*routerv1.CreateTransferResponse, error) {
	t.Helper()
	resp, err := e.client.CreateTransfer(t.Context(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (e *env) mintOK(
	t *testing.T, req *routerv1.CreateTransferRequest,
) *routerv1.CreateTransferResponse {
	t.Helper()
	resp, err := e.mint(t, req)
	if err != nil {
		t.Fatalf("create transfer of %s: %v", req.GetPath(), err)
	}
	return resp
}

// tokenOf is the bnx_ token of a transfer URL
func tokenOf(t *testing.T, m *routerv1.CreateTransferResponse) string {
	t.Helper()
	tok, ok := strings.CutPrefix(m.GetUrl(), "https://"+transferHost+"/")
	if !ok || !strings.HasPrefix(tok, "bnx_") {
		t.Fatalf("transfer URL %q is not https://%s/bnx_...", m.GetUrl(), transferHost)
	}
	return tok
}

func (e *env) openWith(
	t *testing.T, tok string, d routerv1.TransferDirection, probe bool,
) (*routerv1.OpenTransferResponse, error) {
	t.Helper()
	resp, err := e.edge(t).OpenTransfer(t.Context(), connect.NewRequest(
		&routerv1.OpenTransferRequest{Token: tok, Direction: d, Probe: probe, EdgeId: "edge-1"}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (e *env) open(
	t *testing.T, m *routerv1.CreateTransferResponse, d routerv1.TransferDirection,
) *routerv1.OpenTransferResponse {
	t.Helper()
	o, err := e.openWith(t, tokenOf(t, m), d, false)
	if err != nil {
		t.Fatalf("open transfer: %v", err)
	}
	return o
}

// read reads a claimed download, keeping every message
func (e *env) read(
	ctx context.Context, t *testing.T, o *routerv1.OpenTransferResponse, offset, length uint64,
) ([]*routerv1.ReadTransferResponse, error) {
	t.Helper()
	stream, err := e.edge(t).ReadTransfer(ctx, connect.NewRequest(&routerv1.ReadTransferRequest{
		TransferId: o.GetTransferId(), Lease: o.GetLease(), Offset: offset, Length: length,
	}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = stream.Close() }()
	var msgs []*routerv1.ReadTransferResponse
	for stream.Receive() {
		msgs = append(msgs, proto.Clone(stream.Msg()).(*routerv1.ReadTransferResponse))
	}
	return msgs, stream.Err()
}

func joined(msgs []*routerv1.ReadTransferResponse) []byte {
	var b []byte
	for _, m := range msgs {
		b = append(b, m.GetData()...)
	}
	return b
}

// write sends chunks as one upload, the first message carrying the transfer and its lease
func (e *env) write(
	ctx context.Context, t *testing.T, o *routerv1.OpenTransferResponse, chunks ...string,
) (*routerv1.WriteTransferResponse, error) {
	t.Helper()
	stream := e.edge(t).WriteTransfer(ctx)
	first := &routerv1.WriteTransferRequest{TransferId: o.GetTransferId(), Lease: o.GetLease()}
	if len(chunks) > 0 {
		first.Data, chunks = []byte(chunks[0]), chunks[1:]
	}
	if err := stream.Send(first); err == nil {
		for _, c := range chunks {
			if stream.Send(&routerv1.WriteTransferRequest{Data: []byte(c)}) != nil {
				break
			}
		}
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// host is the host path of a sandbox path of the placement behind h
func (e *env) host(t *testing.T, h, p string) string {
	t.Helper()
	return filepath.Join(e.sandboxFS(t, h), filepath.FromSlash(p))
}

func (e *env) put(t *testing.T, h, p, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(e.host(t, h, p)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(e.host(t, h, p), []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (e *env) content(t *testing.T, h, p string) string {
	t.Helper()
	b, err := os.ReadFile(e.host(t, h, p))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// xferKeys lists the transfer keys in Valkey
func (e *env) xferKeys(t *testing.T) []string {
	t.Helper()
	keys, err := e.vk.Do(t.Context(), e.vk.B().Keys().Pattern(e.prefix+"xfer:*").Build()).
		AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func (e *env) indexRows(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(t.Context(), `select count(*) from transfer_index`).
		Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func digest(data string) []byte {
	sum := sha256.Sum256([]byte(data))
	return sum[:]
}

func timeout(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

/* Minting */

func TestCreateTransferPathFormsOverTheAPI(t *testing.T) { // review focus 2
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	for _, p := range []string{"out.csv", "./x", "", "a\nb", "a\x00b"} {
		_, err := e.mint(t, upload(h, p, 1))
		mustCode(t, err, connect.CodeInvalidArgument, "PATH_REFUSED")
	}
	o := e.open(t, e.mintOK(t, upload(h, "~/out.csv", 3)), toSandbox)
	got, err := e.write(timeout(t), t, o, "abc")
	eq(t, err, nil)
	eq(t, got.GetPath(), "/root/out.csv")
	eq(t, e.content(t, h, "/root/out.csv"), "abc")
	eq(t, len(e.xferKeys(t)) > 0, true)
}

func TestCreateUploadRefusesDirectoryTarget(t *testing.T) { // review focus 3
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	if err := os.MkdirAll(e.host(t, h, "/root/project"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := e.mint(t, upload(h, "/root/project", 5))
	mustCode(t, err, connect.CodeInvalidArgument, "IS_DIRECTORY")
	_, err = e.mint(t, upload(h, "/root/new/", 5))
	mustCode(t, err, connect.CodeInvalidArgument, "IS_DIRECTORY")
	eq(t, len(e.xferKeys(t)), 0)
	eq(t, e.indexRows(t), 0)
}

// tinyCatalog adds a start package whose transfers are capped at 1 KiB
var tinyCatalog = strings.TrimSuffix(catalogJSON, "\n]}") + `,
	{"id": "tiny-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000, "edge": {"transfer_max_bytes": "1KiB"}}
]}`

func TestCreateTransferRules(t *testing.T) {
	e := newEnv(t, options{catalog: tinyCatalog})
	h, _ := e.readyPlacement(t)
	_, err := e.mint(t, &routerv1.CreateTransferRequest{
		Handle: h, Direction: toSandbox, Path: "/root/f",
	})
	mustCode(t, err, connect.CodeInvalidArgument, "SIZE_REQUIRED")
	empty := e.mintOK(t, upload(h, "/root/empty", 0))
	eq(t, empty.GetSize(), uint64(0))
	eq(t, empty.GetMaxBytes(), uint64(1<<30))
	eq(t, empty.GetExpiresAt().AsTime().After(time.Now().Add(9*time.Minute)), true)
	_, err = e.mint(t, upload(h, "/root/big", 1<<30+1))
	mustCode(t, err, connect.CodeOutOfRange, "OVER_CAP")
	_, err = e.mint(t, download(h, "/root"))
	mustCode(t, err, connect.CodeInvalidArgument, "IS_DIRECTORY")
	if err := syscall.Mkfifo(e.host(t, h, "/root/fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = e.mint(t, download(h, "/root/fifo"))
	mustCode(t, err, connect.CodeInvalidArgument, "NOT_REGULAR")
	_, err = e.mint(t, download(h, "/root/missing"))
	mustCode(t, err, connect.CodeNotFound, "FILE_NOT_FOUND")
	bad := upload(h, "/root/f", 1)
	bad.Sha256 = []byte{1, 2, 3}
	_, err = e.mint(t, bad)
	wantCode(t, err, connect.CodeInvalidArgument)
	bad = upload(h, "/root/f", 1)
	bad.Mode = 0o4755
	_, err = e.mint(t, bad)
	wantCode(t, err, connect.CodeInvalidArgument)
	_, err = e.mint(t, &routerv1.CreateTransferRequest{Handle: h, Path: "/root/f"})
	wantCode(t, err, connect.CodeInvalidArgument)

	resp, err := e.createWith(t, e.startToken(t, "tiny-10m", smallPrice))
	if err != nil {
		t.Fatal(err)
	}
	tiny := resp.Msg.GetHandle()
	e.waitReady(t, tiny)
	eq(t, e.mintOK(t, upload(tiny, "/root/ok", 1024)).GetMaxBytes(), uint64(1024))
	_, err = e.mint(t, upload(tiny, "/root/no", 1025))
	mustCode(t, err, connect.CodeOutOfRange, "OVER_CAP")
	e.put(t, tiny, "/root/large", strings.Repeat("x", 1025))
	_, err = e.mint(t, download(tiny, "/root/large"))
	mustCode(t, err, connect.CodeOutOfRange, "OVER_CAP")
}

func TestDownloadMintAnswersTheFile(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	e.put(t, h, "/root/d", "0123456789")
	at := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)
	if err := os.Chtimes(e.host(t, h, "/root/d"), at, at); err != nil {
		t.Fatal(err)
	}
	m := e.mintOK(t, download(h, "/root/d"))
	eq(t, m.GetSize(), uint64(10))
	eq(t, m.GetModifiedAt().AsTime().Equal(at), true)
	eq(t, len(m.GetSha256()), 0)
	req := download(h, "/root/d")
	req.WantSha256 = true
	eq(t, e.mintOK(t, req).GetSha256(), digest("0123456789"))
}

func TestTransferTokenIsIdempotent(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	keyed := upload(h, "/root/f", 3)
	keyed.IdempotencyKey = "call-1"
	a, b := e.mintOK(t, keyed), e.mintOK(t, keyed)
	eq(t, a.GetUrl(), b.GetUrl())
	eq(t, a.GetExpiresAt().AsTime().Equal(b.GetExpiresAt().AsTime()), true)
	c, d := e.mintOK(t, upload(h, "/root/f", 3)), e.mintOK(t, upload(h, "/root/f", 3))
	eq(t, c.GetUrl() != d.GetUrl() && c.GetUrl() != a.GetUrl(), true)

	if _, err := e.write(timeout(t), t, e.open(t, a, toSandbox), "abc"); err != nil {
		t.Fatal(err)
	}
	_, err := e.mint(t, keyed)
	mustCode(t, err, connect.CodeFailedPrecondition, "TOKEN_USED")

	lost := upload(h, "/root/g", 3)
	lost.IdempotencyKey = "call-2"
	sum := sha256.Sum256([]byte(tokenOf(t, e.mintOK(t, lost))))
	key := e.prefix + "xfer:" + transfer.ID(sum[:])
	if err := e.vk.Do(t.Context(), e.vk.B().Del().Key(key).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	_, err = e.mint(t, lost)
	mustCode(t, err, connect.CodeFailedPrecondition, "TOKEN_EXPIRED")
}

func TestTransferWindow(t *testing.T) {
	e := newEnv(t, options{transfers: config.Transfers{URLTTL: 1500 * time.Millisecond}})
	h, _ := e.readyPlacement(t)
	keyed := upload(h, "/root/f", 3)
	keyed.IdempotencyKey = "call-1"
	m := e.mintOK(t, keyed)
	time.Sleep(1600 * time.Millisecond)
	_, err := e.mint(t, keyed)
	mustCode(t, err, connect.CodeFailedPrecondition, "TOKEN_EXPIRED")
	_, err = e.openWith(t, tokenOf(t, m), toSandbox, false)
	mustCode(t, err, connect.CodeFailedPrecondition, "TOKEN_EXPIRED")
	_, err = e.openWith(t, tokenOf(t, m), toSandbox, true)
	mustCode(t, err, connect.CodeFailedPrecondition, "TOKEN_EXPIRED")
}

func TestTransferIndexRowOnce(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	keyed := upload(h, "/root/f", 3)
	keyed.IdempotencyKey = "call-1"
	m := e.mintOK(t, keyed)
	e.mintOK(t, keyed)
	eq(t, e.indexRows(t), 1)
	sum := sha256.Sum256([]byte(tokenOf(t, m)))
	var placement string
	err := e.pool.QueryRow(t.Context(), `select placement_id::text from transfer_index
		where token_sha256 = $1`, sum[:]).Scan(&placement)
	eq(t, err, nil)
	eq(t, placement, p.GetPlacementId())
	e.mintOK(t, upload(h, "/root/f", 3))
	eq(t, e.indexRows(t), 2)
}

func TestTransferPerPlacementCaps(t *testing.T) {
	e := newEnv(t, options{opsBurst: 40})
	h, _ := e.readyPlacement(t)
	for i := range 16 {
		e.mintOK(t, upload(h, fmt.Sprintf("/root/f%d", i), 1))
	}
	_, err := e.mint(t, upload(h, "/root/f16", 1))
	mustCode(t, err, connect.CodeResourceExhausted, "TOO_MANY_TRANSFERS")

	other, _ := e.readyPlacement(t)
	var minted []*routerv1.CreateTransferResponse
	for i := range 3 {
		minted = append(minted, e.mintOK(t, upload(other, fmt.Sprintf("/root/f%d", i), 1)))
	}
	e.open(t, minted[0], toSandbox)
	second := e.open(t, minted[1], toSandbox)
	_, err = e.openWith(t, tokenOf(t, minted[2]), toSandbox, false)
	mustCode(t, err, connect.CodeResourceExhausted, "TOO_MANY_TRANSFERS")
	if _, err := e.write(timeout(t), t, second, "x"); err != nil {
		t.Fatal(err)
	}
	e.open(t, minted[2], toSandbox)
}

func TestOpenTransferStates(t *testing.T) {
	e := newEnv(t, options{opsBurst: 40})
	h, _ := e.readyPlacement(t)
	m := e.mintOK(t, upload(h, "/root/f", 3))
	tok := tokenOf(t, m)
	o := e.open(t, m, toSandbox)
	eq(t, o.GetSize(), uint64(3))
	eq(t, o.GetFileName(), "f")
	eq(t, o.GetBudget().GetBytesRemaining(), uint64(2<<30))
	_, err := e.openWith(t, tok, toSandbox, false)
	mustCode(t, err, connect.CodeFailedPrecondition, "IN_USE")
	if _, err := e.write(timeout(t), t, o, "abc"); err != nil {
		t.Fatal(err)
	}
	_, err = e.openWith(t, tok, toSandbox, false)
	mustCode(t, err, connect.CodeFailedPrecondition, "TOKEN_USED")
	_, err = e.openWith(t, "bnx_unknown", toSandbox, false)
	mustCode(t, err, connect.CodeNotFound, "TOKEN_UNKNOWN")

	retried := e.mintOK(t, upload(h, "/root/g", 3))
	_, err = e.openWith(t, tokenOf(t, retried), fromSandbox, false)
	wantCode(t, err, connect.CodeInvalidArgument)
	for i := range 5 {
		_, err := e.write(timeout(t), t, e.open(t, retried, toSandbox), "ab")
		if i == 0 {
			mustCode(t, err, connect.CodeInvalidArgument, "SIZE_MISMATCH")
		}
	}
	_, err = e.openWith(t, tokenOf(t, retried), toSandbox, false)
	mustCode(t, err, connect.CodeFailedPrecondition, "TOKEN_USED")

	late := e.mintOK(t, upload(h, "/root/late", 1))
	e.stop(t, h)
	_, err = e.openWith(t, tokenOf(t, late), toSandbox, false)
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
	_, err = e.mint(t, upload(h, "/root/later", 1))
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
}

/* Streams */

func TestReadWithholdsUntilVerified(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	data := rand.Text() + strings.Repeat("y", 600<<10)
	e.put(t, h, "/root/big", data)
	msgs, err := e.read(timeout(t), t, e.open(t, e.mintOK(t, download(h, "/root/big")),
		fromSandbox), 0, 0)
	eq(t, err, nil)
	eq(t, len(msgs), 3)
	for i, m := range msgs {
		eq(t, m.GetVerified(), i == len(msgs)-1)
		if len(m.GetData()) > 256<<10 {
			t.Fatalf("message %d holds %d bytes", i, len(m.GetData()))
		}
	}
	eq(t, string(joined(msgs)), data)

	e.put(t, h, "/root/empty", "")
	msgs, err = e.read(timeout(t), t, e.open(t, e.mintOK(t, download(h, "/root/empty")),
		fromSandbox), 0, 0)
	eq(t, err, nil)
	eq(t, len(msgs), 1)
	eq(t, msgs[0].GetVerified() && len(msgs[0].GetData()) == 0, true)

	msgs, err = e.read(timeout(t), t, e.open(t, e.mintOK(t, download(h, "/root/big")),
		fromSandbox), 10, 5)
	eq(t, err, nil)
	eq(t, string(joined(msgs)), data[10:15])
	_, err = e.read(timeout(t), t, e.open(t, e.mintOK(t, download(h, "/root/big")),
		fromSandbox), uint64(len(data))+1, 0)
	mustCode(t, err, connect.CodeOutOfRange, "RANGE")
}

func TestDownloadResumeChecksTheFile(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	data := strings.Repeat("z", 600<<10)
	e.put(t, h, "/root/d", data)
	m := e.mintOK(t, download(h, "/root/d"))
	o := e.open(t, m, fromSandbox)
	ctx, cancel := context.WithCancel(t.Context())
	stream, err := e.edge(t).ReadTransfer(ctx, connect.NewRequest(&routerv1.ReadTransferRequest{
		TransferId: o.GetTransferId(), Lease: o.GetLease(),
	}))
	if err != nil || !stream.Receive() {
		t.Fatalf("first message: %v", err)
	}
	got := len(stream.Msg().GetData())
	cancel()
	_ = stream.Close()
	var resumed *routerv1.OpenTransferResponse
	for range 50 {
		if resumed, err = e.openWith(t, tokenOf(t, m), fromSandbox, false); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("resume after a cut read: %v", err)
	}
	eq(t, resumed.GetEtag(), o.GetEtag())
	msgs, err := e.read(timeout(t), t, resumed, uint64(got), 0)
	eq(t, err, nil)
	eq(t, data[:got]+string(joined(msgs)), data)

	grown := e.mintOK(t, download(h, "/root/d"))
	e.put(t, h, "/root/d", data+"!")
	_, err = e.openWith(t, tokenOf(t, grown), fromSandbox, false)
	mustCode(t, err, connect.CodeFailedPrecondition, "FILE_CHANGED")
	touched := e.mintOK(t, download(h, "/root/d"))
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(e.host(t, h, "/root/d"), later, later); err != nil {
		t.Fatal(err)
	}
	_, err = e.openWith(t, tokenOf(t, touched), fromSandbox, true)
	mustCode(t, err, connect.CodeFailedPrecondition, "FILE_CHANGED")

	opened := e.open(t, e.mintOK(t, download(h, "/root/d")), fromSandbox)
	e.put(t, h, "/root/d", "short")
	_, err = e.read(timeout(t), t, opened, 0, 0)
	mustCode(t, err, connect.CodeFailedPrecondition, "FILE_CHANGED")
}

func TestHeadProbeDescribesWithoutClaiming(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	e.put(t, h, "/root/d", "0123456789")
	m := e.mintOK(t, download(h, "/root/d"))
	p, err := e.openWith(t, tokenOf(t, m), fromSandbox, true)
	eq(t, err, nil)
	eq(t, p.GetLease(), "")
	eq(t, p.GetSize(), uint64(10))
	eq(t, strings.HasPrefix(p.GetEtag(), `"a-`), true)
	eq(t, p.GetHardStop().AsTime().After(time.Now()), true)
	o := e.open(t, m, fromSandbox)
	eq(t, o.GetLease() != "", true)
	eq(t, o.GetEtag(), p.GetEtag())
}

func TestWriteCommitsOnlyExactBytes(t *testing.T) {
	e := newEnv(t, options{opsBurst: 40})
	h, _ := e.readyPlacement(t)
	e.put(t, h, "/root/old", "old")
	keepsOld := func(name string) {
		t.Helper()
		if got := e.content(t, h, "/root/old"); got != "old" {
			t.Fatalf("%s left %q in the file", name, got)
		}
		entries, err := os.ReadDir(e.host(t, h, "/root"))
		if err != nil || len(entries) != 1 {
			t.Fatalf("%s left %v, %v in /root, want the old file alone", name, entries, err)
		}
	}
	m := e.mintOK(t, upload(h, "/root/old", 5))
	_, err := e.write(timeout(t), t, e.open(t, m, toSandbox), "ab", "c")
	mustCode(t, err, connect.CodeInvalidArgument, "SIZE_MISMATCH")
	keepsOld("a short upload")
	_, err = e.write(timeout(t), t, e.open(t, m, toSandbox), "abc", "def")
	mustCode(t, err, connect.CodeInvalidArgument, "SIZE_MISMATCH")
	keepsOld("a long upload")

	ctx, cancel := context.WithCancel(t.Context())
	stream := e.edge(t).WriteTransfer(ctx)
	o := e.open(t, m, toSandbox)
	_ = stream.Send(&routerv1.WriteTransferRequest{
		TransferId: o.GetTransferId(), Lease: o.GetLease(), Data: []byte("abcd"),
	})
	time.Sleep(200 * time.Millisecond)
	cancel()
	_, err = stream.CloseAndReceive()
	if err == nil {
		t.Fatal("a cancelled upload committed")
	}
	time.Sleep(200 * time.Millisecond)
	keepsOld("a cancelled upload")

	sumd := upload(h, "/root/old", 5)
	sumd.Sha256 = digest("abcde")
	signed := e.mintOK(t, sumd)
	_, err = e.write(timeout(t), t, e.open(t, signed, toSandbox), "xxxxx")
	mustCode(t, err, connect.CodeDataLoss, "CHECKSUM_MISMATCH")
	keepsOld("a mismatched digest")
	got, err := e.write(timeout(t), t, e.open(t, signed, toSandbox), "abc", "de")
	eq(t, err, nil)
	eq(t, got.GetSize(), uint64(5))
	eq(t, got.GetSha256(), digest("abcde"))
	eq(t, e.content(t, h, "/root/old"), "abcde")

	moded := upload(h, "/root/private", 0)
	moded.Mode = 0o600
	if _, err := e.write(timeout(t), t, e.open(t, e.mintOK(t, moded), toSandbox)); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(e.host(t, h, "/root/private"))
	if err != nil || fi.Size() != 0 || fi.Mode().Perm() != 0o600 {
		t.Fatalf("empty upload with mode 0600 made %v, %v", fi, err)
	}
}

func TestTransferRPCsSpendOneOperation(t *testing.T) {
	e := newEnv(t, options{opsPerSecond: 1, opsBurst: 2})
	h, _ := e.readyPlacement(t)
	m := e.mintOK(t, upload(h, "/root/f", 1))
	e.open(t, m, toSandbox)
	_, err := e.mint(t, upload(h, "/root/g", 1))
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
	_, err = e.openWith(t, tokenOf(t, m), toSandbox, true)
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
}

func TestOpenTransferRefusesSpentBudget(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.put(t, h, "/root/d", "x")
	m := e.mintOK(t, download(h, "/root/d"))
	e.reportEdge(t, usage(p.GetPlacementId(), 0, 2<<30))
	_, err := e.openWith(t, tokenOf(t, m), fromSandbox, false)
	mustCode(t, err, connect.CodeResourceExhausted, "BUDGET_SPENT")
}

// upstream opens an upload and sends its first bytes, leaving the stream open
func (e *env) upstream(
	t *testing.T, h, p string, size uint64, first string,
) *connect.ClientStreamForClient[routerv1.WriteTransferRequest, routerv1.WriteTransferResponse] {
	t.Helper()
	o := e.open(t, e.mintOK(t, upload(h, p, size)), toSandbox)
	stream := e.edge(t).WriteTransfer(timeout(t))
	if err := stream.Send(&routerv1.WriteTransferRequest{
		TransferId: o.GetTransferId(), Lease: o.GetLease(), Data: []byte(first),
	}); err != nil {
		t.Fatal(err)
	}
	return stream
}

// cutWithin sends a byte every 50 ms until the upload ends, failing unless that takes at most d
func cutWithin(
	t *testing.T, d time.Duration,
	stream *connect.ClientStreamForClient[routerv1.WriteTransferRequest,
		routerv1.WriteTransferResponse],
	feed bool,
) error {
	t.Helper()
	start := time.Now()
	for time.Since(start) < d+2*time.Second {
		if feed && stream.Send(&routerv1.WriteTransferRequest{Data: []byte("x")}) != nil {
			break
		}
		if !feed {
			if stream.Send(&routerv1.WriteTransferRequest{}) != nil {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, err := stream.CloseAndReceive()
	if took := time.Since(start); took > d {
		t.Fatalf("the upload ran %v, want it cut within %v", took, d)
	}
	return err
}

func TestTransferStreamEndsIdleAndAtMaxDuration(t *testing.T) {
	api.SetTransferCheck(t, 20*time.Millisecond)
	e := newEnv(t, options{transfers: config.Transfers{
		IdleTimeout: 400 * time.Millisecond, MaxDuration: 1500 * time.Millisecond,
	}})
	h, _ := e.readyPlacement(t)
	e.put(t, h, "/root/old", "old")
	err := cutWithin(t, 1200*time.Millisecond, e.upstream(t, h, "/root/old", 100, "ab"), false)
	wantCode(t, err, connect.CodeDeadlineExceeded)
	eq(t, e.content(t, h, "/root/old"), "old")
	err = cutWithin(t, 2500*time.Millisecond, e.upstream(t, h, "/root/old", 1000, "ab"), true)
	wantCode(t, err, connect.CodeDeadlineExceeded)
	eq(t, e.content(t, h, "/root/old"), "old")
}

func TestStopAndBudgetCutATransferStream(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	stream := e.upstream(t, h, "/root/new", 100, "ab")
	e.reportEdge(t, usage(p.GetPlacementId(), 0, 2<<30))
	err := cutWithin(t, time.Second, stream, true)
	mustCode(t, err, connect.CodeResourceExhausted, "BUDGET_SPENT")

	other, _ := e.readyPlacement(t)
	stream = e.upstream(t, other, "/root/new", 100, "ab")
	e.stop(t, other)
	err = cutWithin(t, time.Second, stream, true)
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
	if _, err := os.Stat(e.host(t, h, "/root/new")); err == nil {
		t.Fatal("a cut upload committed")
	}

	third, _ := e.readyPlacement(t)
	e.put(t, third, "/root/big", strings.Repeat("q", 32<<20))
	o := e.open(t, e.mintOK(t, download(third, "/root/big")), fromSandbox)
	reading, err := e.edge(t).ReadTransfer(timeout(t), connect.NewRequest(
		&routerv1.ReadTransferRequest{TransferId: o.GetTransferId(), Lease: o.GetLease()}))
	if err != nil || !reading.Receive() {
		t.Fatalf("first message of a download: %v", err)
	}
	e.stop(t, third)
	// The edge may still hold sent messages, so the router's own end of the stream is timed
	key := e.prefix + "xfer:" + o.GetTransferId()
	for stopped := time.Now(); ; time.Sleep(20 * time.Millisecond) {
		state := e.vk.B().Hget().Key(key).Field("state").Build()
		if got, err := e.vk.Do(t.Context(), state).ToString(); err == nil && got == "issued" {
			break
		}
		if time.Since(stopped) > time.Second {
			t.Fatal("the router served a download over a second past the stop")
		}
	}
	for reading.Receive() {
		if reading.Msg().GetVerified() {
			t.Fatal("a download cut by a stop was verified")
		}
	}
	wantCode(t, reading.Err(), connect.CodeFailedPrecondition)
}

func TestShutdownDrainsTransferStreams(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	stream := e.upstream(t, h, "/root/f", 4, "ab")
	watch, err := e.edge(t).WatchEdgeEvents(timeout(t), connect.NewRequest(
		&routerv1.WatchEdgeEventsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	e.api.Drain()
	if watch.Receive() {
		t.Fatalf("the event stream sent %v after the drain began", watch.Msg())
	}
	wantCode(t, watch.Err(), connect.CodeUnavailable)
	time.Sleep(300 * time.Millisecond)
	if err := stream.Send(&routerv1.WriteTransferRequest{Data: []byte("cd")}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		t.Fatalf("an upload during the drain: %v", err)
	}
	eq(t, resp.Msg.GetSize(), uint64(4))
	eq(t, e.content(t, h, "/root/f"), "abcd")

	open := e.upstream(t, h, "/root/g", 4, "ab")
	e.api.Close()
	if _, err := open.CloseAndReceive(); err == nil {
		t.Fatal("an upload outlived Close")
	}
}

/* Staging */

// stagedEnv lists a local provider whose exec user is boxd, with a home made in each sandbox
func stagedEnv(t *testing.T) (*env, *transfertest.Sandbox) {
	t.Helper()
	var fake *transfertest.Sandbox
	e := newEnv(t, options{wrap: func(l *local.Provider, root string) provider.Provider {
		fake = transfertest.New(l, root, "boxd")
		return fake
	}})
	return e, fake
}

func (e *env) stagedPlacement(t *testing.T, fake *transfertest.Sandbox) string {
	t.Helper()
	h, _ := e.readyPlacement(t)
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	p, err := e.store.PlacementByHandle(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.Prepare(*p.ProviderRef); err != nil {
		t.Fatal(err)
	}
	return h
}

// ranWith reports whether a command run under sudo started with argv
func ranWith(fake *transfertest.Sandbox, argv ...string) bool {
	for _, r := range fake.Ran() {
		if len(r) >= 2+len(argv) && r[0] == "sudo" && slices.Equal(r[2:2+len(argv)], argv) {
			return true
		}
	}
	return false
}

func TestStagedUploadIsPlaced(t *testing.T) {
	e, fake := stagedEnv(t)
	h := e.stagedPlacement(t, fake)
	m := e.mintOK(t, upload(h, "/opt/x", 4))
	o := e.open(t, m, toSandbox)
	got, err := e.write(timeout(t), t, o, "da", "ta")
	eq(t, err, nil)
	eq(t, got.GetPath(), "/opt/x")
	eq(t, e.content(t, h, "/opt/x"), "data")
	fi, err := os.Stat(e.host(t, h, "/opt/x"))
	eq(t, err == nil && fi.Mode().Perm() == fs.FileMode(0o644), true)
	staged := "/home/boxd/.bugnet/xfer/" + o.GetTransferId()
	eq(t, ranWith(fake, transfer.RunnerPath, "place", "--from", staged+"/x", "--to", "/opt/x",
		"--mode", "644"), true)
	if _, err := os.Stat(e.host(t, h, staged)); !os.IsNotExist(err) {
		t.Fatalf("the staging directory outlived the upload: %v", err)
	}
	eq(t, ranWith(fake, "realpath", "-m", "--", "/opt/x"), true)
}

func TestStagedDownloadUsesStageOut(t *testing.T) {
	e, fake := stagedEnv(t)
	h := e.stagedPlacement(t, fake)
	e.put(t, h, "/opt/x", "secret")
	m := e.mintOK(t, download(h, "/opt/x"))
	eq(t, m.GetSize(), uint64(6))
	o := e.open(t, m, fromSandbox)
	msgs, err := e.read(timeout(t), t, o, 0, 0)
	eq(t, err, nil)
	eq(t, string(joined(msgs)), "secret")
	eq(t, msgs[len(msgs)-1].GetVerified(), true)
	staged := "/home/boxd/.bugnet/xfer/" + o.GetTransferId()
	eq(t, ranWith(fake, transfer.RunnerPath, "stage-out", "--from", "/opt/x", "--to",
		staged+"/x", "--owner", "boxd"), true)
	stats := 0
	for _, r := range fake.Ran() {
		if len(r) > 3 && r[2] == transfer.RunnerPath && r[3] == "stat" {
			stats++
		}
	}
	eq(t, stats >= 3, true)
	if _, err := os.Stat(e.host(t, h, staged)); !os.IsNotExist(err) {
		t.Fatalf("the staged copy outlived the download: %v", err)
	}
	if !bytes.Equal(joined(msgs), []byte(e.content(t, h, "/opt/x"))) {
		t.Fatal("the download differs from the file")
	}
}

/* Leases */

// field reads one field of the transfer record of id
func (e *env) field(t *testing.T, id, name string) string {
	t.Helper()
	got, err := e.vk.Do(t.Context(), e.vk.B().Hget().Key(e.prefix+"xfer:"+id).Field(name).
		Build()).ToString()
	if err != nil {
		t.Fatalf("read %s of the transfer: %v", name, err)
	}
	return got
}

// begin opens o's upload and sends its first bytes, leaving the stream open
func (e *env) begin(
	t *testing.T, o *routerv1.OpenTransferResponse, first string,
) *connect.ClientStreamForClient[routerv1.WriteTransferRequest, routerv1.WriteTransferResponse] {
	t.Helper()
	stream := e.edge(t).WriteTransfer(timeout(t))
	if err := stream.Send(&routerv1.WriteTransferRequest{
		TransferId: o.GetTransferId(), Lease: o.GetLease(), Data: []byte(first),
	}); err != nil {
		t.Fatal(err)
	}
	return stream
}

func TestAStreamRenewsItsLease(t *testing.T) {
	api.SetTransferLease(t, 900*time.Millisecond, 150*time.Millisecond)
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	m := e.mintOK(t, upload(h, "/root/f", 3))
	o := e.open(t, m, toSandbox)
	stream := e.begin(t, o, "a")
	time.Sleep(1300 * time.Millisecond)
	_, err := e.openWith(t, tokenOf(t, m), toSandbox, false)
	mustCode(t, err, connect.CodeFailedPrecondition, "IN_USE")
	eq(t, e.field(t, o.GetTransferId(), "bytes_sent"), "1")
	if err := stream.Send(&routerv1.WriteTransferRequest{Data: []byte("bc")}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		t.Fatalf("an upload that outlived its first lease: %v", err)
	}
	eq(t, resp.Msg.GetSize(), uint64(3))
	eq(t, e.content(t, h, "/root/f"), "abc")
}

func TestAStreamThatLostItsLeaseIsCut(t *testing.T) {
	api.SetTransferLease(t, 200*time.Millisecond, 1500*time.Millisecond)
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	m := e.mintOK(t, upload(h, "/root/f", 100))
	stream := e.begin(t, e.open(t, m, toSandbox), "ab")
	time.Sleep(300 * time.Millisecond)
	taken := e.open(t, m, toSandbox)
	err := cutWithin(t, 2*time.Second, stream, true)
	mustCode(t, err, connect.CodeFailedPrecondition, "IN_USE")
	if _, err := os.Stat(e.host(t, h, "/root/f")); err == nil {
		t.Fatal("a stream that lost its lease committed")
	}
	eq(t, e.field(t, taken.GetTransferId(), "lease"), taken.GetLease())
}
