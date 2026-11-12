package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// transferRouter answers CreateTransfer with a fixed URL and records each request
type transferRouter struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	got chan *routerv1.CreateTransferRequest
}

func (r *transferRouter) CreateTransfer(
	_ context.Context, req *connect.Request[routerv1.CreateTransferRequest],
) (*connect.Response[routerv1.CreateTransferResponse], error) {
	r.got <- req.Msg
	at := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	out := &routerv1.CreateTransferResponse{
		Url: "https://transfer.test.example/bnx_abc", ExpiresAt: timestamppb.New(at),
		Size: req.Msg.GetSizeBytes(), MaxBytes: 1 << 30,
	}
	if req.Msg.GetDirection() == routerv1.TransferDirection_TRANSFER_DIRECTION_DOWNLOAD {
		out.Size, out.ModifiedAt = 10, timestamppb.New(at.Add(-time.Hour))
	}
	return connect.NewResponse(out), nil
}

func TestTransfer(t *testing.T) {
	r := &transferRouter{got: make(chan *routerv1.CreateTransferRequest, 1)}
	settings := serve(t, r)
	vars := env(map[string]string{"BUGNET_INTERNAL_TOKEN": strings.Repeat("t", 32)})
	file := filepath.Join(t.TempDir(), "my data.csv")
	if err := os.WriteFile(file, []byte("a,b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("a,b\n"))
	out, stderr, code := runCLI(t, vars, "--config", settings, "transfer", "--handle", "sbx_x",
		"--upload", file, "--sha256", hex.EncodeToString(sum[:]))
	want := "url=https://transfer.test.example/bnx_abc\nexpires_at=2026-11-07T10:00:00Z\n" +
		"size=4\nmax_bytes=1073741824\ncurl=curl -fsS -T '" + file + "' " +
		"https://transfer.test.example/bnx_abc\n"
	if code != 0 || out != want {
		t.Fatalf("transfer --upload exited %d with %q and %q, want %q", code, out, stderr, want)
	}
	req := <-r.got
	eq(t, req.GetDirection(), routerv1.TransferDirection_TRANSFER_DIRECTION_UPLOAD)
	eq(t, req.GetPath(), "~/my data.csv")
	eq(t, req.GetSizeBytes(), uint64(4))
	eq(t, hex.EncodeToString(req.GetSha256()), hex.EncodeToString(sum[:]))

	out, _, code = runCLI(t, vars, "--config", settings, "transfer", "--handle", "sbx_x",
		"--download", "/root/out.csv")
	if code != 0 || !strings.HasSuffix(out, "modified_at=2026-11-07T09:00:00Z\n"+
		"curl=curl -fSo out.csv https://transfer.test.example/bnx_abc\n") {
		t.Fatalf("transfer --download exited %d with %q", code, out)
	}
	req = <-r.got
	eq(t, req.GetPath(), "/root/out.csv")
	eq(t, req.SizeBytes == nil, true)

	for name, args := range map[string][]string{
		"no handle":          {"--download", "/x"},
		"neither":            {"--handle", "h"},
		"both":               {"--handle", "h", "--upload", file, "--download", "/x"},
		"a download's path":  {"--handle", "h", "--download", "/x", "--path", "/y"},
		"a download's sum":   {"--handle", "h", "--download", "/x", "--sha256", "00"},
		"a short sum":        {"--handle", "h", "--upload", file, "--sha256", "abcd"},
		"an extra argument:": {"--handle", "h", "--download", "/x", "more"},
	} {
		args := append([]string{"--config", settings, "transfer"}, args...)
		if _, _, code := runCLI(t, vars, args...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
}

func TestOwnedCountsBugnetSandboxes(t *testing.T) {
	root := t.TempDir()
	p := local.New(local.Options{Root: root})
	for i, labels := range []map[string]string{
		{"bugnet": "1", "bugnet_deployment": "dev"}, {"bugnet-live": "ab12"},
		{"bugnet-contract": "cd34"}, {"someone": "else"},
	} {
		_, err := p.Create(t.Context(), provider.CreateRequest{
			IdempotencyKey: "k" + string(rune('a'+i)), Labels: labels, Lifetime: time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	settings := writeSettings(t, "providers = [\"local\"]\n[local]\nenabled = true\n"+
		"root = \""+root+"\"\nrunner = \"/bin/false\"\n")
	out, stderr, code := runCLI(t, env(nil), "--config", settings, "owned", "local")
	if code != 0 || !strings.HasPrefix(out, "provider=local\ncount=3\nid=") ||
		strings.Count(out, "\nid=") != 3 {
		t.Fatalf("owned exited %d with %q and %q, want the three Bugnet sandboxes", code, out,
			stderr)
	}
	owned, err := p.ListOwned(t.Context(), map[string]string{"someone": "else"})
	if err != nil || strings.Contains(out, owned[0].Ref.ID) {
		t.Fatalf("owned listed a sandbox of no Bugnet label: %v", err)
	}

	_, stderr, code = runCLI(t, env(nil), "--config", settings, "owned", "boxd")
	if code != 1 || !strings.Contains(stderr, "BOXD_API_KEY is not set") {
		t.Errorf("owned boxd without a key exited %d with %q", code, stderr)
	}
	for _, args := range [][]string{{"owned"}, {"owned", "a", "b"}} {
		if _, _, code := runCLI(t, env(nil), append([]string{"--config", settings},
			args...)...); code != 2 {
			t.Errorf("%v exited %d, want 2", args, code)
		}
	}
}

// liumSizes is one lium size, which a lium build needs
const liumSizes = `[[lium.sizes]]
size = "lium-rtx3090"
model = "NVIDIA RTX 3090"
match = ["NVIDIA GeForce RTX 3090"]
gpus = 1
gpu_memory_gib = 24
vcpu = 8
memory_mib = 28672
disk_gib = 200
max_hourly_micros = 350000
`

func TestOwnedReadsTheProviderSeed(t *testing.T) {
	// Lium answers no pods and no nodes
	testenv.LocalInternet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "[]")
	}))
	settings := writeSettings(t, liumSizes)
	vars := map[string]string{"LIUM_API_KEY": "lium_test"}
	_, stderr, code := runCLI(t, env(vars), "--config", settings, "owned", "lium")
	if code != 1 || !strings.Contains(stderr, "LIUM_SSH_SEED is not set") {
		t.Fatalf("owned lium without its seed exited %d with %q", code, stderr)
	}
	vars["LIUM_SSH_SEED"] = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("s"), 32))
	out, stderr, code := runCLI(t, env(vars), "--config", settings, "owned", "lium")
	if code != 0 || out != "provider=lium\ncount=0\n" {
		t.Fatalf("owned lium exited %d with %q and %q, want no pods", code, out, stderr)
	}
}
