package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"

	"github.com/bugnet-sh/bugnet-router/internal/estimator"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
)

const (
	replayInputs = `{"v":"` + rules.Version +
		`","request":{"source":{"kind":"none","read":false},` +
		`"workload":"tests","entry_hints":{}},"facts_ref":null,"history":{},` +
		`"catalog_sha256":"","max_lifetime_minutes":0}`
	replayCatalog = `{"packages":[{"ID":"p","Resources":{"VCPU":2,"MemoryMiB":2048,"DiskGiB":10},` +
		`"Minutes":30,"PriceMicros":1,"ExtensionOf":"","Edge":{},"EgressAllow":null}],"images":[]}`
)

// estimates answers GetEstimate with a stored estimate
type estimates struct {
	routerv1connect.UnimplementedOperatorServiceHandler
	version string
	output  []byte
}

func (e *estimates) GetEstimate(
	_ context.Context, req *connect.Request[routerv1.GetEstimateRequest],
) (*connect.Response[routerv1.GetEstimateResponse], error) {
	return connect.NewResponse(&routerv1.GetEstimateResponse{
		EstimateId:       req.Msg.GetEstimateId(),
		EstimatorVersion: e.version,
		Inputs:           []byte(replayInputs),
		Output:           e.output, InputsSha256: "ab", Catalog: []byte(replayCatalog),
	}), nil
}

func serveEstimates(t *testing.T, e *estimates) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(routerv1connect.NewOperatorServiceHandler(e))
	srv := httptest.NewUnstartedServer(mux)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)
	return writeSettings(t, "[api]\nlisten_addr = \""+srv.Listener.Addr().String()+"\"\n")
}

func TestEstimateReplay(t *testing.T) {
	stored, err := estimator.Replay([]byte(replayInputs), []byte(replayCatalog),
		[]byte(`{"available":["p"]}`))
	if err != nil {
		t.Fatal(err)
	}
	vars := env(map[string]string{"BUGNET_OPERATOR_TOKEN": strings.Repeat("o", 32)})
	settings := serveEstimates(t, &estimates{version: rules.Version, output: stored})
	out, stderr, code := runCLI(t, vars, "--config", settings, "estimate", "--replay", "e1")
	want := "estimate_id=e1\nestimator_version=" + rules.Version + "\ninputs_sha256=ab\n" +
		"floor=identical\nrecommendation=identical\n"
	if code != 0 || out != want {
		t.Fatalf("estimate exited %d with %q and %q, want %q", code, out, stderr, want)
	}

	changed := strings.Replace(string(stored), `"vcpu":2`, `"vcpu":3`, 1)
	if changed == string(stored) {
		t.Fatalf("no vcpu in the stored output %s", stored)
	}
	settings = serveEstimates(t, &estimates{version: rules.Version, output: []byte(changed)})
	out, stderr, code = runCLI(t, vars, "--config", settings, "estimate", "--replay", "e1")
	if code != 1 || !strings.Contains(out, "floor=differs\n") ||
		!strings.Contains(stderr, "the replay differs") {
		t.Fatalf("a changed floor exited %d with %q and %q", code, out, stderr)
	}

	settings = serveEstimates(t, &estimates{version: "est-2026.1.1", output: stored})
	_, stderr, code = runCLI(t, vars, "--config", settings, "estimate", "--replay", "e1")
	if code != 1 || !strings.Contains(stderr, "est-2026.1.1") {
		t.Fatalf("another version exited %d with %q", code, stderr)
	}
}

func TestEstimateNeedsReplay(t *testing.T) {
	_, stderr, code := runCLI(t, env(nil), "estimate")
	if code != 2 || !strings.Contains(stderr, "--replay is required") {
		t.Fatalf("exited %d with %q", code, stderr)
	}
}
