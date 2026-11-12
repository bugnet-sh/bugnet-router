package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// imageFake is a provider with images whose Create fails with a message holding the key
type imageFake struct {
	provider.Provider
	name    string
	failMsg string
	mu      sync.Mutex
	deleted []string
}

func (f *imageFake) Name() string { return f.name }

func (f *imageFake) Sizes() []provider.Size {
	return []provider.Size{{Name: "small", Resources: provider.Resources{VCPU: 2, MemoryMiB: 4096}}}
}

func (f *imageFake) SizeFor(provider.Resources) (provider.Size, bool) { return f.Sizes()[0], true }

func (f *imageFake) Capabilities() provider.Capabilities { return provider.Capabilities{} }

func (f *imageFake) Create(context.Context, provider.CreateRequest) (provider.Ref, error) {
	return provider.Ref{}, errors.New(f.failMsg)
}

func (f *imageFake) SaveImage(context.Context, provider.Ref, string) (string, error) {
	return "", errors.New(f.failMsg)
}

func (f *imageFake) DeleteImage(_ context.Context, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, ref)
	return nil
}

func TestImageBuildNeverPrintsTheBoatKey(t *testing.T) {
	key := "boat_live_ThisIsTheTypedImageKey0123456789"
	fake := &imageFake{name: "boat", failMsg: "boat answered 401 for " + key}
	var out, errb bytes.Buffer
	var labels []string
	dir := t.TempDir()
	payload := filepath.Join(dir, "payload.tar.gz")
	if err := os.WriteFile(payload, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := &cli{
		ctx: t.Context(), stdout: &out, stderr: &errb, getenv: env(nil),
		configPath: writeSettings(t, ""), environ: []string{},
		provFor: func(cfg config.Config, name string) (provider.Provider, error) {
			if cfg.ProviderKeys[name] != key {
				t.Errorf("the provider was built with another key")
			}
			return fake, nil
		},
		ask: func(label string) (string, error) {
			labels = append(labels, label)
			return key, nil
		},
	}
	err := c.image([]string{"build", "--provider", "boat", "--channel", "dev", "--payload",
		payload, "--version", "2026.11.1", "--inputs", strings.Repeat("a", 64), "--record",
		filepath.Join(dir, "record.json")})
	if err == nil {
		t.Fatal("the build passed")
	}
	if strings.Join(labels, "|") != "Boat image key: " {
		t.Errorf("prompts %q, want the Boat image key prompt once", labels)
	}
	for _, text := range []string{err.Error(), out.String(), errb.String()} {
		if strings.Contains(text, key) {
			t.Fatalf("the key was printed: %q", text)
		}
	}
	if !strings.Contains(err.Error(), "<redacted>") {
		t.Errorf("error %q, want the key replaced", err)
	}
}

// useOperator answers ListImageUse with fixed rows
type useOperator struct {
	routerv1connect.UnimplementedOperatorServiceHandler
	uses []*routerv1.ImageUse
}

func (o *useOperator) ListImageUse(
	context.Context, *connect.Request[routerv1.ListImageUseRequest],
) (*connect.Response[routerv1.ListImageUseResponse], error) {
	return connect.NewResponse(&routerv1.ListImageUseResponse{Uses: o.uses}), nil
}

func TestImageRetireRefusesLiveVersion(t *testing.T) {
	mux := http.NewServeMux()
	op := &useOperator{}
	mux.Handle(routerv1connect.NewOperatorServiceHandler(op))
	srv := httptest.NewUnstartedServer(mux)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)
	api := "[api]\nlisten_addr = \"" + srv.Listener.Addr().String() + "\"\n"

	dir := t.TempDir()
	record := filepath.Join(dir, "dev-2026.11.1.json")
	if err := os.WriteFile(record, []byte(`{"boat":{"ref":"bugnet-dev-2026-11-1"}}`),
		0o600); err != nil {
		t.Fatal(err)
	}
	retire := func(settings string) (*imageFake, error) {
		fake := &imageFake{name: "boat"}
		c := &cli{
			ctx: t.Context(), stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{},
			getenv: env(map[string]string{
				"BUGNET_OPERATOR_TOKEN": strings.Repeat("o", 32),
				"BOAT_API_KEY":          "boat_runtime_key_0123456789",
			}),
			configPath: writeSettings(t, api+settings), environ: []string{},
			provFor: func(config.Config, string) (provider.Provider, error) { return fake, nil },
		}
		err := c.image([]string{"retire", "--provider", "boat", "--version", "2026.11.1",
			"--channel", "dev", "--record", record})
		return fake, err
	}
	use := func(live uint32, ready time.Duration) {
		op.uses = []*routerv1.ImageUse{{
			Provider: "boat", ImageVersion: "2026.11.1", LivePlacements: live,
			LastReadyAt: timestamppb.New(time.Now().Add(-ready)),
		}}
	}

	onAccept := "[images]\naccept = [\"2026.11.1\"]\n[images.boat]\nversion = \"2026.11.1\"\n" +
		"ref = \"bugnet-dev-2026-11-1\"\n"
	for name, c := range map[string]struct {
		settings string
		live     uint32
		ago      time.Duration
		want     string
	}{
		"on accept": {onAccept, 0, 5 * time.Hour, "is on images.accept"},
		"live":      {"", 2, 5 * time.Hour, "placements run on"},
		"too fresh": {"", 0, 30 * time.Minute, "an hour"},
	} {
		t.Run(name, func(t *testing.T) {
			use(c.live, c.ago)
			fake, err := retire(c.settings)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("retire gave %v, want a refusal naming %q", err, c.want)
			}
			if len(fake.deleted) != 0 {
				t.Errorf("deleted %v despite the refusal", fake.deleted)
			}
		})
	}
	use(0, 2*time.Hour)
	fake, err := retire("")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(fake.deleted, ",") != "bugnet-dev-2026-11-1" {
		t.Errorf("deleted %v, want the record's saved snapshot", fake.deleted)
	}
}

func TestImageProviderReadsTheProviderSeed(t *testing.T) {
	seed := bytes.Repeat([]byte("s"), 32)
	vars := map[string]string{"LIUM_SSH_SEED": base64.StdEncoding.EncodeToString(seed)}
	var built config.Config
	c := &cli{
		ctx: t.Context(), stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}, getenv: env(vars),
		configPath: writeSettings(t, ""), environ: []string{},
		provFor: func(cfg config.Config, name string) (provider.Provider, error) {
			built = cfg
			return &imageFake{name: name}, nil
		},
	}
	if _, _, err := c.imageProvider("lium", "lium_test"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(built.ProviderSeeds["lium"], seed) ||
		built.ProviderKeys["lium"] != "lium_test" {
		t.Fatalf("lium built with seeds %v and keys %v", built.ProviderSeeds, built.ProviderKeys)
	}
	if _, _, err := c.imageProvider("hf", "hf_test"); err == nil ||
		!strings.Contains(err.Error(), "HF_SANDBOX_SEED is not set") {
		t.Fatalf("hf without its seed gave %v", err)
	}
	// A provider without a seed builds with none
	if _, _, err := c.imageProvider("boat", "boat_test"); err != nil ||
		len(built.ProviderSeeds) != 0 {
		t.Fatalf("boat gave %v with seeds %v", err, built.ProviderSeeds)
	}
}
