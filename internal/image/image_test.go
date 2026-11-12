package image_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func golden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestManifestParseStrict(t *testing.T) {
	m, err := image.Parse(golden(t, "manifest.json"))
	eq(t, err, nil)
	eq(t, m.Version, "2026.11.1")
	eq(t, m.Tools["node"], "24.11")
	for _, bad := range []string{"unknown_field.json", "unknown_tool.json", "bad_version.json"} {
		_, err := image.Parse(golden(t, bad))
		eq(t, err != nil, true)
	}
	_, err = image.Parse(make([]byte, image.MaxManifestBytes+1))
	eq(t, err != nil, true)
	_, err = image.Parse(append(golden(t, "manifest.json"), []byte(" {}")...))
	eq(t, err != nil, true)
}

func TestManifestRecordsTheBakedSBXServer(t *testing.T) {
	m, err := image.Parse(golden(t, "sbx_server.json"))
	eq(t, err, nil)
	eq(t, m.SBXServerSHA256, map[string]string{
		"amd64": "eb2b04f79fbe765195300aaf1582be2a2a9ea698ff067d0cab428171d5584932",
	})
	// An image older than SBXServerSince holds no sbx-server, so its manifest names none
	m, err = image.Parse(golden(t, "manifest.json"))
	eq(t, err, nil)
	eq(t, len(m.SBXServerSHA256), 0)
	good := string(golden(t, "sbx_server.json"))
	for name, bad := range map[string]string{
		"short":    strings.Replace(good, "0cab428171d5584932", "0cab4281", 1),
		"upper":    strings.Replace(good, "eb2b04f79fbe", "EB2B04F79FBE", 1),
		"riscv":    strings.Replace(good, `{"amd64": "eb2b`, `{"riscv64": "eb2b`, 1),
		"a string": strings.Replace(good, `{"amd64": "eb2b`, `"eb2b`, 1),
	} {
		if bad == good {
			t.Fatalf("%s: the replacement changed nothing", name)
		}
		if _, err := image.Parse([]byte(bad)); err == nil {
			t.Errorf("%s: a malformed sbx_server_sha256 parsed", name)
		}
	}
	eq(t, image.SBXServerPath, "/usr/local/lib/bugnet/sbx-server")
}

func TestVersionAtLeast(t *testing.T) {
	for _, c := range []struct {
		v, floor string
		want     bool
	}{
		{"2026.11.4", "2026.11.4", true},
		{"2026.11.10", "2026.11.4", true},
		{"2026.12.1", "2026.11.4", true},
		{"2027.01.1", "2026.11.4", true},
		{"2026.11.3", "2026.11.4", false},
		{"2026.10.9", "2026.11.4", false},
		{"2025.12.9", "2026.11.4", false},
		{"", "2026.11.4", false},
		{"2026.11", "2026.11.4", false},
		{"2026.11.4", "latest", false},
	} {
		if got := image.VersionAtLeast(c.v, c.floor); got != c.want {
			t.Errorf("VersionAtLeast(%q, %q) = %v, want %v", c.v, c.floor, got, c.want)
		}
	}
	eq(t, image.VersionAtLeast(image.SBXServerSince, "2026.11.4"), true)
	eq(t, image.VersionAtLeast("2026.11.3", image.SBXServerSince), false)
}

func TestHasCA(t *testing.T) {
	m, err := image.Parse(golden(t, "manifest.json"))
	eq(t, err, nil)
	sum := make([]byte, 32)
	for i := range sum {
		sum[i] = 0xaa
	}
	eq(t, m.HasCA(1, sum), true)
	eq(t, m.HasCA(2, sum), false)
	sum[0] = 0
	eq(t, m.HasCA(1, sum), false)
}

func TestRunnerFor(t *testing.T) {
	eq(t, image.RunnerFor(provider.Ref{}), "/tmp/bugnet/bin/bugnet-run")
	eq(t, image.RunnerFor(provider.Ref{Meta: map[string]string{"runner": image.RunnerPath}}),
		image.RunnerPath)
}

func TestReadMissingManifestIsNone(t *testing.T) {
	p := local.New(local.Options{Name: "local", Root: t.TempDir()})
	ref, err := p.Create(context.Background(), provider.CreateRequest{IdempotencyKey: "k"})
	eq(t, err, nil)
	_, ok, err := image.Read(context.Background(), p, ref)
	eq(t, err, nil)
	eq(t, ok, false)
}

func TestReadRefusesAMalformedManifest(t *testing.T) {
	p := local.New(local.Options{Name: "local", Root: t.TempDir()})
	ctx := context.Background()
	ref, err := p.Create(ctx, provider.CreateRequest{IdempotencyKey: "k"})
	eq(t, err, nil)
	eq(t, p.WriteFile(ctx, ref, image.ManifestPath, []byte(strings.Repeat("x", 10)), 0o644), nil)
	_, _, err = image.Read(ctx, p, ref)
	eq(t, err != nil, true)
	eq(t, p.WriteFile(ctx, ref, image.ManifestPath, golden(t, "manifest.json"), 0o644), nil)
	m, ok, err := image.Read(ctx, p, ref)
	eq(t, err, nil)
	eq(t, ok, true)
	eq(t, m.Channel, "prod")
}
