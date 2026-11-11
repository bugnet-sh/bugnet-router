package providers

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

// nativeLium stands in for the Lium adapter, a provider with an SSH server of its own
type nativeLium struct{ *local.Provider }

func (nativeLium) AuthorizeNativeSSH(
	context.Context, provider.Ref, string,
) (provider.NativeSSH, error) {
	return provider.NativeSSH{}, nil
}

// settings reads a settings file with the given content
func settings(t *testing.T, content string) config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "settings.toml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestBuildFeaturesServeLiumNative(t *testing.T) {
	lium := nativeLium{local.New(local.Options{Name: "lium", Root: t.TempDir()})}
	for _, c := range []struct {
		doc    string
		native []string
	}{
		{"[ssh]\nproviders = [\"lium\"]\n", []string{"lium"}},
		{"[ssh]\nproviders = [\"lium\"]\n[boat]\nssh_path = \"native\"\n",
			[]string{"boat", "lium"}},
		// Lium runs no gate, so ssh.providers lists it only while its ssh_path is native
		{"[lium]\nssh_path = \"gate\"\n", nil},
	} {
		cfg := settings(t, c.doc)
		f := features(cfg)
		if !slices.Equal(f.NativeSSH, c.native) {
			t.Errorf("%q: native ssh %v, want %v", c.doc, f.NativeSSH, c.native)
		}
		reg, err := provider.NewRegistry([]provider.Provider{lium}, f)
		if err != nil {
			t.Fatal(err)
		}
		ssh, native := slices.Contains(cfg.SSH.Providers, "lium"), c.native != nil
		if reg.Capabilities(lium).SSH != ssh || reg.NativeSSH(lium) != native {
			t.Errorf("%q: lium ssh %v and native %v, want ssh %v and native %v", c.doc,
				reg.Capabilities(lium).SSH, reg.NativeSSH(lium), ssh, native)
		}
	}
}
