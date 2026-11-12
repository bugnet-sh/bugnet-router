package hf

import (
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestHFNewRefusesBadOptions(t *testing.T) {
	cases := map[string]struct {
		change func(*Options)
		want   string
	}{
		"no token": {func(o *Options) { o.Token = "" }, "a Jobs token is required"},
		"spaced token": {func(o *Options) { o.Token = " " + fakeToken },
			"a Jobs token is required"},
		"token with a newline": {func(o *Options) { o.Token = fakeToken + "\n" },
			"a Jobs token is required"},
		"no namespace": {func(o *Options) { o.Namespace = "" }, "a namespace is required"},
		"namespace with a slash": {func(o *Options) { o.Namespace = "bugnet/other" },
			"namespace must be an organization name"},
		"namespace climbing up": {func(o *Options) { o.Namespace = ".." },
			"namespace must be an organization name"},
		"namespace with dots twice": {func(o *Options) { o.Namespace = "a..b" },
			"namespace must be an organization name"},
		"namespace with dashes twice": {func(o *Options) { o.Namespace = "a--b" },
			"namespace must be an organization name"},
		"unknown flavor": {func(o *Options) { o.Flavors = []string{"t4-small", "cpu-basic"} },
			`flavor "cpu-basic" is not an HF GPU flavor`},
		"negative start": {func(o *Options) { o.StartBilled = -time.Second },
			"start billed must not be negative"},
		"negative pace": {func(o *Options) { o.OpsPerSecond = -1 },
			"ops per second must not be negative"},
		"negative hardware interval": {func(o *Options) { o.HardwareEvery = -time.Second },
			"hardware interval must not be negative"},
		"no seed":    {func(o *Options) { o.SandboxSeed = nil }, "a sandbox seed of 32 bytes"},
		"short seed": {func(o *Options) { o.SandboxSeed = o.SandboxSeed[:31] }, "of 32 bytes"},
		"image by tag": {func(o *Options) { o.Image = "ghcr.io/bugnet-sh/bugnet-base:2026.11.3" },
			"the image must be named by digest"},
		"negative room": {func(o *Options) { o.ExtendRoom = -time.Second },
			"extend room must not be negative"},
		"negative margin": {func(o *Options) { o.TimeoutMargin = -time.Second },
			"timeout margin must not be negative"},
		"negative poll": {func(o *Options) { o.PollEvery = -time.Second },
			"poll interval must not be negative"},
		"no sbx-server digest": {func(o *Options) { o.SBXServerSHA256 = "" },
			"the sbx-server sha256 must be 64 lowercase hex digits"},
		"sbx-server digest in capitals": {
			func(o *Options) { o.SBXServerSHA256 = strings.ToUpper(testSBXSHA256) },
			"the sbx-server sha256 must be 64 lowercase hex digits"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			opts := testOptions()
			tc.change(&opts)
			_, err := newProvider(opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("newProvider = %v, want an error naming %q", err, tc.want)
			}
		})
	}
	for _, ns := range []string{"bugnet", "Bugnet-sh", "bugnet_sh", "bugnet.sh", "b"} {
		opts := testOptions()
		opts.Namespace = ns
		if _, err := newProvider(opts); err != nil {
			t.Errorf("namespace %q: %v", ns, err)
		}
	} // No image is fine, as every create may name its own
	opts := testOptions()
	opts.Image, opts.ExtendRoom, opts.TimeoutMargin = "", 0, 0
	if _, err := newProvider(opts); err != nil {
		t.Fatal(err)
	}
}

func TestHFCapabilities(t *testing.T) {
	p, err := newProvider(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.Name(), "hf")
	eq(t, p.Capabilities(), provider.Capabilities{
		GPU: true, Arch: "amd64", NeedsRunnerUpload: true, MaxLifetime: 2 * time.Hour,
		LossyExecText: true,
	})
	opts := testOptions()
	opts.MaxLifetime = 0
	if p, err = newProvider(opts); err != nil {
		t.Fatal(err)
	}
	eq(t, p.Capabilities().MaxLifetime, 2*time.Hour)
	opts.MaxLifetime = 90 * time.Minute
	if p, err = newProvider(opts); err != nil {
		t.Fatal(err)
	}
	eq(t, p.Capabilities().MaxLifetime, 90*time.Minute)
}
