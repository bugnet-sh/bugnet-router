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
