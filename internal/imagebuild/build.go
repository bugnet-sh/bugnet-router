// Package imagebuild makes and checks base image artifacts, never inside a router process
package imagebuild

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/leakscan"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// MaxPayload is the largest payload tarball a builder takes
	MaxPayload = 64 << 20
	// DefaultLifetime is how long a builder lives at the provider as a backstop
	DefaultLifetime = time.Hour
	// destroyWait is how long a builder's destroy may take once the build ended
	destroyWait = 30 * time.Second
	// maxOutput bounds what one command may print into memory
	maxOutput = 64 << 10
	// setupScript is where a base image keeps its setup script
	setupScript = "/usr/local/lib/bugnet/setup-image.sh"
	// payloadDir and payloadTar are where a builder without our image gets the payload
	payloadDir = "/tmp/bugnet-image"
	payloadTar = "/tmp/bugnet-image.tar.gz"
	// collectPath is where the leak collector is written before it runs as root
	collectPath = "/tmp/bugnet-leak-collect.sh"
	// providerBoxd and providerBoat name the two providers a build runs on
	providerBoxd = "boxd"
	providerBoat = "boat"
)

// Spec is one build of one provider's artifacts
type Spec struct {
	Version, Channel string
	// Inputs is the inputs hash of deploy/image/inputs.sha256, which the manifest records
	Inputs  string
	Payload []byte // payload-<channel>-<version>.tar.gz, at most 64 MiB
	FromOCI string // a boxd builder's image_ref when IMG-BOXD1 passed, else empty
	Sizes   []provider.Size
	// Lifetime is each builder's provider-side lifetime, 1 h when zero
	Lifetime time.Duration
	Needles  leakscan.Needles
}

// Artifact is one saved image and the ref creates of its size name
type Artifact struct {
	Size     string
	Ref      string
	Manifest image.Manifest
}

// FindingsError is a scan that found what an image must never hold, naming rules and paths alone
type FindingsError struct {
	Findings []leakscan.Finding
}

func (e *FindingsError) Error() string {
	return "the leak scan found " + strings.TrimSpace(leakscan.FormatFindings(e.Findings))
}

var (
	// ErrPayloadSize means the payload is empty or over 64 MiB
	ErrPayloadSize = errors.New("imagebuild: the payload must be 1 byte to 64 MiB")
	// ErrManifestMismatch means the builder's manifest is not the build's version or channel
	ErrManifestMismatch = errors.New("imagebuild: the builder's manifest is not this build's")
	setupLine           = regexp.MustCompile(`(?m)^setup-image: .{1,200}$`)
)

// Name is the artifact name of one size at provider p, as CP 19.6.1 gives it
//
// Boat has one artifact for every size, so its name has no size.
func Name(provider, channel, version, size string) string {
	v := strings.ReplaceAll(version, ".", "-")
	switch provider {
	case "e2b":
		if channel == "dev" {
			return "bugnet-dev-" + v + "-" + size
		}
		return "bugnet-" + v + "-" + size
	case providerBoat:
		return "bugnet-" + channel + "-" + v
	}
	return "bugnet-" + channel + "-" + v + "-" + size
}

// Build runs setup-image.sh in one builder per size, scans it, saves it and destroys it
//
// Boat has one artifact for every size, so it builds on the first size alone. Every builder is
// destroyed on every path, with a context of its own. A failure, a finding included, deletes
// each artifact an earlier size already saved.
func Build(
	ctx context.Context, p provider.Provider, s provider.ImageSaver, spec Spec,
) ([]Artifact, error) {
	if err := spec.check(p); err != nil {
		return nil, err
	}
	sizes := spec.Sizes
	if p.Name() == providerBoat {
		sizes = sizes[:1]
	}
	var out []Artifact
	for _, size := range sizes {
		a, err := buildOne(ctx, p, s, spec, size)
		if err != nil {
			err = errors.Join(fmt.Errorf("build %s: %w", size.Name, err), deleteAll(s, out))
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

func (s Spec) check(p provider.Provider) error {
	switch {
	case !image.VersionPattern.MatchString(s.Version):
		return errors.New("imagebuild: the version looks like 2026.11.1")
	case s.Channel != "prod" && s.Channel != "dev":
		return errors.New("imagebuild: the channel is prod or dev")
	case len(s.Sizes) == 0:
		return errors.New("imagebuild: a build needs at least one size")
	case s.FromOCI == "" && (len(s.Payload) == 0 || len(s.Payload) > MaxPayload):
		return ErrPayloadSize
	case len(s.Payload) > MaxPayload:
		return ErrPayloadSize
	case s.FromOCI != "" && p.Name() != providerBoxd:
		return errors.New("imagebuild: only a boxd builder starts from an oci image")
	}
	return nil
}

// buildOne makes the artifact of one size
func buildOne(
	ctx context.Context, p provider.Provider, s provider.ImageSaver, spec Spec, size provider.Size,
) (Artifact, error) {
	req := provider.CreateRequest{
		Size: size, Builder: true, Lifetime: cmpDuration(spec.Lifetime, DefaultLifetime),
	}
	if spec.FromOCI != "" {
		req.Image = "oci:" + spec.FromOCI
	}
	ref, err := p.Create(ctx, req)
	if err != nil {
		return Artifact{}, fmt.Errorf("create the builder: %w", err)
	}
	defer destroy(p, ref)
	if err := p.WaitReady(ctx, ref); err != nil {
		return Artifact{}, fmt.Errorf("wait for the builder: %w", err)
	}
	if err := setup(ctx, p, ref, spec); err != nil {
		return Artifact{}, err
	}
	findings, err := scanSandbox(ctx, p, ref, spec.Needles)
	if err != nil {
		return Artifact{}, fmt.Errorf("scan the builder: %w", err)
	}
	if len(findings) > 0 {
		return Artifact{}, &FindingsError{Findings: findings}
	}
	m, ok, err := image.Read(ctx, p, ref)
	switch {
	case err != nil:
		return Artifact{}, fmt.Errorf("read the builder's manifest: %w", err)
	case !ok:
		return Artifact{}, errors.New("the builder holds no manifest after setup")
	case m.Version != spec.Version || m.Channel != spec.Channel || m.Target != p.Name():
		return Artifact{}, ErrManifestMismatch
	}
	name := Name(p.Name(), spec.Channel, spec.Version, size.Name)
	saved, err := s.SaveImage(ctx, ref, name)
	if err != nil {
		return Artifact{}, fmt.Errorf("save %s: %w", name, err)
	}
	return Artifact{Size: size.Name, Ref: saved, Manifest: m}, nil
}

// setup runs setup-image.sh in the builder, from the payload or from the image it started from
func setup(ctx context.Context, p provider.Provider, ref provider.Ref, spec Spec) error {
	target := p.Name()
	if spec.FromOCI != "" {
		return runChecked(ctx, p, ref, "setup-image finish",
			asRoot(p, setupScript, "finish", "--target", target))
	}
	if err := p.WriteFile(ctx, ref, payloadTar, spec.Payload, 0o600); err != nil {
		return fmt.Errorf("upload the payload: %w", err)
	}
	steps := []struct {
		what string
		argv []string
	}{
		{"unpack the payload", []string{"sh", "-c", "mkdir -p " + payloadDir + " && tar -xzf " +
			payloadTar + " -C " + payloadDir}},
		{"setup-image build", asRoot(p, payloadDir+"/setup-image.sh", "build", "--target",
			target, "--channel", spec.Channel, "--inputs", spec.Inputs)},
		// What the build left in /tmp would reach every sandbox of a memory snapshot
		{"remove the payload", asRoot(p, "rm", "-rf", payloadDir, payloadTar)},
	}
	for _, st := range steps {
		if err := runChecked(ctx, p, ref, st.what, st.argv); err != nil {
			return err
		}
	}
	return nil
}

// asRoot prefixes argv with sudo where the provider's exec user is not root
func asRoot(p provider.Provider, argv ...string) []string {
	if u := p.Capabilities().ExecUser; u != "" && u != "root" {
		return append([]string{"sudo", "-n", "-H"}, argv...)
	}
	return argv
}

// run starts argv and reads its output up to maxOutput and its exit code
func run(
	ctx context.Context, p provider.Provider, ref provider.Ref, argv []string,
) ([]byte, int, error) {
	st, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: argv})
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = st.Close() }()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, io.LimitReader(st, maxOutput)); err != nil {
		return nil, 0, err
	}
	_, _ = io.Copy(io.Discard, st)
	code, err := st.Wait()
	return buf.Bytes(), code, err
}

// runChecked runs argv and names its one setup-image line when it fails, never other output
func runChecked(
	ctx context.Context, p provider.Provider, ref provider.Ref, what string, argv []string,
) error {
	out, code, err := run(ctx, p, ref, argv)
	switch {
	case err != nil:
		return fmt.Errorf("%s: %w", what, err)
	case code != 0:
		if line := setupLine.FindAll(out, -1); len(line) > 0 {
			return fmt.Errorf("%s exited %d, %s", what, code, line[len(line)-1])
		}
		return fmt.Errorf("%s exited %d", what, code)
	}
	return nil
}

// scanSandbox runs the leak collector in a sandbox and scans its archive in image mode
func scanSandbox(
	ctx context.Context, p provider.Provider, ref provider.Ref, n leakscan.Needles,
) ([]leakscan.Finding, error) {
	if err := p.WriteFile(ctx, ref, collectPath, []byte(leakscan.Collect), 0o700); err != nil {
		return nil, fmt.Errorf("write the collector: %w", err)
	}
	if err := runChecked(ctx, p, ref, "the collector", asRoot(p, "sh", collectPath)); err != nil {
		return nil, err
	}
	r, err := p.OpenReader(ctx, ref, leakscan.ArchivePath, 0)
	if err != nil {
		return nil, fmt.Errorf("open the archive: %w", err)
	}
	defer func() { _ = r.Close() }()
	if r.Info().Size >= leakscan.MaxArchive {
		return nil, leakscan.ErrTooLarge
	}
	findings, err := leakscan.ScanImage(r, n, p.Name())
	if err != nil {
		return nil, err
	}
	// The collector and its archive are not part of what an image holds
	_ = runChecked(ctx, p, ref, "clean up the collector",
		asRoot(p, "rm", "-rf", "/tmp/leak", collectPath))
	return findings, nil
}

// destroy removes a builder or check sandbox with a context that outlives a cancelled build
func destroy(p provider.Provider, ref provider.Ref) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), destroyWait)
	defer cancel()
	if err := p.Destroy(ctx, ref); err != nil {
		slog.Error("could not destroy a sandbox of an image build", "provider", p.Name(),
			"sandbox", ref.ID, "err", err)
	}
}

// deleteAll deletes saved artifacts after a failed build, with a context of its own
func deleteAll(d provider.ImageDeleter, arts []Artifact) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), destroyWait)
	defer cancel()
	var errs []error
	for _, a := range arts {
		if err := d.DeleteImage(ctx, a.Ref); err != nil {
			errs = append(errs, fmt.Errorf("delete %s: %w", a.Ref, err))
		}
	}
	return errors.Join(errs...)
}

func cmpDuration(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}
