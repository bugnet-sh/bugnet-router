package imagebuild

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/leakscan"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

const (
	// checkSandboxes is how many fresh sandboxes one check starts, two to compare
	checkSandboxes = 2
	// checkLifetime is the provider-side lifetime of a check sandbox
	checkLifetime = 15 * time.Minute
	// checkLabel marks a check sandbox, which carries no label of the router
	checkLabel = "bugnet_image_check"
	// randomBytes is how much of /dev/urandom each sandbox is asked for
	randomBytes = 32
)

// CheckReport is what fresh sandboxes from one artifact showed, never a value
type CheckReport struct {
	Manifest  image.Manifest
	Findings  []leakscan.Finding
	ReadyMS   []int // one per sandbox started
	FreeBytes int64 // free bytes on / once ready
	SameRand  bool  // two sandboxes read the same bytes or got the same host key
	// ToolDiffs lists each tool whose version a job sees differs from the manifest's
	ToolDiffs []string
	// Reseed is how setup mixed in its seed in each sandbox, ioctl, write or failed
	Reseed []string
}

// Problems lists why the artifact fails the check, none when it passes
func (r CheckReport) Problems() []string {
	var out []string
	if len(r.Findings) > 0 {
		out = append(out, "the leak scan found "+strings.TrimSpace(
			leakscan.FormatFindings(r.Findings)))
	}
	out = append(out, r.ToolDiffs...)
	if r.SameRand {
		out = append(out, "two sandboxes read the same random bytes or got the same host key")
	}
	return out
}

// BootFunc runs a placement's first start steps on a check sandbox
type BootFunc func(
	ctx context.Context, p provider.Provider, ref provider.Ref,
) (provider.Ref, error)

// Check starts sandboxes from ref as a placement would, scans them and compares them
func Check(
	ctx context.Context, p provider.Provider, boot BootFunc, ref string, size provider.Size,
	n leakscan.Needles,
) (CheckReport, error) {
	var rep CheckReport
	var rands, hosts []string
	for i := range checkSandboxes {
		one, err := checkOne(ctx, p, boot, ref, size, n)
		if err != nil {
			return rep, fmt.Errorf("check sandbox %d: %w", i+1, err)
		}
		if i == 0 {
			rep.Manifest, rep.FreeBytes, rep.ToolDiffs = one.manifest, one.free, one.toolDiffs
		}
		rep.Findings = append(rep.Findings, one.findings...)
		rep.ReadyMS = append(rep.ReadyMS, one.readyMS)
		rep.Reseed = append(rep.Reseed, one.reseed)
		rands, hosts = append(rands, one.random), append(hosts, one.hostKey)
	}
	rep.SameRand = repeats(rands) || repeats(hosts)
	return rep, nil
}

// repeats reports whether two non-empty values of the list are equal
func repeats(v []string) bool {
	for i := range v {
		for j := i + 1; j < len(v); j++ {
			if v[i] != "" && v[i] == v[j] {
				return true
			}
		}
	}
	return false
}

// checked is what one check sandbox showed
type checked struct {
	manifest  image.Manifest
	findings  []leakscan.Finding
	readyMS   int
	free      int64
	toolDiffs []string
	random    string
	hostKey   string
	reseed    string
}

func checkOne(
	ctx context.Context, p provider.Provider, boot BootFunc, artifact string, size provider.Size,
	n leakscan.Needles,
) (checked, error) {
	var c checked
	start := time.Now()
	ref, err := p.Create(ctx, provider.CreateRequest{
		Size: size, Image: artifact, Lifetime: checkLifetime,
		Labels: map[string]string{checkLabel: "1"},
	})
	if err != nil {
		return c, fmt.Errorf("create from the artifact: %w", err)
	}
	defer destroy(p, ref)
	if ref, err = saga.WaitReady(ctx, p, ref); err != nil {
		return c, fmt.Errorf("wait ready: %w", err)
	}
	c.readyMS = int(time.Since(start).Milliseconds())
	if c.findings, err = scanSandbox(ctx, p, ref, n); err != nil {
		return c, fmt.Errorf("scan: %w", err)
	}
	m, ok, err := image.Read(ctx, p, ref)
	switch {
	case err != nil:
		return c, fmt.Errorf("read the manifest: %w", err)
	case !ok:
		return c, errors.New("the sandbox holds no image manifest")
	}
	c.manifest = m
	if c.free, err = freeBytes(ctx, p, ref); err != nil {
		return c, err
	}
	if c.toolDiffs, err = toolDiffs(ctx, p, ref, m); err != nil {
		return c, err
	}
	booted, err := boot(ctx, p, ref)
	if err != nil {
		return c, fmt.Errorf("start steps: %w", err)
	}
	c.hostKey, c.reseed = booted.Meta[saga.MetaSSHHostKey], booted.Meta[saga.MetaReseed]
	if c.random, err = readRandom(ctx, p, booted); err != nil {
		return c, err
	}
	return c, nil
}

// freeBytes reads the free bytes on /
func freeBytes(ctx context.Context, p provider.Provider, ref provider.Ref) (int64, error) {
	out, code, err := run(ctx, p, ref, []string{"df", "-B1", "--output=avail", "/"})
	if err != nil || code != 0 {
		return 0, fmt.Errorf("df on /: exit %d, %w", code, err)
	}
	lines := strings.Fields(string(out))
	if len(lines) < 2 {
		return 0, errors.New("df on / printed no figure")
	}
	free, err := strconv.ParseInt(lines[len(lines)-1], 10, 64)
	if err != nil {
		return 0, errors.New("df on / printed no number")
	}
	return free, nil
}

// toolDiffs compares what a job sees as the exec user with the manifest's tools
func toolDiffs(
	ctx context.Context, p provider.Provider, ref provider.Ref, m image.Manifest,
) ([]string, error) {
	out, code, err := run(ctx, p, ref, []string{setupScript, "probe"})
	if err != nil || code != 0 {
		return nil, fmt.Errorf("probe the tools: exit %d, %w", code, err)
	}
	var seen map[string]string
	if err := json.Unmarshal(bytes.TrimSpace(out), &seen); err != nil {
		return nil, errors.New("the tool probe printed no JSON object")
	}
	var diffs []string
	for _, tool := range slices.Sorted(maps.Keys(m.Tools)) {
		if got := seen[tool]; got != m.Tools[tool] {
			diffs = append(diffs, fmt.Sprintf("tool %s: a job sees %q, the manifest says %q",
				tool, got, m.Tools[tool]))
		}
	}
	return diffs, nil
}

// readRandom reads randomBytes of /dev/urandom as hex
func readRandom(ctx context.Context, p provider.Provider, ref provider.Ref) (string, error) {
	out, code, err := run(ctx, p, ref, []string{"od", "-An", "-tx1", "-N" +
		strconv.Itoa(randomBytes), "/dev/urandom"})
	if err != nil || code != 0 {
		return "", fmt.Errorf("read /dev/urandom: exit %d, %w", code, err)
	}
	v := strings.Join(strings.Fields(string(out)), "")
	if len(v) != 2*randomBytes {
		return "", errors.New("/dev/urandom gave the wrong number of bytes")
	}
	return v, nil
}
