// Package image reads the Bugnet base image's manifest inside a sandbox
package image

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// ManifestPath is where every base image build writes its manifest
const ManifestPath = "/etc/bugnet/image.json"

// RunnerPath is where every base image build installs bugnet-run
const RunnerPath = "/usr/local/lib/bugnet/bugnet-run"

// UploadedRunnerPath is where the router uploads bugnet-run when the image has none to use
const UploadedRunnerPath = "/tmp/bugnet/bin/bugnet-run"

// SBXServerPath is where every base image build from SBXServerSince on installs sbx-server
const SBXServerPath = "/usr/local/lib/bugnet/sbx-server"

// SBXServerSince is the first image version that holds sbx-server, which hf's baked mode runs
const SBXServerSince = "2026.11.4"

// MaxManifestBytes bounds the manifest a sandbox may hold
const MaxManifestBytes = 4 << 10

// Tools are the names a manifest may list, CP 4.5's five first
var Tools = []string{
	"git", "tar", "python3", "node", "docker",
	"pnpm", "uv", "hf", "gcc", "make", "sshd", "nft", "curl",
}

// VersionPattern is what an image version looks like, such as 2026.11.1
var VersionPattern = regexp.MustCompile(`^\d{4}\.\d{2}\.\d+$`)

// VersionAtLeast reports whether image version v is floor or newer, false when either is malformed
func VersionAtLeast(v, floor string) bool {
	have, ok := versionParts(v)
	want, wantOK := versionParts(floor)
	return ok && wantOK && slices.Compare(have, want) >= 0
}

// versionParts is a version's year, month and build number
func versionParts(v string) ([]int, bool) {
	if !VersionPattern.MatchString(v) {
		return nil, false
	}
	var out []int
	for part := range strings.SplitSeq(v, ".") {
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, false
		}
		out = append(out, n)
	}
	return out, true
}

var (
	osPattern   = regexp.MustCompile(`^[a-z]+-\d{1,2}\.\d{2}$`)
	toolPattern = regexp.MustCompile(`^(none|\d+(\.\d+)?)$`)
	hexPattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	targets     = []string{"oci", "e2b", "boxd", "boat"}
	arches      = []string{"amd64", "arm64"}
)

// CA is one Bugnet CA certificate an image trusts
type CA struct {
	Version int    `json:"version"`
	SHA256  string `json:"sha256"`
}

// Manifest is what an image build measured and wrote into the image
type Manifest struct {
	V            int               `json:"v"`
	Version      string            `json:"version"`
	Channel      string            `json:"channel"`
	Target       string            `json:"target"`
	OS           string            `json:"os"`
	InputsSHA256 string            `json:"inputs_sha256"`
	BuiltAt      time.Time         `json:"built_at"`
	RunnerSHA256 map[string]string `json:"runner_sha256"`
	// SBXServerSHA256 is empty on an image older than SBXServerSince
	SBXServerSHA256 map[string]string `json:"sbx_server_sha256,omitempty"`
	CAs             []CA              `json:"ca"`
	Tools           map[string]string `json:"tools"`
	DiskBytes       int64             `json:"disk_bytes"`
}

// ErrManifest wraps every refusal of a manifest's content
var ErrManifest = errors.New("image: malformed manifest")

// Parse decodes a manifest strictly, refusing unknown fields, tools and versions
func Parse(b []byte) (Manifest, error) {
	m, err := parse(b)
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: %w", ErrManifest, err)
	}
	return m, nil
}

func parse(b []byte) (Manifest, error) {
	var m Manifest
	if len(b) > MaxManifestBytes {
		return m, errors.New("the manifest is over 4 KiB")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("decode: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Manifest{}, errors.New("data follows the manifest")
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func (m Manifest) validate() error {
	switch {
	case m.V != 1:
		return errors.New("manifest v is not 1")
	case !VersionPattern.MatchString(m.Version):
		return errors.New("manifest version is malformed")
	case m.Channel != "prod" && m.Channel != "dev":
		return errors.New("manifest channel is not prod or dev")
	case !slices.Contains(targets, m.Target):
		return errors.New("manifest target is unknown")
	case !osPattern.MatchString(m.OS):
		return errors.New("manifest os is malformed")
	case m.InputsSHA256 != "" && !hexPattern.MatchString(m.InputsSHA256):
		return errors.New("manifest inputs_sha256 is malformed")
	case m.DiskBytes < 0:
		return errors.New("manifest disk_bytes is negative")
	}
	for arch, sum := range m.RunnerSHA256 {
		if !slices.Contains(arches, arch) || !hexPattern.MatchString(sum) {
			return errors.New("manifest runner_sha256 is malformed")
		}
	}
	for arch, sum := range m.SBXServerSHA256 {
		if !slices.Contains(arches, arch) || !hexPattern.MatchString(sum) {
			return errors.New("manifest sbx_server_sha256 is malformed")
		}
	}
	for _, c := range m.CAs {
		if c.Version < 1 || !hexPattern.MatchString(c.SHA256) {
			return errors.New("manifest ca entry is malformed")
		}
	}
	for tool, v := range m.Tools {
		if !slices.Contains(Tools, tool) {
			return fmt.Errorf("manifest lists the unknown tool %q", tool)
		}
		if !toolPattern.MatchString(v) {
			return fmt.Errorf("manifest tool %s has a malformed version", tool)
		}
	}
	return nil
}

// Read fetches a sandbox's manifest through its file API or Exec, ok false when it has none
func Read(
	ctx context.Context, p provider.Provider, ref provider.Ref,
) (m Manifest, ok bool, err error) {
	var raw []byte
	if p.Name() == "boat" {
		raw, ok, err = readByExec(ctx, p, ref)
	} else {
		raw, ok, err = readByFile(ctx, p, ref)
	}
	if err != nil || !ok {
		return Manifest{}, false, err
	}
	m, err = Parse(raw)
	return m, err == nil, err
}

func readByFile(
	ctx context.Context, p provider.Provider, ref provider.Ref,
) ([]byte, bool, error) {
	// One byte past the cap tells a file that is too large
	b, _, err := p.ReadFile(ctx, ref, ManifestPath, 0, MaxManifestBytes+1)
	if pe, is := provider.AsError(err); is && pe.Code == provider.CodeNotFound {
		return nil, false, nil
	}
	return b, err == nil, err
}

// readByExec reads through cat where the file API refuses the path, Boat's
func readByExec(
	ctx context.Context, p provider.Provider, ref provider.Ref,
) ([]byte, bool, error) {
	s, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"cat", ManifestPath}})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = s.Close() }()
	b, err := io.ReadAll(io.LimitReader(s, MaxManifestBytes+1))
	if err != nil {
		return nil, false, err
	}
	_, _ = io.Copy(io.Discard, s)
	code, err := s.Wait()
	if err != nil {
		return nil, false, err
	}
	if code != 0 {
		return nil, false, nil
	}
	return b, true, nil
}

// HasCA reports whether the image trusts CA version v with the given SHA-256
func (m Manifest) HasCA(v int, sha256 []byte) bool {
	want := hex.EncodeToString(sha256)
	return slices.ContainsFunc(m.CAs, func(c CA) bool { return c.Version == v && c.SHA256 == want })
}

// RunnerFor is the bugnet-run path of a placement, the baked one when its boot used it
func RunnerFor(ref provider.Ref) string {
	if r := ref.Meta["runner"]; r != "" {
		return r
	}
	return UploadedRunnerPath
}
