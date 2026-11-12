package main

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/imagebuild"
	"github.com/bugnet-sh/bugnet-router/internal/leakscan"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

const (
	imageDir       = "deploy/image"
	boatKeyLabel   = "Boat image key: "
	retireGrace    = time.Hour
	redacted       = "<redacted>"
	minSecretBytes = 8
)

// environment is the environment a scan takes its needles from
func (c *cli) environment() []string {
	if c.environ != nil {
		return c.environ
	}
	return os.Environ()
}

// imageSecrets are the values an image command held, which no line it prints may quote
type imageSecrets []string

func (s imageSecrets) redact(msg string) string {
	for _, v := range s {
		if len(v) >= minSecretBytes {
			msg = strings.ReplaceAll(msg, v, redacted)
		}
	}
	return msg
}

// fail wraps err with its message scrubbed of every secret the command held
func (s imageSecrets) fail(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(s.redact(err.Error()))
}

// imageProvider builds one provider from the settings with key as its credential
func (c *cli) imageProvider(name, key string) (provider.Provider, config.Config, error) {
	cfg, err := c.config()
	if err != nil {
		return nil, cfg, err
	}
	cfg.Providers = []string{name}
	cfg.ProviderKeys = map[string]string{name: key}
	if cfg.ProviderSeeds, err = c.providerSeeds(name); err != nil {
		return nil, cfg, err
	}
	if c.provFor != nil {
		p, err := c.provFor(cfg, name)
		return p, cfg, err
	}
	reg, err := providers.Build(cfg, nil, nil)
	if err != nil {
		return nil, cfg, err
	}
	p, ok := reg.Get(name)
	if !ok {
		return nil, cfg, errors.New("the provider was not built")
	}
	return p, cfg, nil
}

// providerKey reads a provider's runtime key from the environment, naming it when it is empty
func (c *cli) providerKey(name string) (string, error) {
	env, ok := config.ProviderKeyEnv(name)
	if !ok {
		return "", fmt.Errorf("%w: %q is not a provider with an API key", errUsage, name)
	}
	if v := c.getenv(env); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("%s is not set", env)
}

// providerSeeds reads the seed a provider derives its sandbox keys from, none for most providers
func (c *cli) providerSeeds(name string) (map[string][]byte, error) {
	seed, err := config.ProviderSeed(name, c.getenv)
	if err != nil || seed == nil {
		return nil, err
	}
	return map[string][]byte{name: seed}, nil
}

// imageNeedles are the needles of the environment, the provider key and the extra secrets
func (c *cli) imageNeedles(extra imageSecrets, names ...string) (leakscan.Needles, error) {
	n, err := leakscan.FromEnv(c.environment())
	if err != nil {
		return nil, err
	}
	for _, v := range extra {
		if len(v) >= minSecretBytes {
			n = append(n, leakscan.Needle{Name: "image build credential", Value: []byte(v)})
		}
	}
	for _, name := range names {
		if v := c.getenv(name); len(v) >= minSecretBytes {
			n = append(n, leakscan.Needle{Name: name, Value: []byte(v)})
		}
	}
	return n, nil
}

// imageVersion reads deploy/image/VERSION unless a flag gave the version
func imageVersion(given string) (string, error) {
	if given != "" {
		return given, nil
	}
	raw, err := os.ReadFile(filepath.Join(imageDir, "VERSION"))
	if err != nil {
		return "", fmt.Errorf("%w: give --version, %v", errUsage, err)
	}
	return strings.TrimSpace(string(raw)), nil
}

// imageInputs reads the first field of deploy/image/inputs.sha256 unless a flag gave the hash
func imageInputs(given string) (string, error) {
	if given != "" {
		return given, nil
	}
	raw, err := os.ReadFile(filepath.Join(imageDir, "inputs.sha256"))
	if err != nil {
		return "", fmt.Errorf("%w: give --inputs, %v", errUsage, err)
	}
	f := strings.Fields(string(raw))
	if len(f) == 0 {
		return "", errors.New("deploy/image/inputs.sha256 is empty")
	}
	return f[0], nil
}

// imageSizes are the sizes the catalog's packages land on at p, smallest first
func imageSizes(cfg config.Config, p provider.Provider) ([]provider.Size, error) {
	sz, ok := provider.As[provider.Sizer](p)
	if !ok {
		return nil, fmt.Errorf("%s lists no sizes", p.Name())
	}
	all := sz.Sizes()
	if cfg.PackagesFile == "" {
		return all, nil
	}
	cat, err := catalog.Load(cfg.PackagesFile)
	if err != nil {
		return nil, err
	}
	used := map[string]bool{}
	for _, pkg := range cat.All() {
		if size, ok := p.SizeFor(pkg.Resources); ok {
			used[size.Name] = true
		}
	}
	var out []provider.Size
	for _, s := range all {
		if used[s.Name] {
			out = append(out, s)
		}
	}
	return out, nil
}

// mergeRecord merges fields into one section of the build record, making the file when absent
func mergeRecord(path, section string, fields map[string]any) error {
	rec := map[string]any{}
	switch raw, err := os.ReadFile(path); {
	case err == nil:
		if err := json.Unmarshal(raw, &rec); err != nil {
			return fmt.Errorf("%s is not a JSON object: %w", path, err)
		}
	case !errors.Is(err, os.ErrNotExist):
		return err
	}
	sec, _ := rec[section].(map[string]any)
	if sec == nil {
		sec = map[string]any{}
	}
	for k, v := range fields {
		sec[k] = v
	}
	rec[section] = sec
	out, err := json.MarshalIndent(rec, "", "    ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, append(out, '\n'), 0o644)
}

// recordPath is the default build record of a channel and version
func recordPath(given, channel, version string) string {
	return cmp.Or(given, filepath.Join(imageDir, "builds", channel+"-"+version+".json"))
}

/* build */

func (c *cli) imageBuild(args []string) error {
	fs := newFlags("image build")
	name := fs.String("provider", "", "boxd or boat")
	channel := fs.String("channel", "", "dev or prod")
	payload := fs.String("payload", "", "payload-<channel>-<version>.tar.gz")
	fromOCI := fs.String("from-oci", "", "a boxd builder's image_ref, once IMG-BOXD1 passed")
	version := fs.String("version", "", "image version, deploy/image/VERSION by default")
	inputs := fs.String("inputs", "", "inputs hash, deploy/image/inputs.sha256 by default")
	record := fs.String("record", "", "build record to merge into, deploy/image/builds by default")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if *name != "boxd" && *name != "boat" {
		return fmt.Errorf("%w: --provider is boxd or boat", errUsage)
	}
	if *channel != "dev" && *channel != "prod" {
		return fmt.Errorf("%w: --channel is dev or prod", errUsage)
	}
	if *payload == "" && *fromOCI == "" {
		return fmt.Errorf("%w: --payload is required without --from-oci", errUsage)
	}
	var secrets imageSecrets
	var key string
	var err error
	if *name == "boat" {
		// Saving needs a scope the runtime key must not hold, so the key is typed, never kept
		if key, err = c.secretInput(boatKeyLabel); err != nil {
			return err
		}
	} else if key, err = c.providerKey(*name); err != nil {
		return err
	}
	secrets = append(secrets, key)
	spec := imagebuild.Spec{Channel: *channel, FromOCI: *fromOCI}
	if spec.Version, err = imageVersion(*version); err != nil {
		return err
	}
	if spec.Inputs, err = imageInputs(*inputs); err != nil {
		return err
	}
	if *payload != "" {
		if spec.Payload, err = os.ReadFile(*payload); err != nil {
			return err
		}
	}
	if spec.Needles, err = c.imageNeedles(secrets, c.registryTokens()...); err != nil {
		return secrets.fail(err)
	}
	p, cfg, err := c.imageProvider(*name, key)
	if err != nil {
		return secrets.fail(err)
	}
	saver, ok := provider.As[provider.ImageSaver](p)
	if !ok {
		return fmt.Errorf("%s cannot save images", *name)
	}
	if spec.Sizes, err = imageSizes(cfg, p); err != nil {
		return secrets.fail(err)
	}
	arts, err := imagebuild.Build(c.ctx, p, saver, spec)
	if err != nil {
		var fe *imagebuild.FindingsError
		if errors.As(err, &fe) {
			_, _ = io.WriteString(c.stderr, leakscan.FormatFindings(fe.Findings))
		}
		return secrets.fail(err)
	}
	return c.printArtifacts(*name, *record, *channel, spec.Version, arts)
}

// registryTokens names the environment variables that may hold a registry token
func (c *cli) registryTokens() []string {
	return []string{"BUGNET_REGISTRY_TOKEN", "GHCR_TOKEN", "CR_PAT"}
}

func (c *cli) printArtifacts(
	name, record, channel, version string, arts []imagebuild.Artifact,
) error {
	refs := map[string]any{}
	for _, a := range arts {
		refs[a.Size] = a.Ref
		if _, err := fmt.Fprintf(c.stdout, "provider=%s size=%s ref=%s os=%s\n", name, a.Size,
			a.Ref, a.Manifest.OS); err != nil {
			return err
		}
	}
	fields := map[string]any{"refs": refs}
	if name == "boat" && len(arts) == 1 {
		fields = map[string]any{"ref": arts[0].Ref}
	}
	if len(arts) > 0 {
		fields["os"] = arts[0].Manifest.OS
		fields["tools"] = arts[0].Manifest.Tools
	}
	return mergeRecord(recordPath(record, channel, version), name, fields)
}

/* check */

// refList collects SIZE=REF or bare REF flags
type refList []string

func (l *refList) String() string     { return strings.Join(*l, ",") }
func (l *refList) Set(v string) error { *l = append(*l, v); return nil }

func (c *cli) imageCheck(args []string) error {
	fs := newFlags("image check")
	name := fs.String("provider", "", "e2b, boxd, koyeb or boat")
	channel := fs.String("channel", "", "dev or prod")
	version := fs.String("version", "", "image version, deploy/image/VERSION by default")
	record := fs.String("record", "", "build record to merge os and tools into")
	var refs, sizes refList
	fs.Var(&refs, "ref", "an artifact, SIZE=REF for e2b and boxd, REF for koyeb and boat")
	fs.Var(&sizes, "size", "a size to check, the smallest when none is given")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if *channel != "dev" && *channel != "prod" {
		return fmt.Errorf("%w: --channel is dev or prod", errUsage)
	}
	if len(refs) == 0 {
		return fmt.Errorf("%w: --ref is required", errUsage)
	}
	key, err := c.providerKey(*name)
	if err != nil {
		return err
	}
	secrets := imageSecrets{key}
	needles, err := c.imageNeedles(secrets, c.registryTokens()...)
	if err != nil {
		return secrets.fail(err)
	}
	p, cfg, err := c.imageProvider(*name, key)
	if err != nil {
		return secrets.fail(err)
	}
	ver, err := imageVersion(*version)
	if err != nil {
		return err
	}
	targets, err := checkTargets(p, *name, refs, sizes)
	if err != nil {
		return err
	}
	boot, err := c.checkBoot(cfg)
	if err != nil {
		return secrets.fail(err)
	}
	var (
		checked = map[string]imagebuild.CheckReport{}
		failed  bool
	)
	for _, t := range targets {
		rep, err := imagebuild.Check(c.ctx, p, boot, t.ref, t.size, needles)
		if err != nil {
			return secrets.fail(fmt.Errorf("check %s: %w", t.size.Name, err))
		}
		checked[t.size.Name] = rep
		c.printCheck(t, rep)
		if bad := rep.Problems(); len(bad) > 0 {
			failed = true
			c.deleteFailed(p, t.ref)
		}
	}
	if failed {
		return exitCode(1)
	}
	return c.printBlock(*name, *channel, ver, *record, targets, checked)
}

// checkTarget is one artifact and the size a check starts it as
type checkTarget struct {
	size provider.Size
	ref  string
}

// checkTargets pairs each artifact with the size it is checked as
func checkTargets(
	p provider.Provider, name string, refs, sizes []string,
) ([]checkTarget, error) {
	sz, ok := provider.As[provider.Sizer](p)
	if !ok || len(sz.Sizes()) == 0 {
		return nil, fmt.Errorf("%s lists no sizes", name)
	}
	bySize := func(n string) (provider.Size, error) {
		for _, s := range sz.Sizes() {
			if s.Name == n {
				return s, nil
			}
		}
		return provider.Size{}, fmt.Errorf("%w: %s has no size %q", errUsage, name, n)
	}
	var out []checkTarget
	if name == "e2b" || name == "boxd" {
		for _, r := range refs {
			size, ref, ok := strings.Cut(r, "=")
			if !ok || size == "" || ref == "" {
				return nil, fmt.Errorf("%w: --ref is SIZE=REF for %s", errUsage, name)
			}
			if len(sizes) > 0 && !slices.Contains(sizes, size) {
				continue
			}
			s, err := bySize(size)
			if err != nil {
				return nil, err
			}
			out = append(out, checkTarget{size: s, ref: ref})
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("%w: no --ref matches --size", errUsage)
		}
		return out, nil
	}
	if len(refs) != 1 || strings.Contains(refs[0], "=") && name == "boat" {
		return nil, fmt.Errorf("%w: %s takes one --ref", errUsage, name)
	}
	if len(sizes) == 0 {
		sizes = []string{sz.Sizes()[0].Name}
	}
	for _, n := range sizes {
		s, err := bySize(n)
		if err != nil {
			return nil, err
		}
		out = append(out, checkTarget{size: s, ref: refs[0]})
	}
	return out, nil
}

// checkBoot builds the start steps of a check from the settings, with no CA and no database
func (c *cli) checkBoot(cfg config.Config) (imagebuild.BootFunc, error) {
	cfg.Secrets.Providers = nil
	boot, err := saga.LoadBootSettings(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	if err != nil {
		return nil, err
	}
	return saga.CheckBoot(boot, saga.SSHSettingsOf(cfg)), nil
}

func (c *cli) printCheck(t checkTarget, rep imagebuild.CheckReport) {
	ms := make([]string, len(rep.ReadyMS))
	for i, v := range rep.ReadyMS {
		ms[i] = strconv.Itoa(v)
	}
	_, _ = fmt.Fprintf(c.stdout, "size=%s ready_ms=%s free_bytes=%d same_rand=%t reseed=%s\n",
		t.size.Name, strings.Join(ms, ","), rep.FreeBytes, rep.SameRand,
		strings.Join(rep.Reseed, ","))
	for _, p := range rep.Problems() {
		_, _ = fmt.Fprintf(c.stderr, "size=%s problem: %s\n", t.size.Name, p)
	}
}

// deleteFailed deletes an artifact that failed its check, where the provider keeps artifacts
func (c *cli) deleteFailed(p provider.Provider, ref string) {
	d, ok := provider.As[provider.ImageDeleter](p)
	if !ok {
		return
	}
	if err := d.DeleteImage(c.ctx, ref); err != nil {
		_, _ = fmt.Fprintf(c.stderr, "could not delete %s: %v\n", ref, err)
		return
	}
	_, _ = fmt.Fprintf(c.stderr, "deleted %s\n", ref)
}

// printBlock prints the TOML block to paste into the router's settings, and fills the record
func (c *cli) printBlock(
	name, channel, version, record string, targets []checkTarget,
	reports map[string]imagebuild.CheckReport,
) error {
	first := reports[targets[0].size.Name].Manifest
	var b strings.Builder
	fmt.Fprintf(&b, "[images.%s]\nversion = %q\n", name, version)
	switch name {
	case "boxd":
		b.WriteString("refs = { ")
		for i, t := range targets {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%q = %q", t.size.Name, t.ref)
		}
		b.WriteString(" }\n")
	case "koyeb", "boat":
		fmt.Fprintf(&b, "ref = %q\n", targets[0].ref)
	}
	fmt.Fprintf(&b, "os = %q\n\n[images.%s.tools]\n", first.OS, name)
	tools := make([]string, 0, len(first.Tools))
	for tool := range first.Tools {
		tools = append(tools, tool)
	}
	slices.Sort(tools)
	for _, tool := range tools {
		fmt.Fprintf(&b, "%s = %q\n", tool, first.Tools[tool])
	}
	if name == "e2b" {
		for _, t := range targets {
			fmt.Fprintf(&b, "\n[[e2b.templates]]\nsize = %q\ntemplate = %q\n", t.size.Name, t.ref)
		}
	}
	if _, err := io.WriteString(c.stdout, b.String()); err != nil {
		return err
	}
	path := recordPath(record, channel, version)
	if _, err := os.Stat(path); err != nil && record == "" {
		// A check of an artifact the record does not name only prints its block
		return nil
	}
	return mergeRecord(path, name, map[string]any{"os": first.OS, "tools": first.Tools})
}

/* retire and use */

func (c *cli) imageRetire(args []string) error {
	fs := newFlags("image retire")
	name := fs.String("provider", "", "e2b, boxd, koyeb or boat")
	version := fs.String("version", "", "the version to retire")
	channel := fs.String("channel", "", "dev or prod, the settings' images.channel by default")
	record := fs.String("record", "", "build record naming the version's artifacts")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("provider", *name); err != nil {
		return err
	}
	if !image.VersionPattern.MatchString(*version) {
		return fmt.Errorf("%w: --version looks like 2026.11.1", errUsage)
	}
	cfg, err := c.config()
	if err != nil {
		return err
	}
	if slices.Contains(cfg.Images.Accept, *version) {
		return fmt.Errorf("%s is on images.accept in %s, remove it and restart first", *version,
			c.configPath)
	}
	if err := c.checkUnused(*name, *version); err != nil {
		return err
	}
	ch := cmp.Or(*channel, cfg.Images.Channel)
	refs, err := recordRefs(recordPath(*record, ch, *version), *name)
	if err != nil {
		return err
	}
	key, err := c.providerKey(*name)
	if err != nil {
		return err
	}
	secrets := imageSecrets{key}
	p, _, err := c.imageProvider(*name, key)
	if err != nil {
		return secrets.fail(err)
	}
	d, ok := provider.As[provider.ImageDeleter](p)
	if !ok || len(refs) == 0 {
		_, err := fmt.Fprintf(c.stdout, "provider=%s version=%s nothing to delete\n", *name,
			*version)
		return err
	}
	for _, ref := range refs {
		if err := d.DeleteImage(c.ctx, ref); err != nil {
			return secrets.fail(fmt.Errorf("delete %s: %w", ref, err))
		}
		_, _ = fmt.Fprintf(c.stdout, "provider=%s version=%s deleted=%s\n", *name, *version, ref)
	}
	return nil
}

// checkUnused refuses a version that a live placement runs on or that became ready in the last hour
func (c *cli) checkUnused(name, version string) error {
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.ListImageUse(c.ctx, connect.NewRequest(
		&routerv1.ListImageUseRequest{Provider: name}))
	if err != nil {
		return err
	}
	for _, u := range res.Msg.GetUses() {
		if u.GetProvider() != name || u.GetImageVersion() != version {
			continue
		}
		if u.GetLivePlacements() > 0 {
			return fmt.Errorf("%d placements run on %s %s, retire it after they end",
				u.GetLivePlacements(), name, version)
		}
		if last := u.GetLastReadyAt(); last != nil && time.Since(last.AsTime()) < retireGrace {
			return fmt.Errorf("a placement became ready on %s %s at %s, wait until an hour has "+
				"passed", name, version, stamp(last))
		}
	}
	return nil
}

// recordRefs lists the artifacts the build record names for one provider
func recordRefs(path, name string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("the build record: %w", err)
	}
	var rec map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	section := rec[name]
	var s struct {
		Ref       string            `json:"ref"`
		Refs      map[string]string `json:"refs"`
		Templates []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"templates"`
	}
	if len(section) > 0 {
		if err := json.Unmarshal(section, &s); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
	}
	var out []string
	if s.Ref != "" && name != "koyeb" {
		out = append(out, s.Ref)
	}
	for _, ref := range s.Refs {
		out = append(out, ref)
	}
	for _, t := range s.Templates {
		out = append(out, cmp.Or(t.ID, t.Name))
	}
	sort.Strings(out)
	return out, nil
}

func (c *cli) imageUse(args []string) error {
	fs := newFlags("image use")
	name := fs.String("provider", "", "one provider, every provider when empty")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.ListImageUse(c.ctx, connect.NewRequest(
		&routerv1.ListImageUseRequest{Provider: *name}))
	if err != nil {
		return err
	}
	for _, u := range res.Msg.GetUses() {
		last := "never"
		if u.GetLastReadyAt() != nil {
			last = stamp(u.GetLastReadyAt())
		}
		_, _ = fmt.Fprintf(c.stdout, "provider=%s image_version=%s live_placements=%d "+
			"last_ready_at=%s\n", u.GetProvider(), cmp.Or(u.GetImageVersion(), "own"),
			u.GetLivePlacements(), last)
	}
	return nil
}
