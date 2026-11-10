package secrets

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"slices"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// guardWait is how long ResolveTunnel waits on Valkey for the read guard
const guardWait = 200 * time.Millisecond

// Mode is one provider's value in [secrets.source_check] modes
type Mode string

// The modes of [secrets.source_check]
const (
	ModeOff     Mode = "off"
	ModeReport  Mode = "report"
	ModeEnforce Mode = "enforce"
)

// SourceSettings is [secrets.source_check], with secrets.egress_ips for the families a
// forwarder can reach
type SourceSettings struct {
	Modes           map[string]Mode // by provider, a provider absent is off
	RefuseUnknown   bool            // on_unknown = "refuse"
	RefreshInterval time.Duration
	ReadTimeout     time.Duration
	EgressIPs       []netip.Addr
}

// SourceSettingsOf is the source settings of the router's configuration
func SourceSettingsOf(cfg config.Config) SourceSettings {
	sc := cfg.Secrets.SourceCheck
	out := SourceSettings{
		Modes: make(map[string]Mode, len(sc.Modes)), RefuseUnknown: sc.RefuseUnknown,
		RefreshInterval: sc.RefreshInterval, ReadTimeout: sc.ReadTimeout,
		EgressIPs: cfg.Secrets.EgressIPs,
	}
	for name, m := range sc.Modes {
		out.Modes[name] = Mode(m)
	}
	return out
}

// Inserter is the part of a River client that inserts a job outside a transaction
type Inserter interface {
	Insert(
		ctx context.Context, args river.JobArgs, opts *river.InsertOpts,
	) (*rivertype.JobInsertResult, error)
}

// SourcesDeps are what Sources reads, stores and tells
type SourcesDeps struct {
	Settings SourceSettings
	Registry *provider.Registry
	Store    *store.Store
	Valkey   valkey.Client
	Prefix   string
	// Events gets SecretsChanged, none sent when nil
	Events *edgeevents.Bus
	// River inserts apply_secrets when a read outside the job changes the source
	River Inserter
	Log   *slog.Logger
	Now   func() time.Time // time.Now when nil
}

// Sources reads, stores and answers where each placement's tunnels may come from
type Sources struct {
	SourcesDeps
}

// NewSources checks the settings against the configured providers and refuses a bad one
func NewSources(d SourcesDeps) (*Sources, error) {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Settings.RefreshInterval <= 0 || d.Settings.ReadTimeout <= 0 {
		return nil, errors.New("secrets.source_check needs a positive refresh_interval " +
			"and read_timeout")
	}
	for name, m := range d.Settings.Modes {
		switch m {
		case ModeOff, ModeReport, ModeEnforce:
		default:
			return nil, fmt.Errorf("secrets.source_check.modes.%s must be off, report or enforce",
				name)
		}
		p, ok := d.Registry.Get(name)
		if !ok {
			return nil, fmt.Errorf("secrets.source_check.modes names %s, which is not configured",
				name)
		}
		if _, ok := provider.As[provider.EgressSourcer](p); !ok {
			return nil, fmt.Errorf("secrets.source_check.modes names %s, which reports no "+
				"egress address", name)
		}
	}
	return &Sources{d}, nil
}

// Providers lists the providers whose mode is not off, sorted
func (s *Sources) Providers() []string {
	var out []string
	for name, m := range s.Settings.Modes {
		if m != ModeOff && m != "" {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// sourcer is p's provider when its mode is not off and its adapter reports an egress source
func (s *Sources) sourcer(p store.Placement) (provider.EgressSourcer, Mode, bool) {
	mode := s.Settings.Modes[p.Provider]
	if mode == "" || mode == ModeOff || p.ProviderRef == nil {
		return nil, ModeOff, false
	}
	prov, ok := s.Registry.Get(p.Provider)
	if !ok {
		return nil, ModeOff, false
	}
	es, ok := provider.As[provider.EgressSourcer](prov)
	return es, mode, ok
}

// Active says whether p's provider is checked, its mode not off and its adapter a source reporter
func (s *Sources) Active(p store.Placement) bool {
	_, _, ok := s.sourcer(p)
	return ok
}

// Fresh says whether a read stored p's source within refresh_interval
func (s *Sources) Fresh(p store.Placement) bool {
	at := p.EgressSource.ReadAt
	return !at.IsZero() && s.Now().Sub(at) < s.Settings.RefreshInterval
}

// Unknown says whether p is checked, has no stored source, and on_unknown is refuse
func (s *Sources) Unknown(p store.Placement) bool {
	return s.Settings.RefuseUnknown && s.Active(p) && !p.EgressSource.Prefix.IsValid()
}

// Read asks p's provider for its egress source and stores a usable one. It keeps what is
// stored when the provider errs, reports none, or reports a family no egress IP serves
func (s *Sources) Read(ctx context.Context, p store.Placement) (store.EgressSource, error) {
	src, _, err := s.read(ctx, p, true)
	return src, err
}

// ReadInRun is Read for apply_secrets, whose run writes the new spec next without a new job.
// It also reports whether the read moved secrets_version, by exactly one
func (s *Sources) ReadInRun(
	ctx context.Context, p store.Placement,
) (store.EgressSource, bool, error) {
	return s.read(ctx, p, false)
}

func (s *Sources) read(
	ctx context.Context, p store.Placement, enqueue bool,
) (store.EgressSource, bool, error) {
	kept := p.EgressSource
	es, _, ok := s.sourcer(p)
	if !ok {
		return kept, false, nil
	}
	rctx, cancel := context.WithTimeout(ctx, s.Settings.ReadTimeout)
	defer cancel()
	src, found, err := es.EgressSource(rctx, *p.ProviderRef)
	switch {
	case err != nil:
		s.logRead(p, "error")
		return kept, false, nil
	case !found:
		s.logRead(p, "none")
		return kept, false, nil
	case !s.serves(src.Bind):
		s.logRead(p, "family_unserved")
		return kept, false, nil
	}
	changed, err := s.Store.SetEgressSource(ctx, p.ID, src)
	if err != nil {
		return kept, false, err
	}
	stored := store.EgressSource{Prefix: src.Prefix.Masked(), Bind: src.Bind, ReadAt: s.Now()}
	if !changed {
		return stored, false, nil
	}
	s.Log.Warn("egress source changed", "provider", p.Provider, "placement", p.ID,
		"old", kept.Prefix.String(), "new", stored.Prefix.String())
	if s.Events != nil {
		s.Events.Publish(ctx, &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_SecretsChanged{
			SecretsChanged: &routerv1.SecretsChanged{PlacementId: p.ID.String()},
		}})
	}
	if enqueue && s.River != nil {
		if _, err := s.River.Insert(ctx, jobargs.ApplySecrets{PlacementID: p.ID}, nil); err != nil {
			return stored, true, fmt.Errorf("insert apply_secrets job: %w", err)
		}
	}
	return stored, true, nil
}

// serves says whether some egress IP is of addr's family
func (s *Sources) serves(addr netip.Addr) bool {
	for _, ip := range s.Settings.EgressIPs {
		if ip.Unmap().Is4() == addr.Unmap().Is4() {
			return true
		}
	}
	return false
}

func (s *Sources) logRead(p store.Placement, result string) {
	s.Log.Info("egress_source_read", "provider", p.Provider, "placement", p.ID, "result", result)
}

// Answer is ResolveTunnel's source_check for p. When observed falls outside the stored
// prefix, it reads the provider again first, at most once per placement every
// RefreshInterval across replicas. It never stores observed
func (s *Sources) Answer(
	ctx context.Context, p store.Placement, observed netip.Addr,
) *routerv1.TunnelSourceCheck {
	_, mode, ok := s.sourcer(p)
	if !ok {
		return nil
	}
	stored := p.EgressSource
	switch {
	case stored.Prefix.IsValid():
		if observed.IsValid() && !stored.Prefix.Contains(observed.Unmap()) {
			stored = s.readGuarded(ctx, p)
		}
	case s.Settings.RefuseUnknown:
		stored = s.readGuarded(ctx, p)
	default:
		return nil
	}
	out := &routerv1.TunnelSourceCheck{Mode: protoMode(mode)}
	if stored.Prefix.IsValid() {
		out.AllowedPrefixes = []string{stored.Prefix.String()}
	}
	return out
}

// readGuarded reads once per refresh_interval across replicas, and answers what is stored
func (s *Sources) readGuarded(ctx context.Context, p store.Placement) store.EgressSource {
	gctx, cancel := context.WithTimeout(ctx, guardWait)
	defer cancel()
	key := s.Prefix + "egress:srcread:" + p.ID.String()
	err := s.Valkey.Do(gctx, s.Valkey.B().Set().Key(key).Value("1").Nx().
		Px(s.Settings.RefreshInterval).Build()).Error()
	if err != nil {
		if !valkey.IsValkeyNil(err) {
			s.Log.Warn("egress source read guard unavailable", "placement", p.ID)
		}
		return p.EgressSource
	}
	got, err := s.Read(ctx, p)
	if err != nil {
		s.Log.Warn("egress source not stored", "placement", p.ID, "err", err)
		return p.EgressSource
	}
	return got
}

func protoMode(m Mode) routerv1.SourceCheckMode {
	switch m {
	case ModeReport:
		return routerv1.SourceCheckMode_SOURCE_CHECK_MODE_REPORT
	case ModeEnforce:
		return routerv1.SourceCheckMode_SOURCE_CHECK_MODE_ENFORCE
	}
	return routerv1.SourceCheckMode_SOURCE_CHECK_MODE_UNSPECIFIED
}

// SpecSource is the "source" object of /tmp/bugnet/egress-spec.json
type SpecSource struct {
	Family string `json:"family"` // ipv4 or ipv6
	Bind   string `json:"bind"`
}

// Spec is the egress spec's source block for p, nil when the forwarder may dial any family
func (s *Sources) Spec(p store.Placement) *SpecSource {
	if _, _, ok := s.sourcer(p); !ok || !p.EgressSource.Prefix.IsValid() {
		return nil
	}
	bind := p.EgressSource.Bind.Unmap()
	family := "ipv6"
	if bind.Is4() {
		family = "ipv4"
	}
	return &SpecSource{Family: family, Bind: bind.String()}
}
