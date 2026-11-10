package secrets_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/protobuf/proto"

	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// sourcerFake reports the egress source it is told to, and counts the reads
type sourcerFake struct {
	provider.Provider
	name string
	mu   sync.Mutex
	src  provider.EgressSource
	ok   bool
	err  error
	n    int
}

func (f *sourcerFake) Name() string { return f.name }

func (f *sourcerFake) EgressSource(
	context.Context, provider.Ref,
) (provider.EgressSource, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return f.src, f.ok, f.err
}

func (f *sourcerFake) reads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

func (f *sourcerFake) report(addr string, ok bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := netip.MustParseAddr(addr)
	f.src = provider.EgressSource{Prefix: netip.PrefixFrom(a, a.BitLen()), Bind: a}
	f.ok, f.err = ok, err
}

// plainFake is a provider with no egress source
type plainFake struct {
	provider.Provider
	name string
}

func (f *plainFake) Name() string { return f.name }

// inserts records the jobs a read inserted
type inserts struct {
	mu   sync.Mutex
	jobs []uuid.UUID
}

func (i *inserts) Insert(
	_ context.Context, args river.JobArgs, _ *river.InsertOpts,
) (*rivertype.JobInsertResult, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.jobs = append(i.jobs, args.(jobargs.ApplySecrets).PlacementID)
	return &rivertype.JobInsertResult{}, nil
}

func (i *inserts) count() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.jobs)
}

var bothFamilies = []netip.Addr{
	netip.MustParseAddr("203.0.113.7"), netip.MustParseAddr("2001:db8::7"),
}

// srcEnv is a ready placement on the fake provider, with a database and Valkey of its own
type srcEnv struct {
	t        *testing.T
	st       *store.Store
	vk       valkey.Client
	prefix   string
	fake     *sourcerFake
	reg      *provider.Registry
	ins      *inserts
	settings secrets.SourceSettings
	id       uuid.UUID
	now      time.Time
}

func newSrcEnv(t *testing.T, tune func(*secrets.SourceSettings)) *srcEnv {
	t.Helper()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(t.Context(), url); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	vk, prefix := testenv.Valkey(t)
	e := &srcEnv{
		t: t, st: store.New(pool), vk: vk, prefix: prefix, ins: &inserts{},
		fake: &sourcerFake{name: "boat"}, now: time.Now(),
		settings: secrets.SourceSettings{
			Modes:           map[string]secrets.Mode{"boat": secrets.ModeEnforce},
			RefreshInterval: time.Minute, ReadTimeout: time.Second, EgressIPs: bothFamilies,
		},
	}
	if tune != nil {
		tune(&e.settings)
	}
	e.reg, err = provider.NewRegistry([]provider.Provider{
		e.fake, &plainFake{name: "e2b"},
	}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// replica is a Sources of its own on the shared database and Valkey
func (e *srcEnv) replica() *secrets.Sources {
	e.t.Helper()
	s, err := secrets.NewSources(secrets.SourcesDeps{
		Settings: e.settings, Registry: e.reg, Store: e.st, Valkey: e.vk, Prefix: e.prefix,
		Events: edgeevents.New(e.vk, e.prefix, slog.New(slog.NewTextHandler(io.Discard, nil))),
		River:  e.ins, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now: func() time.Time { return e.now },
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

// place makes a ready placement on the named provider
func (e *srcEnv) place(name string) store.Placement {
	e.t.Helper()
	id := uuid.New()
	sum := sha256.Sum256([]byte(id.String()))
	_, _, err := e.st.CreatePlacement(e.t.Context(), store.NewPlacement{
		ID: id, HandleSHA256: sum[:], OrderID: "o-" + id.String(), PackageID: "small",
	})
	if err != nil {
		e.t.Fatal(err)
	}
	ref := provider.Ref{Provider: name, ID: "sbx-" + id.String()[:8]}
	now := time.Now()
	err = e.st.MarkPlacementReady(e.t.Context(), id, ref, "small", now, now.Add(time.Hour), nil)
	if err != nil {
		e.t.Fatal(err)
	}
	e.id = id
	return e.load()
}

func (e *srcEnv) load() store.Placement {
	e.t.Helper()
	p, err := e.st.PlacementByID(e.t.Context(), e.id)
	if err != nil {
		e.t.Fatal(err)
	}
	return p
}

// stored puts a source on the placement, as an earlier read did
func (e *srcEnv) stored(addr string) store.Placement {
	e.t.Helper()
	a := netip.MustParseAddr(addr)
	_, err := e.st.SetEgressSource(e.t.Context(), e.id, provider.EgressSource{
		Prefix: netip.PrefixFrom(a, a.BitLen()), Bind: a,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return e.load()
}

// changedEvents counts the SecretsChanged events of the placement
func (e *srcEnv) changedEvents() int {
	e.t.Helper()
	cmd := e.vk.B().Xrange().Key(e.prefix + "edge:events").Start("-").End("+").Build()
	entries, err := e.vk.Do(e.t.Context(), cmd).AsXRange()
	if err != nil && !valkey.IsValkeyNil(err) {
		e.t.Fatal(err)
	}
	n := 0
	for _, en := range entries {
		ev := &routerv1.EdgeEvent{}
		if err := proto.Unmarshal([]byte(en.FieldValues["e"]), ev); err != nil {
			e.t.Fatal(err)
		}
		if ev.GetSecretsChanged().GetPlacementId() == e.id.String() {
			n++
		}
	}
	return n
}

func addr(s string) netip.Addr { return netip.MustParseAddr(s) }

func TestAnswerMatrix(t *testing.T) {
	enforce := routerv1.SourceCheckMode_SOURCE_CHECK_MODE_ENFORCE
	unspecified := routerv1.SourceCheckMode_SOURCE_CHECK_MODE_UNSPECIFIED

	// mode off
	e := newSrcEnv(t, func(s *secrets.SourceSettings) {
		s.Modes = map[string]secrets.Mode{"boat": secrets.ModeOff}
	})
	e.place("boat")
	p := e.stored("2001:db8::5")
	eq(t, e.replica().Answer(t.Context(), p, addr("2001:db8::5")).GetMode(), unspecified)

	// a provider without the interface, and a provider with no setting
	e = newSrcEnv(t, nil)
	p = e.place("e2b")
	eq(t, e.replica().Answer(t.Context(), p, addr("2001:db8::5")).GetMode(), unspecified)
	eq(t, e.replica().Spec(p), (*secrets.SpecSource)(nil))

	// a stored prefix
	e = newSrcEnv(t, nil)
	e.place("boat")
	p = e.stored("2001:db8::5")
	got := e.replica().Answer(t.Context(), p, addr("2001:db8::5"))
	eq(t, got.GetMode(), enforce)
	eq(t, got.GetAllowedPrefixes(), []string{"2001:db8::5/128"})

	// none stored with allow: unspecified, and no read
	p = e.place("boat")
	eq(t, e.replica().Answer(t.Context(), p, addr("198.51.100.1")).GetMode(), unspecified)
	eq(t, e.fake.reads(), 0)

	// none stored with refuse: the mode with no prefixes, after one read that found none
	e = newSrcEnv(t, func(s *secrets.SourceSettings) { s.RefuseUnknown = true })
	p = e.place("boat")
	got = e.replica().Answer(t.Context(), p, addr("198.51.100.1"))
	eq(t, got.GetMode(), enforce)
	eq(t, len(got.GetAllowedPrefixes()), 0)
	eq(t, e.fake.reads(), 1)

	// none stored with refuse, and the provider reports one
	e = newSrcEnv(t, func(s *secrets.SourceSettings) { s.RefuseUnknown = true })
	e.fake.report("2001:db8::5", true, nil)
	p = e.place("boat")
	got = e.replica().Answer(t.Context(), p, addr("2001:db8::5"))
	eq(t, got.GetAllowedPrefixes(), []string{"2001:db8::5/128"})
}

func TestSourceInsidePrefixReadsNothing(t *testing.T) {
	e := newSrcEnv(t, nil)
	e.place("boat")
	p := e.stored("2001:db8::5")
	_ = e.replica().Answer(t.Context(), p, addr("2001:db8::5"))
	_ = e.replica().Answer(t.Context(), p, netip.Addr{})
	eq(t, e.fake.reads(), 0)
}

func TestSourceOutsideReadsOncePerInterval(t *testing.T) {
	e := newSrcEnv(t, nil)
	e.fake.report("2001:db8::5", true, nil)
	e.place("boat")
	p := e.stored("2001:db8::5")
	_ = e.replica().Answer(t.Context(), p, addr("2001:db8::9"))
	_ = e.replica().Answer(t.Context(), e.load(), addr("2001:db8::9"))
	_ = e.replica().Answer(t.Context(), e.load(), addr("2001:db8::a"))
	eq(t, e.fake.reads(), 1)
}

func TestNewAddressMovesVersionOnce(t *testing.T) {
	e := newSrcEnv(t, nil)
	e.fake.report("2001:db8::5", true, nil)
	p := e.place("boat")
	s := e.replica()
	got, err := s.Read(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.Prefix.String(), "2001:db8::5/128")
	after := e.load()
	eq(t, after.SecretsVersion, p.SecretsVersion+1)
	eq(t, e.changedEvents(), 1)
	eq(t, e.ins.count(), 1)

	// The same address moves nothing, but the read time
	if _, err := s.Read(t.Context(), after); err != nil {
		t.Fatal(err)
	}
	eq(t, e.load().SecretsVersion, p.SecretsVersion+1)
	eq(t, e.changedEvents(), 1)
	eq(t, e.ins.count(), 1)

	// A read inside apply_secrets enqueues nothing
	e.fake.report("2001:db8::6", true, nil)
	if _, changed, err := s.ReadInRun(t.Context(), e.load()); err != nil || !changed {
		t.Fatal(changed, err)
	}
	eq(t, e.load().SecretsVersion, p.SecretsVersion+2)
	eq(t, e.changedEvents(), 2)
	eq(t, e.ins.count(), 1)
}

func TestFailedReadKeepsTheStoredPrefix(t *testing.T) {
	for name, tune := range map[string]func(*sourcerFake){
		"error": func(f *sourcerFake) {
			f.report("2001:db8::9", true, errors.New("boom"))
		},
		"false":           func(f *sourcerFake) { f.report("2001:db8::9", false, nil) },
		"unserved family": func(f *sourcerFake) { f.report("198.51.100.9", true, nil) },
	} {
		t.Run(name, func(t *testing.T) {
			e := newSrcEnv(t, func(s *secrets.SourceSettings) {
				s.EgressIPs = []netip.Addr{addr("2001:db8::7")}
			})
			e.place("boat")
			p := e.stored("2001:db8::5")
			tune(e.fake)
			got, err := e.replica().Read(t.Context(), p)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, got.Prefix.String(), "2001:db8::5/128")
			after := e.load()
			eq(t, after.EgressSource.Prefix.String(), "2001:db8::5/128")
			eq(t, after.SecretsVersion, p.SecretsVersion)
			eq(t, e.changedEvents(), 0)
		})
	}
}

func TestObservedSourceIsNeverStored(t *testing.T) {
	e := newSrcEnv(t, nil)
	e.fake.report("2001:db8::9", false, nil)
	e.place("boat")
	p := e.stored("2001:db8::5")
	got := e.replica().Answer(t.Context(), p, addr("198.51.100.66"))
	eq(t, got.GetAllowedPrefixes(), []string{"2001:db8::5/128"})
	eq(t, e.load().EgressSource.Prefix.String(), "2001:db8::5/128")
	// A read that fails right after it changes nothing either
	e.fake.report("2001:db8::9", true, errors.New("boom"))
	e.now = e.now.Add(2 * time.Minute)
	_ = e.replica().Answer(t.Context(), e.load(), addr("198.51.100.66"))
	eq(t, e.load().EgressSource.Prefix.String(), "2001:db8::5/128")
}

func TestSpecCarriesTheStoredBind(t *testing.T) {
	e := newSrcEnv(t, nil)
	e.place("boat")
	eq(t, e.replica().Spec(e.load()), (*secrets.SpecSource)(nil))
	eq(t, e.replica().Spec(e.stored("2001:db8::5")),
		&secrets.SpecSource{Family: "ipv6", Bind: "2001:db8::5"})
	eq(t, e.replica().Spec(e.stored("203.0.113.10")),
		&secrets.SpecSource{Family: "ipv4", Bind: "203.0.113.10"})
	e = newSrcEnv(t, func(s *secrets.SourceSettings) {
		s.Modes = map[string]secrets.Mode{"boat": secrets.ModeOff}
	})
	e.place("boat")
	eq(t, e.replica().Spec(e.stored("2001:db8::5")), (*secrets.SpecSource)(nil))
}

func TestFreshAndUnknown(t *testing.T) {
	e := newSrcEnv(t, func(s *secrets.SourceSettings) { s.RefuseUnknown = true })
	s := e.replica()
	p := e.place("boat")
	eq(t, s.Active(p), true)
	eq(t, s.Unknown(p), true)
	eq(t, s.Fresh(p), false)
	p = e.stored("2001:db8::5")
	p.EgressSource.ReadAt = e.now.Add(-30 * time.Second)
	eq(t, s.Fresh(p), true)
	eq(t, s.Unknown(p), false)
	p.EgressSource.ReadAt = e.now.Add(-2 * time.Minute)
	eq(t, s.Fresh(p), false)
	eq(t, s.Providers(), []string{"boat"})
}

func TestStartRefusesBadSourceSettings(t *testing.T) {
	good := func() secrets.SourcesDeps {
		e := newSrcEnv(t, nil)
		return secrets.SourcesDeps{Settings: e.settings, Registry: e.reg}
	}
	for name, edit := range map[string]func(*secrets.SourcesDeps){
		"a provider not configured": func(d *secrets.SourcesDeps) {
			d.Settings.Modes = map[string]secrets.Mode{"koyeb": secrets.ModeReport}
		},
		"a provider with no egress source": func(d *secrets.SourcesDeps) {
			d.Settings.Modes = map[string]secrets.Mode{"e2b": secrets.ModeReport}
		},
		"a mode in words": func(d *secrets.SourcesDeps) {
			d.Settings.Modes = map[string]secrets.Mode{"boat": "block"}
		},
		"no refresh interval": func(d *secrets.SourcesDeps) { d.Settings.RefreshInterval = 0 },
		"no read timeout":     func(d *secrets.SourcesDeps) { d.Settings.ReadTimeout = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			d := good()
			edit(&d)
			if _, err := secrets.NewSources(d); err == nil {
				t.Fatal("started")
			}
		})
	}
	if _, err := secrets.NewSources(good()); err != nil {
		t.Fatal(err)
	}
}
