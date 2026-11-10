package saga_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/stats"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// statsFile answers the sampler's file with body, or fails the read, and counts the reads
type statsFile struct {
	*local.Provider
	body  []byte
	err   error
	reads atomic.Int32
	// block holds the read until its context ends
	block bool
}

func (s *statsFile) ReadFile(
	ctx context.Context, ref provider.Ref, path string, offset int64, length int,
) ([]byte, bool, error) {
	if path != stats.Path {
		return s.Provider.ReadFile(ctx, ref, path, offset, length)
	}
	s.reads.Add(1)
	if s.block {
		<-ctx.Done()
		return nil, false, ctx.Err()
	}
	if s.err != nil {
		return nil, false, s.err
	}
	return s.body, true, nil
}

const goodStats = `{"v":1,"mem_peak_mib":1500,"swap_peak_mib":0,"disk_peak_mib":900,` +
	`"cpu_seconds":30,"busy_p95_millicores":800,"gpu_mem_peak_mib":0,"oom_kills":1,` +
	`"samples":40,"updated_at":"2026-11-08T10:00:00Z"}`

// estimateRow stores an estimate for the source and returns its id
func (e *env) estimateRow(t *testing.T, entry string) (uuid.UUID, []byte) {
	t.Helper()
	sum := sha256.Sum256([]byte(entry))
	inputs := []byte(`{"request":{"entry_sha256":"` + hex.EncodeToString(sum[:]) + `"}}`)
	cat := sha256.Sum256([]byte("catalog"))
	if err := e.store.PutCatalog(t.Context(), cat[:], []byte("{}")); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	err := e.store.InsertEstimate(t.Context(), store.EstimateRow{
		ID: id, EstimatorVersion: "v", InputsSHA256: sum[:], Inputs: inputs, Output: []byte("{}"),
		Availability: []byte("[]"), CatalogSHA256: cat[:], SourceKind: "github",
		SourceKey: "github.com/acme/app", Revision: "abc", LockKey: "lock", Workload: "tests",
		SourceRead: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return id, sum[:]
}

// placeEstimated places a ready placement that carries an estimate
func (e *env) placeEstimated(t *testing.T, order string) (store.Placement, string, uuid.UUID) {
	t.Helper()
	id, _ := e.estimateRow(t, "python run.py")
	p, h, err := e.saga.Create(t.Context(), saga.CreateInput{Token: e.startToken(t, order), Requirements: placement.Requirements{}, Source: &store.PlacementSource{EstimateID: id}})
	if err != nil {
		t.Fatal(err)
	}
	e.saga.Wait()
	p = e.reload(t, p.ID)
	if p.State != store.PlacementReady {
		t.Fatalf("placement is %s (%s)", p.State, p.FailureReason)
	}
	return p, h, id
}

func TestStopRecordsAnObservation(t *testing.T) {
	a := &statsFile{Provider: newLocal(t, local.Options{Name: "a"}), body: []byte(goodStats)}
	e := newEnv(t, a)
	p, h, est := e.placeEstimated(t, "order-1")
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	o, err := e.store.ObservationOf(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("python run.py"))
	if o.EstimateID != est || o.SourceKey != "github.com/acme/app" || o.Revision != "abc" ||
		o.LockKey != "lock" || o.Workload != "tests" || o.PackageID != smallPkg ||
		o.SizeName != "small" || o.PeakMemoryMiB != 1500 || o.PeakDiskMiB != 900 ||
		o.CPUSeconds != 30 || o.BusyP95Millicores != 800 || o.OOMKills != 1 ||
		o.EndedBy != saga.ReasonStop || o.Flagged || o.Extended ||
		hex.EncodeToString(o.EntrySHA256) != hex.EncodeToString(sum[:]) {
		t.Fatalf("observation %+v", o)
	}
	if a.reads.Load() != 1 {
		t.Fatalf("%d stats reads", a.reads.Load())
	}
}

func TestInstallSecondsReachTheObservation(t *testing.T) {
	a := &statsFile{Provider: newLocal(t, local.Options{Name: "a"}), body: []byte(goodStats)}
	e := newEnv(t, a)
	p, h, _ := e.placeEstimated(t, "order-1")
	e.exec(t, `insert into jobs (id, placement_id, state, install, created_at, finished_at)
		values ('j_1', $1, 'exited', true, now() - interval '90 seconds',
			now() - interval '60 seconds'),
			('j_2', $1, 'exited', false, now() - interval '50 seconds', now())`, p.ID)
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	o, err := e.store.ObservationOf(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if o.InstallSeconds != 30 {
		t.Fatalf("install seconds %d, want 30", o.InstallSeconds)
	}
}

func TestBadStatsRecordNothing(t *testing.T) {
	for name, body := range map[string]string{
		"above the size": `{"v":1,"mem_peak_mib":999999}`,
		"unknown field":  `{"v":1,"extra":1}`,
		"a fraction":     `{"v":1,"cpu_seconds":1.5}`,
		"too large":      `{"v":1}` + string(make([]byte, 5<<10)),
	} {
		t.Run(name, func(t *testing.T) {
			a := &statsFile{Provider: newLocal(t, local.Options{Name: "a"}), body: []byte(body)}
			e := newEnv(t, a)
			p, h, _ := e.placeEstimated(t, "order-1")
			got, err := e.saga.Stop(t.Context(), h)
			if err != nil || got.State != store.PlacementTerminated {
				t.Fatalf("stop %+v, %v", got, err)
			}
			_, err = e.store.ObservationOf(t.Context(), p.ID)
			if !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("observation: %v", err)
			}
		})
	}
}

func TestNoEstimateReadsNothing(t *testing.T) {
	a := &statsFile{Provider: newLocal(t, local.Options{Name: "a"}), body: []byte(goodStats)}
	e := newEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if a.reads.Load() != 0 {
		t.Fatalf("%d stats reads without an estimate", a.reads.Load())
	}
	if _, err := e.store.ObservationOf(t.Context(), p.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("observation: %v", err)
	}
}

func TestUnreadableStatsNeverDelayTheDestroy(t *testing.T) {
	a := &statsFile{Provider: newLocal(t, local.Options{Name: "a"}), block: true}
	e := newEnv(t, a)
	p, h, _ := e.placeEstimated(t, "order-1")
	start := time.Now()
	got, err := e.saga.Stop(t.Context(), h)
	if err != nil || got.State != store.PlacementTerminated {
		t.Fatalf("stop %+v, %v", got, err)
	}
	if took := time.Since(start); took > 6*time.Second {
		t.Fatalf("the stop took %v", took)
	}
	if _, err := e.store.ObservationOf(t.Context(), p.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("observation: %v", err)
	}
}

func TestAbuseFlagsTheObservation(t *testing.T) {
	a := &statsFile{Provider: newLocal(t, local.Options{Name: "a"}), body: []byte(goodStats)}
	e := newEnv(t, a)
	p, h, _ := e.placeEstimated(t, "order-1")
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	o, err := e.store.ObservationOf(t.Context(), p.ID)
	if err != nil || o.Flagged {
		t.Fatalf("observation %+v, %v", o, err)
	}
	// Flagged after the end, the observation stops counting
	abuse := saga.End{Reason: saga.ReasonAbuse}
	if _, _, err := e.saga.FlagEnded(t.Context(), p.ID, abuse); err != nil {
		t.Fatal(err)
	}
	if o, err = e.store.ObservationOf(t.Context(), p.ID); err != nil || !o.Flagged {
		t.Fatalf("observation %+v, %v", o, err)
	}
}

func TestEndByAbuseFlagsTheObservation(t *testing.T) {
	a := &statsFile{Provider: newLocal(t, local.Options{Name: "a"}), body: []byte(goodStats)}
	e := newEnv(t, a)
	p, _, _ := e.placeEstimated(t, "order-1")
	if _, err := e.saga.End(t.Context(), p.ID, saga.End{Reason: saga.ReasonAbuse}); err != nil {
		t.Fatal(err)
	}
	o, err := e.store.ObservationOf(t.Context(), p.ID)
	if err != nil || !o.Flagged || o.EndedBy != saga.ReasonAbuse {
		t.Fatalf("observation %+v, %v", o, err)
	}
}

func (e *env) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := e.pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}
