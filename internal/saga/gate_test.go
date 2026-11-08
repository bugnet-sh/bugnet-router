package saga_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/gateclient"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/local"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
)

// countedGate is hostGate counting its PrepareGate calls
type countedGate struct {
	hostGate
	prepares *atomic.Int32
}

func (c countedGate) PrepareGate(
	ctx context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	c.prepares.Add(1)
	return c.hostGate.PrepareGate(ctx, ref, port)
}

// forget clears a placement's gate time and the route entry PrepareGate gave it
func forget(t *testing.T, e *env, p store.Placement) store.Placement {
	t.Helper()
	ref := *p.ProviderRef
	ref.Meta = map[string]string{}
	for k, v := range p.ProviderRef.Meta {
		if k != "gate_host" {
			ref.Meta[k] = v
		}
	}
	if _, err := e.pool.Exec(t.Context(),
		`update placements set gate_ready_at = null where id = $1`, p.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.store.SetPlacementRoute(t.Context(), p.ID, ref, nil); err != nil {
		t.Fatal(err)
	}
	return e.reload(t, p.ID)
}

func TestOpenGateRunsOnceAndStoresTheRoute(t *testing.T) {
	var prepares atomic.Int32
	a := countedGate{hostGate{newLocal(t, local.Options{Name: "a"})}, &prepares}
	e := newEnv(t, a)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.GateReadyAt != nil, true)
	p = forget(t, e, p)
	if _, _, err := e.saga.Gate(p); err == nil {
		t.Fatal("a route without its gate_host entry built")
	}
	prepares.Store(0)
	var wg sync.WaitGroup
	got := make([]store.Placement, 4)
	errs := make([]error, len(got))
	for i := range got {
		wg.Go(func() { got[i], errs[i] = e.saga.OpenGate(t.Context(), p) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil || got[i].GateReadyAt == nil {
			t.Fatalf("OpenGate %d: %v, gate ready at %v", i, err, got[i].GateReadyAt)
		}
	}
	eq(t, prepares.Load(), int32(1))
	stored := e.reload(t, p.ID)
	eq(t, stored.ProviderRef.Meta["gate_host"], "x")
	eq(t, stored.GateReadyAt != nil, true)
	route, key, err := e.saga.Gate(stored)
	if err != nil {
		t.Fatal(err)
	}
	if err := gateclient.Health(t.Context(), route, key); err != nil {
		t.Fatalf("the stored route's gate: %v", err)
	}
	// An open gate is left alone
	if _, err := e.saga.OpenGate(t.Context(), stored); err != nil {
		t.Fatal(err)
	}
	eq(t, prepares.Load(), int32(1))
}

func TestOpenGateRefusesAPlacementNotReady(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	p = forget(t, e, p)
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if _, err := e.saga.OpenGate(t.Context(), p); !errors.Is(err, saga.ErrNotReady) {
		t.Fatalf("OpenGate on a stopped placement: %v", err)
	}
}

func TestOpenGateFailureIsGateUnavailable(t *testing.T) {
	a := closedGate{newLocal(t, local.Options{Name: "a"})}
	e := newEnv(t, a)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.GateReadyAt == nil, true)
	if _, err := e.saga.OpenGate(t.Context(), p); !errors.Is(err, saga.ErrGateUnavailable) {
		t.Fatalf("OpenGate through a closed route: %v", err)
	}
	eq(t, e.reload(t, p.ID).GateReadyAt == nil, true)
}

func TestRepairGateRunsSetupRepairAndPrepareGate(t *testing.T) {
	var prepares atomic.Int32
	l := newLocal(t, local.Options{Name: "a"})
	s := newScripted(l, nil)
	s.caps = func(c provider.Capabilities) provider.Capabilities {
		c.ExecUser = "boxd"
		return c
	}
	a := scriptedGate{s, countedGate{hostGate{l}, &prepares}}
	e := newEnv(t, a)
	p, _ := e.placeReady(t, "order-1")
	p = forget(t, e, p)
	prepares.Store(0)
	before := len(s.argvs())
	repaired, err := e.saga.RepairGate(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, prepares.Load(), int32(1))
	argv := s.argvs()[before:]
	eq(t, argv, [][]string{{"sudo", "-n", "/tmp/bugnet/bin/bugnet-run", "setup", "--repair"}})
	eq(t, repaired.ProviderRef.Meta["gate_host"], "x")
	eq(t, e.reload(t, p.ID).ProviderRef.Meta["gate_host"], "x")
	// A repair leaves the gate time alone
	eq(t, e.reload(t, p.ID).GateReadyAt == nil, true)
}

// scriptedGate is a scripted provider whose gate route PrepareGate counts
type scriptedGate struct {
	scripted
	gate countedGate
}

func (s scriptedGate) PrepareGate(
	ctx context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	return s.gate.PrepareGate(ctx, ref, port)
}

func (s scriptedGate) GateRoute(ref provider.Ref, port int) (provider.GateRoute, error) {
	return s.gate.GateRoute(ref, port)
}

func TestPortRefusedFollowsTheGatesDenyList(t *testing.T) {
	cases := map[string][]int{
		"e2b":   {49983},
		"koyeb": {3030, 3031},
		"boat":  {22, 6080, 8090, 8911, 47984, 48000, 48010},
		"boxd":  {},
	}
	for name, own := range cases {
		t.Run(name, func(t *testing.T) {
			a := newLocal(t, local.Options{Name: name})
			e := newEnv(t, a)
			p, _ := e.placeReady(t, "order-1")
			refused := []int{0, -1, 65536, 29417, 22, 2022}
			for _, k := range []string{"gate_port", "sshd_port"} {
				port, err := strconv.Atoi(p.ProviderRef.Meta[k])
				if err != nil {
					t.Fatalf("meta %s: %v", k, err)
				}
				refused = append(refused, port)
			}
			for _, port := range slices.Concat(refused, own) {
				if !e.saga.PortRefused(p, port) {
					t.Errorf("port %d is allowed", port)
				}
			}
			for _, port := range []int{1, 80, 3000, 5173, 8080, 47983, 48011, 65535} {
				if slices.Contains(own, port) {
					continue
				}
				if e.saga.PortRefused(p, port) {
					t.Errorf("port %d is refused", port)
				}
			}
		})
	}
}

func TestRepairGateFailureIsGateUnavailable(t *testing.T) {
	var prepares atomic.Int32
	l := newLocal(t, local.Options{Name: "a"})
	s := newScripted(l, map[string]answer{})
	a := scriptedGate{s, countedGate{hostGate{l}, &prepares}}
	e := newEnv(t, a)
	p, _ := e.placeReady(t, "order-1")
	s.answers["setup"] = answer{out: `{"error":"runner says no"}`, code: 3}
	prepares.Store(0)
	if _, err := e.saga.RepairGate(t.Context(), p); !errors.Is(err, saga.ErrGateUnavailable) {
		t.Fatalf("RepairGate whose setup --repair failed: %v", err)
	}
	eq(t, prepares.Load(), int32(0))
	eq(t, e.reload(t, p.ID).ProviderRef.Meta, p.ProviderRef.Meta)
}

func TestRepairGatePrepareFailureIsGateUnavailable(t *testing.T) {
	l := newLocal(t, local.Options{Name: "a"})
	var broken atomic.Bool
	e := newEnv(t, brokenGate{hostGate{l}, &broken})
	p, _ := e.placeReady(t, "order-1")
	broken.Store(true)
	if _, err := e.saga.RepairGate(t.Context(), p); !errors.Is(err, saga.ErrGateUnavailable) {
		t.Fatalf("RepairGate whose PrepareGate failed: %v", err)
	}
	eq(t, e.reload(t, p.ID).ProviderRef.Meta, p.ProviderRef.Meta)
}

func TestGateStepsRacingAStopAreNotReady(t *testing.T) {
	steps := map[string]func(*saga.Saga, context.Context, store.Placement) error{
		"open": func(s *saga.Saga, ctx context.Context, p store.Placement) error {
			_, err := s.OpenGate(ctx, p)
			return err
		},
		"repair": func(s *saga.Saga, ctx context.Context, p store.Placement) error {
			_, err := s.RepairGate(ctx, p)
			return err
		},
	}
	for name, step := range steps {
		t.Run(name, func(t *testing.T) {
			g := heldGate{hostGate: hostGate{newLocal(t, local.Options{Name: "a"})},
				armed: &atomic.Bool{}, entered: make(chan struct{}), release: make(chan struct{})}
			e := newEnv(t, g)
			p, h := e.placeReady(t, "order-1")
			p = forget(t, e, p)
			g.armed.Store(true)
			done := make(chan error, 1)
			go func() { done <- step(e.saga, t.Context(), p) }()
			<-g.entered
			if _, err := e.saga.Stop(t.Context(), h); err != nil {
				t.Fatal(err)
			}
			close(g.release)
			if err := <-done; !errors.Is(err, saga.ErrNotReady) {
				t.Fatalf("gate steps racing a stop: %v", err)
			}
		})
	}
}

// brokenGate is hostGate whose PrepareGate fails while broken is set
type brokenGate struct {
	hostGate
	broken *atomic.Bool
}

func (b brokenGate) PrepareGate(
	ctx context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	if b.broken.Load() {
		return nil, errors.New("injected by provider a")
	}
	return b.hostGate.PrepareGate(ctx, ref, port)
}

// heldGate is hostGate whose next PrepareGate, once armed, answers only when release closes
type heldGate struct {
	hostGate
	armed   *atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (h heldGate) PrepareGate(
	ctx context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	meta, err := h.hostGate.PrepareGate(ctx, ref, port)
	if h.armed.CompareAndSwap(true, false) {
		close(h.entered)
		<-h.release
	}
	return meta, err
}

func TestRepairGateWaitsForTheGateLock(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	p, _ := e.placeReady(t, "order-1")
	done := make(chan error, 1)
	err := e.store.WithLock(t.Context(), "gate:"+p.ID.String(), func(*store.Store) error {
		go func() {
			_, err := e.saga.RepairGate(t.Context(), p)
			done <- err
		}()
		select {
		case err := <-done:
			done <- err
			t.Errorf("RepairGate finished while another replica held the gate lock: %v", err)
		case <-time.After(500 * time.Millisecond):
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
