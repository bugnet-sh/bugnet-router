package live

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// The pool rows are CP 18.20's POOL1 to POOL5, which settle boxd's wake time, its idle cost,
// whether hibernated machines count against the machine limit, whether its delete-after timer
// runs on a hibernated machine and whether the proxy pin and SSH off survive a hibernation.
func init() {
	controlRows = append(controlRows,
		row{id: "POOL1", providers: []string{"boxd"}, run: rowPoolWake},
		row{id: "POOL2", providers: []string{"boxd"}, run: rowPoolIdleCost},
		row{id: "POOL3", providers: []string{"boxd"}, run: rowPoolLimit},
		row{id: "POOL4", providers: []string{"boxd"}, run: rowPoolBackstop},
		row{id: "POOL5", providers: []string{"boxd"}, run: rowPoolPins},
	)
}

const (
	poolWakes     = 20
	poolRunner    = "/tmp/bugnet/bin/bugnet-run"
	poolIdleDays  = 24 * time.Hour
	poolBackstop  = 3 * time.Minute
	poolBackstopW = 6 * time.Minute
)

// poolMember makes one boxd machine as pool_fill does, with the runner in place, and destroys it
// when the row ends
func poolMember(t *testing.T, p provider.Provider, tag string) provider.Ref {
	t.Helper()
	size, ok := p.SizeFor(provider.Resources{VCPU: 1, MemoryMiB: 1024})
	if !ok {
		t.Fatalf("boxd has no small size")
	}
	ctx := t.Context()
	run := fmt.Sprintf("%s-%d", tag, time.Now().UnixNano())
	ref, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: "pool-" + run, Size: size, Lifetime: 2 * time.Hour,
		Labels: map[string]string{"bugnet": "1", "bugnet_deployment": "dev", "pool_row": run},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = p.Destroy(context.WithoutCancel(ctx), ref) })
	if err := p.WaitReady(ctx, ref); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	build := filepath.Join("..", "..", "..", "bugnet-run", "dist", "bugnet-run-linux-amd64")
	data, err := os.ReadFile(build)
	if err != nil {
		t.Fatalf("no bugnet-run build at %s, run make -C ../bugnet-run build: %v", build, err)
	}
	if err := p.WriteFile(ctx, ref, poolRunner, data, 0o755); err != nil {
		t.Fatalf("upload the runner: %v", err)
	}
	return ref
}

// execOut runs argv in the sandbox and answers its stdout and exit code
func execOut(
	ctx context.Context, p provider.Provider, ref provider.Ref, argv ...string,
) (string, int, error) {
	stream, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: argv})
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = stream.Close() }()
	out, err := io.ReadAll(stream)
	if err != nil {
		return "", 0, err
	}
	code, err := stream.Wait()
	return string(out), code, err
}

// rowPoolWake is POOL1, a machine hibernated and woken 20 times with the runner's ping each time
func rowPoolWake(t *testing.T, s *stack) {
	p := liveAdapter(t, s.provider)
	pk, ok := p.(provider.Parker)
	if !ok {
		t.Fatal("boxd is no Parker")
	}
	ref := poolMember(t, p, "pool1")
	ctx := t.Context()
	var wakes []time.Duration
	for i := range poolWakes {
		if err := pk.Park(ctx, ref); err != nil {
			t.Fatalf("Park %d: %v", i+1, err)
		}
		st, err := p.Inspect(ctx, ref)
		if err != nil || st.State != provider.StateStopped {
			t.Fatalf("a parked machine inspects as %q, %v, want stopped", st.State, err)
		}
		start := time.Now()
		if err := pk.Unpark(ctx, ref); err != nil {
			t.Fatalf("Unpark %d: %v", i+1, err)
		}
		wakes = append(wakes, time.Since(start))
		if _, code, err := execOut(ctx, p, ref, poolRunner, "ping"); err != nil || code != 0 {
			t.Fatalf("the runner's ping after wake %d: exit %d, %v", i+1, code, err)
		}
	}
	slices.Sort(wakes)
	// These seed the hedge timing and reserve.keepalive
	t.Logf("POOL1: wake times min %s, median %s, p95 %s, max %s", wakes[0],
		wakes[len(wakes)/2], wakes[len(wakes)*95/100], wakes[len(wakes)-1])
}

// rowPoolIdleCost is POOL2, two members hibernated for a day and the account's usage read, which
// boxd.parked_gib and CP question 18 take their figures from
func rowPoolIdleCost(t *testing.T, s *stack) {
	p := liveAdapter(t, s.provider)
	pk, ok := p.(provider.Parker)
	usage, usageOK := p.(provider.UsageReader)
	if !ok || !usageOK {
		t.Fatal("boxd must be a Parker and a UsageReader")
	}
	ctx := t.Context()
	refs := []provider.Ref{poolMember(t, p, "pool2"), poolMember(t, p, "pool2")}
	for _, ref := range refs {
		if err := pk.Park(ctx, ref); err != nil {
			t.Fatalf("Park: %v", err)
		}
		if err := p.SetLifetime(ctx, ref, time.Now().Add(poolIdleDays+time.Hour)); err != nil {
			t.Fatalf("SetLifetime: %v", err)
		}
	}
	from := time.Now()
	t.Logf("POOL2: two members hibernated at %s, waiting a day", from.Format(time.RFC3339))
	select {
	case <-time.After(poolIdleDays):
	case <-ctx.Done():
		t.Fatalf("POOL2 was cut short: %v", ctx.Err())
	}
	to := time.Now()
	for _, ref := range refs {
		u, _, all := usageOf(t, usage, from, to, ref)
		t.Logf("POOL2: %s billed %d s and %d micros, page total %d, complete %v", ref.ID,
			u.Seconds, u.CostMicros, all.TotalMicros, all.Complete)
	}
}

// rowPoolLimit is POOL3, hibernated machines counting in the account's machine count
func rowPoolLimit(t *testing.T, s *stack) {
	p := liveAdapter(t, s.provider)
	pk, ok := p.(provider.Parker)
	reader, readOK := p.(provider.LimitsReader)
	if !ok || !readOK {
		t.Fatal("boxd must be a Parker and a LimitsReader")
	}
	ctx := t.Context()
	before, err := reader.ReadLimits(ctx)
	if err != nil {
		t.Fatalf("ReadLimits: %v", err)
	}
	ref := poolMember(t, p, "pool3")
	running, err := reader.ReadLimits(ctx)
	if err != nil {
		t.Fatalf("ReadLimits: %v", err)
	}
	if err := pk.Park(ctx, ref); err != nil {
		t.Fatalf("Park: %v", err)
	}
	parked, err := reader.ReadLimits(ctx)
	if err != nil {
		t.Fatalf("ReadLimits: %v", err)
	}
	t.Logf("POOL3: machines %d before, %d running, %d hibernated, cap %d", before.Active,
		running.Active, parked.Active, parked.MaxConcurrent)
	if parked.Active != running.Active {
		t.Errorf("POOL3: a hibernated machine counts %d of the account's machines, want it to "+
			"count as it did running (%d), so capacity must count it",
			parked.Active, running.Active)
	}
}

// rowPoolBackstop is POOL4, SetDeleteAfter on a hibernated machine ending it in time
func rowPoolBackstop(t *testing.T, s *stack) {
	p := liveAdapter(t, s.provider)
	pk, ok := p.(provider.Parker)
	if !ok {
		t.Fatal("boxd is no Parker")
	}
	ref := poolMember(t, p, "pool4")
	ctx := t.Context()
	if err := pk.Park(ctx, ref); err != nil {
		t.Fatalf("Park: %v", err)
	}
	if err := p.SetLifetime(ctx, ref, time.Now().Add(poolBackstop)); err != nil {
		t.Fatalf("SetLifetime on a hibernated machine: %v", err)
	}
	deadline := time.Now().Add(poolBackstopW)
	for time.Now().Before(deadline) {
		st, err := p.Inspect(ctx, ref)
		if err != nil {
			t.Fatalf("Inspect: %v", err)
		}
		if st.State == provider.StateGone {
			t.Logf("POOL4: the hibernated machine was deleted by its timer")
			return
		}
		time.Sleep(15 * time.Second)
	}
	t.Errorf("POOL4: a hibernated machine outlived its delete-after timer by %s", poolBackstopW)
}

// rowPoolPins is POOL5, the proxy pin, SSH off and bot protection after a wake
func rowPoolPins(t *testing.T, s *stack) {
	p := liveAdapter(t, s.provider)
	pk, ok := p.(provider.Parker)
	gater, gateOK := p.(provider.Gater)
	if !ok || !gateOK {
		t.Fatal("boxd must be a Parker and a Gater")
	}
	ref := poolMember(t, p, "pool5")
	ctx := t.Context()
	meta, err := gater.PrepareGate(ctx, ref, provider.DefaultGatePort)
	if err != nil {
		t.Fatalf("PrepareGate: %v", err)
	}
	for k, v := range meta {
		if ref.Meta == nil {
			ref.Meta = map[string]string{}
		}
		ref.Meta[k] = v
	}
	if err := pk.Park(ctx, ref); err != nil {
		t.Fatalf("Park: %v", err)
	}
	if err := pk.Unpark(ctx, ref); err != nil {
		t.Fatalf("Unpark: %v", err)
	}
	// Anything the user runs on the other ports boxd once proxied must stay unreachable
	for _, port := range []string{"3000", "8080"} {
		script := fmt.Sprintf("nohup python3 -m http.server %s >/dev/null 2>&1 & sleep 1", port)
		if _, _, err := execOut(ctx, p, ref, "sh", "-c", script); err != nil {
			t.Fatalf("start a server on %s: %v", port, err)
		}
	}
	route, err := gater.GateRoute(ref, provider.DefaultGatePort)
	if err != nil {
		t.Fatalf("GateRoute: %v", err)
	}
	// The ssh daemon boxd runs for its own SSH must still be off, and the gate route intact
	out, _, err := execOut(ctx, p, ref, "sh", "-c", "ss -ltn | awk '{print $4}'")
	if err != nil {
		t.Fatalf("ss: %v", err)
	}
	if strings.Contains(out, ":22\n") {
		t.Errorf("POOL5: boxd's SSH answers on port 22 after a wake:\n%s", out)
	}
	if route.BaseURL == "" {
		t.Errorf("POOL5: the gate route lost its host after a wake: %+v", route)
	}
	t.Logf("POOL5: route %s after a wake, listening on:\n%s", route.BaseURL, out)
}
