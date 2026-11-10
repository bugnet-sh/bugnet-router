package live

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/koyeb"
)

// The hedge rows are CP 18.20's HEDGE1 and HEDGE2 and the Koyeb pool rows KOYEBP1 to KOYEBP5. The
// hedge rows run on a stack with two providers and hedge.default_after of 200 ms, so every start
// that takes longer than that hedges.
func init() {
	controlRows = append(controlRows,
		row{id: "HEDGE1", providers: []string{"e2b", "boxd", "koyeb"}, settings: hedgeStack,
			run: rowHedgeLosers},
		row{id: "HEDGE2", providers: []string{"e2b"}, settings: hedgeStack,
			run: rowHedgeLoserLife},
		row{id: "KOYEBP1", providers: []string{"koyeb"}, run: rowKoyebSecrets},
		row{id: "KOYEBP2", providers: []string{"koyeb"}, run: rowKoyebWake},
		row{id: "KOYEBP3", providers: []string{"koyeb"}, run: rowKoyebHeld},
		row{id: "KOYEBP4", providers: []string{"koyeb"}, run: rowKoyebIdleDay},
		row{id: "KOYEBP5", providers: []string{"koyeb"}, run: rowKoyebUnusedClaim},
	)
}

const (
	hedgeStarts = 10
	// hedgeSettle is how long a row waits for the router to destroy a loser it never saw end
	hedgeSettle = 3 * time.Minute
	// koyebPoolSize is the target of the pools the Koyeb rows make
	koyebPoolSize = 2
)

// hedgePartner is the provider a hedge row puts beside the provider it runs on
var hedgePartner = map[string]string{"e2b": "boxd", "boxd": "e2b", "koyeb": "e2b"}

// hedgeStack gives the stack the row's provider and its partner, and a hedge after 200 ms with a
// router-wide bucket wide enough for every start of the row
func hedgeStack(name string, m map[string]any) {
	m["providers"] = []string{name, hedgePartner[name]}
	h := table(m, "hedge")
	h["enabled"], h["default_after"], h["min_after"] = true, "200ms", "100ms"
	h["per_minute"], h["burst"], h["min_samples"] = 600, 20, 1
}

// hedgeOwned counts the sandboxes of the stack's deployment on each of its providers
func hedgeOwned(t *testing.T, s *stack) map[string]int {
	t.Helper()
	cfg, err := config.LoadFile(s.settings)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for _, name := range []string{s.provider, hedgePartner[s.provider]} {
		owned, err := liveAdapter(t, name).ListOwned(t.Context(),
			map[string]string{"bugnet_deployment": cfg.DeploymentID})
		if err != nil {
			t.Fatalf("list the %s sandboxes: %v", name, err)
		}
		out[name] = len(owned)
	}
	return out
}

// hedgeStartsOn orders n starts one after the other and answers how many hedged, stopping none
// until the row ends
func hedgeStartsOn(t *testing.T, s *stack, n int) (hedged int, handles []string) {
	t.Helper()
	for i := range n {
		created := kv(s.dev(t, "create", "--package", reservePkg))
		h := created["handle"]
		t.Cleanup(func() { _, _, _ = s.devTry(t, "stop", "--handle", h) })
		if created["state"] != "ready" {
			t.Fatalf("start %d printed %v, want a ready placement", i+1, created)
		}
		handles = append(handles, h)
		found := kv(s.dev(t, "abuse", "find", created["placement_id"]))
		if found["hedged"] == "true" {
			hedged++
		}
		t.Logf("start %d: %s, path %s, hedged %q", i+1, found["provider"], found["start_path"],
			found["hedged"])
	}
	return hedged, handles
}

// rowHedgeLosers is HEDGE1, ten starts on two providers with a hedge after 200 ms. Every loser
// must be destroyed, so the sandboxes left are the winners alone, and each provider's bill is read
// to show the seconds the losers ran.
func rowHedgeLosers(t *testing.T, s *stack) {
	from := time.Now().UTC().Add(-time.Minute)
	hedged, handles := hedgeStartsOn(t, s, hedgeStarts)
	t.Logf("HEDGE1: %d of %d starts hedged", hedged, hedgeStarts)
	if hedged == 0 {
		t.Errorf("HEDGE1: no start hedged, though each takes longer than 200 ms")
	}
	deadline := time.Now().Add(hedgeSettle)
	var owned map[string]int
	for time.Now().Before(deadline) {
		owned = hedgeOwned(t, s)
		total := 0
		for _, n := range owned {
			total += n
		}
		if total == len(handles) {
			break
		}
		time.Sleep(10 * time.Second)
	}
	total := 0
	for _, n := range owned {
		total += n
	}
	if total != len(handles) {
		t.Errorf("HEDGE1: %d sandboxes exist for %d placements after %s, so a loser was not "+
			"destroyed: %v", total, len(handles), hedgeSettle, owned)
	}
	for _, name := range []string{s.provider, hedgePartner[s.provider]} {
		reader, ok := liveAdapter(t, name).(provider.UsageReader)
		if !ok {
			continue
		}
		_, _, page := usageOf(t, reader, from, time.Now().UTC().Add(time.Minute),
			provider.Ref{})
		t.Logf("HEDGE1: %s billed %d micros in the row's window, complete %v, over %d winners "+
			"and %d hedged starts", name, page.TotalMicros, page.Complete, len(handles), hedged)
	}
}

// rowHedgeLoserLife is HEDGE2, which follows each loser from the start until the provider no longer
// lists it, against hedge.loser_life of 90 s. A loser cancelled inside its create is found by the
// placement's sweep or the worker's, so none may outlive that by more than the worker's minute.
func rowHedgeLoserLife(t *testing.T, s *stack) {
	cfg, err := config.LoadFile(s.settings)
	if err != nil {
		t.Fatal(err)
	}
	p := liveAdapter(t, s.provider)
	winners := map[string]bool{}
	longest := time.Duration(0)
	for i := range hedgeStarts {
		start := time.Now()
		created := kv(s.dev(t, "create", "--package", reservePkg))
		h := created["handle"]
		t.Cleanup(func() { _, _, _ = s.devTry(t, "stop", "--handle", h) })
		found := kv(s.dev(t, "abuse", "find", created["placement_id"]))
		winners[found["provider_sandbox_id"]] = true
		// Poll until the provider lists nothing of this start but the winner
		for time.Since(start) < cfg.Hedge.LoserLife+2*time.Minute {
			owned, err := p.ListOwned(t.Context(),
				map[string]string{"bugnet_deployment": cfg.DeploymentID})
			if err != nil {
				t.Fatalf("list the sandboxes: %v", err)
			}
			losers := 0
			for _, o := range owned {
				if !winners[o.Ref.ID] {
					losers++
				}
			}
			if losers == 0 {
				break
			}
			time.Sleep(2 * time.Second)
		}
		life := time.Since(start)
		longest = max(longest, life)
		t.Logf("HEDGE2: start %d, loser gone %s after the start", i+1, life.Round(time.Second))
	}
	limit := cfg.Hedge.LoserLife + time.Minute
	t.Logf("HEDGE2: the longest loser life was %s against hedge.loser_life of %s and a worker "+
		"sweep of a minute", longest.Round(time.Second), cfg.Hedge.LoserLife)
	if longest > limit+30*time.Second {
		t.Errorf("HEDGE2: a loser lived %s, past hedge.loser_life of %s and the worker's minute",
			longest.Round(time.Second), cfg.Hedge.LoserLife)
	}
}

// koyebPoolFor makes a Koyeb pool of its own for a row, which the row deletes with its members
func koyebPoolFor(t *testing.T, tag string) (*koyeb.Provider, provider.PoolSpec) {
	t.Helper()
	p := liveAdapter(t, "koyeb")
	kp, ok := p.(*koyeb.Provider)
	if !ok {
		t.Fatalf("the koyeb adapter is a %T", p)
	}
	size, ok := p.SizeFor(provider.Resources{VCPU: 1, MemoryMiB: 1024})
	if !ok {
		t.Fatal("koyeb has no small size")
	}
	run := strings.ToLower(fmt.Sprintf("%s%d", tag, time.Now().UnixNano()%1_000_000))
	spec := provider.PoolSpec{
		Name: "bn-live" + run + "-" + size.Name, Size: size, Target: koyebPoolSize,
		Labels: map[string]string{
			"bugnet": "1", "bugnet_deployment": "live" + run, "bugnet_pool": size.Name,
		},
	}
	if err := kp.EnsurePool(t.Context(), spec); err != nil {
		t.Fatalf("EnsurePool: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		owned, _ := p.ListOwned(ctx, map[string]string{"bugnet_deployment": "live" + run})
		for _, o := range owned {
			_ = p.Destroy(ctx, o.Ref)
		}
		_ = kp.DeletePool(ctx, spec.Name)
	})
	return kp, spec
}

// rowKoyebSecrets is KOYEBP1. Two claimed members of one pool each have a secret, which the other's
// executor refuses, and a call with no secret is refused by both. Until it passes no Koyeb pool is
// listed in the settings.
func rowKoyebSecrets(t *testing.T, _ *stack) {
	kp, spec := koyebPoolFor(t, "p1")
	ctx := t.Context()
	var members [2]provider.Claim
	for i := range members {
		c, err := kp.Claim(ctx, spec.Name, fmt.Sprintf("p1-%d-%d", i, time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("Claim %d: %v", i+1, err)
		}
		members[i] = c
		if err := kp.WaitReady(ctx, c.Ref); err != nil {
			t.Fatalf("member %d is not ready: %v", i+1, err)
		}
	}
	secrets := [2]string{members[0].Ref.Meta["secret"], members[1].Ref.Meta["secret"]}
	if secrets[0] == "" || secrets[0] == secrets[1] {
		t.Fatalf("KOYEBP1: the members share a secret or have none, so no Koyeb pool may be listed")
	}
	// try runs a command on a member's executor under a secret and says whether it ran
	try := func(member provider.Claim, secret string) bool {
		ref := member.Ref
		ref.Meta = map[string]string{}
		for k, v := range member.Ref.Meta {
			ref.Meta[k] = v
		}
		ref.Meta["secret"] = secret
		stream, err := kp.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"true"}})
		if err != nil {
			return false
		}
		defer func() { _ = stream.Close() }()
		_, _ = stream.Read(make([]byte, 1))
		code, err := stream.Wait()
		return err == nil && code == 0
	}
	for i := range members {
		if !try(members[i], secrets[i]) {
			t.Errorf("KOYEBP1: member %d refused its own secret", i+1)
		}
		if try(members[i], secrets[1-i]) {
			t.Errorf("KOYEBP1: member %d accepted the secret of member %d", i+1, 2-i)
		}
		if try(members[i], "") {
			t.Errorf("KOYEBP1: member %d ran a command with no secret", i+1)
		}
	}
	t.Logf("KOYEBP1: two members, two secrets, each refused by the other and by no secret")
}

// rowKoyebWake is KOYEBP2, the first HTTP/1.1 /health after a claim, from the claim to a ready
// executor, which is the wake time the pooled path counts
func rowKoyebWake(t *testing.T, _ *stack) {
	kp, spec := koyebPoolFor(t, "p2")
	ctx := t.Context()
	// A claim waits for its service, so the pool gets time to hold ready members first
	time.Sleep(60 * time.Second)
	var waits []time.Duration
	for i := range 3 {
		start := time.Now()
		c, err := kp.Claim(ctx, spec.Name, fmt.Sprintf("p2-%d-%d", i, start.UnixNano()))
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		claimed := time.Since(start)
		if err := kp.WaitReady(ctx, c.Ref); err != nil {
			t.Fatalf("WaitReady: %v", err)
		}
		waits = append(waits, time.Since(start))
		t.Logf("KOYEBP2: claim %d took %s, ready after %s, prewarmed %v", i+1,
			claimed.Round(time.Millisecond), time.Since(start).Round(time.Millisecond), c.Warm)
	}
	t.Logf("KOYEBP2: claim to a ready executor %v", waits)
}

// rowKoyebHeld is KOYEBP3. ListOwned with one claimed and the other members unclaimed must set
// PoolHeld on the unclaimed ones alone, which the sweep depends on
func rowKoyebHeld(t *testing.T, _ *stack) {
	kp, spec := koyebPoolFor(t, "p3")
	ctx := t.Context()
	time.Sleep(60 * time.Second)
	c, err := kp.Claim(ctx, spec.Name, fmt.Sprintf("p3-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	owned, err := kp.ListOwned(ctx,
		map[string]string{"bugnet_deployment": spec.Labels["bugnet_deployment"]})
	if err != nil {
		t.Fatalf("ListOwned: %v", err)
	}
	var held, taken int
	for _, o := range owned {
		switch {
		case o.Ref.Meta["service_id"] == c.Ref.Meta["service_id"] && o.PoolHeld:
			t.Errorf("KOYEBP3: the claimed member reads as held")
		case o.Ref.Meta["service_id"] == c.Ref.Meta["service_id"]:
			taken++
		case o.PoolHeld:
			held++
		default:
			t.Errorf("KOYEBP3: an unclaimed member %s reads as not held", o.Ref.ID)
		}
	}
	t.Logf("KOYEBP3: %d members listed, %d claimed and %d held", len(owned), taken, held)
	if taken != 1 || held == 0 {
		t.Errorf("KOYEBP3: %d claimed and %d held, want 1 claimed and the rest held", taken, held)
	}
}

// koyebIdleState is what KOYEBP4 keeps between its two runs, a day apart
type koyebIdleState struct {
	Pool    string    `json:"pool"`
	Started time.Time `json:"started"`
}

// rowKoyebIdleDay is KOYEBP4. Its first run makes a pool of idle members and leaves it, and a run
// a day later reads the usage of the window and deletes the pool, whose figure goes to CP question
// 19 and koyeb.pool_idle_hourly_micros.
func rowKoyebIdleDay(t *testing.T, _ *stack) {
	state := filepath.Join(os.TempDir(), "bugnet-koyebp4.json")
	p := liveAdapter(t, "koyeb")
	kp, ok := p.(*koyeb.Provider)
	if !ok {
		t.Fatalf("the koyeb adapter is a %T", p)
	}
	ctx := t.Context()
	raw, err := os.ReadFile(state)
	if err != nil {
		size, _ := p.SizeFor(provider.Resources{VCPU: 1, MemoryMiB: 1024})
		run := strings.ToLower(fmt.Sprintf("p4%d", time.Now().UnixNano()%1_000_000))
		spec := provider.PoolSpec{
			Name: "bn-live" + run + "-" + size.Name, Size: size, Target: koyebPoolSize,
			Labels: map[string]string{
				"bugnet": "1", "bugnet_deployment": "live" + run, "bugnet_pool": size.Name,
			},
		}
		if err := kp.EnsurePool(ctx, spec); err != nil {
			t.Fatalf("EnsurePool: %v", err)
		}
		out, _ := json.Marshal(koyebIdleState{Pool: spec.Name, Started: time.Now().UTC()})
		if err := os.WriteFile(state, out, 0o600); err != nil {
			t.Fatal(err)
		}
		t.Skipf("KOYEBP4: %s holds %d idle members from now, run this row again after 24 h",
			spec.Name, koyebPoolSize)
	}
	var st koyebIdleState
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("KOYEBP4: %s is not this row's state: %v", state, err)
	}
	if since := time.Since(st.Started); since < 24*time.Hour {
		t.Skipf("KOYEBP4: the pool %s is %s old, run this row again after 24 h", st.Pool,
			since.Round(time.Minute))
	}
	reader, ok := p.(provider.UsageReader)
	if !ok {
		t.Fatal("koyeb reads no usage")
	}
	_, _, page := usageOf(t, reader, st.Started.Add(-time.Hour), time.Now().UTC(), provider.Ref{})
	t.Logf("KOYEBP4: %d idle members for %s cost %d micros by Koyeb's usage details, complete %v,"+
		" so about %d micros an hour for each", koyebPoolSize,
		time.Since(st.Started).Round(time.Hour), page.TotalMicros, page.Complete,
		page.TotalMicros*int64(time.Hour)/int64(time.Since(st.Started))/koyebPoolSize)
	if err := kp.DeletePool(ctx, st.Pool); err != nil {
		t.Errorf("KOYEBP4: delete %s: %v", st.Pool, err)
	}
	_ = os.Remove(state)
}

// rowKoyebUnusedClaim is KOYEBP5. Destroying a claimed member that was never used must cost about
// $0.001, and the pool must make a member to take its place.
func rowKoyebUnusedClaim(t *testing.T, _ *stack) {
	kp, spec := koyebPoolFor(t, "p5")
	ctx := t.Context()
	time.Sleep(60 * time.Second)
	count := func() (unclaimed int) {
		owned, err := kp.ListOwned(ctx, map[string]string{
			"bugnet_deployment": spec.Labels["bugnet_deployment"]})
		if err != nil {
			t.Fatalf("ListOwned: %v", err)
		}
		for _, o := range owned {
			if o.PoolHeld {
				unclaimed++
			}
		}
		return unclaimed
	}
	before := count()
	start := time.Now()
	c, err := kp.Claim(ctx, spec.Name, fmt.Sprintf("p5-%d", start.UnixNano()))
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := kp.Destroy(ctx, c.Ref); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	alive := time.Since(start)
	t.Logf("KOYEBP5: %d unclaimed members before, the claim lived %s", before,
		alive.Round(time.Millisecond))
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if n := count(); n >= koyebPoolSize {
			t.Logf("KOYEBP5: the pool refilled to %d unclaimed members %s after the destroy", n,
				time.Since(start).Round(time.Second))
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Errorf("KOYEBP5: the pool never refilled to %d unclaimed members, it has %d",
		koyebPoolSize, count())
}
