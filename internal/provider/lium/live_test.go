package lium

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/leakscan"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/contract"
	"github.com/bugnet-sh/bugnet-router/internal/provider/livetest"
	"github.com/bugnet-sh/bugnet-router/internal/reconcile"
)

// The live rows of design 7.1 rent real pods on the dev key, one row per run of
// scripts/live-test.sh --run, and every rent asks Lium to remove its pod within the hour.

const (
	// liveFloorMicros is the account balance below which no live row rents
	liveFloorMicros = 6_500_000
	// liveRowMicros is what a row of one or two short pods is planned to spend at most
	liveRowMicros = 150_000
	// liveLifetime keeps termination_hours at 1 for every live rent
	liveLifetime = 30 * time.Minute
	// liveReady bounds a rent and its wait for SSH, a cold pull of our image included
	liveReady = 15 * time.Minute
	liveOp    = 3 * time.Minute
)

// liveFloor is the dev packages' GPU, the floor every live rent asks SizeFor to cover
var liveFloor = provider.Resources{GPUCount: 1, GPUModel: gpu.RTX3090}

// liveProvider is the adapter on the dev key with config/dev.toml's settings, skipping without
// LIUM_API_KEY, its node listing read once
func liveProvider(t *testing.T) *Provider {
	t.Helper()
	key := os.Getenv("LIUM_API_KEY")
	if key == "" {
		t.Skip("LIUM_API_KEY is not set")
	}
	seed, err := config.ProviderSeed("lium", os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(liveOptions(livetest.Settings(t), key, seed))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	until := time.Now().Add(time.Minute)
	for {
		if _, fresh := p.nodes.fresh(); fresh {
			return p
		}
		if time.Now().After(until) {
			t.Fatal("lium's node listing was not read within a minute")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// liveOptions maps the settings as providers.Build does, every rent's termination set to 1 hour
func liveOptions(cfg config.Config, key string, seed []byte) Options {
	var sizes []SizeSpec
	for _, s := range cfg.Lium.Sizes {
		sizes = append(sizes, SizeSpec{
			Name: s.Size, Model: s.Model, Match: s.Match, GPUs: s.GPUs,
			GPUMemoryGiB: s.GPUMemoryGiB, VCPU: s.VCPU, MemoryMiB: s.MemoryMiB,
			DiskGiB: s.DiskGiB, MaxHourlyMicros: s.MaxHourlyMicros,
		})
	}
	l := cfg.Lium
	return Options{
		APIKey: key, SSHSeed: seed, Sizes: sizes, Countries: l.Countries,
		OtherCountries: l.OtherCountries, BlockedCountries: l.BlockedCountries,
		MinDownloadMbps: l.MinDownloadMbps, MaxLifetime: l.MaxLifetime,
		StartBilled: l.StartBilled, OpsPerSecond: l.OpsPerSecond,
		CreatesPerSecond: cfg.CreatesPerSecond["lium"], Image: cfg.Images.Lium.Ref,
		ImageVersion: cfg.Images.Lium.Version, TemplateVolume: l.TemplateVolume,
		Termination:   terminationLifetime,
		MaxConcurrent: cmp.Or(cfg.MaxConcurrent["lium"], config.DefaultMaxConcurrent),
		AccountEvery:  cfg.LimitsPoll.AccountEvery,
	}
}

// liveGuard fails the row before it rents when planned micros could take the balance under the
// floor, and logs the balance as the row's starting point
func liveGuard(t *testing.T, p *Provider, planned int64) int64 {
	t.Helper()
	b := liveBalance(t, p)
	t.Logf("balance before the row: %s", usd(b))
	if b-planned < liveFloorMicros {
		t.Fatalf("balance %s less the row's %s would cross the floor of %s", usd(b),
			usd(planned), usd(liveFloorMicros))
	}
	return b
}

// liveBalance reads the account's balance in micros
func liveBalance(t *testing.T, p *Provider) int64 {
	t.Helper()
	var me userMe
	ctx, cancel := context.WithTimeout(context.Background(), liveOp)
	defer cancel()
	if _, err := p.api.do(ctx, call{method: http.MethodGet, route: "/users/me"}, &me); err != nil {
		t.Fatalf("read the balance: %v", err)
	}
	b, ok := microsOf(me.Balance)
	if !ok {
		t.Fatal("GET /users/me names no balance")
	}
	return b
}

// liveHex is n random bytes in hex
func liveHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// liveLabels are a fresh live run's labels, which bugnet-dev owned counts
func liveLabels() map[string]string {
	return map[string]string{"bugnet-live": liveHex(8)}
}

// livePod is one pod a row rented and when each step of its start ended
type livePod struct {
	ref    provider.Ref
	key    string
	labels map[string]string
	// sent is just before the rent, rented when Create returned, running at RUNNING, ready once
	// the host key was pinned
	sent, rented, running, ready time.Time
}

// liveRent rents a pod under labels, waits for SSH and deletes it when the test ends
func liveRent(t *testing.T, p *Provider, labels map[string]string) *livePod {
	t.Helper()
	lp := &livePod{key: liveHex(16), labels: labels}
	if labels == nil {
		lp.labels = liveLabels()
	}
	t.Cleanup(func() { liveSweep(t, p, lp.labels) })
	size, ok := p.SizeFor(liveFloor)
	if !ok {
		t.Fatal("lium offers no size for the dev GPU now")
	}
	ctx, cancel := context.WithTimeout(t.Context(), liveReady)
	defer cancel()
	lp.sent = time.Now()
	ref, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: lp.key, Size: size, Labels: lp.labels, Lifetime: liveLifetime,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	lp.rented, lp.ref = time.Now(), ref
	t.Cleanup(func() { liveDestroy(t, p, lp.ref) })
	liveLogBody(t, p, lp.key)
	lp.wait(ctx, t, p)
	return lp
}

// wait waits for RUNNING, then for ReadyMeta, keeping its meta in the ref
func (lp *livePod) wait(ctx context.Context, t *testing.T, p *Provider) {
	t.Helper()
	if _, err := p.waitRunning(ctx, lp.ref.ID); err != nil {
		t.Fatalf("wait for RUNNING: %v", err)
	}
	lp.running = time.Now()
	detail := liveDetail(t, p, lp.ref.ID)
	meta, err := p.ReadyMeta(ctx, lp.ref)
	if err != nil {
		t.Fatalf("ReadyMeta: %v", err)
	}
	lp.ready = time.Now()
	node := lp.ref.Meta[metaNode]
	lp.ref = withMetaOf(lp.ref, meta)
	t.Logf("timing: pod %s rent %v, RUNNING %v after the rent was sent, SSH pinned %v later, "+
		"node %s at rent and %s at RUNNING (%s %s %s), %s micros an hour",
		lp.ref.ID, lp.rented.Sub(lp.sent).Round(time.Millisecond),
		lp.running.Sub(lp.sent).Round(time.Second), lp.ready.Sub(lp.running).Round(time.Second),
		node, lp.ref.Meta[metaNode], liveField(detail, "executor", "machine_name"),
		liveField(detail, "executor", "location", "country_code"),
		liveField(detail, "executor", "tier"), lp.ref.Meta[metaHourlyMicros])
}

// withMetaOf is ref with meta's entries over its own
func withMetaOf(ref provider.Ref, meta map[string]string) provider.Ref {
	m := map[string]string{}
	for k, v := range ref.Meta {
		m[k] = v
	}
	for k, v := range meta {
		m[k] = v
	}
	ref.Meta = m
	return ref
}

// liveLogBody logs the rent body the key sent, its termination_hours above all
func liveLogBody(t *testing.T, p *Provider, key string) {
	t.Helper()
	r, ok := p.remembered(podKey(key))
	if !ok {
		t.Log("no rent body was kept for the key")
		return
	}
	var b rentBody
	if err := json.Unmarshal(r.body, &b); err != nil {
		t.Fatal(err)
	}
	t.Logf("rent body: pod_name %s (%d chars), termination_hours %d, expected price %.4f, "+
		"skip_add_agent_ssh_key %v, template %s", b.PodName, len(b.PodName), b.TerminationHours,
		b.ExpectedPricePerGPUHour, b.SkipAddAgentSSHKey, b.TemplateID)
	if b.TerminationHours != 1 {
		t.Fatalf("termination_hours is %d, want 1", b.TerminationHours)
	}
}

// liveDetail reads GET /pods/{id} as Lium writes it and logs what the rows look at
func liveDetail(t *testing.T, p *Provider, id string) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveOp)
	defer cancel()
	var d map[string]any
	if _, err := p.api.do(ctx, call{method: http.MethodGet, route: podRoute(id)}, &d); err != nil {
		t.Logf("pod %s: read failed: %v", id, err)
		return nil
	}
	var words []string
	for _, k := range []string{"status", "price", "termination_hours", "removal_scheduled_at",
		"removal_at", "created_at", "updated_at", "phase", "eta_basis", "rental_tier",
		"skip_add_agent_ssh_key", "volume_limit_gb", "disk_size_gb", "node_offline",
		"billing_paused", "ssh_unreachable_since", "spend_to_date", "billed_until",
		"machine_changed_at"} {
		words = append(words, fmt.Sprintf("%s=%v", k, d[k]))
	}
	t.Logf("pod %s: %s", id, strings.Join(words, " "))
	return d
}

// liveField reads a nested field of a decoded answer as text, empty when it is absent
func liveField(d map[string]any, path ...string) string {
	var v any = d
	for _, k := range path {
		m, ok := v.(map[string]any)
		if !ok {
			return ""
		}
		v = m[k]
	}
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

// liveDestroy deletes the pod, then checks Inspect reads it as gone, R1 on every row
func liveDestroy(t *testing.T, p *Provider, ref provider.Ref) {
	t.Helper()
	if ref.ID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveOp)
	defer cancel()
	if err := p.Destroy(ctx, ref); err != nil {
		t.Errorf("destroy %s: %v", ref.ID, err)
		return
	}
	var st provider.Status
	var err error
	for range 10 {
		if st, err = p.Inspect(ctx, ref); err == nil && st.State == provider.StateGone {
			break
		}
		time.Sleep(3 * time.Second)
	}
	t.Logf("pod %s: Inspect after the delete gives %s %q kill %q, %v", ref.ID, st.State,
		st.Reason, st.KillReason, err)
	if err != nil || st.State != provider.StateGone {
		t.Errorf("Inspect after the delete gave %s, %v, want gone", st.State, err)
	}
	if err := p.Destroy(ctx, ref); err != nil {
		t.Errorf("a second destroy of %s gave %v, want nil", ref.ID, err)
	}
}

// liveSweep deletes every pod under labels, the last line of defence of a row
func liveSweep(t *testing.T, p *Provider, labels map[string]string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveOp)
	defer cancel()
	owned, err := p.ListOwned(ctx, labels)
	if err != nil {
		t.Errorf("list pods left by the row: %v", err)
	}
	for _, s := range owned {
		if err := p.Destroy(ctx, s.Ref); err != nil {
			t.Errorf("destroy %s: %v", s.Ref.ID, err)
		}
	}
}

// liveSh runs a shell script on the pod and returns its stdout and exit code
func liveSh(t *testing.T, p *Provider, ref provider.Ref, script string) (string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	out, code, err := liveExec(ctx, p, ref, script)
	if err != nil {
		t.Fatalf("exec %q: %v", script, err)
	}
	return out, code
}

// liveExec runs a shell script on the pod, an error when the call itself fails
func liveExec(ctx context.Context, p *Provider, ref provider.Ref, script string) (string,
	int, error,
) {
	s, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"sh", "-c", script}})
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = s.Close() }()
	out, rerr := io.ReadAll(io.LimitReader(s, 1<<20))
	code, err := s.Wait()
	return string(out), code, cmp.Or(err, rerr)
}

// liveShow runs a script and logs its output under a title
func liveShow(t *testing.T, p *Provider, ref provider.Ref, title, script string) string {
	t.Helper()
	out, code := liveSh(t, p, ref, script)
	t.Logf("%s (exit %d):\n%s", title, code, strings.TrimRight(out, "\n"))
	return out
}

// LIUM1: the template boots, the image is ours, and /workspace leaves /root intact
func TestLiveLIUM1Template(t *testing.T) {
	p := liveProvider(t)
	liveGuard(t, p, liveRowMicros)
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	spec, err := p.templateSpecFor(p.image)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := p.accountID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("account id read from GET /users/me: %v", owner != "")
	before, err := p.findTemplate(ctx, spec, owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("template %s listed before the row: %q", spec.Name, before)
	for _, tag := range []string{spec.DockerImageTag, ""} {
		s := spec
		s.DockerImageTag = tag
		body, _ := json.Marshal(s)
		var issues []map[string]any
		_, err := p.api.do(ctx, call{method: http.MethodPost, route: "/templates/check",
			body: body}, &issues)
		t.Logf("POST /templates/check with tag %q: issues %v, %v", tag, issues, err)
	}

	lp := liveRent(t, p, nil)
	ref := lp.ref
	again := liveProviderAgain(t)
	id, err := again.template(ctx, again.image)
	if err != nil {
		t.Fatal(err)
	}
	made, _ := p.template(ctx, p.image)
	t.Logf("template %s: the rent used %s, a fresh process finds %s", spec.Name, made, id)
	if id != made {
		t.Errorf("a fresh process took template %s, the rent %s, so takes refuses the live "+
			"listing", id, made)
	}

	manifest := liveShow(t, p, ref, "image manifest", "cat "+image.ManifestPath)
	var m struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal([]byte(manifest), &m); err != nil || m.Version != "2026.11.3" {
		t.Errorf("the pod's manifest gives version %q, %v, want 2026.11.3", m.Version, err)
	}
	liveShow(t, p, ref, "/root and /workspace", `ls -la /root /workspace 2>&1
echo; df -h / /root /workspace 2>&1
echo; grep -E ' /(root|workspace)? ' /proc/self/mounts 2>&1
echo; for d in /root /workspace; do mountpoint "$d" 2>&1; done`)
	out, _ := liveSh(t, p, ref, `test -f /root/.bashrc && echo bashrc
test -d /root/.ssh && echo ssh
mountpoint -q /root && echo root-mounted
echo hello > /workspace/w && cat /workspace/w`)
	t.Logf("/root and /workspace checks: %q", out)
	if strings.Contains(out, "root-mounted") {
		t.Error("/root is a mount point, so the volume hides the image's /root")
	}
	if !strings.Contains(out, "hello") {
		t.Error("/workspace takes no write")
	}
}

// liveProviderAgain is a second adapter, as another router process starts with no state
func liveProviderAgain(t *testing.T) *Provider {
	t.Helper()
	return liveProvider(t)
}

// liveProbeLabels are a bench probe's labels, the longest pod name the router makes
func liveProbeLabels() map[string]string {
	return map[string]string{
		"bugnet": "1", "bugnet_deployment": "dev", "bugnet_probe": "canary",
		"probe": "0b8d1c2e-3f4a-4b5c-8d6e-" + liveHex(6), "bugnet-live": liveHex(8),
	}
}

// LIUM2 and LIUM13: one key replays one pod, the record bills our start, and usage reads it
func TestLiveLIUM2Idempotency(t *testing.T) {
	p := liveProvider(t)
	liveGuard(t, p, liveRowMicros)
	lp := liveRent(t, p, liveProbeLabels())
	ref := lp.ref
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()

	r, ok := p.remembered(podKey(lp.key))
	if !ok {
		t.Fatal("the rent kept no rental for the key")
	}
	h := http.Header{}
	h.Set("X-Customer-Ref", r.customerRef)
	h.Set("Idempotency-Key", r.idemKey)
	var out struct {
		PodID string `json:"pod_id"`
		ID    string `json:"id"`
	}
	started := time.Now()
	hdr, err := p.api.do(ctx, call{
		method: http.MethodPost, route: "/executors/" + r.node + "/rent", body: r.body,
		header: h, rent: true,
	}, &out)
	t.Logf("the same rent again took %v: pod %q, Idempotent-Replayed %q, %v",
		time.Since(started).Round(time.Millisecond), cmp.Or(out.PodID, out.ID),
		hdr.Get("Idempotent-Replayed"), err)
	if err != nil || cmp.Or(out.PodID, out.ID) != ref.ID {
		t.Errorf("a replay of the rent gave pod %q, %v, want %s", cmp.Or(out.PodID, out.ID), err,
			ref.ID)
	}
	size, _ := p.SizeFor(liveFloor)
	adopted, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: lp.key, Size: size, Labels: lp.labels, Lifetime: liveLifetime,
	})
	if err != nil || adopted.ID != ref.ID {
		t.Errorf("a second Create of the key gave %s, %v, want %s", adopted.ID, err, ref.ID)
	}
	owned, err := p.ListOwned(ctx, lp.labels)
	if err != nil || len(owned) != 1 {
		t.Errorf("ListOwned of the row's labels gave %d pods, %v, want 1", len(owned), err)
	}
	for _, s := range owned {
		t.Logf("listed: %s labels %v state %s created %v expires %v", s.Ref.ID, s.Labels,
			s.State, s.CreatedAt, s.ExpiresAt)
	}
	liveSh(t, p, ref, "true")
	use, err := p.ReadUsage(ctx, lp.sent.Add(-time.Minute), time.Now().Add(time.Minute),
		[]provider.Ref{ref}, "")
	t.Logf("LIUM13 usage while the pod runs: %+v, %v", use, err)

	deleted := time.Now()
	liveDestroy(t, p, ref)
	var rec map[string]any
	for until := time.Now().Add(3 * time.Minute); time.Now().Before(until); {
		rec = nil
		_, err := p.api.do(ctx, call{method: http.MethodGet, route: podRoute(ref.ID, "/record")},
			&rec)
		if err == nil && rec["removed"] == true && rec["billed_seconds"] != nil {
			break
		}
		time.Sleep(10 * time.Second)
	}
	billed, _ := rec["billed_seconds"].(float64)
	total, _ := rec["total"].(float64)
	hourly, _ := strconv.ParseInt(ref.Meta[metaHourlyMicros], 10, 64)
	t.Logf("record: billed_seconds %v total %v close_reason %v created_at %v removed_at %v "+
		"segments %v", rec["billed_seconds"], rec["total"], rec["close_reason"],
		rec["created_at"], rec["removed_at"], rec["price_segments"])
	t.Logf("timing: wall %v from the rent to the delete, %v from RUNNING, %v from ready; "+
		"billed %.0f s, which at %d micros an hour is %.6f USD against total %.6f",
		deleted.Sub(lp.sent).Round(time.Second), deleted.Sub(lp.running).Round(time.Second),
		deleted.Sub(lp.ready).Round(time.Second), billed, hourly,
		billed*float64(hourly)/3600/1e6, total)
	t.Run("LIUM13", func(t *testing.T) {
		use, err := p.ReadUsage(ctx, lp.sent.Add(-time.Minute), time.Now().Add(time.Minute),
			[]provider.Ref{ref}, "")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("usage after the delete: %+v", use)
		if len(use.Sandboxes) != 1 || use.Sandboxes[0].ID != ref.ID {
			t.Fatalf("usage names %+v, want the pod alone", use.Sandboxes)
		}
		u := use.Sandboxes[0]
		if !use.Complete || u.Seconds != int64(billed+0.5) || u.CostMicros != micros(total) ||
			u.Deployment != "dev" {
			t.Errorf("usage line %+v complete %v, want %.0f s, %d micros, deployment dev",
				u, use.Complete, billed, micros(total))
		}
	})
}

// LIUM3: only our key is in the pod, Lium's sshd runs in our image, and the host key holds
func TestLiveLIUM3SSH(t *testing.T) {
	p := liveProvider(t)
	liveGuard(t, p, 2*liveRowMicros)
	lp := liveRent(t, p, nil)
	ref := lp.ref
	keys := liveShow(t, p, ref, "authorized_keys", `cut -d' ' -f1,3- /root/.ssh/authorized_keys`)
	ours := strings.Fields(p.podPublicKey(ref.Meta[metaPodName]))
	lines := strings.Split(strings.TrimSpace(keys), "\n")
	if len(lines) != 1 || lines[0] != ours[0]+" "+ours[2] {
		t.Errorf("authorized_keys holds %q, want only our key", lines)
	}
	full, _ := liveSh(t, p, ref, "cat /root/.ssh/authorized_keys")
	if strings.TrimSpace(full) != p.podPublicKey(ref.Meta[metaPodName]) {
		t.Error("authorized_keys does not hold exactly our key")
	}
	liveShow(t, p, ref, "sshd", `ls -ld /run/sshd 2>&1; echo
grep -v '^#' /etc/ssh/sshd_config | grep -v '^$'; echo; ls -la /etc/ssh/sshd_config.d 2>&1; echo
k='maxsessions|passwordauthentication|permitrootlogin|subsystem|allowtcpforwarding'
k="$k|x11forwarding|maxstartups|clientalive|pubkeyauth"
/usr/sbin/sshd -T 2>/dev/null | grep -iE "^($k)"
echo; ps -eo pid,ppid,user,args 2>&1 | head -40`)
	again := liveProviderAgain(t)
	out, code := liveSh(t, again, ref, "echo pinned")
	if strings.TrimSpace(out) != "pinned" || code != 0 {
		t.Errorf("a second process with the pinned host key gave %q exit %d", out, code)
	}
	liveDestroy(t, p, ref)

	// The same rent with skip_add_agent_ssh_key false shows what Lium adds of its own
	ctx, cancel := context.WithTimeout(t.Context(), liveReady)
	defer cancel()
	labels := liveLabels()
	t.Cleanup(func() { liveSweep(t, p, labels) })
	size, _ := p.SizeFor(liveFloor)
	o, err := p.orderFor(provider.CreateRequest{IdempotencyKey: liveHex(16), Size: size,
		Labels: labels, Lifetime: liveLifetime})
	if err != nil {
		t.Fatal(err)
	}
	nodes, _ := p.nodes.fresh()
	cands := p.candidates(o.size, nodes)
	if len(cands) == 0 {
		t.Fatal("no node to rent")
	}
	template, err := p.template(ctx, o.image)
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.newRental(o, cands[0], template)
	if err != nil {
		t.Fatal(err)
	}
	var b map[string]any
	_ = json.Unmarshal(r.body, &b)
	b["skip_add_agent_ssh_key"] = false
	r.body, _ = json.Marshal(b)
	id, err := p.sendRent(ctx, r)
	if err != nil {
		t.Fatalf("rent with skip_add_agent_ssh_key false: %v", err)
	}
	other := &livePod{ref: p.rented(o, r, id), labels: labels, sent: r.sentAt}
	other.rented = time.Now()
	t.Cleanup(func() { liveDestroy(t, p, other.ref) })
	other.wait(ctx, t, p)
	keys = liveShow(t, p, other.ref, "authorized_keys without the skip",
		`cut -d' ' -f1,3- /root/.ssh/authorized_keys`)
	t.Logf("without skip_add_agent_ssh_key, authorized_keys holds %d lines",
		len(strings.Split(strings.TrimSpace(keys), "\n")))
}

// LIUM5: the container's isolation, Docker, and the GPU in our plain Ubuntu image
func TestLiveLIUM5GPU(t *testing.T) {
	p := liveProvider(t)
	liveGuard(t, p, liveRowMicros)
	ref := liveRent(t, p, nil).ref
	liveShow(t, p, ref, "isolation", `echo uid_map; cat /proc/self/uid_map; echo gid_map
cat /proc/self/gid_map; echo; grep -iE 'sysbox|shiftfs|idmapped' /proc/self/mounts | head
echo; grep -E '^Cap(Eff|Bnd)' /proc/self/status; echo; uname -a; cat /proc/1/cmdline | tr '\0' ' '
echo; echo; docker info 2>&1 | head -5; echo; ls /var/run/docker.sock 2>&1; echo
cat /proc/self/cgroup; nproc; free -m; df -h / | tail -1`)
	out := liveShow(t, p, ref, "GPU", `ls -la /dev/nvidia* 2>&1; echo
command -v nvidia-smi; nvidia-smi 2>&1; echo
nvidia-smi --query-gpu=name,driver_version,memory.total,uuid --format=csv 2>&1; echo
cat /proc/driver/nvidia/version 2>&1; echo
ls /usr/lib/x86_64-linux-gnu 2>/dev/null | grep -E '^lib(cuda|nvidia-ml)' ; echo
env | grep -E '^NVIDIA_' | cut -d= -f1`)
	if !strings.Contains(out, "RTX 3090") {
		t.Error("nvidia-smi names no RTX 3090 in the pod")
	}
}

// LIUM4: the contract with the GPU floor, the stream rows, SSH sessions and the cancel's kill
func TestLiveLIUM4Contract(t *testing.T) {
	p := liveProvider(t)
	liveGuard(t, p, 2*liveRowMicros)
	contract.Run(t, func(*testing.T) provider.Provider { return p },
		contract.Floor(liveFloor), contract.ReadyTimeout(liveReady))
	if t.Failed() {
		return
	}
	lp := liveRent(t, p, nil)
	ref := lp.ref
	dir := "/tmp/bugnet-streams"
	t.Run("Streams", func(t *testing.T) { contract.Streams(t, p, ref, dir) })
	livetest.RoundTrips(t, p, ref, dir)
	t.Run("FilesystemTypes", func(t *testing.T) {
		livetest.FilesystemTypes(t, p, ref, "/root")
	})
	t.Run("MaxSessions", func(t *testing.T) { liveMaxSessions(t, p, ref) })
	t.Run("CancelKills", func(t *testing.T) { liveCancelKills(t, p, ref) })
}

// liveMaxSessions opens sessions on one connection until the pod's sshd refuses one
func liveMaxSessions(t *testing.T, p *Provider, ref provider.Ref) {
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	addr := ref.Meta[metaSSHHost] + ":" + ref.Meta[metaSSHPort]
	pin, err := parseHostKey(ref.Meta[provider.MetaHostKey])
	if err != nil {
		t.Fatal(err)
	}
	client, err := p.dialPod(ctx, ref.Meta[metaPodName], addr, &hostCheck{pin: pin})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	n := 0
	for ; n < 20; n++ {
		s, err := client.NewSession()
		if err != nil {
			t.Logf("session %d refused: %v", n+1, err)
			break
		}
		defer func() { _ = s.Close() }()
	}
	t.Logf("the pod's sshd took %d sessions on one connection", n)
	if n < p.ssh.perConn {
		t.Errorf("the pod's sshd took %d sessions, under the pool's %d", n, p.ssh.perConn)
	}
}

// liveCancelKills cancels a running command and checks its process group is gone
func liveCancelKills(t *testing.T, p *Provider, ref provider.Ref) {
	ctx, cancel := context.WithCancel(t.Context())
	s, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"sh", "-c",
		"sleep 317 & sleep 318; echo never"}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	out, _ := liveSh(t, p, ref, "pgrep -fc 'sleep 31[78]'; true")
	t.Logf("before the cancel: %s sleeps", strings.TrimSpace(out))
	started := time.Now()
	cancel()
	_, _ = io.Copy(io.Discard, s)
	code, werr := s.Wait()
	_ = s.Close()
	t.Logf("the cancelled stream ended in %v with %d, %v", time.Since(started), code, werr)
	for until := time.Now().Add(30 * time.Second); ; {
		out, _ = liveSh(t, p, ref, "pgrep -fc 'sleep 31[78]'; ls /tmp/bugnet/exec; true")
		if strings.HasPrefix(strings.TrimSpace(out), "0") {
			t.Logf("after the cancel, %v later: %q", time.Since(started).Round(time.Second), out)
			return
		}
		if time.Now().After(until) {
			t.Fatalf("30 s after the cancel the group still runs: %q", out)
		}
		time.Sleep(2 * time.Second)
	}
}

// liveRunner puts the lane's bugnet-run on the pod when the baked one differs, as boot does
func liveRunner(t *testing.T, p *Provider, ref provider.Ref) string {
	t.Helper()
	data, err := os.ReadFile(livetest.Settings(t).RunnerLinuxAMD64)
	if err != nil {
		t.Fatalf("the lane's runner: %v", err)
	}
	sum := sha256.Sum256(data)
	baked, _ := liveSh(t, p, ref, "sha256sum "+image.RunnerPath+" 2>/dev/null | cut -c1-64")
	t.Logf("baked runner %s, the lane's %x", strings.TrimSpace(baked), sum)
	if strings.TrimSpace(baked) == hex.EncodeToString(sum[:]) {
		return image.RunnerPath
	}
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	if err := p.WriteFile(ctx, ref, image.UploadedRunnerPath, data, 0o755); err != nil {
		t.Fatalf("upload the runner: %v", err)
	}
	return image.UploadedRunnerPath
}

// liveHardenSpec is the harden spec the saga writes for lium from config/dev.toml
func liveHardenSpec(t *testing.T) []byte {
	t.Helper()
	cfg := livetest.Settings(t)
	rule, ok := cfg.Hardening.Rules["lium"]
	if !ok || !rule.NoGateway || !rule.Required {
		t.Fatalf("lium's hardening rule is %+v, want required with no_gateway", rule)
	}
	pools, err := config.LoadPoolList(cfg.Hardening.PoolList)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := json.Marshal(map[string]any{
		"v": 1, "mail_ports": cfg.Hardening.MailPorts, "deny_ranges": cfg.Hardening.DenyRanges,
		"pools": append([]string{}, pools...), "allow": append([]string{}, rule.Allow...),
		"no_gateway": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

// liveProbeScript prints open or closed for each target, the gateway's ports last
const liveProbeScript = `gw=$(python3 -c '
import socket, struct
for l in list(open("/proc/net/route"))[1:]:
    f = l.split()
    if f[1] == "00000000":
        print(socket.inet_ntoa(struct.pack("<L", int(f[2], 16))))
        break
')
for t in "aspmx.l.google.com 25" "169.254.169.254 80" "10.0.0.1 80" \
	"fd00:ec2::254 80" "$gw 8080" "$gw 22"; do
	set -- $t
	timeout 5 bash -c "exec 3<>/dev/tcp/$1/$2" 2> /dev/null && r=open || r=closed
	echo "$1:$2 $r"
done`

// LIUM6: harden loads twice in a Lium pod, blocks the host and the private ranges, and leaves
// DNS, the public internet, exec and SFTP working
func TestLiveLIUM6Harden(t *testing.T) {
	p := liveProvider(t)
	liveGuard(t, p, liveRowMicros)
	ref := liveRent(t, p, nil).ref
	liveShow(t, p, ref, "before harden", liveProbeScript+`
ip route 2>&1; cat /etc/resolv.conf`)
	runner := liveRunner(t, p, ref)
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	if err := p.WriteFile(ctx, ref, "/tmp/bugnet/harden-spec.json", liveHardenSpec(t),
		0o600); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		out, code := liveSh(t, p, ref, runner+
			" harden --spec /tmp/bugnet/harden-spec.json 2>&1")
		t.Logf("harden %d (exit %d): %s", i+1, code, strings.TrimSpace(out))
		lines := strings.Split(strings.TrimSpace(out), "\n")
		var ans struct{ Firewall, Error string }
		if err := json.Unmarshal([]byte(lines[len(lines)-1]), &ans); err != nil || code != 0 ||
			ans.Error != "" || ans.Firewall == "" {
			t.Fatalf("harden %d failed: exit %d, %+v, %v", i+1, code, ans, err)
		}
	}
	probes := liveShow(t, p, ref, "after harden", liveProbeScript)
	for _, line := range strings.Split(strings.TrimSpace(probes), "\n") {
		if strings.HasSuffix(line, " open") {
			t.Errorf("after harden %s", line)
		}
	}
	dns := liveShow(t, p, ref, "DNS and the public internet",
		`getent hosts example.com || echo no-dns
curl -sS -o /dev/null -m 15 -w 'https %{http_code}\n' https://example.com 2>&1`)
	if strings.Contains(dns, "no-dns") || !strings.Contains(dns, "https 200") {
		t.Errorf("after harden DNS or HTTPS fails: %q", dns)
	}
	liveShow(t, p, ref, "rules",
		"nft list ruleset 2>&1 | head -80; head -5 /etc/bugnet/firewall.rules 2>&1")
	if err := p.WriteFile(ctx, ref, "/root/bugnet-n1", []byte("n1"), 0o644); err != nil {
		t.Fatalf("SFTP after harden: %v", err)
	}
	got, _, err := p.ReadFile(ctx, ref, "/root/bugnet-n1", 0, 16)
	if err != nil || string(got) != "n1" {
		t.Errorf("SFTP read after harden gave %q, %v", got, err)
	}
	again := liveProviderAgain(t)
	if out, code := liveSh(t, again, ref, "echo in"); strings.TrimSpace(out) != "in" ||
		code != 0 {
		t.Errorf("a new SSH connection after harden gave %q exit %d", out, code)
	}
}

// LIUM10: no Bugnet secret and no gate key in a Lium pod, and what Lium puts in its env
func TestLiveLIUM10NoSecrets(t *testing.T) {
	p := liveProvider(t)
	liveGuard(t, p, liveRowMicros)
	needles, err := leakscan.FromEnv(os.Environ())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(needles, func(n leakscan.Needle) bool {
		return n.Name == "LIUM_API_KEY"
	}) {
		t.Fatal("the needles hold no LIUM_API_KEY")
	}
	ref := liveRent(t, p, nil).ref
	if _, code := liveSh(t, p, ref, "test ! -e /etc/bugnet/gate.key"); code != 0 {
		t.Error("/etc/bugnet/gate.key exists in a Lium pod")
	}
	script := strings.ReplaceAll(leakscan.Collect, "\r", "")
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	if err := p.WriteFile(ctx, ref, "/tmp/leak-collect.sh", []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	out, code := liveSh(t, p, ref, "sh /tmp/leak-collect.sh 2>&1 | tail -5")
	t.Logf("collect (exit %d): %s", code, out)
	r, err := p.OpenReader(ctx, ref, leakscan.ArchivePath, 0)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := io.ReadAll(io.LimitReader(r, leakscan.MaxArchive))
	_ = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	res, err := leakscan.ScanFull(bytes.NewReader(archive), needles, leakscan.For("lium", false))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("archive %d bytes; every variable in a process environment: %v", len(archive),
		res.Vars)
	t.Logf("variables named like secrets: %v; own files: %d", res.Names, len(res.Own))
	for _, f := range res.Findings {
		t.Errorf("finding: %s", f)
	}
	for _, o := range res.Own {
		if o.Name == "gate key" {
			t.Error("the archive holds a gate key")
		}
	}
}

// LIUM8: whether schedule-removal replaces termination_hours, its precision, and kill reasons
func TestLiveLIUM8Removal(t *testing.T) {
	p := liveProvider(t)
	liveGuard(t, p, 600_000)
	long := liveRent(t, p, nil)
	created := liveCreated(t, p, long.ref)
	t.Logf("long pod %s created_at %v, RUNNING at %v", long.ref.ID, created, long.running.UTC())
	liveDetail(t, p, long.ref.ID)
	waitUntil(t, created.Add(5*time.Minute))
	removeAt := created.Add(65 * time.Minute).Truncate(time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	if err := p.SetLifetime(ctx, long.ref, removeAt); err != nil {
		t.Fatalf("SetLifetime to %v: %v", removeAt, err)
	}
	cancel()
	d := liveDetail(t, p, long.ref.ID)
	t.Logf("after schedule-removal to %v: termination_hours %v removal_scheduled_at %v", removeAt,
		d["termination_hours"], d["removal_scheduled_at"])

	t.Run("short", func(t *testing.T) { liveShortRemoval(t, p) })

	waitUntil(t, created.Add(62*time.Minute))
	st := liveInspect(t, p, long.ref)
	t.Logf("at 62 minutes the long pod is %s", st.State)
	twoTimers := st.State == provider.StateGone
	gone := liveWaitGone(t, p, long.ref, created.Add(75*time.Minute))
	t.Logf("result: the long pod went at %v after created_at, removal asked at 65 minutes and "+
		"termination_hours 1; two timers %v", gone.Sub(created).Round(time.Second), twoTimers)
	liveEvents(t, p, long.ref.ID)
}

// liveShortRemoval schedules a pod's removal 3 minutes out and times it
func liveShortRemoval(t *testing.T, p *Provider) {
	short := liveRent(t, p, nil)
	at := time.Now().Add(3 * time.Minute).Truncate(time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	if err := p.SetLifetime(ctx, short.ref, at); err != nil {
		t.Fatalf("SetLifetime: %v", err)
	}
	st := liveInspect(t, p, short.ref)
	if !st.ExpiresAt.Equal(at) {
		t.Errorf("Inspect gives ExpiresAt %v after SetLifetime to %v", st.ExpiresAt, at)
	}
	liveDetail(t, p, short.ref.ID)
	gone := liveWaitGone(t, p, short.ref, at.Add(10*time.Minute))
	t.Logf("result: removal asked for %v, the pod went %v after it", at.UTC(),
		gone.Sub(at).Round(time.Second))
	st = liveInspect(t, p, short.ref)
	if st.State != provider.StateGone {
		t.Errorf("Inspect of a removed pod gives %s, want gone", st.State)
	}
	liveEvents(t, p, short.ref.ID)
}

// liveCreated is the pod's created_at as Lium writes it
func liveCreated(t *testing.T, p *Provider, ref provider.Ref) time.Time {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	d, err := p.getPod(ctx, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	c, ok := parseTime(d.CreatedAt)
	if !ok {
		t.Fatal("the pod names no created_at")
	}
	return c
}

// waitUntil sleeps until at, failing when the test's deadline comes first
func waitUntil(t *testing.T, at time.Time) {
	t.Helper()
	if dl, ok := t.Deadline(); ok && dl.Before(at) {
		t.Fatalf("the test's deadline %v comes before %v", dl, at)
	}
	time.Sleep(time.Until(at))
}

// liveInspect calls Inspect and logs what it gives
func liveInspect(t *testing.T, p *Provider, ref provider.Ref) provider.Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	st, err := p.Inspect(ctx, ref)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	t.Logf("Inspect %s: %s %q kill %q expires %v", ref.ID, st.State, st.Reason, st.KillReason,
		st.ExpiresAt)
	return st
}

// liveWaitGone reads the pod every 15 s until Inspect gives gone, deleting it at until
func liveWaitGone(t *testing.T, p *Provider, ref provider.Ref, until time.Time) time.Time {
	t.Helper()
	for {
		ctx, cancel := context.WithTimeout(t.Context(), liveOp)
		st, err := p.Inspect(ctx, ref)
		cancel()
		if err == nil && st.State == provider.StateGone {
			return time.Now()
		}
		if time.Now().After(until) {
			t.Errorf("pod %s is still %s at %v, deleting it", ref.ID, st.State, until)
			liveDestroy(t, p, ref)
			return time.Now()
		}
		time.Sleep(15 * time.Second)
	}
}

// liveEvents logs the pod's events, its newest kill reason last
func liveEvents(t *testing.T, p *Provider, id string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	var events []map[string]any
	if _, err := p.api.do(ctx, call{method: http.MethodGet, route: podRoute(id, "/events")},
		&events); err != nil {
		t.Logf("events of %s: %v", id, err)
	}
	for _, e := range events {
		t.Logf("event %v %v/%v %v->%v reason %v detail %v error %v", e["created_at"],
			e["event_type"], e["sub_event_type"], e["from_status"], e["to_status"], e["reason"],
			e["detail"], e["error"])
	}
	k, err := p.killReason(ctx, id)
	t.Logf("kill reason of %s: %q, %v", id, k, err)
	return k
}

// liveGuestStops are CP G1's acts, each run on a pod of its own
var liveGuestStops = []struct{ name, script string }{
	{"poweroff", `(poweroff 2>&1 || true); sleep 3; poweroff -f 2>&1; echo "poweroff -f $?"`},
	{"kill-1", `kill -9 1 2>&1; echo "kill $?"; sleep 2; ps -o pid,args -p 1 2>&1`},
	{"sshd", `cat > /tmp/g1.sh <<'EOF'
sleep 2
for p in $(ps -eo pid,args | awk '/[s]shd|[l]ium-ssh-bootstrap/ {print $1}'); do
	kill -9 "$p"
done
EOF
nohup sh /tmp/g1.sh > /dev/null 2>&1 &
echo started`},
	{"nft22", `cat > /tmp/g1.sh <<'EOF'
sleep 2
nft add table inet g1
nft add chain inet g1 in '{ type filter hook input priority -10; }'
nft add rule inet g1 in tcp dport 22 drop
EOF
nohup sh /tmp/g1.sh > /tmp/g1.log 2>&1 &
echo started`},
}

// LIUM16: what each guest act does to a pod as Lium reports it and as reconcile classes it
func TestLiveLIUM16GuestStops(t *testing.T) {
	p := liveProvider(t)
	only := os.Getenv("LIUM16_ONLY")
	for _, g := range liveGuestStops {
		if only != "" && only != g.name {
			continue
		}
		t.Run(g.name, func(t *testing.T) {
			liveGuard(t, p, liveRowMicros)
			liveGuestStop(t, p, g.script)
		})
	}
}

// liveGuestStop runs one act on a fresh pod and watches it up to 15 minutes
func liveGuestStop(t *testing.T, p *Provider, script string) {
	ref := liveRent(t, p, nil).ref
	liveShow(t, p, ref, "processes before", "ps -eo pid,ppid,args | head -30")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	out, code, err := liveExec(ctx, p, ref, script)
	cancel()
	t.Logf("act: exit %d, %v, output %q", code, err, out)
	abuse := livetest.Settings(t).Abuse
	acted := time.Now()
	for {
		time.Sleep(20 * time.Second)
		since := time.Since(acted).Round(time.Second)
		st := liveInspect(t, p, ref)
		d := liveDetail(t, p, ref.ID)
		v := reconcile.Classify("lium", st, reconcile.Live{State: "ready",
			Deadline: time.Now().Add(20 * time.Minute)}, time.Now(), abuse)
		ectx, ecancel := context.WithTimeout(t.Context(), 20*time.Second)
		eout, ecode, eerr := liveExec(ectx, p, ref, "echo up")
		ecancel()
		t.Logf("%v after: status %v, Inspect %s %q kill %q, class %q, exec %q %d %v", since,
			d["status"], st.State, st.Reason, st.KillReason, v.Class, strings.TrimSpace(eout),
			ecode, eerr)
		switch {
		case st.State == provider.StateGone || st.State == provider.StateFailed ||
			st.State == provider.StateStopped:
			liveEvents(t, p, ref.ID)
			return
		case eerr == nil && since >= 3*time.Minute && d["ssh_unreachable_since"] == nil:
			t.Logf("result: the pod still runs and answers %v after the act", since)
			liveEvents(t, p, ref.ID)
			return
		case since >= 15*time.Minute:
			t.Logf("result: after 15 minutes the pod is %s with exec %v", st.State, eerr)
			liveEvents(t, p, ref.ID)
			return
		}
	}
}

// LIUM11's reads: the account check, the limits and the answers of routes the key may not use
func TestLiveLIUM11Account(t *testing.T) {
	p := liveProvider(t)
	ctx, cancel := context.WithTimeout(t.Context(), liveOp)
	defer cancel()
	acc, err := p.CheckAccount(ctx)
	t.Logf("CheckAccount: block %q detail %q, %v", acc.Block, acc.Detail, err)
	if err != nil || acc.Block != provider.BlockNone {
		t.Errorf("the dev account is blocked or unread: %+v, %v", acc, err)
	}
	lim, err := p.ReadLimits(ctx)
	t.Logf("ReadLimits: %+v, %v", lim, err)
	if err != nil {
		t.Error(err)
	}
	t.Logf("headroom with no pods: %s", usd(p.headroom(nil)))
	var me map[string]any
	if _, err := p.api.do(ctx, call{method: http.MethodGet, route: "/users/me"}, &me); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"id", "balance", "balance_warning", "estimated_balance",
		"burn_per_hour", "billing_granularity", "api_key_scopes", "email_verified"} {
		t.Logf("/users/me %s: %T", k, me[k])
	}
	id, _ := me["id"].(string)
	if id == "" {
		t.Error("GET /users/me names no id, so the template owner check is skipped")
	}
	for _, route := range []string{"/keys", "/users/me/auto-top-up",
		"/users/me/low-balance-threshold", "/users/me/low-balance-webhook"} {
		_, err := p.api.do(ctx, call{method: http.MethodGet, route: route}, nil)
		e, _ := provider.AsError(err)
		var code provider.Code
		if e != nil {
			code = e.Code
		}
		t.Logf("GET %s: lium code %q, provider code %q, %v", route, codeOf(err), code, err)
	}
}
