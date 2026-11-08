package worker

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/transfer"
)

const (
	// applyTimeout is DP 4.6's, under River's 3-minute rescue
	applyTimeout = 90 * time.Second
	// applySnooze is how long a job waits while another run holds the placement's apply lock
	applySnooze = 2 * time.Second
	// egressBudget bounds both egress apply attempts of a run, leaving time to record the result
	egressBudget = 75 * time.Second
	// forwardListen is where every sandbox's forwarder listens, never 127.0.0.1 or IPv6 loopback
	forwardListen = "127.77.0.1:443"
	egressPort    = 443
	// tokenMissing is egress apply's answer when the sandbox lost its tunnel token
	tokenMissing = "token_missing"
	// reasonUnavailable fails secrets on a provider or router that cannot serve them
	reasonUnavailable = "FEATURE_UNAVAILABLE"
	reasonApplyFailed = "APPLY_FAILED"
	// reasonSourceUnknown fails secrets when on_unknown is refuse and no source is stored
	reasonSourceUnknown = "SOURCE_UNKNOWN"
)

// Egress is where apply_secrets points every forwarder
type Egress struct {
	// Host is names.egress_host, apply_secrets failing every secret while it is empty
	Host string
	// IPs are secrets.egress_ips, pinned in every forwarder
	IPs []netip.Addr
	// Listen and Port replace the forwarder's address and the proxy's port, set only by a
	// localrun build
	Listen string
	Port   int
}

// egressSpec is bugnet-run's egress Spec, its fields in the same order and with the same names
type egressSpec struct {
	V int `json:"v"`
	// Token is sent only when this run minted it
	Token          string      `json:"token,omitempty"`
	Endpoint       string      `json:"endpoint"`
	IPs            []string    `json:"ips"`
	Port           int         `json:"port"`
	Listen         string      `json:"listen"`
	Hosts          []string    `json:"hosts"`
	Env            []egressEnv `json:"env"`
	Home           string      `json:"home"`
	GitPlaceholder string      `json:"git_placeholder"`
	NPMPlaceholder string      `json:"npm_placeholder"`
	// X402Hosts are the hosts of x402 services, each also in Hosts, omitted when there are none
	X402Hosts []string `json:"x402_hosts,omitempty"`
	// Source pins the forwarder's family and bind address, omitted when none is stored
	Source *secrets.SpecSource `json:"source,omitempty"`
}

// egressEnv is one NAME=placeholder line of the sandbox's secrets.env
type egressEnv struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type applySecretsWorker struct {
	river.WorkerDefaults[jobargs.ApplySecrets]
	w *Worker
}

func (*applySecretsWorker) Timeout(*river.Job[jobargs.ApplySecrets]) time.Duration {
	return applyTimeout
}

// Work runs one apply under the placement's apply lock, snoozing while another run holds it
func (a *applySecretsWorker) Work(ctx context.Context, job *river.Job[jobargs.ApplySecrets]) error {
	id := job.Args.PlacementID
	held, err := a.w.store.TryLock(ctx, "apply:"+id.String(), func() error {
		return a.w.applySecrets(ctx, id)
	})
	if err != nil {
		return err
	}
	if !held {
		return river.JobSnooze(applySnooze)
	}
	return nil
}

// applySecrets writes the placement's full current set, so any number of queued jobs converge
func (w *Worker) applySecrets(ctx context.Context, id uuid.UUID) error {
	p, err := w.store.PlacementByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) || (err == nil && p.State != store.PlacementReady) {
		return nil
	}
	if err != nil {
		return err
	}
	pending, lag, err := w.store.SecretsPending(ctx, id)
	if err != nil || !pending {
		return err
	}
	all, err := w.store.Secrets(ctx, id)
	if err != nil {
		return err
	}
	loaded := slices.DeleteFunc(all, func(s store.Secret) bool { return s.FetchOnly })
	x402, err := w.store.LoadX402ForApply(ctx, id)
	if err != nil {
		return err
	}
	set := applySet{secrets: loaded, x402: x402}
	prov, ok := w.registry.Get(p.Provider)
	if !ok || p.ProviderRef == nil || !w.registry.Capabilities(prov).Secrets ||
		w.d.Egress.Host == "" || len(w.d.Egress.IPs) == 0 || len(w.keys.Roots()) == 0 {
		return w.finishApply(ctx, p, lag, set, store.SecretFailed, reasonUnavailable)
	}
	has, err := w.store.HasEgressToken(ctx, id)
	if err != nil {
		return err
	}
	// Nothing was ever written into this sandbox, so there is nothing to take back
	if len(loaded) == 0 && len(x402) == 0 && !has {
		return w.finishApply(ctx, p, lag, applySet{}, store.SecretActive, "")
	}
	p, err = w.readSource(ctx, p)
	if err != nil {
		return err
	}
	// The lag is read after the source check, so a source change in this run is applied by it
	_, lag, err = w.store.SecretsPending(ctx, id)
	if err != nil {
		return err
	}
	if w.d.Sources != nil && w.d.Sources.Unknown(p) {
		w.log.Warn("secrets not applied, no egress source is known", "placement", id)
		return w.finishApply(ctx, p, lag, set, store.SecretFailed, reasonSourceUnknown)
	}
	err = w.applyEgress(ctx, p, prov, set)
	var re *saga.RunnerError
	switch {
	case err == nil:
		w.log.Info("secrets applied", "placement", id, "secrets", len(loaded),
			"x402", len(x402))
		set.wrote = true
		return w.finishApply(ctx, p, lag, set, store.SecretActive, "")
	case errors.As(err, &re):
		w.log.Warn("secrets apply refused", "placement", id, "err", err)
		reason := cmp.Or(strings.ToUpper(re.Value), reasonApplyFailed)
		return w.finishApply(ctx, p, lag, set, store.SecretFailed, reason)
	}
	return err
}

// readSource is step 2b, a read of the sandbox's egress source unless a read is fresh
func (w *Worker) readSource(ctx context.Context, p store.Placement) (store.Placement, error) {
	if w.d.Sources == nil || !w.d.Sources.Active(p) || w.d.Sources.Fresh(p) {
		return p, nil
	}
	src, err := w.d.Sources.ReadInRun(ctx, p)
	p.EgressSource = src
	return p, err
}

// applyEgress writes the spec, once more with a fresh token after token_missing, in one budget
func (w *Worker) applyEgress(
	ctx context.Context, p store.Placement, prov provider.Provider, set applySet,
) error {
	ctx, cancel := context.WithTimeout(ctx, egressBudget)
	defer cancel()
	err := w.writeEgress(ctx, p, prov, set)
	var re *saga.RunnerError
	if !errors.As(err, &re) || re.Value != tokenMissing {
		return err
	}
	// The sandbox lost its token, so a fresh one goes with the next write
	if err := w.store.ClearEgressToken(ctx, p.ID); err != nil {
		return err
	}
	return w.writeEgress(ctx, p, prov, set)
}

// writeEgress mints a token when the placement has none and runs egress apply with the spec
func (w *Worker) writeEgress(
	ctx context.Context, p store.Placement, prov provider.Provider, set applySet,
) error {
	spec, err := w.egressSpec(p, prov, set)
	if err != nil {
		return err
	}
	tok := dpkeys.NewTunnelToken()
	sum := sha256.Sum256([]byte(tok))
	minted, err := w.store.MintEgressToken(ctx, p.ID, sum[:])
	if err != nil {
		return err
	}
	if minted {
		spec.Token = tok
	}
	err = w.saga.ApplyEgress(ctx, p, spec)
	if err != nil && minted {
		// The sandbox may lack the new token, so the next run mints another
		if cerr := w.store.ClearEgressToken(context.WithoutCancel(ctx), p.ID); cerr != nil {
			return errors.Join(err, cerr)
		}
	}
	return err
}

// applySet is what one run loaded, and what it wrote when it succeeded
type applySet struct {
	secrets []store.Secret
	x402    []store.X402Loaded
	// wrote is true once egress apply took the spec, so applied_hosts follows it
	wrote bool
}

// x402Hosts are the hosts of the loaded x402 rows, sorted and de-duplicated
func (a applySet) x402Hosts() []string {
	var out []string
	for _, r := range a.x402 {
		out = append(out, r.Host)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// hosts are the secret hosts and x402 hosts, sorted and de-duplicated
func (a applySet) hosts() []string {
	out := append([]string{}, a.x402Hosts()...)
	for _, s := range a.secrets {
		out = append(out, s.Hosts...)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// egressSpec is DP 10.3 step 3's spec for the loaded rows, without a token
func (w *Worker) egressSpec(
	p store.Placement, prov provider.Provider, set applySet,
) (egressSpec, error) {
	caps := w.registry.Capabilities(prov)
	spec := egressSpec{
		V: 1, Endpoint: w.d.Egress.Host, Port: cmp.Or(w.d.Egress.Port, egressPort),
		Listen: cmp.Or(w.d.Egress.Listen, forwardListen),
		Env:    []egressEnv{},
		Home:   transfer.Home(prov, *p.ProviderRef, caps),
	}
	for _, ip := range w.d.Egress.IPs {
		spec.IPs = append(spec.IPs, ip.String())
	}
	keyID := cmp.Or(p.KeyID, w.keys.Current().ID)
	for _, s := range set.secrets {
		ph, err := w.keys.Placeholder(keyID, p.ID, s.Name)
		if err != nil {
			return egressSpec{}, fmt.Errorf("placeholder: %w", err)
		}
		spec.Env = append(spec.Env, egressEnv{Name: s.Name, Value: ph})
		preset, ok := secrets.PresetOf(s.Name)
		if !ok || !slices.Contains(s.Hosts, preset.Hosts[0]) {
			continue
		}
		switch {
		case preset.Extra == secrets.ExtraGit && spec.GitPlaceholder == "":
			spec.GitPlaceholder = ph
		case preset.Extra == secrets.ExtraNPM && spec.NPMPlaceholder == "":
			spec.NPMPlaceholder = ph
		}
	}
	spec.Hosts = set.hosts()
	spec.X402Hosts = set.x402Hosts()
	if w.d.Sources != nil {
		spec.Source = w.d.Sources.Spec(p)
	}
	return spec, nil
}

// finishApply is step 5 and X402 step 7, marking only the rows still as loaded and clearing lag
func (w *Worker) finishApply(
	ctx context.Context, p store.Placement, lag int64, set applySet, state, reason string,
) error {
	ready := true
	err := w.store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := w.store.Tx(tx)
		cur, err := ts.PlacementForUpdate(ctx, p.ID)
		if err != nil {
			return err
		}
		if cur.State != store.PlacementReady {
			ready = false
			return nil
		}
		if err := ts.MarkX402Applied(ctx, p.ID, set.x402, x402State(state), reason); err != nil {
			return err
		}
		if _, err := ts.MarkSecrets(ctx, p.ID, set.secrets, state, reason); err != nil {
			return err
		}
		if set.wrote {
			if err := ts.SetAppliedHosts(ctx, p.ID, set.hosts()); err != nil {
				return err
			}
		}
		return ts.FinishSecretsApply(ctx, p.ID, lag)
	})
	if err != nil || !ready {
		return err
	}
	if w.events != nil {
		w.events.Publish(ctx, &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_SecretsChanged{
			SecretsChanged: &routerv1.SecretsChanged{PlacementId: p.ID.String()},
		}})
	}
	return nil
}

// x402State maps a secret state to the x402 row state of the same outcome
func x402State(secret string) string {
	if secret == store.SecretActive {
		return store.X402Active
	}
	return store.X402Failed
}
