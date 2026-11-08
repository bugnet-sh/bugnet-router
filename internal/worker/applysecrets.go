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
	// forwardListen is where the forwarder listens in every sandbox, never 127.0.0.1 or ::1
	forwardListen = "127.77.0.1:443"
	egressPort    = 443
	// tokenMissing is egress apply's answer when the sandbox lost its tunnel token
	tokenMissing = "token_missing"
	// reasonUnavailable fails secrets on a provider or router that cannot serve them
	reasonUnavailable = "FEATURE_UNAVAILABLE"
	reasonApplyFailed = "APPLY_FAILED"
)

// Egress is where apply_secrets points every forwarder
type Egress struct {
	// Host is names.egress_host, apply_secrets failing every secret while it is empty
	Host string
	// IPs are secrets.egress_ips, pinned in every forwarder
	IPs []netip.Addr
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
	pending, err := w.store.SecretsPending(ctx, id)
	if err != nil || !pending {
		return err
	}
	all, err := w.store.Secrets(ctx, id)
	if err != nil {
		return err
	}
	loaded := slices.DeleteFunc(all, func(s store.Secret) bool { return s.FetchOnly })
	prov, ok := w.registry.Get(p.Provider)
	if !ok || p.ProviderRef == nil || !w.registry.Capabilities(prov).Secrets ||
		w.d.Egress.Host == "" || len(w.d.Egress.IPs) == 0 || len(w.keys.Roots()) == 0 {
		return w.finishApply(ctx, p, loaded, store.SecretFailed, reasonUnavailable)
	}
	has, err := w.store.HasEgressToken(ctx, id)
	if err != nil {
		return err
	}
	// Nothing was ever written into this sandbox, so there is nothing to take back
	if len(loaded) == 0 && !has {
		return w.finishApply(ctx, p, nil, store.SecretActive, "")
	}
	err = w.writeEgress(ctx, p, prov, loaded)
	var re *saga.RunnerError
	if errors.As(err, &re) && re.Value == tokenMissing {
		// The sandbox lost its token, so a fresh one goes with the next write
		if err := w.store.ClearEgressToken(ctx, id); err != nil {
			return err
		}
		err = w.writeEgress(ctx, p, prov, loaded)
	}
	switch {
	case err == nil:
		w.log.Info("secrets applied", "placement", id, "secrets", len(loaded))
		return w.finishApply(ctx, p, loaded, store.SecretActive, "")
	case errors.As(err, &re):
		w.log.Warn("secrets apply refused", "placement", id, "err", err)
		reason := cmp.Or(strings.ToUpper(re.Value), reasonApplyFailed)
		return w.finishApply(ctx, p, loaded, store.SecretFailed, reason)
	}
	return err
}

// writeEgress mints a token when the placement has none and runs egress apply with the spec
func (w *Worker) writeEgress(
	ctx context.Context, p store.Placement, prov provider.Provider, loaded []store.Secret,
) error {
	spec, err := w.egressSpec(p, prov, loaded)
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

// egressSpec is DP 10.3 step 3's spec for the loaded secrets, without a token
func (w *Worker) egressSpec(
	p store.Placement, prov provider.Provider, loaded []store.Secret,
) (egressSpec, error) {
	caps := w.registry.Capabilities(prov)
	spec := egressSpec{
		V: 1, Endpoint: w.d.Egress.Host, Port: egressPort, Listen: forwardListen,
		Hosts: []string{}, Env: []egressEnv{},
		Home: transfer.Home(prov, *p.ProviderRef, caps),
	}
	for _, ip := range w.d.Egress.IPs {
		spec.IPs = append(spec.IPs, ip.String())
	}
	keyID := cmp.Or(p.KeyID, w.keys.Current().ID)
	for _, s := range loaded {
		ph, err := w.keys.Placeholder(keyID, p.ID, s.Name)
		if err != nil {
			return egressSpec{}, fmt.Errorf("placeholder: %w", err)
		}
		spec.Env = append(spec.Env, egressEnv{Name: s.Name, Value: ph})
		spec.Hosts = append(spec.Hosts, s.Hosts...)
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
	slices.Sort(spec.Hosts)
	spec.Hosts = slices.Compact(spec.Hosts)
	return spec, nil
}

// finishApply is step 5, marking only the loaded secrets still as loaded and moving the version
func (w *Worker) finishApply(
	ctx context.Context, p store.Placement, loaded []store.Secret, state, reason string,
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
		if _, err := ts.MarkSecrets(ctx, p.ID, loaded, state, reason); err != nil {
			return err
		}
		return ts.FinishSecretsApply(ctx, p.ID)
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
