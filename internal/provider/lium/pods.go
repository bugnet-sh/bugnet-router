package lium

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/gpustatus"
)

// podPollEvery spaces the pod reads of a wait for RUNNING
const podPollEvery = 2 * time.Second

// Pod statuses, every value of PodStatus in Lium's OpenAPI
const (
	statusPending        = "PENDING"
	statusStartPending   = "START_PENDING"
	statusRebootPending  = "REBOOT_PENDING"
	statusRunning        = "RUNNING"
	statusStopped        = gpustatus.LiumStatusStopped
	statusStopPending    = gpustatus.LiumStatusStopPending
	statusFailed         = gpustatus.LiumStatusFailed
	statusCreationFailed = gpustatus.LiumStatusCreationFailed
	statusRebootFailed   = gpustatus.LiumStatusRebootFailed
	statusBroken         = gpustatus.LiumStatusBroken
	statusDeleting       = "DELETING"
)

// Ref.Meta entries the adapter keeps
const (
	metaPodName      = "pod_name"
	metaCreatedAt    = "created_at"
	metaNode         = "node"
	metaHourlyMicros = "hourly_micros"
	metaArch         = "arch"
	metaSize         = "size"
	metaSSHHost      = "ssh_host"
	metaSSHPort      = "ssh_port"
	metaReadyAt      = "ready_at"
)

var (
	// startingStatuses are a pod on its way to RUNNING
	startingStatuses = codes(statusPending, statusStartPending, statusRebootPending)
	// adoptable are the statuses of a pod a create of its key may take as its own
	adoptable = codes(statusPending, statusStartPending, statusRunning)
	// failedStatuses are a pod that ended and that the router destroys
	failedStatuses = codes(statusFailed, statusCreationFailed, statusRebootFailed,
		statusBroken)
	stoppedStatuses = codes(statusStopped, statusStopPending)
)

// knownStatus reports a status Lium documents, the adapter never guessing at another
func knownStatus(s string) bool {
	return startingStatuses[s] || s == statusRunning || failedStatuses[s] ||
		stoppedStatuses[s] || s == statusDeleting
}

// podItem holds the fields of one GET /pods entry the adapter reads
type podItem struct {
	ID                 string   `json:"id"`
	Name               string   `json:"pod_name"`
	Status             string   `json:"status"`
	ExecutorID         string   `json:"executor_id"`
	CreatedAt          *string  `json:"created_at"`
	Price              *float64 `json:"price"`
	RemovalScheduledAt *string  `json:"removal_scheduled_at"`
}

// podDetail holds the fields of GET /pods/{id} the adapter reads
type podDetail struct {
	podItem
	PortsMapping  json.RawMessage `json:"ports_mapping"`
	GPUCount      json.RawMessage `json:"gpu_count"`
	NodeOffline   bool            `json:"node_offline"`
	NodeOutage    json.RawMessage `json:"node_outage"`
	BillingPaused bool            `json:"billing_paused"`
	DiskPaused    bool            `json:"disk_paused"`
	Executor      podExecutor     `json:"executor"`
}

// podExecutor holds the fields of a pod's node, ExecutorForPodResponse in the OpenAPI
type podExecutor struct {
	ID          string  `json:"id"`
	MachineName *string `json:"machine_name"`
	Address     string  `json:"executor_ip_address"`
	GPUCount    *int    `json:"gpu_count"`
	Tier        *string `json:"tier"`
	Location    *struct {
		CountryCode *string `json:"country_code"`
	} `json:"location"`
}

// podEvent holds the fields of one GET /pods/{id}/events entry
type podEvent struct {
	CreatedAt *string `json:"created_at"`
	Reason    *string `json:"reason"`
}

// sshPortOf reads port 22's public port from ports_mapping, an object or one written as a string
func (d podDetail) sshPortOf() (int, bool) {
	raw := d.PortsMapping
	var s string
	if json.Unmarshal(raw, &s) == nil {
		raw = json.RawMessage(s)
	}
	var ports map[string]json.RawMessage
	if json.Unmarshal(raw, &ports) != nil {
		return 0, false
	}
	n, ok := intOf(ports[strconv.Itoa(sshPort)])
	return n, ok && n > 0 && n < 1<<16
}

// flags are the pod flags that are set, as the words gpustatus names
func (d podDetail) flags() []string {
	var out []string
	if d.NodeOffline {
		out = append(out, gpustatus.LiumFlagNodeOffline)
	}
	if present(d.NodeOutage) {
		out = append(out, gpustatus.LiumFlagNodeOutage)
	}
	if d.BillingPaused {
		out = append(out, gpustatus.LiumFlagBillingPaused)
	}
	if d.DiskPaused {
		out = append(out, gpustatus.LiumFlagDiskPaused)
	}
	return out
}

// parseTime reads a time Lium writes, with a zone or in UTC without one
func parseTime(s *string) (time.Time, bool) {
	if s == nil || *s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999"} {
		if t, err := time.Parse(layout, strings.TrimSpace(*s)); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// podID returns the pod id of a ref this provider made
func podID(ref provider.Ref) (string, bool) {
	return ref.ID, ref.Provider == providerName && ref.ID != ""
}

func podRoute(id string, rest ...string) string {
	return "/pods/" + url.PathEscape(id) + strings.Join(rest, "")
}

// listPods reads every pod the key sees, none on an account with no balance and no pods
func (p *Provider) listPods(ctx context.Context) ([]podItem, error) {
	var pods []podItem
	_, err := p.api.do(ctx, call{method: http.MethodGet, route: "/pods"}, &pods)
	if codeOf(err) == codeBalanceRequired {
		return nil, nil
	}
	return pods, err
}

// getPod reads one pod, a not_found error when Lium no longer has it
func (p *Provider) getPod(ctx context.Context, id string) (podDetail, error) {
	var d podDetail
	_, err := p.api.do(ctx, call{method: http.MethodGet, route: podRoute(id)}, &d)
	return d, err
}

// killReason is the reason of the pod's newest pod-lifecycle event, empty when it has none
func (p *Provider) killReason(ctx context.Context, id string) (string, error) {
	var events []podEvent
	_, err := p.api.do(ctx, call{method: http.MethodGet, route: podRoute(id, "/events")},
		&events)
	if err != nil {
		return "", err
	}
	best, at := "", time.Time{}
	// Events come oldest first, so a later one wins a tie
	for _, e := range events {
		if e.Reason == nil || *e.Reason == "" {
			continue
		}
		t, _ := parseTime(e.CreatedAt)
		if best == "" || !t.Before(at) {
			best, at = *e.Reason, t
		}
	}
	return best, nil
}

// destroyPod deletes a pod, only when it still has name if one is given, nil once it is gone
func (p *Provider) destroyPod(ctx context.Context, id, name string) error {
	q := url.Values{}
	if name != "" {
		q.Set("expected_name", name)
	}
	_, err := p.api.do(ctx, call{method: http.MethodDelete, route: podRoute(id), query: q}, nil)
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete lium pod %s: %w", id, err)
	}
	return nil
}

// node is the id of the pod's node, from executor_id or its executor
func (d podDetail) node() string {
	if d.ExecutorID != "" {
		return d.ExecutorID
	}
	return d.Executor.ID
}

// Inspect reads the pod, and for every state but running and starting its newest pod event
func (p *Provider) Inspect(ctx context.Context, ref provider.Ref) (provider.Status, error) {
	id, ok := podID(ref)
	if !ok {
		return provider.Status{State: provider.StateGone}, nil
	}
	d, err := p.getPod(ctx, id)
	e, _ := provider.AsError(err)
	var st provider.Status
	switch {
	case e != nil && e.Code == provider.CodeNotFound:
		st.State = provider.StateGone
	case err != nil:
		return provider.Status{}, err
	default:
		if st, err = statusOf(ref, d); err != nil {
			return provider.Status{}, err
		}
	}
	if st.State != provider.StateRunning && st.State != provider.StateStarting {
		if st.KillReason, err = p.killReason(ctx, id); err != nil {
			return provider.Status{}, err
		}
	}
	return st, nil
}

// statusOf maps a pod's status, a reboot or a move after ready ending the run
func statusOf(ref provider.Ref, d podDetail) (provider.Status, error) {
	ready, node := ref.Meta[metaReadyAt] != "", ref.Meta[metaNode]
	var state provider.State
	var own string
	switch s := d.Status; {
	case !knownStatus(s):
		return provider.Status{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium pod %s has status %q", d.ID, s)}
	case startingStatuses[s] && ready:
		// A reboot recreates the container, so our setup and firewall are gone
		state, own = provider.StateFailed, gpustatus.LiumRebooted
	case startingStatuses[s]:
		state = provider.StateStarting
	case s == statusRunning && ready && node != "" && d.node() != node:
		state, own = provider.StateFailed, gpustatus.LiumMoved
	case s == statusRunning && (d.NodeOffline || present(d.NodeOutage)):
		state = provider.StateDegraded
	case s == statusRunning:
		state = provider.StateRunning
	case stoppedStatuses[s]:
		state = provider.StateStopped
	case failedStatuses[s]:
		state = provider.StateFailed
	default:
		state = provider.StateGone
	}
	words := append([]string{d.Status}, d.flags()...)
	if own != "" {
		words = append(words, own)
	}
	st := provider.Status{State: state, Reason: gpustatus.LiumReason(words...)}
	st.ExpiresAt, _ = parseTime(d.RemovalScheduledAt)
	return st, nil
}

// listedState maps a listed pod's status as before ready, empty for one Lium does not document
func listedState(status string) provider.State {
	switch {
	case startingStatuses[status]:
		return provider.StateStarting
	case status == statusRunning:
		return provider.StateRunning
	case stoppedStatuses[status]:
		return provider.StateStopped
	case failedStatuses[status]:
		return provider.StateFailed
	case status == statusDeleting:
		return provider.StateGone
	}
	return ""
}

// ListOwned lists the pods whose names carry every label, every pod the key sees for none
func (p *Provider) ListOwned(
	ctx context.Context, labels map[string]string,
) ([]provider.Summary, error) {
	pods, err := p.listPods(ctx)
	if err != nil {
		return nil, err
	}
	var out []provider.Summary
	for _, pod := range pods {
		_, have, ours := parsePodName(pod.Name)
		// A pod being deleted is gone already
		if pod.Status == statusDeleting || !ours && len(labels) > 0 || !hasLabels(have, labels) {
			continue
		}
		s := provider.Summary{
			Ref: provider.Ref{Provider: providerName, ID: pod.ID, Meta: map[string]string{
				metaPodName: pod.Name, metaNode: pod.ExecutorID, metaArch: arch,
			}},
			Labels: have, State: listedState(pod.Status),
		}
		if t, ok := parseTime(pod.CreatedAt); ok {
			s.CreatedAt = t
			s.Ref.Meta[metaCreatedAt] = t.Truncate(time.Second).Format(time.RFC3339)
		}
		s.ExpiresAt, _ = parseTime(pod.RemovalScheduledAt)
		out = append(out, s)
	}
	return out, nil
}

// hasLabels reports whether have carries every label of want
func hasLabels(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}
	return true
}

// Destroy deletes the pod, only while it has the name the ref knows, and closes its connections
func (p *Provider) Destroy(ctx context.Context, ref provider.Ref) error {
	id, ok := podID(ref)
	if !ok {
		return nil
	}
	err := p.destroyPod(ctx, id, ref.Meta[metaPodName])
	p.dropPool(id)
	return err
}

// SetLifetime schedules the pod's removal at until, at most max_lifetime from its creation
func (p *Provider) SetLifetime(ctx context.Context, ref provider.Ref, until time.Time) error {
	id, ok := podID(ref)
	if !ok {
		return notLium(ref)
	}
	created, err := p.createdAt(ctx, ref)
	if err != nil {
		return err
	}
	if life := until.Sub(created); life > p.maxLifetime {
		return invalid(fmt.Errorf("lifetime %v from creation is over lium's limit of %v",
			life.Round(time.Second), p.maxLifetime), false)
	}
	body, err := json.Marshal(map[string]string{
		"removal_scheduled_at": until.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	_, err = p.api.do(ctx, call{
		method: http.MethodPost, route: podRoute(id, "/schedule-removal"), body: body,
	}, nil)
	return err
}

// createdAt reads the creation time Create kept in the ref, or asks Lium when the ref lacks it
func (p *Provider) createdAt(ctx context.Context, ref provider.Ref) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, ref.Meta[metaCreatedAt]); err == nil {
		return t, nil
	}
	d, err := p.getPod(ctx, ref.ID)
	if err != nil {
		return time.Time{}, err
	}
	t, ok := parseTime(d.CreatedAt)
	if !ok {
		return time.Time{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium pod %s names no creation time", ref.ID)}
	}
	return t, nil
}

// notLium is the not_found error of a ref another provider made
func notLium(ref provider.Ref) error {
	return &provider.Error{Code: provider.CodeNotFound,
		Err: fmt.Errorf("sandbox %s/%s is not a lium pod", ref.Provider, ref.ID)}
}
