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
	// Events come oldest first, so a later one wins a tie or a time that does not parse
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
