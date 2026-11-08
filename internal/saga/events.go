package saga

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Outbox event types
const (
	EventReady      = "placement.ready"
	EventFailed     = "placement.failed"
	EventExtended   = "placement.extended"
	EventTerminated = "placement.terminated"
)

const publishTimeout = 5 * time.Second

// Event is a placement change as the outbox delivers it and Valkey publishes it
type Event struct {
	PlacementID uuid.UUID `json:"placement_id"`
	OrderID     string    `json:"order_id"`
	// ExtendOrderID is the extension order a placement.extended event settles, beside the start one
	ExtendOrderID string     `json:"extend_order_id,omitempty"`
	State         string     `json:"state"`
	Provider      string     `json:"provider"`
	Deadline      *time.Time `json:"deadline"`
	BilledSeconds int64      `json:"billed_seconds"`
	Refundable    bool       `json:"refundable"`
	Reason        string     `json:"reason,omitempty"`
}

func newEvent(p store.Placement, reason string) Event {
	return Event{
		PlacementID:   p.ID,
		OrderID:       p.OrderID,
		State:         p.State,
		Provider:      p.Provider,
		Deadline:      p.Deadline,
		BilledSeconds: billedSeconds(p),
		Refundable:    p.Refundable,
		Reason:        reason,
	}
}

// billedSeconds counts whole seconds from ready to terminated, zero for a sandbox never ready
func billedSeconds(p store.Placement) int64 {
	if p.ReadyAt == nil || p.TerminatedAt == nil {
		return 0
	}
	return max(int64(p.TerminatedAt.Sub(*p.ReadyAt)/time.Second), 0)
}

const channelKind = "placement:"

// Channel is the Valkey channel that carries a placement's changes
func Channel(prefix string, id uuid.UUID) string {
	return prefix + channelKind + id.String()
}

// ChannelPattern is the Valkey pattern matching every placement's Channel under prefix
func ChannelPattern(prefix string) string {
	var b strings.Builder
	for _, r := range prefix {
		if strings.ContainsRune(`\*?[]`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String() + channelKind + "*"
}

// publish wakes live readers after a commit, a lost message only delays them until they poll
func (s *Saga) publish(ctx context.Context, ev Event) {
	raw, err := json.Marshal(ev)
	if err != nil {
		s.d.Log.Error("encode placement event", "placement", ev.PlacementID, "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	vk := s.d.Valkey
	channel := Channel(s.d.Prefix, ev.PlacementID)
	cmd := vk.B().Publish().Channel(channel).Message(string(raw)).Build()
	if err := vk.Do(ctx, cmd).Error(); err != nil {
		s.d.Log.Warn("publish placement event", "placement", ev.PlacementID, "err", err)
	}
}
