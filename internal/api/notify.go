package api

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"
)

const resubscribeDelay = time.Second

// notifier fans one pattern subscription to every placement channel out to the waiting requests
type notifier struct {
	vk      valkey.Client
	pattern string
	log     *slog.Logger
	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
}

func newNotifier(vk valkey.Client, pattern string, log *slog.Logger) *notifier {
	return &notifier{
		vk: vk, pattern: pattern, log: log, waiters: map[string]map[chan struct{}]struct{}{},
	}
}

// watch returns a channel that fires after each message on channel until stop is called
func (n *notifier) watch(channel string) (wake <-chan struct{}, stop func()) {
	c := make(chan struct{}, 1)
	n.mu.Lock()
	defer n.mu.Unlock()
	set := n.waiters[channel]
	if set == nil {
		set = map[chan struct{}]struct{}{}
		n.waiters[channel] = set
	}
	set[c] = struct{}{}
	return c, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(set, c)
		if len(set) == 0 {
			delete(n.waiters, channel)
		}
	}
}

func (n *notifier) notify(channel string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for c := range n.waiters[channel] {
		fire(c)
	}
}

// notifyAll wakes every waiter, since messages may have been missed while unsubscribed
func (n *notifier) notifyAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, set := range n.waiters {
		for c := range set {
			fire(c)
		}
	}
}

func fire(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

// run holds the subscription until ctx ends, subscribing again whenever the connection drops
func (n *notifier) run(ctx context.Context) {
	for {
		err := n.vk.Dedicated(func(c valkey.DedicatedClient) error {
			hooked := valkey.WithOnSubscriptionHook(ctx, func(s valkey.PubSubSubscription) {
				if s.Kind == "psubscribe" {
					n.notifyAll()
				}
			})
			cmd := c.B().Psubscribe().Pattern(n.pattern).Build()
			return c.Receive(hooked, cmd, func(m valkey.PubSubMessage) { n.notify(m.Channel) })
		})
		if ctx.Err() != nil {
			return
		}
		n.log.Warn("placement subscription ended", "pattern", n.pattern, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(resubscribeDelay):
		}
	}
}
