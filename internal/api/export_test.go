package api

import (
	"testing"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
)

// SetBeforeReport runs f between each report reading its budgets and counting its bytes
func SetBeforeReport(t testing.TB, f func()) {
	old := beforeReport
	beforeReport = f
	t.Cleanup(func() { beforeReport = old })
}

// SetTransferLease makes transfer claims hold for lease and streams renew them every renew
func SetTransferLease(t testing.TB, lease, renew time.Duration) {
	oldLease, oldRenew := xferLease, renewEvery
	xferLease, renewEvery = lease, renew
	t.Cleanup(func() { xferLease, renewEvery = oldLease, oldRenew })
}

// SetTransferCheck makes transfer streams check their idle time and hard stop every d
func SetTransferCheck(t testing.TB, d time.Duration) {
	old := xferCheck
	xferCheck = d
	t.Cleanup(func() { xferCheck = old })
}

// SetBeforeWatch runs f between each transfer stream reading its placement and watching its events
func SetBeforeWatch(t testing.TB, f func()) {
	old := beforeWatch
	beforeWatch = f
	t.Cleanup(func() { beforeWatch = old })
}

// WatchTransferEvents returns the edge events the transfer hub hands the streams of placement
func WatchTransferEvents(s *Server, placement string) (<-chan *routerv1.EdgeEvent, func()) {
	return s.hub.watch(placement)
}
