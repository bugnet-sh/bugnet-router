package api

import (
	"testing"
	"time"
)

// SetBeforeReport runs f between each report reading its budgets and counting its bytes
func SetBeforeReport(t testing.TB, f func()) {
	old := beforeReport
	beforeReport = f
	t.Cleanup(func() { beforeReport = old })
}

// SetTransferCheck makes transfer streams check their idle time and hard stop every d
func SetTransferCheck(t testing.TB, d time.Duration) {
	old := xferCheck
	xferCheck = d
	t.Cleanup(func() { xferCheck = old })
}
