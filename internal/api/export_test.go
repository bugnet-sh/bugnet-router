package api

import "testing"

// SetBeforeReport runs f between each report reading its budgets and counting its bytes
func SetBeforeReport(t testing.TB, f func()) {
	old := beforeReport
	beforeReport = f
	t.Cleanup(func() { beforeReport = old })
}
