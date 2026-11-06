package edgeevents

import "testing"

// SetMaxLen trims the stream at n entries for one test
func SetMaxLen(t testing.TB, n int) {
	old := maxLen
	maxLen = n
	t.Cleanup(func() { maxLen = old })
}
