package streams

// SetMaxEntries shortens the length a stream is trimmed to until the test ends
func SetMaxEntries(t interface{ Cleanup(func()) }, n int64) {
	old := maxEntries
	maxEntries = n
	t.Cleanup(func() { maxEntries = old })
}
