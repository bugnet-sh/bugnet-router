//go:build !localrun

package saga_test

// localrunCA is the CA version a local sandbox gets from version v, none in a normal build
func localrunCA(int) int { return 0 }
