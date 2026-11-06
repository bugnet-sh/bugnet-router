package local

// FileErr classifies a host file error the way the adapter does
var FileErr = fileErr

// SetBeforeOpen sets what a reader runs between its Lstat and its open, nil for nothing
func SetBeforeOpen(f func(host string)) { beforeOpen = f }
