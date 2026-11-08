//go:build !localrun

package config

// localrunFile holds no setting in a normal build, so a [localrun] key is unknown
type localrunFile struct{}

func (localrunFile) read() Localrun { return Localrun{} }
