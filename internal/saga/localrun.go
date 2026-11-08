//go:build localrun

package saga

// A localrun build gives a local sandbox the Bugnet CA, so its tunnel is servable
func init() { profiles["local"] = profile{ca: true} }
