// Package vps holds the files of the VPS deploy that bugnet-dev renders
package vps

import _ "embed"

// HAProxyTemplate is haproxy.cfg.tmpl, DP 15.2's config with its bind lines left to site.toml
//
//go:embed haproxy.cfg.tmpl
var HAProxyTemplate string
