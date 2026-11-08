package leakscan

import (
	"path"
	"regexp"
	"strings"
)

// Shape is a named form of secret that no sandbox file or variable may hold unless allowed
type Shape struct {
	Name string
	Re   *regexp.Regexp
}

// token is the body a prefixed token needs before a word counts as one
const token = `[A-Za-z0-9_-]{20,}`

// Shapes are the secret forms of CP 11.4
var Shapes = []Shape{
	{"e2b", regexp.MustCompile(`e2b_[0-9a-f]{40}`)},
	{"boxd", regexp.MustCompile(`bxd_[A-Za-z0-9]{43}`)},
	{"boat", regexp.MustCompile(`boat_` + token)},
	{"box", regexp.MustCompile(`box_` + token)},
	{"webhook", regexp.MustCompile(`whsec_[0-9a-f]{64}`)},
	{"handle", regexp.MustCompile(`sbx_` + token)},
	{"order", regexp.MustCompile(`v1\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}`)},
	{"private-key", regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`)},
	{"github", regexp.MustCompile(`ghp_` + token)},
	{"github-pat", regexp.MustCompile(`github_pat_` + token)},
	{"hugging-face", regexp.MustCompile(`hf_` + token)},
	{"openai", regexp.MustCompile(`sk-` + token)},
	{"aws", regexp.MustCompile(`AKIA[0-9A-Z]{16}`)},
	{"preview", regexp.MustCompile(`bnp_` + token)},
	{"transfer", regexp.MustCompile(`bnx_` + token)},
	{"link", regexp.MustCompile(`bnsl_` + token)},
	{"put", regexp.MustCompile(`bnsp_` + token)},
	{"tunnel", regexp.MustCompile(`bnt_` + token)},
	{"placeholder", regexp.MustCompile(`bnsec_` + token)},
}

// Anywhere in an Allow means every shape, every path or every variable
const Anywhere = "*"

// Allow lets one named shape stand at a path or in a variable, never a needle
type Allow struct {
	// Shape is a Shape name, or Anywhere
	Shape string
	// Path is an archive path with the globs of path.Match, a prefix when it ends in a slash,
	// and empty for every path
	Path string
	// Env is the variable the shape sits in, empty for every place
	Env string
}

// OwnFile is a file that holds one value for its own sandbox only
type OwnFile struct {
	Name string
	Path string
}

// Allowlist is what CP 11.5 allows, grouped by what it grants
type Allowlist struct {
	Allow []Allow
	// Own lists the files whose contents are that sandbox's own, found nowhere else
	Own []OwnFile
	// Names lists the variables expected in an environment, left out of the review list
	Names []string
}

// Archive paths of the files a sandbox owns
const (
	GateKeyPath = "etc/bugnet/gate.key"
	TunnelPath  = "etc/bugnet/egress.token"
	HostKeyPath = "etc/bugnet/ssh/host_ed25519_key"
)

// Base is the allowlist every provider shares, with the secrets rows only when secrets is true
func Base(secrets bool) Allowlist {
	a := Allowlist{
		Allow: []Allow{{Shape: "private-key", Path: HostKeyPath}},
		Own:   []OwnFile{{"gate key", GateKeyPath}, {"host key", HostKeyPath}},
	}
	if secrets {
		a.Allow = append(a.Allow, Allow{Shape: "tunnel", Path: TunnelPath},
			Allow{Shape: "placeholder"})
		a.Own = append(a.Own, OwnFile{"tunnel token", TunnelPath})
	}
	return a
}

// providerEnv lists the variables each provider always injects, UNVERIFIED for E2B
var providerEnv = map[string][]string{
	"boat":  {"ASCII_TOKEN", "BOAT_ID"},
	"koyeb": {"SANDBOX_SECRET", "BUGNET_LABELS"},
	"e2b": {"E2B_SANDBOX_ID", "E2B_TEAM_ID", "E2B_TEMPLATE_ID", "E2B_SANDBOX_DOMAIN",
		"E2B_ENVD_ACCESS_TOKEN", "ENVD_ACCESS_TOKEN"},
}

// For is Base plus the variables provider always injects
func For(provider string, secrets bool) Allowlist {
	a := Base(secrets)
	for _, name := range providerEnv[provider] {
		a.Allow = append(a.Allow, Allow{Shape: Anywhere, Env: name})
		a.Names = append(a.Names, name)
	}
	return a
}

// allows reports whether a shape found at path in variable env is on the list
func (a Allowlist) allows(shape, p, env string) bool {
	for _, x := range a.Allow {
		if (x.Shape == Anywhere || x.Shape == shape) && pathMatches(x.Path, p) &&
			(x.Env == "" || x.Env == env) {
			return true
		}
	}
	return false
}

func pathMatches(pat, p string) bool {
	switch {
	case pat == "":
		return true
	case strings.HasSuffix(pat, "/"):
		return strings.HasPrefix(p, pat)
	}
	ok, _ := path.Match(pat, p)
	return ok
}
