package secrets

import "slices"

// Extra files egress apply writes for a preset
const (
	ExtraGit = "git"
	ExtraNPM = "npm"
)

// Preset is one service of DP 10.3's table, the only hosts a link may suggest
type Preset struct {
	Service string
	Names   []string
	// Hosts are the preset's hosts, the first one the host its extra file is for
	Hosts []string
	// Headers are the named headers besides Authorization, which is always scanned
	Headers []string
	// Extra names the file egress apply writes for the preset, empty for none
	Extra string
}

// Presets is DP 10.3's table
var Presets = []Preset{
	{Service: "Hugging Face", Names: []string{"HF_TOKEN"}, Hosts: []string{"huggingface.co"}},
	{
		Service: "GitHub", Names: []string{"GH_TOKEN", "GITHUB_TOKEN"},
		Hosts: []string{"github.com", "api.github.com", "uploads.github.com"}, Extra: ExtraGit,
	},
	{Service: "OpenAI", Names: []string{"OPENAI_API_KEY"}, Hosts: []string{"api.openai.com"}},
	{
		Service: "Anthropic", Names: []string{"ANTHROPIC_API_KEY"},
		Hosts: []string{"api.anthropic.com"}, Headers: []string{"x-api-key"},
	},
	{
		Service: "npm", Names: []string{"NPM_TOKEN"}, Hosts: []string{"registry.npmjs.org"},
		Extra: ExtraNPM,
	},
}

// PresetOf answers the preset whose variables hold name exactly
func PresetOf(name string) (Preset, bool) {
	for _, p := range Presets {
		if slices.Contains(p.Names, name) {
			return p, true
		}
	}
	return Preset{}, false
}

// Custom says whether host is outside every preset
func Custom(host string) bool {
	return !slices.ContainsFunc(Presets, func(p Preset) bool {
		return slices.Contains(p.Hosts, host)
	})
}
