package secrets

import (
	"testing"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/proto"
)

func TestPresetsTable(t *testing.T) {
	gh, ok := PresetOf("GITHUB_TOKEN")
	eq(t, ok, true)
	eq(t, gh.Hosts, []string{"github.com", "api.github.com", "uploads.github.com"})
	eq(t, gh.Extra, ExtraGit)
	same, _ := PresetOf("GH_TOKEN")
	eq(t, same.Service, gh.Service)
	a, _ := PresetOf("ANTHROPIC_API_KEY")
	eq(t, a.Headers, []string{"x-api-key"})
	n, _ := PresetOf("NPM_TOKEN")
	eq(t, n.Extra, ExtraNPM)
	_, ok = PresetOf("hf_token")
	eq(t, ok, false)
	for _, p := range Presets {
		for _, h := range p.Hosts {
			eq(t, Custom(h), false)
			eq(t, validate(t, name(p.Names[0]), hosts(h), headers(p.Headers...)), nil)
		}
	}
	for _, h := range []string{"codeload.github.com", "raw.githubusercontent.com",
		"api.example.com"} {
		eq(t, Custom(h), true)
	}
}

func TestSuggestedCustomHostDropped(t *testing.T) {
	want := func(got, w *routerv1.SecretSpec) {
		t.Helper()
		if !proto.Equal(got, w) {
			t.Fatalf("got %v, want %v", got, w)
		}
	}
	want(Suggest("HF_TOKEN", []string{"evil.example.com", "huggingface.co"}),
		&routerv1.SecretSpec{Name: "HF_TOKEN", Hosts: []string{"huggingface.co"}})
	want(Suggest("HF_TOKEN", []string{"evil.example.com"}),
		&routerv1.SecretSpec{Name: "HF_TOKEN"})
	// A preset host of another name is dropped too
	want(Suggest("HF_TOKEN", []string{"api.openai.com"}), &routerv1.SecretSpec{Name: "HF_TOKEN"})
	want(Suggest("MY_KEY", []string{"api.example.com"}), &routerv1.SecretSpec{Name: "MY_KEY"})
	want(Suggest("", []string{"huggingface.co"}), &routerv1.SecretSpec{})
	// No host given suggests the whole preset, with its named headers
	want(Suggest("ANTHROPIC_API_KEY", nil), &routerv1.SecretSpec{
		Name: "ANTHROPIC_API_KEY", Hosts: []string{"api.anthropic.com"},
		HeaderNames: []string{"x-api-key"},
	})
	want(Suggest("GH_TOKEN", []string{"api.github.com", "api.github.com"}),
		&routerv1.SecretSpec{Name: "GH_TOKEN", Hosts: []string{"api.github.com"}})
	eq(t, LinkURL("secrets.bugnet.sh", "bnsl_abc"), "https://secrets.bugnet.sh/l/bnsl_abc")
}
