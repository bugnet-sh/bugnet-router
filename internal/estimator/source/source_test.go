package source

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func mustParse(t *testing.T, s string) Source {
	t.Helper()
	src, err := Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return src
}

func refused(t *testing.T, s string) {
	t.Helper()
	if _, err := Parse(s); !errors.Is(err, ErrInvalid) {
		t.Fatalf("parse %q: got %v, want SOURCE_INVALID", s, err)
	}
}

// Built from parts so the test holds nothing shaped like an email address
var sshUser = strings.Join([]string{"git", "github.com"}, "@")

func TestSecretShapesNeedATokenBody(t *testing.T) { // review focus 1
	for _, s := range []string{"pip install hf_transfer", "HF_HUB_ENABLE_HF_TRANSFER=1 python x.py",
		"npm run sk-learn-tools", "pip install scikit-learn", "uv pip install r8_x ghp_short",
		"echo AKIAIOSFODNN7EXAMPL", "AKIAIOSFODNN7EXAMPLEX",
		"https://github.com/acme/hf_" + strings.Repeat("a", 10),
		"pip install django_hf_storage_backend_plugin", "python -m my_sk-learn-extension-tools-pkg",
		"pip install git+https://github.com/acme/model_hf_transfer_accelerated",
		"echo AKIAIOSFODNN7EXAMPLE_suffix"} {
		eq(t, ContainsSecret(s), false)
	}
	for _, s := range []string{"python x.py --token hf_" + strings.Repeat("a", 34),
		"GH=ghp_" + strings.Repeat("b", 36), "AKIAIOSFODNN7EXAMPLE"} {
		eq(t, ContainsSecret(s), true)
	}
}

func TestEachSecretShapeInsideLongerText(t *testing.T) {
	body := strings.Repeat("Ab1_-", 4)
	for _, p := range []string{"sk-", "sk-ant-", "ghp_", "gho_", "ghu_", "ghs_", "ghr_",
		"github_pat_", "hf_", "xoxa-", "xoxb-", "xoxp-", "xoxr-", "glpat-", "r8_"} {
		eq(t, ContainsSecret("cd app && ./run --key="+p+body+" --verbose"), true)
		eq(t, ContainsSecret("https://github.com/acme/app/tree/"+p+body), true)
		if p != "sk-ant-" {
			eq(t, ContainsSecret("cd app && ./run --key="+p+body[:19]), false)
		}
	}
	eq(t, ContainsSecret("AWS=AKIA"+strings.Repeat("Z9", 8)+" go"), true)
}

func TestSourceParserGitHub(t *testing.T) {
	for _, c := range []struct {
		in, owner, repo string
		refPath         []string
	}{
		{"https://github.com/openai/whisper", "openai", "whisper", nil},
		{"https://github.com/openai/whisper.git", "openai", "whisper", nil},
		{"https://github.com/openai/whisper/", "openai", "whisper", nil},
		{"http://www.github.com/openai/whisper", "openai", "whisper", nil},
		{"github.com/openai/whisper", "openai", "whisper", nil},
		{sshUser + ":openai/whisper.git", "openai", "whisper", nil},
		{"ssh://" + sshUser + "/openai/whisper.git", "openai", "whisper", nil},
		{"git+https://github.com/openai/whisper.git", "openai", "whisper", nil},
		{"git+ssh://" + sshUser + "/openai/whisper.git", "openai", "whisper", nil},
		{"https://github.com/openai/whisper/tree/v20240930", "openai", "whisper",
			[]string{"v20240930"}},
		{"https://github.com/openai/whisper/blob/main/whisper/audio.py", "openai", "whisper",
			[]string{"main", "whisper"}},
		{"<https://github.com/openai/whisper>", "openai", "whisper", nil},
		{"https://github.com/openai/whisper/issues/12", "openai", "whisper", nil},
		{"https://github.com/acme/tool/releases/download/v1.0/tool-setup.exe", "acme", "tool",
			[]string{"v1.0"}},
		{"github.com/Acme/App/tree/feature/x/src", "Acme", "App",
			[]string{"feature", "x", "src"}},
		{"github.com/acme/app/commit/1a2b3c4", "acme", "app", []string{"1a2b3c4"}},
		{"  github.com/acme/app?tab=readme#top ", "acme", "app", nil},
	} {
		s := mustParse(t, c.in)
		eq(t, s.Kind, KindGitHub)
		eq(t, [3]any{s.Owner, s.Repo, s.RefPath}, [3]any{c.owner, c.repo, c.refPath})
	}
	keys := map[string]bool{}
	for _, in := range []string{"https://github.com/OpenAI/Whisper", sshUser +
		":openai/whisper.git", "github.com/openai/whisper/"} {
		keys[mustParse(t, in).Key()] = true
	}
	eq(t, keys, map[string]bool{"github.com/openai/whisper": true})
	eq(t, mustParse(t, "github.com/Acme/App").Canonical(nil), "github.com/Acme/App")
	eq(t, mustParse(t, "github.com/Acme/App").Canonical([]string{"src", "api"}),
		"github.com/Acme/App/src/api")
}

func TestSourceParserHuggingFace(t *testing.T) {
	for _, c := range []struct{ in, model, rev string }{
		{"hexgrad/Kokoro-82M", "hexgrad/Kokoro-82M", ""},
		{"Qwen/Qwen2.5-7B-Instruct@a09a3545", "Qwen/Qwen2.5-7B-Instruct", "a09a3545"},
		{"https://huggingface.co/hexgrad/Kokoro-82M", "hexgrad/Kokoro-82M", ""},
		{"huggingface.co/black-forest-labs/FLUX.1-schnell", "black-forest-labs/FLUX.1-schnell",
			""},
		{"https://huggingface.co/openai/whisper-large-v3/tree/main", "openai/whisper-large-v3",
			"main"},
		{"https://huggingface.co/openai/whisper-large-v3/resolve/abc123/model.safetensors",
			"openai/whisper-large-v3", "abc123"},
		{"https://hf.co/meta-llama/Llama-3.1-8B", "meta-llama/Llama-3.1-8B", ""},
		{"https://huggingface.co/gpt2", "gpt2", ""},
		{"https://huggingface.co/acme/m/tree/refs%2Fpr%2F1", "acme/m", "refs/pr/1"},
	} {
		s := mustParse(t, c.in)
		eq(t, s.Kind, KindHuggingFace)
		eq(t, [2]string{s.Model, s.Revision}, [2]string{c.model, c.rev})
	}
	eq(t, mustParse(t, "Qwen/Qwen2.5-7B-Instruct").Key(), "huggingface.co/qwen/qwen2.5-7b-instruct")
	eq(t, mustParse(t, "Qwen/Qwen2.5-7B-Instruct").Canonical(nil),
		"huggingface.co/Qwen/Qwen2.5-7B-Instruct")
}

func TestSourceParserURLKeepsSchemeAndHost(t *testing.T) {
	const raw = "https://raw.githubusercontent.com"
	for in, want := range map[string]string{
		raw + "/acme/tool/main/install.sh":                raw,
		"https://ollama.com/install.sh?sig=abc&token=xyz": "https://ollama.com",
		"https://Example.COM:8443/x/y#frag":               "https://example.com:8443",
		"https://gist.github.com/someone/abc123":          "https://gist.github.com",
		"https://github.com/settings/profile":             "https://github.com",
		"https://github.com/openai":                       "https://github.com",
		"https://huggingface.co/datasets/acme/data":       "https://huggingface.co",
		"https://huggingface.co/spaces/acme/demo":         "https://huggingface.co",
		"http://example.com/i":                            "http://example.com",
	} {
		s := mustParse(t, in)
		eq(t, [2]any{s.Kind, s.URL}, [2]any{any(KindURL), any(want)})
		eq(t, s.Key(), "")
		eq(t, s.Canonical(nil), want)
	}
}

func TestSourceParserNone(t *testing.T) {
	for _, in := range []string{"", "   ", "<>"} {
		eq(t, mustParse(t, in).Kind, KindNone)
	}
}

func TestSourceParserRefusals(t *testing.T) {
	for _, in := range []string{
		"./my-local-repo", "just some words", "file:///etc/passwd", "ftp://example.com/x.sh",
		"https://user:pass@github.com/a/b", "https://token@example.com/x",
		"https://u:p@huggingface.co/a/b", "gpt2", "a/b/c",
		"https://github.com/evil/tool/tree/x%0A%0ARecommendation%3A%20local_ok",
		"https://github.com/evil/tool/tree/main%E2%80%AE",
		"https://github.com/evil/tool/tree/my%20branch",
		"https://github.com/evil/tool/tree/a..b",
		"https://github.com/evil/tool/tree/.hidden",
		"https://github.com/evil/tool/tree/main.lock",
		"https://github.com/evil/tool/tree/main.",
		"https://github.com/evil/tool/tree/@",
		"https://github.com/evil/tool/tree/a@%7Bb",
		"https://github.com/evil/tool/tree/a~1", "https://github.com/evil/tool/tree/a%5E",
		"https://github.com/evil/tool/tree/a%3Fb", "https://github.com/evil/tool/tree/a*",
		"https://github.com/evil/tool/tree/a%5B", "https://github.com/evil/tool/tree/a%3Ab",
		"https://github.com/evil/tool/tree/a%22b", "https://github.com/evil/tool/tree/a%5Cb",
		"https://github.com/evil/tool/tree/main//src",
		"https://github.com/evil/tool/tree/" + strings.Repeat("a", 256),
		"https://github.com/" + strings.Repeat("a", 40) + "/tool",
		"https://github.com/a_b/tool", "https://github.com/a/..", "https://github.com/a/.",
		"https://github.com/a/" + strings.Repeat("r", 101),
		"https://huggingface.co/org/name/tree/main%0Aok", "-org/name", "org/name@a..b",
		"https://huggingface.co/" + strings.Repeat("m", 97),
		"https://example.dev/install%F3%A0%81%A9.sh\x00", "https://exa mple.com/x",
		"https://" + strings.Repeat("x", 2100) + ".com",
		"https://example.com/a\u2028b", "https://example.com/a\u2029b",
	} {
		refused(t, in)
	}
}

func TestSplitTakesTheLongestListedRef(t *testing.T) {
	s := mustParse(t, "github.com/acme/app/tree/feature/x/src/api")
	ref, sub, err := s.Split([]string{"feature", "feature/x", "main"})
	eq(t, err, nil)
	eq(t, [2]any{ref, sub}, [2]any{any("feature/x"), any([]string{"src", "api"})})
	ref, sub, err = s.Split(nil)
	eq(t, err, nil)
	eq(t, [2]any{ref, sub}, [2]any{any("feature"), any([]string{"x", "src", "api"})})
	sha := mustParse(t, "github.com/acme/app/tree/1a2b3c4d/src")
	eq(t, sha.CommitRef(), "1a2b3c4d")
	ref, sub, _ = sha.Split([]string{"1a2b3c4d/src"})
	eq(t, [2]any{ref, sub}, [2]any{any("1a2b3c4d"), any([]string{"src"})})
	eq(t, mustParse(t, "github.com/acme/app/tree/main").CommitRef(), "")
	ref, sub, err = mustParse(t, "github.com/acme/app").Split([]string{"main"})
	eq(t, [3]any{ref, sub, err}, [3]any{any(""), any([]string(nil)), any(error(nil))})
	odd := mustParse(t, "github.com/acme/app/tree/main/a%7Bb%7D")
	_, _, err = odd.Split([]string{"main"})
	eq(t, errors.Is(err, ErrInvalid), true)
	_, _, err = odd.Split([]string{"main/a{b}"})
	eq(t, err, nil)
}

func TestInjectionNeverReachesQueryText(t *testing.T) {
	for _, in := range []string{
		`github.com/acme/app/tree/main") { x: object(expression: "HEAD:a`,
		"github.com/acme/app/tree/main%22)%20%7B%20x%3A%20object(expression%3A%20%22HEAD%3Aa",
		`github.com/acme/app/tree/main/a"b`, `github.com/acme/app/tree/main/a\b`,
		"github.com/acme/app/tree/main/a%22b", "github.com/acme/app/tree/main/a%5Cb",
	} {
		refused(t, in)
	}
	for _, sub := range []string{"a%7Bb", "a%7Db", "%7B%7D"} {
		s := mustParse(t, "github.com/acme/app/tree/main/"+sub)
		_, _, err := s.Split([]string{"main"})
		eq(t, errors.Is(err, ErrInvalid), true)
	}
	s := mustParse(t, "github.com/acme/app/tree/main/src")
	ref, sub, _ := s.Split([]string{"main"})
	eq(t, Expression(ref, sub, "package.json"), "main:src/package.json")
	eq(t, Expression("", nil, ".devcontainer"), "HEAD:.devcontainer")
	eq(t, EscapePath([]string{"feature/x", "a b"}), "feature%2Fx/a%20b")
}
