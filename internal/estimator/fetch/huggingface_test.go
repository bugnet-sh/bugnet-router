package fetch_test

import (
	"net/http"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/estimatortest"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/fetch"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
)

const qwenConfig = `{"num_hidden_layers": 28, "num_key_value_heads": 4, "hidden_size": 3584,
	"num_attention_heads": 28, "max_position_embeddings": 32768, "torch_dtype": "bfloat16"}`

func hfModels() []*estimatortest.Model {
	return []*estimatortest.Model{
		{ID: "Qwen/Qwen2.5-7B-Instruct", SHA: sha1, Library: "transformers", Type: "qwen2",
			Pipeline: "text-generation", Params: map[string]int64{"BF16": 7615616512},
			Files: map[string]int64{"model-00001-of-00002.safetensors": 4000000000,
				"model-00002-of-00002.safetensors": 3000000000, "README.md": 5000,
				"config.json": 600},
			Config: qwenConfig},
		{ID: "meta-llama/Llama-3.1-8B-Instruct", SHA: sha2, Gated: "manual", Type: "llama",
			Params: map[string]int64{"BF16": 8030261248},
			Files:  map[string]int64{"model.safetensors": 16000000000}},
		{ID: "bartowski/tiny-GGUF", SHA: sha1, Library: "gguf",
			Files: map[string]int64{"tiny-Q4_K_M.gguf": 2000000000},
			GGUF: map[string]any{"total": 3212749888, "architecture": "llama",
				"context_length": 131072}},
	}
}

func newHF(t *testing.T, h *estimatortest.HuggingFace, token string) (*fetch.HuggingFace, *stack) {
	t.Helper()
	s := newStack(t, h.URL())
	cache := fetch.NewCache(s.vk, s.prefix, s.st, s.cfg, "r", nil)
	guard := fetch.NewGuard(s.vk, s.prefix, s.cfg.GithubReserve, nil)
	return fetch.NewHuggingFace(s.cfg, token, cache, guard, nil), s
}

func TestHuggingFaceReadsModelAndConfigOnce(t *testing.T) {
	h := estimatortest.NewHuggingFace(t, hfModels()...)
	f, _ := newHF(t, h, "")
	fv, res := read(t, f, "Qwen/Qwen2.5-7B-Instruct")
	eq(t, res.Unread, rules.UnreadReason(""))
	eq(t, res.Revision, sha1)
	m := fv.Model
	eq(t, m.Library, "transformers")
	eq(t, m.ModelType, "qwen2")
	eq(t, m.Config.Layers, 28)
	eq(t, m.Config.DType, "bfloat16")
	eq(t, m.Parameters[0].Count, int64(7615616512))
	eq(t, len(m.Files), 2)
	eq(t, []int32{h.API.Load(), h.Config.Load(), h.GGUF.Load()}, []int32{1, 1, 0})
	read(t, f, "qwen/qwen2.5-7b-instruct")
	eq(t, []int32{h.API.Load(), h.Config.Load()}, []int32{1, 1})
}

func TestHuggingFaceGatedModelSkipsConfig(t *testing.T) {
	h := estimatortest.NewHuggingFace(t, hfModels()...)
	f, _ := newHF(t, h, "hf_token")
	fv, res := read(t, f, "meta-llama/Llama-3.1-8B-Instruct")
	eq(t, res.Unread, rules.UnreadReason(""))
	eq(t, fv.Model.Gated, "manual")
	eq(t, fv.Model.Config == nil, true)
	eq(t, h.Config.Load(), int32(0))
	eq(t, h.Auths.Load(), "Bearer hf_token")
}

func TestHuggingFaceGGUFAddsOneCall(t *testing.T) {
	h := estimatortest.NewHuggingFace(t, hfModels()...)
	f, _ := newHF(t, h, "")
	fv, _ := read(t, f, "bartowski/tiny-GGUF")
	eq(t, h.GGUF.Load(), int32(1))
	eq(t, fv.Model.GGUF.ContextLength, 131072)
	eq(t, fv.Model.GGUF.Architecture, "llama")
}

func TestHuggingFaceNegativesAndRateLimit(t *testing.T) {
	h := estimatortest.NewHuggingFace(t, hfModels()...)
	f, _ := newHF(t, h, "")
	_, res := read(t, f, "nobody/missing")
	eq(t, res.Unread, rules.UnreadNotFound)
	calls := h.API.Load()
	_, res = read(t, f, "nobody/missing")
	eq(t, res.Unread, rules.UnreadNotFound)
	eq(t, h.API.Load(), calls)

	h.Status = http.StatusTooManyRequests
	_, res = read(t, f, "bartowski/tiny-GGUF")
	eq(t, res.Unread, rules.UnreadRateLimited)
	calls = h.API.Load()
	h.Status = 0
	_, res = read(t, f, "Qwen/Qwen2.5-7B-Instruct")
	eq(t, res.Unread, rules.UnreadRateLimited)
	eq(t, h.API.Load(), calls)
}

func TestHuggingFaceLegal(t *testing.T) {
	h := estimatortest.NewHuggingFace(t, hfModels()...)
	h.Status = http.StatusUnavailableForLegalReasons
	f, _ := newHF(t, h, "")
	_, res := read(t, f, "Qwen/Qwen2.5-7B-Instruct")
	eq(t, res.Unread, rules.UnreadLegal)
}
