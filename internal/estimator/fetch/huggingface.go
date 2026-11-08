package fetch

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/canon"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/source"
)

// HuggingFace reads model metadata from the Hub's API and the model's config.json
type HuggingFace struct {
	cfg   config.Estimator
	token string
	cache *Cache
	guard *Guard
	log   *slog.Logger
	up    *upstream
}

// NewHuggingFace returns a client over the configured Hub, token empty for anonymous reads
func NewHuggingFace(
	cfg config.Estimator, token string, cache *Cache, guard *Guard, log *slog.Logger,
) *HuggingFace {
	if log == nil {
		log = slog.Default()
	}
	follow := sameHost(hostOf(cfg.HuggingFace), "/api/resolve-cache/", 1)
	return &HuggingFace{cfg: cfg, token: token, cache: cache, guard: guard, log: log,
		up: newUpstream(4, follow)}
}

var _ Fetcher = (*HuggingFace)(nil)

func (h *HuggingFace) headers() map[string]string {
	hd := map[string]string{"Accept": "application/json"}
	if h.token != "" {
		hd["Authorization"] = "Bearer " + h.token
	}
	return hd
}

// modelPath is the escaped model id, one segment per part
func modelPath(id string) string {
	return source.EscapePath(strings.Split(id, "/"))
}

// Facts reads the model at the revision the source names, or at main
func (h *HuggingFace) Facts(ctx context.Context, src source.Source) (facts.Facts, Resolved, error) {
	key := src.Key()
	rev := src.Revision
	unread := func(r rules.UnreadReason) (facts.Facts, Resolved, error) {
		return facts.Facts{}, Resolved{Ref: rev, Unread: r}, nil
	}
	if r, ok := h.cache.Neg(ctx, "hf", key); ok {
		return unread(r)
	}
	if ok, _ := h.guard.Allow(ctx, ResourceHuggingFace); !ok {
		return unread(rules.UnreadRateLimited)
	}
	refHash := hash(rev)
	lookup := func(ctx context.Context) (facts.Facts, Resolved, bool) {
		sha, ok := h.cache.Ref(ctx, "hf", key, refHash)
		if !ok {
			return facts.Facts{}, Resolved{}, false
		}
		f, ok := h.cache.Facts(ctx, "hf", key, sha)
		return f, Resolved{Ref: rev, Revision: sha}, ok
	}
	if f, r, ok := lookup(ctx); ok {
		return f, r, nil
	}
	return h.cache.Do(ctx, "hf:"+key+":"+refHash, lookup,
		func(ctx context.Context) (facts.Facts, Resolved, error) {
			return h.run(ctx, src, refHash)
		})
}

// hubModel is the part of the Hub's model answer the estimator reads
type hubModel struct {
	ID       string          `json:"id"`
	SHA      string          `json:"sha"`
	Library  string          `json:"library_name"`
	Pipeline string          `json:"pipeline_tag"`
	Gated    json.RawMessage `json:"gated"`
	Config   struct {
		ModelType string `json:"model_type"`
	} `json:"config"`
	Safetensors struct {
		Parameters map[string]int64 `json:"parameters"`
	} `json:"safetensors"`
	Siblings []struct {
		Name string `json:"rfilename"`
		Size *int64 `json:"size"`
	} `json:"siblings"`
	GGUF *struct {
		Total         int64  `json:"total"`
		Architecture  string `json:"architecture"`
		ContextLength int    `json:"context_length"`
	} `json:"gguf"`
}

var shortWord = regexp.MustCompile(`^[a-z0-9][a-z0-9._/:-]{0,63}$`)

// word keeps a string from the Hub only when it is a plain lowercase word
func word(s string) string {
	s = strings.ToLower(s)
	if shortWord.MatchString(s) {
		return s
	}
	return ""
}

func (h *HuggingFace) run(
	ctx context.Context, src source.Source, refHash string,
) (facts.Facts, Resolved, error) {
	key := src.Key()
	rev := cmpOr(src.Revision, "main")
	res := Resolved{Ref: src.Revision}
	fail := func(r rules.UnreadReason) (facts.Facts, Resolved, error) {
		if negative(r) {
			h.cache.PutNeg(ctx, "hf", key, r)
		}
		res.Unread = r
		return facts.Facts{}, res, nil
	}
	m, unread, err := h.model(ctx, src.Model, rev, "?blobs=true")
	if err != nil {
		h.log.Warn("estimator: read hugging face model", "err", err)
		return fail(rules.UnreadUpstreamError)
	}
	if unread != "" {
		return fail(unread)
	}
	res.Revision = m.SHA
	h.cache.PutRef(ctx, "hf", key, refHash, m.SHA, false)
	if f, ok := h.cache.Facts(ctx, "hf", key, m.SHA); ok {
		return f, res, nil
	}
	model := toModel(src.Model, m)
	if slices.ContainsFunc(model.Files, func(f facts.File) bool {
		return strings.HasSuffix(strings.ToLower(f.Path), ".gguf")
	}) {
		g, unread, err := h.model(ctx, src.Model, m.SHA, "?expand[]=gguf")
		if err == nil && unread == "" && g.GGUF != nil {
			model.GGUF = &facts.GGUFMetadata{Architecture: word(g.GGUF.Architecture),
				ContextLength: g.GGUF.ContextLength, Parameters: g.GGUF.Total}
		}
	}
	if model.Gated == "" {
		model.Config = h.config(ctx, src.Model, m.SHA)
	}
	f := facts.Facts{Model: model}
	f.Normalize()
	b, err := canon.Marshal(f)
	if err == nil && int64(len(b)) > h.cfg.MaxFactsBytes {
		squeeze(model)
		f.Normalize()
		b, err = canon.Marshal(f)
	}
	if err != nil || int64(len(b)) > h.cfg.MaxFactsBytes {
		h.log.Warn("estimator: model facts past the size cap", "bytes", len(b))
		return fail(rules.UnreadUpstreamError)
	}
	h.cache.PutFacts(ctx, "hf", key, m.SHA, b, sum(b))
	return f, res, nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// model calls the Hub's revision endpoint, query holding its fixed parameters
func (h *HuggingFace) model(
	ctx context.Context, id, rev, query string,
) (hubModel, rules.UnreadReason, error) {
	u := h.cfg.HuggingFace + "/api/models/" + modelPath(id) + "/revision/" +
		url.PathEscape(rev) + query
	resp, err := h.up.do(ctx, http.MethodGet, u, h.headers(), nil, maxModelBytes)
	if errors.Is(err, errTooBig) {
		return hubModel{}, rules.UnreadUpstreamError, nil
	}
	if err != nil {
		return hubModel{}, "", err
	}
	if resp.status != http.StatusOK {
		if resp.status == http.StatusTooManyRequests {
			wait := time.Duration(max(retryAfter(resp.header), 300)) * time.Second
			h.guard.Exhaust(ctx, ResourceHuggingFace, time.Now().Add(wait))
		}
		return hubModel{}, classify(resp.status, resp.header, true), nil
	}
	var m hubModel
	if json.Unmarshal(resp.body, &m) != nil || len(m.SHA) < 7 || len(m.SHA) > 64 ||
		strings.ContainsAny(m.SHA, "/:\\ \x00") {
		return hubModel{}, rules.UnreadUpstreamError, nil
	}
	return m, "", nil
}

// toModel distils the Hub's answer, keeping only the files a run loads
func toModel(id string, m hubModel) *facts.Model {
	out := &facts.Model{ID: id, Revision: m.SHA, Library: word(m.Library),
		Pipeline: word(m.Pipeline), ModelType: word(m.Config.ModelType)}
	var gated string
	if json.Unmarshal(m.Gated, &gated) == nil && (gated == "auto" || gated == "manual") {
		out.Gated = gated
	}
	for dt, n := range m.Safetensors.Parameters {
		out.Parameters = append(out.Parameters, facts.DTypeCount{DType: word(dt), Count: n})
	}
	for _, s := range m.Siblings {
		if s.Size != nil && weightFile(s.Name) {
			out.Files = append(out.Files, facts.File{Path: s.Name, Size: *s.Size})
		}
	}
	return out
}

var weightExt = []string{".safetensors", ".gguf", ".bin", ".pt", ".pth", ".onnx", ".ckpt"}

// weightFile says a file is one the memory rules count, or the diffusers marker
func weightFile(name string) bool {
	l := strings.ToLower(name)
	return l == "model_index.json" || slices.ContainsFunc(weightExt,
		func(e string) bool { return strings.HasSuffix(l, e) })
}

var shardSeries = regexp.MustCompile(`^(.*)-\d{3,5}-of-\d{3,5}(\.safetensors|\.bin)$`)

// squeeze joins the shards of a root-level series into one file of their total size, which
// changes no floor since the rules count only the size of those files
func squeeze(m *facts.Model) {
	type series struct{ size int64 }
	totals := map[string]*series{}
	var order []string
	var kept []facts.File
	for _, f := range m.Files {
		match := shardSeries.FindStringSubmatch(f.Path)
		if match == nil || strings.Contains(f.Path, "/") {
			kept = append(kept, f)
			continue
		}
		name := match[1] + "-merged" + match[2]
		if totals[name] == nil {
			totals[name] = &series{}
			order = append(order, name)
		}
		totals[name].size += f.Size
	}
	for _, n := range order {
		kept = append(kept, facts.File{Path: n, Size: totals[n].size})
	}
	m.Files = kept
}

// config reads config.json at the revision, nil when it is unreadable or gated
func (h *HuggingFace) config(ctx context.Context, id, sha string) *facts.ModelConfig {
	u := h.cfg.HuggingFace + "/" + modelPath(id) + "/resolve/" + url.PathEscape(sha) +
		"/config.json"
	resp, err := h.up.do(ctx, http.MethodGet, u, h.headers(), nil, h.cfg.MaxFileBytes)
	if err != nil || resp.status != http.StatusOK {
		return nil
	}
	var c struct {
		Layers  int             `json:"num_hidden_layers"`
		KVHeads int             `json:"num_key_value_heads"`
		HeadDim int             `json:"head_dim"`
		Hidden  int             `json:"hidden_size"`
		Heads   int             `json:"num_attention_heads"`
		MaxPos  int             `json:"max_position_embeddings"`
		DType   string          `json:"torch_dtype"`
		DType2  string          `json:"dtype"`
		Quant   json.RawMessage `json:"quantization_config"`
	}
	if json.Unmarshal(resp.body, &c) != nil {
		return nil
	}
	return &facts.ModelConfig{Layers: bounded(c.Layers), KVHeads: bounded(c.KVHeads),
		HeadDim: bounded(c.HeadDim), HiddenSize: bounded(c.Hidden),
		AttentionHeads: bounded(c.Heads), MaxPosition: bounded(c.MaxPos),
		DType: word(cmpOr(c.DType, c.DType2)), Quantized: len(c.Quant) > 0 &&
			string(c.Quant) != "null"}
}

// bounded keeps a config number a person could have meant, and zero otherwise
func bounded(n int) int {
	if n < 0 || n > 100_000_000 {
		return 0
	}
	return n
}
