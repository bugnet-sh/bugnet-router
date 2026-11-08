package live

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
)

// githubToken is BUGNET_GITHUB_TOKEN, and a row fails at once naming it when it is empty
func githubToken(t *testing.T) string {
	t.Helper()
	tok := os.Getenv("BUGNET_GITHUB_TOKEN")
	if tok == "" {
		t.Fatal("live: BUGNET_GITHUB_TOKEN is empty, add a public-only GitHub token to .env " +
			"(CP question 1)")
	}
	return tok
}

// bearer sends every request with the stack's internal token
type bearer struct{ token string }

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

// quoter answers the stack router's Estimate for a source
func (s *stack) quoter() routerv1connect.SandboxRouterServiceClient {
	hc := &http.Client{Transport: bearer{s.keys["BUGNET_INTERNAL_TOKEN"]}}
	return routerv1connect.NewSandboxRouterServiceClient(hc, s.apiURL)
}

// quote asks for an estimate of source and answers it and how long the call took
func (s *stack) quote(
	t *testing.T, c routerv1connect.SandboxRouterServiceClient, source string,
) (*routerv1.EstimateResponse, time.Duration) {
	t.Helper()
	start := time.Now()
	resp, err := c.Estimate(context.Background(), connect.NewRequest(&routerv1.EstimateRequest{
		Source: source, Workload: routerv1.Workload_WORKLOAD_TESTS}))
	took := time.Since(start)
	if err != nil {
		t.Fatalf("live: estimate %s: %v", source, err)
	}
	return resp.Msg, took
}

// githubLimits reads the token's remaining core and GraphQL points, which cost nothing
func githubLimits(t *testing.T, token string) (core, graphql int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "https://api.github.com/rate_limit", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("live: github rate limit: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var ans struct {
		Resources struct {
			Core    struct{ Remaining int } `json:"core"`
			GraphQL struct{ Remaining int } `json:"graphql"`
		} `json:"resources"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ans); err != nil {
		t.Fatalf("live: decode github rate limit: %v", err)
	}
	return ans.Resources.Core.Remaining, ans.Resources.GraphQL.Remaining
}

// e1Repos are ten public repositories of different sizes and languages
var e1Repos = []string{
	"psf/requests", "pallets/flask", "BurntSushi/ripgrep", "sharkdp/fd", "expressjs/express",
	"vitejs/vite", "tiangolo/fastapi", "spf13/cobra", "serde-rs/serde", "huggingface/tokenizers",
}

// rowE1 runs the GraphQL facts query on ten repositories and records its cost
func rowE1(t *testing.T, s *stack) {
	tok := githubToken(t)
	c := s.quoter()
	var total int
	for _, r := range e1Repos {
		_, before := githubLimits(t, tok)
		got, _ := s.quote(t, c, "github.com/"+r)
		_, after := githubLimits(t, tok)
		if !got.GetSource().GetRead() {
			t.Errorf("E1: %s was not read, reason %v", r, got.GetSource().GetUnreadReason())
		}
		if got.GetPartial() {
			t.Errorf("E1: %s answered partial, so a fetch ran past the budget", r)
		}
		total += before - after
		t.Logf("E1: %s cost %d GraphQL points, revision %s", r, before-after,
			got.GetSource().GetRevision())
	}
	if total > len(e1Repos) {
		t.Errorf("E1: %d points for %d repositories, the query was meant to cost 1 each", total,
			len(e1Repos))
	}
}

// rowE2 fetches a raw file 200 times in sequence, with and without the token
func rowE2(t *testing.T, _ *stack) {
	tok := githubToken(t)
	url := "https://raw.githubusercontent.com/psf/requests/main/README.md"
	for _, mode := range []string{"anonymous", "token"} {
		codes := map[int]int{}
		start := time.Now()
		for range 200 {
			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "token" {
				req.Header.Set("Authorization", "Bearer "+tok)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("E2 %s: %v", mode, err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			codes[resp.StatusCode]++
		}
		t.Logf("E2 %s: %v in %s", mode, codes, time.Since(start).Round(time.Millisecond))
		if codes[http.StatusTooManyRequests] > 0 {
			t.Errorf("E2 %s: raw answered 429 %d times of 200", mode,
				codes[http.StatusTooManyRequests])
		}
	}
}

// rowE3 fetches an old commit by SHA in a sandbox, the shallow fetch FetchSource relies on
func rowE3(t *testing.T, s *stack) {
	githubToken(t)
	// The initial revision of git, from 2005
	const sha = "e83c5163316f89bfbde7d9ab23ca2e25604af290"
	h := s.sandbox(t, xferPackage)
	out := s.sh(t, h, fmt.Sprintf(`command -v git > /dev/null || { echo no-git; exit 0; }
d=$(mktemp -d) && cd "$d" && git init -q && \
git remote add origin https://github.com/git/git.git && \
if git fetch -q --depth 1 origin %s 2> /dev/null; then echo shallow-ok; \
else echo shallow-refused; fi`, sha))
	t.Logf("E3 on %s: %s", s.provider, strings.TrimSpace(out))
	if strings.TrimSpace(out) == "no-git" {
		t.Error("E3: the image has no git, FetchSource answers TOOL_MISSING here")
	}
}

// e4Models are twenty public models of many sizes
var e4Models = []string{
	"Qwen/Qwen2.5-0.5B-Instruct", "Qwen/Qwen2.5-7B-Instruct", "openai-community/gpt2",
	"google-bert/bert-base-uncased", "facebook/opt-125m", "mistralai/Mistral-7B-Instruct-v0.2",
	"microsoft/phi-2", "TinyLlama/TinyLlama-1.1B-Chat-v1.0", "hexgrad/Kokoro-82M",
	"stabilityai/sdxl-turbo", "black-forest-labs/FLUX.1-schnell",
	"bartowski/Llama-3.2-3B-Instruct-GGUF",
	"distilbert/distilbert-base-uncased", "FacebookAI/roberta-base", "google/flan-t5-base",
	"sentence-transformers/all-MiniLM-L6-v2", "openai/whisper-small", "Qwen/Qwen2.5-1.5B-Instruct",
	"HuggingFaceTB/SmolLM2-135M", "EleutherAI/pythia-160m",
}

// e4Repos are fifty public repositories, none asked for before
var e4Repos = []string{
	"golang/example", "torvalds/uemacs", "rust-lang/rustlings", "denoland/deno_std",
	"nodejs/node-addon-api", "django/django", "numpy/numpy", "pandas-dev/pandas",
	"scikit-learn/scikit-learn", "pytorch/examples", "tensorflow/examples", "keras-team/keras",
	"huggingface/transformers", "huggingface/diffusers", "ggerganov/llama.cpp",
	"vllm-project/vllm", "facebook/react", "vuejs/core", "sveltejs/svelte", "angular/angular",
	"microsoft/TypeScript", "prettier/prettier", "eslint/eslint", "webpack/webpack",
	"rollup/rollup", "evanw/esbuild", "gin-gonic/gin", "labstack/echo", "gofiber/fiber",
	"grpc/grpc-go", "etcd-io/etcd", "prometheus/prometheus", "grafana/grafana",
	"hashicorp/terraform",
	"kubernetes/kubernetes", "docker/compose", "moby/moby", "nginx/nginx", "redis/redis",
	"postgres/postgres", "sqlite/sqlite", "curl/curl", "git/git", "tokio-rs/tokio",
	"rust-lang/cargo", "clap-rs/clap", "dtolnay/anyhow", "ruby/ruby", "rails/rails",
	"laravel/laravel",
}

// rowE4 times 50 cold repositories and 20 cold models, and checks the 95th percentile
func rowE4(t *testing.T, s *stack) {
	githubToken(t)
	c := s.quoter()
	var took []time.Duration
	for i, src := range slices.Concat(e4Repos, e4Models) {
		if i > 0 {
			// The router lets 30 new sources through a minute
			time.Sleep(2100 * time.Millisecond)
		}
		kind := "github.com/"
		if slices.Contains(e4Models, src) {
			kind = ""
		}
		_, d := s.quote(t, c, kind+src)
		took = append(took, d)
	}
	slices.Sort(took)
	p95 := took[len(took)*95/100]
	t.Logf("E4: %d cold sources, median %s, p95 %s, max %s", len(took),
		took[len(took)/2].Round(time.Millisecond), p95.Round(time.Millisecond),
		took[len(took)-1].Round(time.Millisecond))
	if p95 > 2*time.Second {
		t.Errorf("E4: p95 is %s, the gateway's read-only tools must answer within 2 s", p95)
	}
}
