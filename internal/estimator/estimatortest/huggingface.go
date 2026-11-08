package estimatortest

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Model is one fake Hub model
type Model struct {
	ID       string
	SHA      string
	Library  string
	Pipeline string
	// Gated is false, "auto" or "manual" as the Hub sends it
	Gated string
	Type  string
	// Params maps a dtype to a parameter count
	Params map[string]int64
	Files  map[string]int64
	// Config is config.json, empty for a gated model
	Config string
	// GGUF is the gguf summary, nil for none
	GGUF map[string]any
}

// HuggingFace is a fake of the Hub on one test server
type HuggingFace struct {
	Srv *httptest.Server
	// API, GGUF and Config count the calls of each step
	API, GGUF, Config atomic.Int32
	// Delay slows every answer
	Delay time.Duration
	// Status overrides every API answer when non-zero
	Status int
	// Auths records the Authorization header of every call
	Auths atomic.Value

	models map[string]*Model
}

// NewHuggingFace starts a fake with models, closed when the test ends
func NewHuggingFace(t *testing.T, models ...*Model) *HuggingFace {
	t.Helper()
	h := &HuggingFace{models: map[string]*Model{}}
	for _, m := range models {
		h.models[strings.ToLower(m.ID)] = m
	}
	h.Srv = httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(h.Srv.Close)
	return h
}

// URL is the Hub's base URL
func (h *HuggingFace) URL() string { return h.Srv.URL }

func (h *HuggingFace) serve(w http.ResponseWriter, r *http.Request) {
	if h.Delay > 0 {
		select {
		case <-time.After(h.Delay):
		case <-r.Context().Done():
			return
		}
	}
	h.Auths.Store(r.Header.Get("Authorization"))
	p := r.URL.Path
	if rest, ok := strings.CutPrefix(p, "/api/models/"); ok {
		h.API.Add(1)
		if h.Status != 0 {
			w.WriteHeader(h.Status)
			return
		}
		i := strings.Index(rest, "/revision/")
		m := h.models[strings.ToLower(rest[:max(i, 0)])]
		if i < 0 || m == nil {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Has("expand[]") {
			h.GGUF.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"sha": m.SHA, "gguf": m.GGUF})
			return
		}
		var sib []map[string]any
		for n, s := range m.Files {
			sib = append(sib, map[string]any{"rfilename": n, "size": s})
		}
		gated := any(false)
		if m.Gated != "" {
			gated = m.Gated
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": m.ID, "sha": m.SHA, "library_name": m.Library, "pipeline_tag": m.Pipeline,
			"gated": gated, "config": map[string]any{"model_type": m.Type},
			"safetensors": map[string]any{"parameters": m.Params}, "siblings": sib})
		return
	}
	if strings.HasSuffix(p, "/config.json") {
		h.Config.Add(1)
		id := strings.TrimSuffix(strings.TrimPrefix(p, "/"), "/config.json")
		id, _, _ = strings.Cut(id, "/resolve/")
		m := h.models[strings.ToLower(id)]
		if m == nil || m.Gated != "" || m.Config == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, m.Config)
		return
	}
	http.NotFound(w, r)
}
