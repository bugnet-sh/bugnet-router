package facts

import (
	"path"
	"slices"
	"strconv"
	"strings"
)

// EntryHints is everything the rules read from the entry command, which itself is never kept
type EntryHints struct {
	// Jobs is the largest -j N
	Jobs int `json:"jobs,omitempty"`
	// ContextTokens is the largest --max-model-len, -c or --ctx-size
	ContextTokens int `json:"context_tokens,omitempty"`
	// DType is auto, f32, f16 or bf16 from --dtype
	DType string `json:"dtype,omitempty"`
	// Tools is a sorted set of the tool names below
	Tools []string `json:"tools,omitempty"`
	// GGUFFile is the model file the command names, only when it is one of the model's own
	GGUFFile string `json:"gguf_file,omitempty"`
}

// The tool names a hint may hold
const (
	ToolVLLM     = "vllm"
	ToolSGLang   = "sglang"
	ToolLlamaCPP = "llamacpp"
	ToolDocker   = "docker"
	ToolCompose  = "compose"
	ToolCUDA     = "cuda"
	ToolGPUs     = "gpus"
)

// Installers are the installer tool names, CP 4.7's list
var Installers = []string{"apt-get", "cargo", "go", "make", "npm", "pip", "pnpm", "uv", "yarn"}

const (
	maxJobs    = 1024
	maxContext = 10_000_000
)

var installerAlias = map[string]string{"pip3": "pip", "apt": "apt-get"}

var llamaCPPBinaries = map[string]bool{
	"llama-server": true, "llama-cli": true, "llama.cpp": true, "llama-cpp": true,
}

// prefixCommands run the command after them, which keeps its place as the command
var prefixCommands = map[string]bool{
	"sudo": true, "env": true, "time": true, "nohup": true, "exec": true,
}

var shells = map[string]bool{"sh": true, "bash": true}

var dtypes = map[string]string{
	"auto": "auto", "float32": "f32", "float": "f32", "fp32": "f32", "float16": "f16",
	"half": "f16", "fp16": "f16", "bfloat16": "bf16", "bf16": "bf16",
}

// Hints reads typed values out of an entry command, a pure function of cmd and f
func Hints(cmd string, f Facts) EntryHints {
	var h EntryHints
	tools := map[string]bool{}
	words := shellWords(cmd)
	atCommand := true
	for i, w := range words {
		if w.op {
			atCommand = true
			continue
		}
		lw := strings.ToLower(w.s)
		base := path.Base(lw)
		next := ""
		if i+1 < len(words) && !words[i+1].op {
			next = words[i+1].s
		}
		flag, _, hasInline := strings.Cut(lw, "=")
		value := next
		if hasInline {
			value = w.s[len(flag)+1:]
		}
		if strings.Contains(lw, "cuda") {
			tools[ToolCUDA] = true
		}
		if atCommand && (hasInline && !strings.HasPrefix(lw, "-") || prefixCommands[base] ||
			shells[base] && next == "-c") {
			continue
		}
		if lw == "-c" && i > 0 && shells[path.Base(words[i-1].s)] {
			atCommand = true
			continue
		}
		if atCommand {
			name := base
			if a, ok := installerAlias[name]; ok {
				name = a
			}
			if slices.Contains(Installers, name) {
				tools[name] = true
			}
		}
		if (base == "python" || base == "python3") && next == "-m" && i+2 < len(words) {
			if mod := strings.ToLower(words[i+2].s); mod == "pip" {
				tools["pip"] = true
			}
		}
		atCommand = false
		switch {
		case base == "vllm" || strings.HasPrefix(lw, "vllm.") || strings.HasPrefix(lw, "vllm/"):
			tools[ToolVLLM] = true
		case base == "sglang" || strings.HasPrefix(lw, "sglang.") ||
			strings.HasPrefix(lw, "lmsysorg/sglang"):
			tools[ToolSGLang] = true
		case llamaCPPBinaries[base] || strings.HasPrefix(lw, "llama_cpp"):
			tools[ToolLlamaCPP] = true
		case base == "docker-compose":
			tools[ToolCompose] = true
			tools[ToolDocker] = true
		case base == "docker":
			tools[ToolDocker] = true
			if strings.ToLower(next) == "compose" {
				tools[ToolCompose] = true
			}
		}
		switch flag {
		case "--gpus":
			tools[ToolGPUs] = true
		case "-j", "--jobs":
			if n, ok := count(value, maxJobs); ok {
				h.Jobs = max(h.Jobs, n)
			}
		case "--max-model-len", "--ctx-size", "-c", "--context-length":
			if n, ok := count(value, maxContext); ok {
				h.ContextTokens = max(h.ContextTokens, n)
			}
		case "--dtype":
			if d, ok := dtypes[strings.ToLower(value)]; ok {
				h.DType = d
			}
		case "-hf", "--hf-repo", "-hfr":
			if _, quant, ok := strings.Cut(value, ":"); ok {
				h.GGUFFile = cmpOr(h.GGUFFile, ggufByQuant(f, quant))
			}
		case "-m", "--model", "--hf-file", "-hff":
			h.GGUFFile = cmpOr(h.GGUFFile, ggufByName(f, value))
		}
		if strings.HasPrefix(lw, "-j") && len(lw) > 2 && !hasInline {
			if n, ok := count(lw[2:], maxJobs); ok {
				h.Jobs = max(h.Jobs, n)
			}
		}
		if strings.HasSuffix(lw, ".gguf") {
			h.GGUFFile = cmpOr(h.GGUFFile, ggufByName(f, w.s))
		}
	}
	for t := range tools {
		h.Tools = append(h.Tools, t)
	}
	slices.Sort(h.Tools)
	return h
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func count(s string, limit int) (int, bool) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > limit {
		return 0, false
	}
	return n, true
}

// ggufByName is the model's own .gguf file whose name the command gives, empty when none is
func ggufByName(f Facts, name string) string {
	if f.Model == nil {
		return ""
	}
	want := strings.ToLower(path.Base(name))
	for _, file := range f.Model.Files {
		if strings.ToLower(path.Base(file.Path)) == want && strings.HasSuffix(want, ".gguf") {
			return file.Path
		}
	}
	return ""
}

// ggufByQuant is the model's first .gguf file named with the quantisation tag, such as Q5_K_M
func ggufByQuant(f Facts, quant string) string {
	if f.Model == nil || quant == "" {
		return ""
	}
	q := strings.ToLower(quant)
	files := slices.Clone(f.Model.Files)
	slices.SortFunc(files, func(a, b File) int { return strings.Compare(a.Path, b.Path) })
	for _, file := range files {
		lp := strings.ToLower(path.Base(file.Path))
		if strings.HasSuffix(lp, ".gguf") && strings.Contains(lp, q) {
			return file.Path
		}
	}
	return ""
}

type word struct {
	s  string
	op bool
}

// shellWords splits a command into words and operators, reading inside quotes too for sh -c
func shellWords(cmd string) []word {
	var out []word
	var cur strings.Builder
	inWord := false
	quote := rune(0)
	flush := func() {
		if inWord {
			out = append(out, word{s: cur.String()})
			cur.Reset()
			inWord = false
		}
	}
	for _, r := range cmd {
		switch {
		case quote != 0 && r == quote:
			quote = 0
		case quote != 0 && strings.ContainsRune(";&|()", r):
			flush()
			out = append(out, word{s: string(r), op: true})
		case quote != 0 && (r == ' ' || r == '\t' || r == '\n'):
			flush()
		case quote != 0:
			cur.WriteRune(r)
			inWord = true
		case r == '\'' || r == '"':
			quote = r
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			flush()
		case strings.ContainsRune(";&|()", r):
			flush()
			out = append(out, word{s: string(r), op: true})
		default:
			cur.WriteRune(r)
			inWord = true
		}
	}
	flush()
	return out
}
