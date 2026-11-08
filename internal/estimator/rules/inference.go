package rules

import (
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
)

const (
	// gpuBaseBytes is the CUDA context, cpuBaseBytes the process and the OS
	gpuBaseBytes = 512 * mib
	cpuBaseBytes = 1 * gib
	// loadBaseBytes is the system memory a GPU load stages through beside a quarter of the weights
	loadBaseBytes = 2 * gib
	// diskBaseBytes is the disk beside the model files
	diskBaseBytes = 5 * gib
	// downloadBytesPerSecond is 50 MiB a second, a STARTING VALUE
	downloadBytesPerSecond = 50 * mib
	// ratioDivisor makes 4e-5 bytes of KV cache a token per parameter
	ratioDivisor = 25_000
	// minContext is the smallest context a package may fit through
	minContext = 2048
	// recommendGPUParams is where a text generation model starts to want a GPU
	recommendGPUParams = 3_000_000_000
)

// kvRow is the published architecture of a model family near a parameter count
type kvRow struct {
	arch                     string
	params                   int64
	layers, kvHeads, headDim int64
}

// kvTable answers the KV cache of gated or GGUF models whose config is unread
var kvTable = []kvRow{
	{"gemma2", 2_610_000_000, 26, 4, 256},
	{"gemma2", 9_240_000_000, 42, 8, 256},
	{"llama", 1_240_000_000, 16, 8, 64},
	{"llama", 3_210_000_000, 28, 8, 128},
	{"llama", 8_030_000_000, 32, 8, 128},
	{"llama", 70_600_000_000, 80, 8, 128},
	{"mistral", 7_250_000_000, 32, 8, 128},
	{"phi3", 3_820_000_000, 32, 32, 96},
	{"qwen2", 494_000_000, 24, 2, 64},
	{"qwen2", 1_540_000_000, 28, 2, 128},
	{"qwen2", 3_090_000_000, 36, 2, 128},
	{"qwen2", 7_620_000_000, 28, 4, 128},
	{"qwen2", 14_800_000_000, 48, 8, 128},
	{"qwen2", 32_800_000_000, 64, 8, 128},
	{"qwen2", 72_700_000_000, 80, 8, 128},
}

var generationTags = map[string]bool{
	"text-to-image": true, "image-to-image": true, "text-to-video": true, "image-to-video": true,
	"video-to-video": true, "text-to-3d": true, "image-to-3d": true,
	"unconditional-image-generation": true,
}

var textGenerationTags = map[string]bool{
	"text-generation": true, "text2text-generation": true, "image-text-to-text": true,
}

// modelFloor is what one model needs, before the GPU need picks a path
type modelFloor struct {
	weights, cpuWeights, filesBytes int64
	kvPerToken                      int64
	context                         int
	contextFromCommand              bool
	llm, gguf, image                bool
	gpu                             GPUNeed
	gpuMem, cpuMem, sysMem, disk    int64
	kvFrom                          string
	unknownDType, transformers4     bool
	gated                           bool
}

func analyzeModel(in Input, m *facts.Model) *modelFloor {
	mf := &modelFloor{gated: m.Gated == "auto" || m.Gated == "manual"}
	files := countedFiles(m, in.Request.Hints.GGUFFile)
	for _, f := range files {
		mf.filesBytes += max(f.Size, 0)
	}
	mf.gguf = slices.ContainsFunc(m.Files, func(f facts.File) bool { return isGGUF(f.Path) })
	diffusers := isDiffusers(m)
	params := int64(0)
	for _, p := range m.Parameters {
		params += max(p.Count, 0)
	}
	switch {
	case mf.gguf || diffusers || len(m.Parameters) == 0:
		mf.weights = mf.filesBytes
	default:
		for _, p := range m.Parameters {
			b, ok := dtypeBytes(p.DType)
			mf.unknownDType = mf.unknownDType || !ok
			mf.weights += max(p.Count, 0) * b
		}
	}
	if params == 0 && m.GGUF != nil {
		params = m.GGUF.Parameters
	}
	if params == 0 {
		params = paramsFromName(m.ID)
	}
	mf.cpuWeights = mf.weights
	repo := in.Facts.Repo
	if repo != nil && repo.Transformers4 && in.Request.Hints.DType == "" && !mf.gguf &&
		!diffusers && len(m.Parameters) > 0 {
		mf.transformers4 = true
		mf.cpuWeights = params * 4
	}
	mf.llm = mf.gguf || textGenerationTags[m.Pipeline]
	if mf.llm {
		mf.kvPerToken, mf.kvFrom = kvPerToken(m, params)
		mf.context = in.Request.DefaultContextTokens
		if c := in.Request.Hints.ContextTokens; c > 0 {
			mf.context, mf.contextFromCommand = c, true
		}
	}
	switch {
	case generationTags[m.Pipeline] || diffusers:
		mf.gpu, mf.image = GPURequired, true
	case textGenerationTags[m.Pipeline] && params > recommendGPUParams && !mf.gguf:
		mf.gpu = GPURecommended
	default:
		mf.gpu = GPUNone
	}
	kv := mf.kvPerToken * int64(mf.context)
	mf.gpuMem = ceilDiv(mf.weights*6, 5) + kv + gpuBaseBytes
	mf.cpuMem = ceilDiv(mf.cpuWeights*6, 5) + kv + cpuBaseBytes
	mf.sysMem = mf.weights/4 + loadBaseBytes
	mf.disk = ceilDiv(mf.filesBytes*6, 5) + diskBaseBytes
	return mf
}

func (e *eval) modelRules() {
	mf := e.model
	e.signals = true
	e.low = e.low || mf.kvFrom == "ratio" || mf.llm && mf.gated && e.in.Facts.Model.Config == nil
	e.downloadBytes = mf.filesBytes
	e.seconds.hardAdd += ceilDiv(mf.filesBytes, downloadBytesPerSecond)
	e.diskMiB.setHard(ceilDiv(mf.disk, mib))
	if mf.unknownDType {
		e.reason("hf.dtype", "An unknown dtype counts 2 bytes a parameter")
	}
	switch {
	case mf.gguf:
		e.reason("hf.weights", "The chosen GGUF file holds %s GiB of weights", gib2(mf.weights))
	case mf.image:
		e.reason("hf.weights", "Every component together holds %s GiB of weights",
			gib2(mf.weights))
	default:
		e.reason("hf.weights", "The weights take %s GiB", gib2(mf.weights))
	}
	if mf.transformers4 {
		e.reason("hf.transformers4", "Transformers below 5 loads float 32 on CPU, 4 bytes a "+
			"parameter")
	}
	if mf.llm {
		e.reason("hf.kv", "The KV cache takes %d KiB a token from %s, %s GiB at %d tokens",
			ceilDiv(mf.kvPerToken, 1024), kvFromText[mf.kvFrom], gib2(mf.kvPerToken*
				int64(mf.context)), mf.context)
	}
	e.needGPU(mf.gpu)
	switch mf.gpu {
	case GPURequired:
		e.reason("hf.gpu", "An image or video generation model needs a GPU")
	case GPURecommended:
		e.reason("hf.gpu", "A text generation model above 3 billion parameters runs best on "+
			"a GPU")
	}
	switch e.gpu {
	case GPUNone:
		e.memMiB.setHard(ceilDiv(mf.cpuMem, mib))
		e.reason("hf.memory", "Needs %s GiB of memory on CPU", gib2(mf.cpuMem))
	case GPURecommended:
		e.memMiB.setHard(ceilDiv(mf.sysMem, mib))
		e.gpuMemBytes = max(e.gpuMemBytes, mf.gpuMem)
		e.reason("hf.memory", "Needs %s GiB of GPU memory, or %s GiB of memory on CPU",
			gib2(mf.gpuMem), gib2(mf.cpuMem))
	default:
		e.memMiB.setHard(ceilDiv(mf.sysMem, mib))
		e.gpuMemBytes = max(e.gpuMemBytes, mf.gpuMem)
		e.reason("hf.memory", "Needs %s GiB of GPU memory and %s GiB of memory to load it",
			gib2(mf.gpuMem), gib2(mf.sysMem))
	}
	e.reason("hf.disk", "Downloads %s GiB of model files", gib2(mf.filesBytes))
}

var kvFromText = map[string]string{
	"config": "the model's config", "table": "its architecture", "ratio": "a fallback ratio",
}

func isGGUF(p string) bool { return strings.HasSuffix(strings.ToLower(p), ".gguf") }

func isDiffusers(m *facts.Model) bool {
	return m.Library == "diffusers" || slices.ContainsFunc(m.Files, func(f facts.File) bool {
		return f.Path == "model_index.json"
	})
}

var pickleExt = []string{".bin", ".pt", ".pth", ".onnx", ".ckpt"}

// countedFiles is the files a run loads, CP 4.5's list with duplicates skipped
func countedFiles(m *facts.Model, ggufHint string) []facts.File {
	var gguf, root, pickle []facts.File
	for _, f := range m.Files {
		lp := strings.ToLower(f.Path)
		switch {
		case isGGUF(lp):
			gguf = append(gguf, f)
		case !strings.Contains(lp, "/") && strings.HasSuffix(lp, ".safetensors"):
			root = append(root, f)
		case !strings.HasPrefix(lp, "original/") && slices.ContainsFunc(pickleExt,
			func(ext string) bool { return strings.HasSuffix(lp, ext) }):
			pickle = append(pickle, f)
		}
	}
	switch {
	case len(gguf) > 0:
		return chosenGGUF(gguf, ggufHint)
	case isDiffusers(m):
		return diffusersFiles(m.Files)
	case len(root) > 0:
		return root
	}
	return pickle
}

var ggufPart = regexp.MustCompile(`-\d{5}-of-\d{5}\.gguf$`)

// chosenGGUF is the named file, else Q4_K_M as llama.cpp picks, else the first, with its parts
func chosenGGUF(files []facts.File, hint string) []facts.File {
	pick := -1
	for i, f := range files {
		if f.Path == hint {
			pick = i
		}
	}
	if pick < 0 {
		pick = slices.IndexFunc(files, func(f facts.File) bool {
			return strings.Contains(strings.ToLower(f.Path), "q4_k_m")
		})
	}
	pick = max(pick, 0)
	chosen := files[pick]
	loc := ggufPart.FindStringIndex(strings.ToLower(chosen.Path))
	if loc == nil {
		return []facts.File{chosen}
	}
	prefix := strings.ToLower(chosen.Path[:loc[0]])
	var parts []facts.File
	for _, f := range files {
		lp := strings.ToLower(f.Path)
		if l := ggufPart.FindStringIndex(lp); l != nil && lp[:l[0]] == prefix {
			parts = append(parts, f)
		}
	}
	return parts
}

// diffusersFiles keeps one variant of each component folder, the default when present
func diffusersFiles(files []facts.File) []facts.File {
	type variant struct {
		files []facts.File
		size  int64
	}
	byFolder := map[string]map[string]*variant{}
	var folders []string
	for _, f := range files {
		folder, name, ok := strings.Cut(f.Path, "/")
		if !ok || strings.Contains(name, "/") || !strings.HasSuffix(name, ".safetensors") {
			continue
		}
		v := variantOf(name)
		if byFolder[folder] == nil {
			byFolder[folder] = map[string]*variant{}
			folders = append(folders, folder)
		}
		if byFolder[folder][v] == nil {
			byFolder[folder][v] = &variant{}
		}
		byFolder[folder][v].files = append(byFolder[folder][v].files, f)
		byFolder[folder][v].size += f.Size
	}
	slices.Sort(folders)
	var out []facts.File
	for _, folder := range folders {
		vs := byFolder[folder]
		names := make([]string, 0, len(vs))
		for n := range vs {
			names = append(names, n)
		}
		slices.Sort(names)
		best := names[0]
		if _, ok := vs[""]; !ok {
			for _, n := range names {
				if vs[n].size < vs[best].size {
					best = n
				}
			}
		}
		out = append(out, vs[best].files...)
	}
	return out
}

// variantOf reads fp16 out of diffusion_pytorch_model.fp16.safetensors, empty for the default
func variantOf(name string) string {
	stem := strings.TrimSuffix(name, ".safetensors")
	if i := strings.LastIndex(stem, "."); i >= 0 {
		return stem[i+1:]
	}
	return ""
}

func dtypeBytes(d string) (int64, bool) {
	switch u := strings.ToUpper(d); {
	case u == "F64" || u == "I64" || u == "U64":
		return 8, true
	case u == "F32" || u == "I32" || u == "U32":
		return 4, true
	case u == "F16" || u == "BF16" || u == "I16" || u == "U16":
		return 2, true
	case strings.HasPrefix(u, "F8") || u == "I8" || u == "U8" || u == "BOOL":
		return 1, true
	}
	return 2, false
}

var nameSize = regexp.MustCompile(`(?i)(?:^|[^A-Za-z0-9.])(\d{1,4})(?:\.(\d{1,3}))?([MB])\b`)

// paramsFromName reads a suffix such as -82M or -3.2B, the last one in the id
func paramsFromName(id string) int64 {
	all := nameSize.FindAllStringSubmatch(id, -1)
	if len(all) == 0 {
		return 0
	}
	m := all[len(all)-1]
	whole, _ := strconv.ParseInt(m[1], 10, 64)
	frac := m[2] + strings.Repeat("0", 3-len(m[2]))
	thousandths, _ := strconv.ParseInt(frac, 10, 64)
	unit := int64(1_000_000)
	if strings.EqualFold(m[3], "B") {
		unit = 1_000_000_000
	}
	return (whole*1000 + thousandths) * unit / 1000
}

// kvPerToken is 2 x layers x kv heads x head size x 2 bytes, from config, table or ratio
func kvPerToken(m *facts.Model, params int64) (int64, string) {
	if c := m.Config; c != nil && c.Layers > 0 {
		heads := int64(c.KVHeads)
		if heads == 0 {
			heads = int64(c.AttentionHeads)
		}
		dim := int64(c.HeadDim)
		if dim == 0 && c.AttentionHeads > 0 {
			dim = int64(c.HiddenSize / c.AttentionHeads)
		}
		if heads > 0 && dim > 0 {
			return 2 * int64(c.Layers) * heads * dim * 2, "config"
		}
	}
	arch := m.ModelType
	if arch == "" && m.GGUF != nil {
		arch = m.GGUF.Architecture
	}
	var best *kvRow
	for i := range kvTable {
		r := &kvTable[i]
		if r.arch != arch || params < r.params*3/4 || params > r.params*5/4 {
			continue
		}
		if best == nil || absDiff(params, r.params) < absDiff(params, best.params) {
			best = r
		}
	}
	if best != nil {
		return 2 * best.layers * best.kvHeads * best.headDim * 2, "table"
	}
	return ceilDiv(params, ratioDivisor), "ratio"
}

func absDiff(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return b - a
}
