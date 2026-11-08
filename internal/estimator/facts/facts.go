// Package facts distils a source's files into typed facts the estimator's rules read
package facts

import (
	"cmp"
	"slices"
)

// ExtractorVersion names the facts format, facts are cached under it
const ExtractorVersion = "facts-1"

// SignalFiles are the paths the estimator reads at a commit, relative to the source's root
var SignalFiles = []string{
	".devcontainer.json", ".devcontainer/devcontainer.json", "Dockerfile", "compose.yaml",
	"compose.yml", "docker-compose.yaml", "docker-compose.yml", "package-lock.json",
	"pnpm-lock.yaml", "yarn.lock", "bun.lock", "uv.lock", "poetry.lock", "Pipfile.lock",
	"requirements.txt", "requirements-dev.txt", "pyproject.toml", "Cargo.lock", "go.mod",
	"go.sum", "Gemfile.lock", "composer.lock", ".python-version", ".nvmrc", ".node-version",
	".tool-versions", "rust-toolchain", "rust-toolchain.toml", ".gitattributes", ".gitmodules",
}

// SignalDirs are the directories whose every entry the estimator reads
var SignalDirs = []string{".github/workflows", ".devcontainer"}

// Facts is everything the estimator knows about one source at one revision
type Facts struct {
	Repo  *Repo  `json:"repo,omitempty"`
	Model *Model `json:"model,omitempty"`
}

// Blob is one entry of a repository tree
type Blob struct {
	Path string
	SHA  string
	Size int64
	// Type is blob, tree or commit, as GitHub's tree answer names it
	Type string
}

// Repo holds what a repository's files say about the run
type Repo struct {
	// Devcontainers counts the devcontainer files found
	Devcontainers int `json:"devcontainers,omitempty"`
	// Host is the largest of their hostRequirements
	Host          *Host       `json:"host,omitempty"`
	Dockerfile    bool        `json:"dockerfile,omitempty"`
	CUDABase      bool        `json:"cuda_base,omitempty"`
	Compose       *Compose    `json:"compose,omitempty"`
	CI            *CI         `json:"ci,omitempty"`
	Lockfiles     []Lockfile  `json:"lockfiles,omitempty"`
	ML            []string    `json:"ml,omitempty"`
	Transformers4 bool        `json:"transformers_below_5,omitempty"`
	CargoCrates   int         `json:"cargo_crates,omitempty"`
	NativeBuild   bool        `json:"native_build,omitempty"`
	Pins          []Pin       `json:"pins,omitempty"`
	Tree          TreeSummary `json:"tree"`
	// LockKey is the hex SHA-256 of every lockfile's path and blob SHA in the tree
	LockKey string `json:"lock_key,omitempty"`
}

// Host is a devcontainer's hostRequirements
type Host struct {
	CPUs       int `json:"cpus,omitempty"`
	MemoryMiB  int `json:"memory_mib,omitempty"`
	StorageGiB int `json:"storage_gib,omitempty"`
	// GPU is required, optional or empty
	GPU          string `json:"gpu,omitempty"`
	GPUCores     int    `json:"gpu_cores,omitempty"`
	GPUMemoryGiB int    `json:"gpu_memory_gib,omitempty"`
}

// Compose is every service of the compose files found
type Compose struct {
	Files    int       `json:"files"`
	Services []Service `json:"services,omitempty"`
}

// Service is one compose or CI service, its image kept only as a known class
type Service struct {
	Class             string `json:"class"`
	ReservedMilliCPU  int    `json:"reserved_millicpu,omitempty"`
	ReservedMemoryMiB int    `json:"reserved_memory_mib,omitempty"`
	LimitMilliCPU     int    `json:"limit_millicpu,omitempty"`
	LimitMemoryMiB    int    `json:"limit_memory_mib,omitempty"`
	GPU               bool   `json:"gpu,omitempty"`
}

// CI is what the GitHub workflows say about the test jobs
type CI struct {
	Jobs int `json:"jobs"`
	// TestTimeoutMinutes is the test jobs' largest timeout when under 360
	TestTimeoutMinutes int  `json:"test_timeout_minutes,omitempty"`
	LargestCores       int  `json:"largest_cores,omitempty"`
	GPURunner          bool `json:"gpu_runner,omitempty"`
	// StandardOnly is true when every test job runs on a standard hosted Ubuntu runner
	StandardOnly bool     `json:"standard_only,omitempty"`
	Services     []string `json:"services,omitempty"`
}

// Lockfile is the package count of one lockfile kind
type Lockfile struct {
	Kind     string `json:"kind"`
	Packages int    `json:"packages"`
}

// Pin is a toolchain version a repository asks for
type Pin struct {
	Tool    string `json:"tool"`
	Version string `json:"version"`
}

// TreeSummary is the size of a checkout
type TreeSummary struct {
	Bytes      int64 `json:"bytes"`
	Blobs      int   `json:"blobs"`
	Submodules int   `json:"submodules,omitempty"`
	// LFS is true when .gitattributes routes files through Git LFS
	LFS         bool  `json:"lfs,omitempty"`
	LFSPointers int   `json:"lfs_pointers,omitempty"`
	LFSBytes    int64 `json:"lfs_bytes,omitempty"`
}

// Model holds what the Hugging Face Hub says about a model at one revision
type Model struct {
	ID        string `json:"id"`
	Revision  string `json:"revision"`
	Library   string `json:"library,omitempty"`
	Pipeline  string `json:"pipeline,omitempty"`
	Gated     string `json:"gated,omitempty"`
	ModelType string `json:"model_type,omitempty"`
	// Parameters is safetensors.parameters by dtype
	Parameters []DTypeCount  `json:"parameters,omitempty"`
	Files      []File        `json:"files,omitempty"`
	Config     *ModelConfig  `json:"config,omitempty"`
	GGUF       *GGUFMetadata `json:"gguf,omitempty"`
}

// DTypeCount is a parameter count of one dtype
type DTypeCount struct {
	DType string `json:"dtype"`
	Count int64  `json:"count"`
}

// File is one file of a model repository
type File struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// ModelConfig is the part of config.json the memory rules read
type ModelConfig struct {
	Layers         int    `json:"layers,omitempty"`
	KVHeads        int    `json:"kv_heads,omitempty"`
	HeadDim        int    `json:"head_dim,omitempty"`
	HiddenSize     int    `json:"hidden_size,omitempty"`
	AttentionHeads int    `json:"attention_heads,omitempty"`
	MaxPosition    int    `json:"max_position,omitempty"`
	DType          string `json:"dtype,omitempty"`
	Quantized      bool   `json:"quantized,omitempty"`
}

// GGUFMetadata is the Hub's gguf summary of a repository
type GGUFMetadata struct {
	Architecture  string `json:"architecture,omitempty"`
	ContextLength int    `json:"context_length,omitempty"`
	Parameters    int64  `json:"parameters,omitempty"`
}

// Normalize sorts every list, so permuted facts give the same bytes
func (f *Facts) Normalize() {
	if r := f.Repo; r != nil {
		if r.Compose != nil {
			slices.SortFunc(r.Compose.Services, compareService)
		}
		if r.CI != nil {
			slices.Sort(r.CI.Services)
		}
		slices.SortFunc(r.Lockfiles, func(a, b Lockfile) int {
			return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Packages, b.Packages))
		})
		slices.Sort(r.ML)
		slices.SortFunc(r.Pins, func(a, b Pin) int {
			return cmp.Or(cmp.Compare(a.Tool, b.Tool), cmp.Compare(a.Version, b.Version))
		})
	}
	if m := f.Model; m != nil {
		slices.SortFunc(m.Parameters, func(a, b DTypeCount) int {
			return cmp.Or(cmp.Compare(a.DType, b.DType), cmp.Compare(a.Count, b.Count))
		})
		slices.SortFunc(m.Files, func(a, b File) int {
			return cmp.Or(cmp.Compare(a.Path, b.Path), cmp.Compare(a.Size, b.Size))
		})
	}
}

func compareService(a, b Service) int {
	return cmp.Or(cmp.Compare(a.Class, b.Class),
		cmp.Compare(a.ReservedMilliCPU, b.ReservedMilliCPU),
		cmp.Compare(a.ReservedMemoryMiB, b.ReservedMemoryMiB),
		cmp.Compare(a.LimitMilliCPU, b.LimitMilliCPU),
		cmp.Compare(a.LimitMemoryMiB, b.LimitMemoryMiB),
		cmp.Compare(boolInt(a.GPU), boolInt(b.GPU)))
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Clone copies f deeply enough that sorting the copy leaves f untouched
func (f Facts) Clone() Facts {
	if f.Repo != nil {
		r := *f.Repo
		if r.Compose != nil {
			c := *r.Compose
			c.Services = slices.Clone(c.Services)
			r.Compose = &c
		}
		if r.CI != nil {
			c := *r.CI
			c.Services = slices.Clone(c.Services)
			r.CI = &c
		}
		r.Lockfiles, r.ML, r.Pins = slices.Clone(r.Lockfiles), slices.Clone(r.ML),
			slices.Clone(r.Pins)
		f.Repo = &r
	}
	if f.Model != nil {
		m := *f.Model
		m.Parameters, m.Files = slices.Clone(m.Parameters), slices.Clone(m.Files)
		f.Model = &m
	}
	return f
}
