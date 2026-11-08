// Package rules turns an estimate's normalised request and facts into a floor and package fits
package rules

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
)

// Version names the rule set, any change to a rule or a constant bumps it
const Version = "est-2026.11.1"

const (
	mib = int64(1) << 20
	gib = int64(1) << 30
)

// Workload is what the agent says it will run
type Workload string

// The workloads of bugnet_quote
const (
	WorkloadUnset     Workload = ""
	WorkloadTests     Workload = "tests"
	WorkloadBuild     Workload = "build"
	WorkloadScript    Workload = "script"
	WorkloadServe     Workload = "serve"
	WorkloadInference Workload = "inference"
	WorkloadOther     Workload = "other"
)

// Need is a feature the work needs
type Need string

// The needs of bugnet_quote
const (
	NeedSSH     Need = "ssh"
	NeedPreview Need = "preview"
	NeedSecrets Need = "secrets"
	NeedGPU     Need = "gpu"
)

// GPUNeed says how much the run wants a GPU
type GPUNeed string

// GPU needs, weakest first
const (
	GPUNone        GPUNeed = "none"
	GPURecommended GPUNeed = "recommended"
	GPURequired    GPUNeed = "required"
)

func (g GPUNeed) rank() int {
	return map[GPUNeed]int{GPURecommended: 1, GPURequired: 2}[g]
}

// Basis says what the floor rests on
type Basis string

// The bases of EstimateBasis
const (
	BasisDefaults Basis = "defaults"
	BasisSignals  Basis = "signals"
	BasisHistory  Basis = "history"
)

// Confidence says how far the floor can be trusted
type Confidence string

// The confidences of Confidence
const (
	ConfidenceLow    Confidence = "low"
	ConfidenceMedium Confidence = "medium"
	ConfidenceHigh   Confidence = "high"
)

// Dimension is one resource a package can lack
type Dimension string

// The dimensions, in the order of the proto enum
const (
	DimensionVCPU      Dimension = "vcpu"
	DimensionMemory    Dimension = "memory"
	DimensionDisk      Dimension = "disk"
	DimensionGPU       Dimension = "gpu"
	DimensionGPUMemory Dimension = "gpu_memory"
	DimensionMinutes   Dimension = "minutes"
)

// UnreadReason says why a source was not read
type UnreadReason string

// The reasons of UnreadReason
const (
	UnreadNotFound      UnreadReason = "not_found"
	UnreadLegal         UnreadReason = "legal"
	UnreadRateLimited   UnreadReason = "rate_limited"
	UnreadTimeout       UnreadReason = "timeout"
	UnreadNotSupported  UnreadReason = "not_supported"
	UnreadUpstreamError UnreadReason = "upstream_error"
)

// ResolvedSource is the source as the fetchers resolved it, never the raw string
type ResolvedSource struct {
	// Kind is a source.Kind
	Kind      string       `json:"kind"`
	Canonical string       `json:"canonical,omitempty"`
	Ref       string       `json:"ref,omitempty"`
	Subdir    []string     `json:"subdir,omitempty"`
	Revision  string       `json:"revision,omitempty"`
	Read      bool         `json:"read"`
	Unread    UnreadReason `json:"unread,omitempty"`
}

// Request is the normalised arguments of one Estimate
type Request struct {
	Source   ResolvedSource `json:"source"`
	Workload Workload       `json:"workload,omitempty"`
	// Minutes is expected_minutes, 0 when the agent gave none
	Minutes int              `json:"minutes,omitempty"`
	Needs   []Need           `json:"needs,omitempty"`
	Hints   facts.EntryHints `json:"entry_hints"`
	// EntrySHA256 is the hex SHA-256 of the entry command, which stands in for its text
	EntrySHA256 string `json:"entry_sha256,omitempty"`
	// DefaultContextTokens is the setting default_context_tokens, 8,192 when zero
	DefaultContextTokens int `json:"default_context_tokens,omitempty"`
	// Partial is true when the fetch budget ended before every fact was read
	Partial bool `json:"partial,omitempty"`
}

// History is the observed peaks for a source, the zero value when there are none
type History struct {
	Observations  int      `json:"observations,omitempty"`
	Payers        int      `json:"payers,omitempty"`
	MemoryMiB     int      `json:"memory_mib,omitempty"`
	DiskMiB       int      `json:"disk_mib,omitempty"`
	GPUMemoryMiB  int      `json:"gpu_memory_mib,omitempty"`
	ActiveMinutes int      `json:"active_minutes,omitempty"`
	OOMPackages   []string `json:"oom_packages,omitempty"`
}

// ImageTools is one provider's image manifest, a version, none, or empty when not measured
type ImageTools struct {
	Git     string `json:"git"`
	Tar     string `json:"tar"`
	Python3 string `json:"python3"`
	Node    string `json:"node"`
	Docker  string `json:"docker"`
}

// Image is the manifest of one configured provider
type Image struct {
	Provider string     `json:"provider"`
	Tools    ImageTools `json:"tools"`
}

// Input is everything a rule may read, and nothing else
type Input struct {
	Request Request
	Facts   facts.Facts
	History History
	Catalog []catalog.Package
	// Images holds every configured provider's manifest
	Images []Image
	// MaxLifetimeMinutes is the longest run any provider allows, 0 when unlimited
	MaxLifetimeMinutes int
}

// Floor is what the run needs at least
type Floor struct {
	VCPU          int     `json:"vcpu"`
	MemoryMiB     int     `json:"memory_mib"`
	DiskGiB       int     `json:"disk_gib"`
	GPUCount      int     `json:"gpu_count"`
	GPUMemoryGiB  int     `json:"gpu_memory_gib"`
	Minutes       int     `json:"minutes"`
	DownloadBytes int64   `json:"download_bytes"`
	ContextTokens int     `json:"context_tokens"`
	GPU           GPUNeed `json:"gpu"`
}

// Reason is one line of why the floor is what it is
type Reason struct {
	RuleID  string `json:"rule_id"`
	Message string `json:"message"`
}

// Fit is how one start package meets the floor
type Fit struct {
	PackageID          string      `json:"package_id"`
	Fits               bool        `json:"fits"`
	ExtensionsNeeded   int         `json:"extensions_needed,omitempty"`
	ExtensionPackageID string      `json:"extension_package_id,omitempty"`
	TotalPriceMicros   int64       `json:"total_price_micros"`
	ShortOn            []Dimension `json:"short_on,omitempty"`
	// ContextTokens is the largest context that fits, set when under the one assumed
	ContextTokens int `json:"context_tokens,omitempty"`
}

// Output is what the rules answer, before availability picks a package
type Output struct {
	Floor      Floor      `json:"floor"`
	Reasons    []Reason   `json:"reasons"`
	Warnings   []Warning  `json:"warnings"`
	Fits       []Fit      `json:"fits"`
	Basis      Basis      `json:"basis"`
	Confidence Confidence `json:"confidence"`
	Partial    bool       `json:"partial"`
}

/* Workload defaults */

type defaults struct{ vcpu, memMiB, diskGiB, minutes int }

var workloadDefaults = map[Workload]defaults{
	WorkloadTests:     {2, 2048, 10, 10},
	WorkloadBuild:     {4, 4096, 20, 20},
	WorkloadScript:    {1, 1024, 5, 10},
	WorkloadServe:     {2, 2048, 10, 30},
	WorkloadInference: {2, 2048, 10, 20},
	WorkloadOther:     {2, 2048, 10, 10},
}

// workloadDefault is the floor of a workload with no signals at all
func workloadDefault(w Workload) Floor {
	d, ok := workloadDefaults[w]
	if !ok {
		d = workloadDefaults[WorkloadOther]
	}
	return Floor{VCPU: d.vcpu, MemoryMiB: d.memMiB, DiskGiB: d.diskGiB, Minutes: d.minutes,
		GPU: GPUNone}
}

/* Accumulating one dimension */

// dim combines one dimension's contributions by maximum, caps lowering only the soft ones
type dim struct {
	base, hardAdd, hard, softAdd, soft, cap int64
}

func (d *dim) setHard(v int64) { d.hard = max(d.hard, v) }
func (d *dim) setSoft(v int64) { d.soft = max(d.soft, v) }
func (d *dim) capAt(v int64) {
	if d.cap == 0 || v < d.cap {
		d.cap = v
	}
}

func (d dim) value() int64 {
	floor := d.base + d.hardAdd
	soft := max(d.soft, floor+d.softAdd)
	if d.cap > 0 {
		soft = min(soft, d.cap)
	}
	return max(floor, d.hard, soft)
}

/* Evaluation */

type eval struct {
	in       Input
	workload Workload
	reasons  []Reason
	signals  bool
	high     bool
	low      bool

	vcpu, memMiB, diskMiB, seconds dim
	gpu                            GPUNeed
	gpuCount                       int
	gpuMemBytes                    int64
	downloadBytes                  int64
	docker                         bool
	model                          *modelFloor
	// lifetimeCapped is set when the lifetime alone keeps some package short on minutes
	lifetimeCapped bool
}

func (e *eval) reason(rule, format string, args ...any) {
	e.reasons = append(e.reasons, Reason{RuleID: rule, Message: fmt.Sprintf(format, args...)})
}

func (e *eval) needGPU(g GPUNeed) {
	if g.rank() > e.gpu.rank() {
		e.gpu = g
	}
}

// Run applies every rule to in, a pure function of its argument
func Run(in Input) Output {
	in = normalize(in)
	e := &eval{in: in, gpu: GPUNone, workload: in.Request.Workload}
	if e.workload == WorkloadUnset && in.Facts.Model != nil {
		e.workload = WorkloadInference
	}
	e.defaults()
	if r := in.Facts.Repo; r != nil {
		e.repoRules(r)
	}
	e.commandRules()
	e.needRules()
	if m := in.Facts.Model; m != nil {
		e.model = analyzeModel(in, m)
		e.modelRules()
	}
	floor := e.floor()
	fits := e.fits(floor)
	if fits == nil {
		fits = []Fit{}
	}
	out := Output{
		Floor: floor, Reasons: e.reasons, Fits: fits, Basis: BasisDefaults,
		Confidence: ConfidenceLow, Partial: in.Request.Partial,
	}
	if e.signals {
		out.Basis = BasisSignals
		out.Confidence = ConfidenceMedium
	}
	switch {
	case e.low || in.Request.Partial:
		out.Confidence = ConfidenceLow
	case e.high:
		out.Confidence = ConfidenceHigh
	}
	out.Warnings = e.gates(floor, fits)
	return out
}

// normalize copies in with every list sorted, so permuted input gives the same output
func normalize(in Input) Input {
	in.Facts = in.Facts.Clone()
	in.Facts.Normalize()
	in.Request.Needs = slices.Clone(in.Request.Needs)
	slices.Sort(in.Request.Needs)
	in.Request.Needs = slices.Compact(in.Request.Needs)
	in.Request.Hints.Tools = slices.Clone(in.Request.Hints.Tools)
	slices.Sort(in.Request.Hints.Tools)
	if in.Request.DefaultContextTokens <= 0 {
		in.Request.DefaultContextTokens = 8192
	}
	in.Images = slices.Clone(in.Images)
	slices.SortFunc(in.Images, func(a, b Image) int { return cmp.Compare(a.Provider, b.Provider) })
	return in
}

func (e *eval) defaults() {
	d := workloadDefault(e.workload)
	e.vcpu.base = int64(d.VCPU)
	e.memMiB.base = int64(d.MemoryMiB)
	e.diskMiB.base = int64(d.DiskGiB) * 1024
	e.seconds.base = int64(d.Minutes) * 60
	if m := e.in.Request.Minutes; m > 0 {
		e.seconds.base = int64(m) * 60
		e.reason("workload.default", "Starts from the defaults for %s work and %d minutes "+
			"from the agent", workloadName(e.workload), m)
		return
	}
	e.reason("workload.default", "Starts from the defaults for %s work", workloadName(e.workload))
}

func workloadName(w Workload) string {
	if _, ok := workloadDefaults[w]; ok {
		return string(w)
	}
	return string(WorkloadOther)
}

// softVCPU raises vCPU by a bounded step, at most twice the workload default
func (e *eval) softVCPU(n int) {
	e.vcpu.setSoft(min(int64(n), e.vcpu.base*softVCPUStep))
}

// addInstallSeconds counts install time only when the agent gave no minutes of its own
func (e *eval) addInstallSeconds(s int64) {
	if e.in.Request.Minutes == 0 {
		e.seconds.softAdd += s
	}
}

func (e *eval) commandRules() {
	h := e.in.Request.Hints
	if h.Jobs > 0 {
		e.signals = true
		e.softVCPU(h.Jobs)
		e.reason("cmd.jobs", "The command builds with %d jobs", h.Jobs)
	}
	for _, t := range []string{facts.ToolVLLM, facts.ToolSGLang, facts.ToolGPUs,
		facts.ToolCUDA} {
		if slices.Contains(h.Tools, t) {
			e.signals = true
			e.needGPU(GPURequired)
			e.reason("cmd.gpu", "The command runs a GPU server or asks for CUDA")
			break
		}
	}
	if slices.Contains(h.Tools, facts.ToolDocker) || slices.Contains(h.Tools, facts.ToolCompose) {
		e.signals = true
		e.docker = true
		e.reason("cmd.docker", "The command runs Docker")
	}
}

func (e *eval) needRules() {
	if slices.Contains(e.in.Request.Needs, NeedGPU) {
		e.needGPU(GPURequired)
		e.reason("need.gpu", "The agent asks for a GPU")
	}
}

// floor rounds every dimension, memory to 256 MiB, disk to 1 GiB and minutes to 1
func (e *eval) floor() Floor {
	f := Floor{
		VCPU:          int(e.vcpu.value()),
		MemoryMiB:     int(roundUp(e.memMiB.value(), 256)),
		DiskGiB:       int(ceilDiv(e.diskMiB.value(), 1024)),
		Minutes:       int(ceilDiv(e.seconds.value(), 60)),
		DownloadBytes: e.downloadBytes,
		GPU:           e.gpu,
	}
	if e.gpu != GPUNone {
		f.GPUCount = max(1, e.gpuCount)
		f.GPUMemoryGiB = int(ceilDiv(e.gpuMemBytes, gib))
	}
	if e.model != nil && e.model.kvPerToken > 0 {
		f.ContextTokens = e.model.context
	}
	return f
}

func ceilDiv(a, b int64) int64 {
	if a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

func roundUp(a, step int64) int64 {
	return ceilDiv(a, step) * step
}

// gib2 renders bytes as GiB with two decimals, rounded half up
func gib2(b int64) string {
	h := (b*100 + gib/2) / gib
	return fmt.Sprintf("%d.%02d", h/100, h%100)
}

/* Repository rules */

const (
	// softVCPUStep bounds a soft vCPU raise to this many times the workload default
	softVCPUStep = 2
	// mlDiskMiB and mlMemMiB are what torch and its kind pull in, CUDA libraries included
	mlDiskMiB = 8 * 1024
	mlMemMiB  = 2 * 1024
	// perPackageDiskMiB and perPackageMillis price one locked package
	perPackageDiskMiB = 4
	perPackageMillis  = 300
	// nativeVCPU and nativeMemMiB are a native build's floor, nativeSeconds its extra time
	nativeVCPU    = 4
	nativeMemMiB  = 4096
	nativeSeconds = 5 * 60
	// crateSeconds is the build time of one Rust crate
	crateSeconds = 1
)

// standardRunner is a hosted ubuntu-latest runner, 4 vCPU, 16 GiB and 14 GiB of disk
var standardRunner = [3]int64{4, 16 * 1024, 14 * 1024}

// serviceMiB is the memory of a service class, STARTING VALUES of CP 4.5
var serviceMiB = map[string]int64{
	"postgres": 256, "mysql": 512, "redis": 64, "mongo": 512, "rabbitmq": 256, "minio": 256,
	"kafka": 1024, "zookeeper": 256, "elasticsearch": 2048, "clickhouse": 1024, "other": 256,
}

func (e *eval) repoRules(r *facts.Repo) {
	if h := r.Host; h != nil {
		e.signals = true
		e.high = e.high || h.CPUs > 0 || h.MemoryMiB > 0 || h.StorageGiB > 0
		e.vcpu.setHard(int64(h.CPUs))
		e.memMiB.setHard(int64(h.MemoryMiB))
		e.diskMiB.setHard(int64(h.StorageGiB) * 1024)
		if e.high {
			e.reason("dc.host", "%s asks for %s", devcontainersText(r.Devcontainers),
				hostText(h))
		}
		switch h.GPU {
		case "required":
			e.needGPU(GPURequired)
			e.gpuCount = max(e.gpuCount, 1)
			e.gpuMemBytes = max(e.gpuMemBytes, int64(h.GPUMemoryGiB)*gib)
			e.reason("dc.gpu", "A devcontainer needs a GPU with %d GiB of memory",
				h.GPUMemoryGiB)
		case "optional":
			e.needGPU(GPURecommended)
			e.reason("dc.gpu_optional", "A devcontainer can use a GPU")
		}
	}
	if r.CUDABase {
		e.signals = true
		e.needGPU(GPURecommended)
		e.reason("docker.cuda_base", "The Dockerfile builds on a CUDA base image")
	}
	e.composeRules(r)
	e.ciRules(r)
	e.depsRules(r)
	e.pinRules(r)
	e.treeRules(r)
}

func devcontainersText(n int) string {
	if n > 1 {
		return fmt.Sprintf("The largest of %d devcontainers", n)
	}
	return "A devcontainer"
}

// hostText lists the hostRequirements a devcontainer sets
func hostText(h *facts.Host) string {
	var parts []string
	if h.CPUs > 0 {
		parts = append(parts, fmt.Sprintf("%d vCPU", h.CPUs))
	}
	if h.MemoryMiB > 0 {
		parts = append(parts, fmt.Sprintf("%d MiB of memory", h.MemoryMiB))
	}
	if h.StorageGiB > 0 {
		parts = append(parts, fmt.Sprintf("%d GiB of disk", h.StorageGiB))
	}
	return strings.Join(parts, ", ")
}

func (e *eval) composeRules(r *facts.Repo) {
	c := r.Compose
	if c == nil {
		return
	}
	e.signals, e.docker = true, true
	var milli, memMiB, share int64
	gpu := false
	for _, s := range c.Services {
		milli += int64(s.ReservedMilliCPU)
		memMiB += int64(s.ReservedMemoryMiB)
		share += serviceShare(s)
		gpu = gpu || s.GPU
	}
	if milli > 0 || memMiB > 0 {
		e.high = true
		e.vcpu.setHard(ceilDiv(milli, 1000))
		e.memMiB.setHard(memMiB)
		e.reason("compose.limits", "Compose reserves %d millicores and %d MiB of memory",
			milli, memMiB)
	}
	if gpu {
		e.needGPU(GPURequired)
		e.gpuCount = max(e.gpuCount, 1)
		e.reason("compose.gpu", "A compose service needs a GPU")
	}
	ci := int64(0)
	if r.CI != nil {
		for _, s := range r.CI.Services {
			ci += serviceMiB[s]
		}
	}
	e.memMiB.softAdd += max(share, ci)
	if ci > share {
		e.serviceReason(len(r.CI.Services), "CI", ci)
	} else {
		e.serviceReason(len(c.Services), "compose", share)
	}
}

// serviceReason says how much memory n services of one kind need, and nothing when it is none
func (e *eval) serviceReason(n int, kind string, mib int64) {
	if mib <= 0 {
		return
	}
	if n == 1 {
		e.reason("svc.memory", "1 %s service needs about %d MiB of memory", kind, mib)
		return
	}
	e.reason("svc.memory", "%d %s services need about %d MiB of memory", n, kind, mib)
}

// serviceShare is a service's memory, its class's figure bounded by its limit and reservation
func serviceShare(s facts.Service) int64 {
	m, ok := serviceMiB[s.Class]
	if !ok {
		m = serviceMiB["other"]
	}
	if s.LimitMemoryMiB > 0 {
		m = min(m, int64(s.LimitMemoryMiB))
	}
	return max(m, int64(s.ReservedMemoryMiB))
}

func (e *eval) ciRules(r *facts.Repo) {
	ci := r.CI
	if ci == nil {
		return
	}
	e.signals = true
	if ci.TestTimeoutMinutes > 0 {
		e.reason("ci.timeout", "CI stops the test job after %d minutes", ci.TestTimeoutMinutes)
	}
	if ci.LargestCores > 0 {
		e.softVCPU(ci.LargestCores)
		e.reason("ci.runner", "CI tests on a runner with %d cores", ci.LargestCores)
	}
	if ci.GPURunner {
		e.needGPU(GPURecommended)
		e.reason("ci.runner", "CI tests on a GPU runner")
	}
	if r.Compose == nil && len(ci.Services) > 0 {
		var m int64
		for _, s := range ci.Services {
			m += serviceMiB[s]
		}
		e.memMiB.softAdd += m
		e.serviceReason(len(ci.Services), "CI", m)
	}
	if ci.StandardOnly {
		e.vcpu.capAt(standardRunner[0])
		e.memMiB.capAt(standardRunner[1])
		e.diskMiB.capAt(standardRunner[2])
		e.reason("ci.std_runner", "CI passes on a standard runner, so soft signals stop at 4 "+
			"vCPU, 16 GiB of memory and 14 GiB of disk")
	}
}

func (e *eval) depsRules(r *facts.Repo) {
	if len(r.ML) > 0 {
		e.signals = true
		e.diskMiB.softAdd += mlDiskMiB
		e.memMiB.softAdd += mlMemMiB
		e.reason("deps.ml", "Machine learning packages pull in large libraries")
	}
	n := 0
	for _, l := range r.Lockfiles {
		n += l.Packages
	}
	if n > 0 {
		e.signals = true
		e.diskMiB.softAdd += int64(n) * perPackageDiskMiB
		e.addInstallSeconds(ceilDiv(int64(n)*perPackageMillis, 1000))
		e.reason("deps.count", "The lockfiles hold %d packages", n)
	}
	if r.CargoCrates > 0 {
		e.signals = true
		e.vcpu.setSoft(nativeVCPU)
		e.memMiB.setSoft(nativeMemMiB)
		e.addInstallSeconds(int64(r.CargoCrates) * crateSeconds)
		e.reason("build.native", "%d Rust crates to compile", r.CargoCrates)
	}
	if r.NativeBuild {
		e.signals = true
		e.vcpu.setSoft(nativeVCPU)
		e.memMiB.setSoft(nativeMemMiB)
		e.addInstallSeconds(nativeSeconds)
		e.reason("build.native", "C or C++ build files")
	}
}

func (e *eval) pinRules(r *facts.Repo) {
	for _, p := range r.Pins {
		var seconds, diskMiB int64
		switch {
		case p.Tool == "go-toolchain" || p.Tool == "rust" && p.Version == "nightly":
			seconds, diskMiB = 120, 1024
		case (p.Tool == "python3" || p.Tool == "node") && e.pinMatched(p):
			continue
		default:
			seconds, diskMiB = 60, 1024
		}
		e.signals = true
		e.diskMiB.softAdd += diskMiB
		e.addInstallSeconds(seconds)
		e.reason("tc.pin", "A pinned %s toolchain the images may lack adds %s", pinTool(p.Tool),
			minutesText(seconds/60))
	}
}

func minutesText(n int64) string {
	if n == 1 {
		return "1 minute"
	}
	return fmt.Sprintf("%d minutes", n)
}

func pinTool(t string) string {
	return map[string]string{"python3": "Python", "node": "Node", "go": "Go",
		"go-toolchain": "Go", "rust": "Rust"}[t]
}

// pinMatched says whether every provider's image has the pinned version
func (e *eval) pinMatched(p facts.Pin) bool {
	if len(e.in.Images) == 0 {
		return false
	}
	for _, img := range e.in.Images {
		have := img.Tools.Python3
		if p.Tool == "node" {
			have = img.Tools.Node
		}
		if !sameVersion(p.Version, have) {
			return false
		}
	}
	return true
}

// sameVersion compares up to the major and minor parts both sides give
func sameVersion(pin, have string) bool {
	if have == "" || have == "none" {
		return false
	}
	a, b := splitVersion(pin), splitVersion(have)
	n := min(len(a), len(b), 2)
	return n > 0 && slices.Equal(a[:n], b[:n])
}

func splitVersion(v string) []string {
	var out []string
	cur := ""
	for _, r := range v + "." {
		if r == '.' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	return out
}

func (e *eval) treeRules(r *facts.Repo) {
	t := r.Tree
	checkout := t.Bytes + t.LFSBytes
	if checkout > 0 {
		e.signals = true
		e.diskMiB.hardAdd += ceilDiv(2*checkout, mib)
		e.reason("tree.size", "The checkout takes %s GiB, counted twice for the tree and .git",
			gib2(checkout))
	}
	if t.LFS && t.LFSPointers == 0 {
		e.reason("tree.lfs", "Git LFS files may make the checkout larger")
	}
	if t.Submodules > 0 {
		e.reason("tree.submodules", "%d submodules may make the checkout larger", t.Submodules)
	}
}
