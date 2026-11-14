package rules

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
)

// WarningCode is a typed warning, numbered as the proto enum
type WarningCode int

// The warning codes of CP 4.2
const (
	WarningNeedsGPU         WarningCode = 1
	WarningGPURecommended   WarningCode = 2
	WarningTooLarge         WarningCode = 3
	WarningTryQuantized     WarningCode = 4
	WarningGatedModel       WarningCode = 5
	WarningSecrets          WarningCode = 6
	WarningSourceUnread     WarningCode = 7
	WarningNeedsDocker      WarningCode = 8
	WarningNeedsExtension   WarningCode = 9
	WarningPastMaxLifetime  WarningCode = 10
	WarningBusy             WarningCode = 11
	WarningContextAssumed   WarningCode = 12
	WarningRanOutOfMemory   WarningCode = 13
	warningCodeAfterTheLast WarningCode = 14
)

var warningNames = []string{"", "NEEDS_GPU", "GPU_RECOMMENDED", "TOO_LARGE", "TRY_QUANTIZED",
	"GATED_MODEL", "SECRETS", "SOURCE_UNREAD", "NEEDS_DOCKER", "NEEDS_EXTENSION",
	"PAST_MAX_LIFETIME", "BUSY", "CONTEXT_ASSUMED", "RAN_OUT_OF_MEMORY"}

// String is the code's proto name without its prefix
func (c WarningCode) String() string {
	if c <= 0 || c >= warningCodeAfterTheLast {
		return fmt.Sprintf("WARNING_%d", int(c))
	}
	return warningNames[c]
}

// MarshalText writes the code's name, so stored outputs read plainly
func (c WarningCode) MarshalText() ([]byte, error) {
	if c <= 0 || c >= warningCodeAfterTheLast {
		return nil, fmt.Errorf("rules: unknown warning code %d", int(c))
	}
	return []byte(c.String()), nil
}

// UnmarshalText reads a code's name
func (c *WarningCode) UnmarshalText(b []byte) error {
	i := slices.Index(warningNames, string(b))
	if i <= 0 {
		return fmt.Errorf("rules: unknown warning code")
	}
	*c = WarningCode(i)
	return nil
}

// Warning is one typed warning with a line built only from typed values
type Warning struct {
	Code    WarningCode `json:"code"`
	Message string      `json:"message"`
}

// Recommendation is the package availability picks, with the warnings that pick adds
type Recommendation struct {
	// PackageID is empty when no package fits
	PackageID string    `json:"package_id,omitempty"`
	Warnings  []Warning `json:"warnings"`
}

/* Fit */

// path is the floor one package is measured against
type path struct {
	vcpu, memMiB, diskGiB, gpuCount, gpuMemGiB int
	gpuPath                                    bool
}

func startPackages(c []catalog.Package) []catalog.Package {
	var out []catalog.Package
	for _, p := range c {
		if p.ExtensionOf == "" {
			out = append(out, p)
		}
	}
	return out
}

func (e *eval) fits(f Floor) []Fit {
	gpuPath := path{vcpu: f.VCPU, memMiB: f.MemoryMiB, diskGiB: f.DiskGiB,
		gpuCount: f.GPUCount, gpuMemGiB: f.GPUMemoryGiB, gpuPath: true}
	cpuPath := path{vcpu: f.VCPU, memMiB: f.MemoryMiB, diskGiB: f.DiskGiB}
	if f.GPU == GPURecommended && e.model != nil {
		cpuPath.memMiB = int(roundUp(max(int64(f.MemoryMiB), ceilDiv(e.model.cpuMem, mib)),
			256))
	}
	var out []Fit
	cpuFits := false
	for _, p := range startPackages(e.in.Catalog) {
		hasGPU := p.Resources.GPUCount > 0
		pa := cpuPath
		if f.GPU == GPURequired || f.GPU == GPURecommended && hasGPU {
			pa = gpuPath
		}
		fit := e.fit(p, pa, f)
		cpuFits = cpuFits || fit.Fits && !hasGPU
		out = append(out, fit)
	}
	if f.GPU == GPUNone && cpuFits {
		for i, p := range startPackages(e.in.Catalog) {
			if p.Resources.GPUCount > 0 {
				out[i].Fits = false
			}
		}
	}
	return out
}

func (e *eval) fit(p catalog.Package, pa path, f Floor) Fit {
	r := p.Resources
	fit := Fit{PackageID: p.ID, TotalPriceMicros: e.in.price(p)}
	short := func(d Dimension) { fit.ShortOn = append(fit.ShortOn, d) }
	if r.VCPU < pa.vcpu {
		short(DimensionVCPU)
	}
	memShort := r.MemoryMiB < pa.memMiB
	gpuMemShort := pa.gpuPath && r.GPUMemoryGiB < pa.gpuMemGiB
	if ctx, ok := e.contextFit(p, pa, memShort, gpuMemShort); ok {
		fit.ContextTokens = ctx
		memShort = memShort && pa.gpuPath
		gpuMemShort = false
	}
	if memShort {
		short(DimensionMemory)
	}
	if r.DiskGiB < pa.diskGiB {
		short(DimensionDisk)
	}
	if pa.gpuPath && r.GPUCount < pa.gpuCount {
		short(DimensionGPU)
	}
	if gpuMemShort {
		short(DimensionGPUMemory)
	}
	capped := false
	if f.Minutes > p.Minutes {
		ext, n, ok, pastLifetime := e.extension(p, f.Minutes)
		if ok {
			fit.ExtensionsNeeded, fit.ExtensionPackageID = n, ext.ID
			fit.TotalPriceMicros += int64(n) * e.in.price(ext)
		} else {
			short(DimensionMinutes)
			capped = pastLifetime
		}
	}
	fit.Fits = len(fit.ShortOn) == 0
	if capped && len(fit.ShortOn) == 1 {
		e.lifetimeCapped = true
	}
	return fit
}

// contextFit is the largest context the package holds when memory alone falls short
func (e *eval) contextFit(p catalog.Package, pa path, memShort, gpuMemShort bool) (int, bool) {
	mf := e.model
	if mf == nil || mf.kvPerToken == 0 {
		return 0, false
	}
	var avail int64
	switch {
	case pa.gpuPath && gpuMemShort:
		avail = int64(p.Resources.GPUMemoryGiB)*gib - ceilDiv(mf.weights*6, 5) - gpuBaseBytes
	case !pa.gpuPath && memShort:
		avail = int64(p.Resources.MemoryMiB)*mib - ceilDiv(mf.cpuWeights*6, 5) - cpuBaseBytes
	default:
		return 0, false
	}
	ctx := avail / mf.kvPerToken / 256 * 256
	if ctx < minContext || ctx >= int64(mf.context) {
		return 0, false
	}
	return int(ctx), true
}

// extension is the cheapest extension of p and how many cover the minutes, with pastLifetime
// set when they exist but pass the longest lifetime on sale
func (e *eval) extension(p catalog.Package, minutes int) (q catalog.Package, n int, ok,
	pastLifetime bool) {
	var exts []catalog.Package
	for _, q := range e.in.Catalog {
		if q.ExtensionOf == p.ID && q.Minutes > 0 {
			exts = append(exts, q)
		}
	}
	if len(exts) == 0 {
		return catalog.Package{}, 0, false, false
	}
	slices.SortFunc(exts, func(a, b catalog.Package) int {
		return cmp.Or(cmp.Compare(e.in.price(a), e.in.price(b)), cmp.Compare(a.ID, b.ID))
	})
	q = exts[0]
	n = int(ceilDiv(int64(minutes-p.Minutes), int64(q.Minutes)))
	if limit := e.in.MaxLifetimeMinutes; limit > 0 && p.Minutes+n*q.Minutes > limit {
		return catalog.Package{}, 0, false, true
	}
	return q, n, true, false
}

/* Gates */

func (e *eval) gates(f Floor, fits []Fit) []Warning {
	var w []Warning
	add := func(c WarningCode, format string, args ...any) {
		w = append(w, Warning{Code: c, Message: fmt.Sprintf(format, args...)})
	}
	starts := startPackages(e.in.Catalog)
	var largest struct{ vcpu, memMiB, diskGiB, gpuMemGiB int }
	anyGPU := false
	for _, p := range starts {
		r := p.Resources
		largest.vcpu = max(largest.vcpu, r.VCPU)
		largest.memMiB = max(largest.memMiB, r.MemoryMiB)
		largest.diskGiB = max(largest.diskGiB, r.DiskGiB)
		largest.gpuMemGiB = max(largest.gpuMemGiB, r.GPUMemoryGiB)
		anyGPU = anyGPU || r.GPUCount > 0
	}
	anyFits, cpuFits := false, false
	for i, fit := range fits {
		anyFits = anyFits || fit.Fits
		cpuFits = cpuFits || fit.Fits && starts[i].Resources.GPUCount == 0
	}
	if f.GPU == GPURequired && !anyGPU {
		add(WarningNeedsGPU, "Needs a GPU, none is on sale")
	}
	gpuTooLarge := f.GPU != GPUNone && anyGPU && f.GPUMemoryGiB > largest.gpuMemGiB
	if !anyFits && len(starts) > 0 {
		var over []string
		if f.VCPU > largest.vcpu {
			over = append(over, fmt.Sprintf("%d vCPU, the most on sale is %d", f.VCPU,
				largest.vcpu))
		}
		if f.MemoryMiB > largest.memMiB {
			over = append(over, fmt.Sprintf("%d MiB of memory, the most on sale is %d MiB",
				f.MemoryMiB, largest.memMiB))
		}
		if f.DiskGiB > largest.diskGiB {
			over = append(over, fmt.Sprintf("%d GiB of disk, the most on sale is %d GiB",
				f.DiskGiB, largest.diskGiB))
		}
		if gpuTooLarge {
			over = append(over, fmt.Sprintf("%d GiB of GPU memory, the largest GPU on sale "+
				"has %d GiB", f.GPUMemoryGiB, largest.gpuMemGiB))
		}
		if len(over) > 0 {
			add(WarningTooLarge, "Needs %s", strings.Join(over, ", and "))
		}
	}
	if mf := e.model; mf != nil && gpuTooLarge && !cpuFits && mf.weights > 0 {
		kv := mf.kvPerToken * int64(mf.context)
		at := func(bitsHundredths int64) int64 {
			return ceilDiv(ceilDiv(mf.weights*bitsHundredths, 1600)*6, 5) + kv + gpuBaseBytes
		}
		msg := "At 8 bits a weight it needs about %d GiB of GPU memory, at 4 bits about %d GiB"
		if mf.image {
			msg += ", a rough guide for image models"
		}
		add(WarningTryQuantized, msg, ceilDiv(at(850), gib), ceilDiv(at(489), gib))
	}
	if f.GPU == GPURecommended && cpuFits {
		add(WarningGPURecommended, "Runs on CPU, slower than on a GPU")
	}
	if mf := e.model; mf != nil && mf.gated {
		add(WarningGatedModel, "The model is gated, accept its terms and add HF_TOKEN by "+
			"secret link")
	}
	if slices.Contains(e.in.Request.Needs, NeedSecrets) {
		add(WarningSecrets, "Pass secrets by the secret link or the bridge, never in arguments")
	}
	if s := e.in.Request.Source; e.in.Request.Partial || s.Kind != "" && s.Kind != "none" &&
		!s.Read {
		add(WarningSourceUnread, "Defaults used, %s", e.unreadText())
	}
	if e.docker && !e.imagesHaveDocker() {
		add(WarningNeedsDocker, "Docker is not installed in the sandbox image")
	}
	switch limit := e.in.MaxLifetimeMinutes; {
	case limit > 0 && f.Minutes > limit:
		add(WarningPastMaxLifetime, "Needs %d minutes, the longest run on sale is %d minutes",
			f.Minutes, limit)
	case e.lifetimeCapped:
		add(WarningPastMaxLifetime, "Needs %d minutes, whole extensions on some packages pass "+
			"the longest run on sale, %d minutes", f.Minutes, limit)
	}
	if mf := e.model; mf != nil && mf.kvPerToken > 0 {
		add(WarningContextAssumed, "%s", e.contextText(fits))
	}
	if e.ranOut {
		add(WarningRanOutOfMemory, "Earlier runs ran out of memory on %s with %d MiB, so more "+
			"memory is advised", e.ranOutPackage, e.ranOutMiB)
	}
	slices.SortStableFunc(w, func(a, b Warning) int { return cmp.Compare(a.Code, b.Code) })
	if w == nil {
		w = []Warning{}
	}
	return w
}

// unreadText says why the source went unread, a cut-short fetch with no reason a timeout
func (e *eval) unreadText() string {
	reason := e.in.Request.Source.Unread
	if reason == "" && e.in.Request.Partial {
		reason = UnreadTimeout
	}
	if s, ok := unreadTexts[reason]; ok {
		return s
	}
	return unreadTexts[""]
}

var unreadTexts = map[UnreadReason]string{
	"":                  "the source was not read",
	UnreadNotFound:      "the source was not found or is private",
	UnreadLegal:         "the source is unavailable for legal reasons",
	UnreadRateLimited:   "the source could not be read under the rate limit for now",
	UnreadTimeout:       "the source took too long to read",
	UnreadNotSupported:  "a URL source is never read",
	UnreadUpstreamError: "the source's host failed",
}

func (e *eval) contextText(fits []Fit) string {
	mf := e.model
	flag := "--max-model-len"
	if mf.gguf || slices.Contains(e.in.Request.Hints.Tools, facts.ToolLlamaCPP) {
		flag = "--ctx-size"
	}
	s := fmt.Sprintf("Assumes a context of %d tokens, %s sets it", mf.context, flag)
	if mf.contextFromCommand {
		s = fmt.Sprintf("Uses the command's context of %d tokens, %s sets it", mf.context, flag)
	}
	smallest := 0
	for _, fit := range fits {
		if fit.Fits && fit.ContextTokens > 0 && (smallest == 0 || fit.ContextTokens < smallest) {
			smallest = fit.ContextTokens
		}
	}
	if smallest > 0 {
		s += fmt.Sprintf(", some packages fit only with %d tokens", smallest)
	}
	return s
}

// imagesHaveDocker says whether every provider's image has Docker, unmeasured counting as absent
func (e *eval) imagesHaveDocker() bool {
	if len(e.in.Images) == 0 {
		return false
	}
	for _, img := range e.in.Images {
		if d := img.Tools.Docker; d == "" || d == "none" {
			return false
		}
	}
	return true
}

/* Recommendation */

// Recommend picks the cheapest fitting available package, or the cheapest fitting one with
// BUSY when none is available
func Recommend(in Input, out Output, available func(packageID string) bool) Recommendation {
	byID := map[string]catalog.Package{}
	for _, p := range in.Catalog {
		byID[p.ID] = p
	}
	var fitting, ready []Fit
	for _, f := range out.Fits {
		if !f.Fits {
			continue
		}
		fitting = append(fitting, f)
		if available(f.PackageID) {
			ready = append(ready, f)
		}
	}
	rank := func(a, b Fit) int {
		ra, rb := byID[a.PackageID].Resources, byID[b.PackageID].Resources
		return cmp.Or(cmp.Compare(a.TotalPriceMicros, b.TotalPriceMicros),
			cmp.Compare(a.ExtensionsNeeded, b.ExtensionsNeeded),
			cmp.Compare(ra.VCPU, rb.VCPU), cmp.Compare(ra.MemoryMiB, rb.MemoryMiB),
			cmp.Compare(ra.DiskGiB, rb.DiskGiB), cmp.Compare(ra.GPUCount, rb.GPUCount),
			cmp.Compare(ra.GPUMemoryGiB, rb.GPUMemoryGiB), cmp.Compare(a.PackageID, b.PackageID))
	}
	rec := Recommendation{Warnings: append([]Warning{}, out.Warnings...)}
	var pick Fit
	switch {
	case len(ready) > 0:
		pick = slices.MinFunc(ready, rank)
	case len(fitting) > 0:
		pick = slices.MinFunc(fitting, rank)
		rec.Warnings = append(rec.Warnings, Warning{Code: WarningBusy,
			Message: "No capacity for it right now, try again shortly"})
	default:
		return rec
	}
	rec.PackageID = pick.PackageID
	if pick.ExtensionsNeeded > 0 {
		rec.Warnings = append(rec.Warnings, Warning{Code: WarningNeedsExtension,
			Message: extensionsText(pick.ExtensionsNeeded)})
	}
	slices.SortStableFunc(rec.Warnings, func(a, b Warning) int {
		return cmp.Compare(a.Code, b.Code)
	})
	return rec
}

func extensionsText(n int) string {
	if n == 1 {
		return "The run needs 1 extension"
	}
	return fmt.Sprintf("The run needs %d extensions", n)
}
