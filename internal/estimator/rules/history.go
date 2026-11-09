package rules

import (
	"slices"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
)

// stepValues lists the distinct values the start packages give one dimension, sorted
func stepValues(vals []int) []int {
	out := slices.Clone(vals)
	slices.Sort(out)
	return slices.Compact(out)
}

func firstAtLeast(values []int, v int) (int, bool) {
	for _, x := range values {
		if x >= v {
			return x, true
		}
	}
	return 0, false
}

// raised moves floor towards target by at most one value of the sorted list above the one that
// covers floor, and not at all when no value covers it
func raised(floor, target int, values []int) int {
	cover, ok := firstAtLeast(values, floor)
	if !ok {
		return floor
	}
	limit := cover
	if next, ok := firstAtLeast(values, cover+1); ok {
		limit = next
	}
	return max(floor, min(target, limit))
}

// moved applies one dimension's history target to its rules floor, history only raising below
// the lowering thresholds and never lowering past lowest
func (h History) moved(floor, target, lowest int, values []int) int {
	switch {
	case target > floor:
		return raised(floor, target, values)
	case target > 0 && target < floor && h.Lower:
		return min(floor, max(target, lowest))
	}
	return floor
}

// withHeadroom is v with the history headroom added, rounded up
func (h History) withHeadroom(v int) int {
	return int(ceilDiv(int64(v)*int64(100+h.HeadroomPercent), 100))
}

func direction(to, from int) string {
	if to > from {
		return "rises to"
	}
	return "drops to"
}

// applyHistory moves the floor by what past runs of the source needed, CP 4.7
func (e *eval) applyHistory(f Floor) Floor {
	h := e.in.History
	if h.Observations == 0 {
		return f
	}
	var mem, disk, minutes, gpuMem []int
	for _, p := range startPackages(e.in.Catalog) {
		mem = append(mem, p.Resources.MemoryMiB)
		disk = append(disk, p.Resources.DiskGiB)
		minutes = append(minutes, p.Minutes)
		if p.Resources.GPUCount > 0 {
			gpuMem = append(gpuMem, p.Resources.GPUMemoryGiB)
		}
	}
	f = e.historyMemory(f, h, stepValues(mem))
	if h.DiskMiB > 0 {
		target := int(ceilDiv(int64(h.withHeadroom(h.DiskMiB)), 1024))
		lowest := int(ceilDiv(max(e.diskMiB.base+e.diskMiB.hardAdd, e.diskMiB.hard), 1024))
		if v := h.moved(f.DiskGiB, target, lowest, stepValues(disk)); v != f.DiskGiB {
			e.historyReason("history.disk", "%d past runs from %d payers peaked at %d MiB of "+
				"disk, so the floor %s %d GiB", h.Observations, h.Payers, h.DiskMiB,
				direction(v, f.DiskGiB), v)
			f.DiskGiB = v
		}
	}
	if f.GPU != GPUNone && h.GPUMemoryMiB > 0 {
		target := int(ceilDiv(int64(h.withHeadroom(h.GPUMemoryMiB)), 1024))
		// Nothing but the model sets a GPU memory floor, so history only raises it
		raiseOnly := h
		raiseOnly.Lower = false
		v := raiseOnly.moved(f.GPUMemoryGiB, target, 0, stepValues(gpuMem))
		if v != f.GPUMemoryGiB {
			e.historyReason("history.gpu_memory", "%d past runs from %d payers peaked at %d MiB "+
				"of GPU memory, so the floor rises to %d GiB", h.Observations, h.Payers,
				h.GPUMemoryMiB, v)
			f.GPUMemoryGiB = v
		}
	}
	if h.ActiveMinutes > 0 {
		lowest := int(ceilDiv(max(e.seconds.base+e.seconds.hardAdd, e.seconds.hard), 60))
		target := h.withHeadroom(h.ActiveMinutes)
		if v := h.moved(f.Minutes, target, lowest, stepValues(minutes)); v != f.Minutes {
			e.historyReason("history.minutes", "%d past runs from %d payers were active for %d "+
				"minutes, so the floor %s %d minutes", h.Observations, h.Payers, h.ActiveMinutes,
				direction(v, f.Minutes), v)
			f.Minutes = v
		}
	}
	return f
}

// historyReason records a reason that history moved the floor
func (e *eval) historyReason(rule, format string, args ...any) {
	e.historyUsed = true
	e.reason(rule, format, args...)
}

// historyMemory moves the memory floor by the peak, then pushes it above a package that counted
// payers ran out of memory on, both inside the one step a history raise may take
func (e *eval) historyMemory(f Floor, h History, values []int) Floor {
	target := 0
	if h.MemoryMiB > 0 {
		target = int(roundUp(int64(h.withHeadroom(h.MemoryMiB)), 256))
	}
	var pushed []catalog.Package
	for _, p := range startPackages(e.in.Catalog) {
		if slices.Contains(h.OOMPackages, p.ID) && f.MemoryMiB <= p.Resources.MemoryMiB {
			pushed = append(pushed, p)
			target = max(target, int(roundUp(int64(p.Resources.MemoryMiB)+1, 256)))
		}
	}
	lowest := int(roundUp(max(e.memMiB.base+e.memMiB.hardAdd, e.memMiB.hard), 256))
	v := h.moved(f.MemoryMiB, target, lowest, values)
	if v == f.MemoryMiB {
		return f
	}
	e.historyReason("history.memory", "%d past runs from %d payers peaked at %d MiB of memory, "+
		"so the floor %s %d MiB", h.Observations, h.Payers, h.MemoryMiB, direction(v, f.MemoryMiB),
		v)
	for _, p := range pushed {
		if p.Resources.MemoryMiB < v && p.Resources.MemoryMiB >= e.ranOutMiB {
			e.ranOut, e.ranOutPackage, e.ranOutMiB = true, p.ID, p.Resources.MemoryMiB
		}
	}
	f.MemoryMiB = v
	return f
}
