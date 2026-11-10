package bench

import (
	"cmp"
	"fmt"
	"io"
	"slices"

	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Steps are the nearest-rank percentiles of one step over the samples that ran it
type Steps struct {
	Samples       int
	P50, P90, P99 int // milliseconds
}

// Report is what the samples of one provider, size and kind say
type Report struct {
	Provider, Size string
	Kind           store.SampleKind
	OK, Failed     int
	// Refused counts the capacity, rate limit and quota refusals, which are not failures
	Refused      int
	SuccessRate  float64
	SuccessLower float64
	Create       Steps
	Ready        Steps
	Runner       Steps
	Gate         Steps
	FirstCmd     Steps
	WarmCmd      Steps
	Destroy      Steps
	Total        Steps
	// StartPath is cold, pooled, reserved or pool, empty for samples that name none
	StartPath string
	// Bind is CreatePlacement to ready, PaidToReady payment to ready, FirstFrame the first job's
	// first frame and PaidToFirstFrame payment to that frame, Wake the unpark or claim
	Bind, PaidToReady, FirstFrame, PaidToFirstFrame, Wake Steps
	CostMicros                                            int64
	// Errors counts the failures by the provider's error code
	Errors map[string]int
}

type groupKey struct {
	provider, size string
	kind           store.SampleKind
	path           string
}

// Build groups samples by provider, size, kind and start path and reports each group, in that
// order
func Build(samples []store.StartSample) []Report {
	groups := map[groupKey][]store.StartSample{}
	for _, m := range samples {
		k := groupKey{m.Provider, m.SizeName, m.Kind, m.StartPath}
		groups[k] = append(groups[k], m)
	}
	keys := make([]groupKey, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b groupKey) int {
		return cmp.Or(cmp.Compare(a.provider, b.provider), cmp.Compare(a.size, b.size),
			cmp.Compare(a.kind, b.kind), cmp.Compare(a.path, b.path))
	})
	out := make([]Report, 0, len(keys))
	for _, k := range keys {
		out = append(out, reportOf(k, groups[k]))
	}
	return out
}

func reportOf(k groupKey, samples []store.StartSample) Report {
	r := Report{
		Provider: k.provider, Size: k.size, Kind: k.kind, StartPath: k.path,
		Errors: map[string]int{},
	}
	var create, ready, runner, gate, first, warm, destroy, total []int
	var bind, paid, frame, paidFrame, wake []int
	for _, m := range samples {
		r.CostMicros += m.CostMicros
		switch m.Outcome {
		case store.OutcomeFailed:
			r.Failed++
			r.Errors[m.ErrorCode]++
			continue
		case store.OutcomeOK:
			r.OK++
		default:
			r.Refused++
			continue
		}
		create = ran(create, m.CreateMS)
		ready = ran(ready, m.ReadyMS)
		runner = ran(runner, m.RunnerMS)
		gate = ran(gate, m.GateMS)
		first = ran(first, m.FirstCmdMS)
		warm = ran(warm, m.WarmCmdMS)
		destroy = ran(destroy, m.DestroyMS)
		if m.ReadyMS >= 0 && m.FirstCmdMS >= 0 {
			total = append(total, m.ReadyMS+max(m.RunnerMS, 0)+max(m.GateMS, 0)+m.FirstCmdMS)
		}
		wake = ran(wake, m.WakeMS)
		bind, paid = positive(bind, m.BindMS), positive(paid, m.PaidToReadyMS)
		frame = positive(frame, m.FirstFrameMS)
		if m.PaidToReadyMS > 0 && m.FirstFrameMS > 0 {
			paidFrame = append(paidFrame, m.PaidToReadyMS+m.FirstFrameMS)
		}
	}
	if n := r.OK + r.Failed; n > 0 {
		r.SuccessRate = float64(r.OK) / float64(n)
		r.SuccessLower = health.LowerBound(r.OK, n)
	}
	r.Create, r.Ready, r.Runner, r.Gate = steps(create), steps(ready), steps(runner), steps(gate)
	r.FirstCmd, r.WarmCmd, r.Destroy = steps(first), steps(warm), steps(destroy)
	r.Total = steps(total)
	r.Bind, r.PaidToReady, r.FirstFrame = steps(bind), steps(paid), steps(frame)
	r.PaidToFirstFrame, r.Wake = steps(paidFrame), steps(wake)
	return r
}

// positive adds a step's time when it was measured, a zero meaning it was not
func positive(into []int, ms int) []int {
	if ms <= 0 {
		return into
	}
	return append(into, ms)
}

// ran adds a step's time when the step ran
func ran(into []int, ms int) []int {
	if ms < 0 {
		return into
	}
	return append(into, ms)
}

func steps(ms []int) Steps {
	return Steps{
		Samples: len(ms), P50: Percentile(ms, 0.5), P90: Percentile(ms, 0.9),
		P99: Percentile(ms, 0.99),
	}
}

// Format writes the table of 12.6 for r under head. A bench's estimated cost, when known,
// goes beside the measured one.
func Format(w io.Writer, head string, r Report, estimatedMicros int64) error {
	var b []byte
	line := func(f string, a ...any) { b = fmt.Appendf(b, f+"\n", a...) }
	line("%s", head)
	line("outcome    ok %d   failed %d (refused %d)   success %.1f%% (95%% lower bound %.1f%%)",
		r.OK, r.Failed, r.Refused, r.SuccessRate*100, r.SuccessLower*100)
	line("%-11s %8s %8s %8s", "", "p50", "p90", "p99")
	for _, row := range []struct {
		name string
		s    Steps
		// optional leaves the row out when no sample measured the step
		optional bool
	}{
		{"create", r.Create, false}, {"ready", r.Ready, false}, {"runner", r.Runner, false},
		{"gate", r.Gate, false}, {"first cmd", r.FirstCmd, false},
		{"warm cmd", r.WarmCmd, false}, {"destroy", r.Destroy, false}, {"total", r.Total, false},
		{"bind", r.Bind, true}, {"paid>ready", r.PaidToReady, true},
		{"first frame", r.FirstFrame, true}, {"paid>frame", r.PaidToFirstFrame, true},
		{"wake", r.Wake, true},
	} {
		if row.optional && row.s.Samples == 0 {
			continue
		}
		if row.s.Samples == 0 {
			line("%-11s %8s %8s %8s", row.name, "-", "-", "-")
			continue
		}
		line("%-11s %8s %8s %8s", row.name, sec(row.s.P50), sec(row.s.P90), sec(row.s.P99))
	}
	if estimatedMicros > 0 {
		line("cost       %s measured   (%s estimated)", usd(r.CostMicros), usd(estimatedMicros))
	} else {
		line("cost       %s measured", usd(r.CostMicros))
	}
	_, err := w.Write(b)
	return err
}

func sec(ms int) string { return fmt.Sprintf("%.2f s", float64(ms)/1000) }

func usd(micros int64) string { return fmt.Sprintf("$%.3f", float64(micros)/1e6) }
