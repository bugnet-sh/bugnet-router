package sessions

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bugnet-sh/bugnet-run/frame"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/streams"
)

// recentEntries is how many of a running job's newest entries its snapshot offsets read
const recentEntries = 64

// Snapshot builds live_json, the running page's metadata, with no output text
func Snapshot(ctx context.Context, st *store.Store, bus *streams.Bus, p store.Placement,
	deadline, now time.Time) ([]byte, error) {
	rec, err := st.SessionRecordByPlacement(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	jobs, err := st.SessionJobs(ctx, rec.ID)
	if err != nil {
		return nil, err
	}
	events, err := st.SessionEvents(ctx, rec.ID)
	if err != nil {
		return nil, err
	}
	ends := jobEnds(events)
	list := make([]object, 0, len(jobs))
	for _, j := range jobs {
		out, errBytes := j.StdoutBytes, j.StderrBytes
		if j.State == store.JobRunning && bus != nil {
			out, errBytes = recentOffsets(ctx, bus, j.JobID)
		}
		o := object{{"seq", j.Seq}, {"command", j.Command}, {"state", j.State},
			{"startedAt", stamp(j.StartedAt)}, {"outBytes", out}, {"errBytes", errBytes}}
		if j.State != store.JobRunning {
			if j.FinishedAt != nil {
				o = o.with("finishedAt", stamp(*j.FinishedAt))
			}
			if j.ExitCode != nil && j.State != store.JobLost {
				o = o.with("exitCode", *j.ExitCode)
			}
			end := ends[j.Seq]
			o = o.with("oom", end.OOM).with("peakMib", end.PeakMiB)
		}
		list = append(list, o)
	}
	raw, err := json.Marshal(object{
		{"v", Version}, {"startedAt", stamp(rec.CreatedAt)}, {"deadline", stamp(deadline)},
		{"now", stamp(now)}, {"jobs", list}, {"jobsDropped", rec.JobsDropped},
	})
	if err != nil {
		return nil, fmt.Errorf("encode the session snapshot: %w", err)
	}
	return raw, nil
}

// recentOffsets reads a running job's output so far from its newest entries, 0 when unread
func recentOffsets(ctx context.Context, bus *streams.Bus, jobID string) (out, errBytes int64) {
	entries, err := bus.Before(ctx, jobID, "+", recentEntries)
	if err != nil {
		return 0, 0
	}
	for _, e := range entries {
		o, e2 := ends(e.Frame)
		out, errBytes = max(out, o), max(errBytes, e2)
	}
	return out, errBytes
}

// ends are the output offsets a frame reaches
func ends(f frame.Frame) (out, errBytes int64) {
	switch f.Kind {
	case frame.KindStdout:
		return f.Offset + int64(len(f.Data)), 0
	case frame.KindStderr:
		return 0, f.Offset + int64(len(f.Data))
	case frame.KindSkip, frame.KindHeartbeat, frame.KindExit:
		return f.Out, f.Err
	}
	return 0, 0
}

/* Replay */

const (
	// replayLookback bounds the entries before the replay read to place each output's start
	replayLookback = 4 * recentEntries
	// replayPageMax bounds a page that doubles each read, so many tiny frames take few reads
	replayPageMax = 4096
)

// Replay is where a first live connection starts reading a job's stream
type Replay struct {
	// Cursor is the entry to read after, empty to read the stream from its start
	Cursor string
	// Out and Err are the offsets each output goes on at, as the skip frame names them
	Out, Err int64
	// MidOut and MidErr say an output's replay starts inside a line
	MidOut, MidErr bool
	// Skip is false when the replay holds the whole output
	Skip bool
}

// ReplayStart finds the entry a first connection starts at to see about the newest replay bytes
func ReplayStart(ctx context.Context, bus *streams.Bus, jobID string,
	replay int64) (Replay, error) {
	outs := [2]replayOutput{{at: -1}, {at: -1}}
	var seen int64
	start, end, back, size := "", "+", 0, int64(recentEntries)
	for {
		page, err := bus.Before(ctx, jobID, end, size)
		if err != nil {
			return Replay{}, err
		}
		for _, e := range page {
			f := e.Frame
			if start == "" {
				replayed(&outs, f)
				if f.Kind != frame.KindStdout && f.Kind != frame.KindStderr {
					continue
				}
				if seen += int64(len(f.Data)); seen < replay {
					continue
				}
				start = e.ID
			} else {
				back++
				outs[0].before(f, f.Kind == frame.KindStdout, f.Out)
				outs[1].before(f, f.Kind == frame.KindStderr, f.Err)
			}
			if outs[0].placed() && outs[1].placed() {
				return replayFrom(start, outs), nil
			}
		}
		if int64(len(page)) < size || (start != "" && back >= replayLookback) {
			if start == "" {
				return Replay{}, nil
			}
			return replayFrom(start, outs), nil
		}
		end = "(" + page[len(page)-1].ID
		size = recentEntries
		if start == "" {
			size = min(2*int64(len(page)), replayPageMax)
		}
	}
}

// replayOutput is where one output stands at the replay's first entry
type replayOutput struct {
	at      int64 // -1 until known
	mid     bool  // the replay starts inside a line
	settled bool  // the text before at was read
}

// replayed places each output by an entry the replay holds, newest first, so the oldest wins
func replayed(outs *[2]replayOutput, f frame.Frame) {
	switch f.Kind {
	case frame.KindStdout:
		outs[0].at = f.Offset
	case frame.KindStderr:
		outs[1].at = f.Offset
	case frame.KindHeartbeat, frame.KindExit:
		outs[0].at, outs[1].at = f.Out, f.Err
	case frame.KindSkip:
		// Where the output stood before a skip is read from older entries
		outs[0].at, outs[1].at = -1, -1
	}
}

// before reads an entry older than the replay, pos being the offset a skip or heartbeat names
func (o *replayOutput) before(f frame.Frame, data bool, pos int64) {
	if o.settled {
		return
	}
	switch {
	case data && len(f.Data) > 0:
		from, to := f.Offset, f.Offset+int64(len(f.Data))
		if o.at < 0 {
			o.at = to
		}
		switch {
		case to < o.at:
			// The stream left out the text just before the replay
			o.mid, o.settled = true, true
		case from < o.at:
			c := f.Data[o.at-1-from]
			o.mid, o.settled = c != '\n' && c != '\r', true
		}
	case f.Kind == frame.KindSkip:
		if o.at < 0 {
			o.at = pos
		}
		if pos >= o.at {
			o.mid, o.settled = o.at > 0, true
		}
	case f.Kind == frame.KindHeartbeat || f.Kind == frame.KindExit:
		if o.at < 0 {
			o.at = pos
		}
	}
}

// placed reports whether the output's start is known, as it is for one that starts at 0
func (o *replayOutput) placed() bool { return o.settled || o.at == 0 }

// replayFrom answers a replay from start, an output with unread text before it starting mid-line
func replayFrom(start string, outs [2]replayOutput) Replay {
	for i := range outs {
		if !outs[i].placed() {
			outs[i].at = max(outs[i].at, 0)
			outs[i].mid = outs[i].at > 0
		}
	}
	r := Replay{Out: outs[0].at, Err: outs[1].at, MidOut: outs[0].mid, MidErr: outs[1].mid}
	if r.Out == 0 && r.Err == 0 {
		return Replay{}
	}
	r.Cursor, r.Skip = before(start), true
	return r
}

// before is the stream entry ID just before id, so a read after it starts at id
func before(id string) string {
	ms, seq, _ := strings.Cut(id, "-")
	n, err := strconv.ParseUint(seq, 10, 64)
	if err == nil && n > 0 {
		return ms + "-" + strconv.FormatUint(n-1, 10)
	}
	m, err := strconv.ParseUint(ms, 10, 64)
	if err != nil || m == 0 {
		return "0"
	}
	return strconv.FormatUint(m-1, 10) + "-" + strconv.FormatUint(math.MaxUint64, 10)
}

/* The copy */

// Keeper keeps the newest bytes of each output of one job's frames, in order
type Keeper struct {
	limit  int64
	total  int64
	frames []Frame
	size   map[string]int64
	cut    int64
}

// NewKeeper keeps at most limit bytes of text per output and total across both, 0 for no total
func NewKeeper(limit, total int64) *Keeper {
	return &Keeper{limit: limit, total: total, size: map[string]int64{}}
}

// Add appends frames, a replacing frame taking the place of the line start it replaces
func (k *Keeper) Add(frames ...Frame) {
	for _, f := range frames {
		k.add(f)
	}
}

func (k *Keeper) add(f Frame) {
	n := len(k.frames)
	if n > 0 {
		last := &k.frames[n-1]
		switch {
		case last.S == f.S && !f.CR:
			k.size[f.S] += int64(len(f.T))
			last.T += f.T
			k.trim(f.S)
			return
		case last.S == f.S && f.CR && !strings.Contains(last.T, "\n") &&
			(last.CR || n == 1 || strings.HasSuffix(k.frames[n-2].T, "\n")):
			// The line being replaced starts in last, so last becomes the new line
			k.size[f.S] += int64(len(f.T)) - int64(len(last.T))
			last.T = f.T
			k.trim(f.S)
			return
		}
	}
	k.frames = append(k.frames, f)
	k.size[f.S] += int64(len(f.T))
	k.trim(f.S)
}

// trim drops the oldest text of output s past the limit, then the oldest of any past the total
func (k *Keeper) trim(s string) {
	for k.size[s] > k.limit {
		k.drop(slices.IndexFunc(k.frames, func(f Frame) bool { return f.S == s }),
			k.size[s]-k.limit)
	}
	for k.total > 0 && k.Kept() > k.total {
		k.drop(0, k.Kept()-k.total)
	}
}

// drop removes up to over bytes from the front of frame i, never splitting a character
func (k *Keeper) drop(i int, over int64) {
	f := k.frames[i]
	if int64(len(f.T)) <= over {
		k.frames = slices.Delete(k.frames, i, i+1)
		k.size[f.S] -= int64(len(f.T))
		k.cut += int64(len(f.T))
		return
	}
	at := int(over)
	for at < len(f.T) && !utf8.RuneStart(f.T[at]) {
		at++
	}
	k.frames[i].T = f.T[at:]
	k.size[f.S] -= int64(at)
	k.cut += int64(at)
}

// Skip counts output left out before it reached the keeper
func (k *Keeper) Skip(n int64) { k.cut += max(n, 0) }

// Frames returns the kept frames
func (k *Keeper) Frames() []Frame { return k.frames }

// Kept is the text kept across both outputs
func (k *Keeper) Kept() int64 { return k.size[Stdout] + k.size[Stderr] }

// Cut is the output left out
func (k *Keeper) Cut() int64 { return k.cut }
