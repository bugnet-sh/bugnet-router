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
	now time.Time) ([]byte, error) {
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
		{"v", Version}, {"startedAt", stamp(rec.CreatedAt)}, {"deadline", stampPtr(p.Deadline)},
		{"now", stamp(now)}, {"jobs", list},
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

// ReplayStart finds where a first live connection starts so it sees about the newest replay
// bytes, from the start of the entry that reaches them, answering the cursor to read after and
// the offsets the skip frame names, skip false when the replay holds the whole output
func ReplayStart(ctx context.Context, bus *streams.Bus, jobID string,
	replay int64) (cursor string, skipOut, skipErr int64, skip bool, err error) {
	var seen int64
	end := "+"
	for {
		page, err := bus.Before(ctx, jobID, end, recentEntries)
		if err != nil {
			return "", 0, 0, false, err
		}
		for i, e := range page {
			f := e.Frame
			if f.Kind != frame.KindStdout && f.Kind != frame.KindStderr {
				continue
			}
			seen += int64(len(f.Data))
			if seen < replay {
				continue
			}
			skipOut, skipErr = startOffsets(page[:i+1])
			if skipOut == 0 && skipErr == 0 {
				return "", 0, 0, false, nil
			}
			return before(e.ID), skipOut, skipErr, true, nil
		}
		if len(page) < recentEntries {
			return "", 0, 0, false, nil
		}
		end = "(" + page[len(page)-1].ID
	}
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

// startOffsets reads where the replay starts, from newest-first entries ending at the first one
// replayed, as the offset of each output's oldest replayed frame
func startOffsets(page []streams.Entry) (out, errBytes int64) {
	out, errBytes = -1, -1
	// Newest first, so the last frame of an output seen here is its oldest replayed one
	for _, e := range page {
		switch e.Frame.Kind {
		case frame.KindStdout:
			out = e.Frame.Offset
		case frame.KindStderr:
			errBytes = e.Frame.Offset
		}
	}
	return max(out, 0), max(errBytes, 0)
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
