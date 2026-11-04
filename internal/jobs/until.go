package jobs

import (
	"bytes"
	"regexp"
	"time"
)

// patternWindow is how much of each stream's latest output Pattern is matched against
const patternWindow = 64 << 10

// Until says when Exec or Watch returns early, Pattern matching within each stream's last 64 KiB
type Until struct {
	Exit    bool           // the default, return when the job ends, which every read does anyway
	Pattern *regexp.Regexp // return once the latest output matches, the window cut at a line start
	Idle    time.Duration  // return after this long without output once some output arrived
}

// progress tracks one read's output against its Until
type progress struct {
	until      Until
	recent     [2][]byte
	lastOutput time.Time
}

// output records a chunk of one stream's output and reports whether Pattern now matches
func (p *progress) output(stream int, data []byte, at time.Time) bool {
	p.lastOutput = at
	if p.until.Pattern == nil {
		return false
	}
	// The byte kept before the window tells whether the window starts a line
	w := keepLast(p.recent[stream], data, patternWindow+1)
	if len(w) > patternWindow {
		// A line longer than the window keeps its tail so unanchored patterns still match
		start := 1
		if i := bytes.IndexByte(w[:len(w)-1], '\n'); i >= 0 {
			start = i + 1
		}
		w = w[start:]
	}
	p.recent[stream] = w
	return p.until.Pattern.Match(w)
}

// idleLeft returns how long until Idle is met, reporting false while Idle does not apply
func (p *progress) idleLeft(now time.Time) (time.Duration, bool) {
	if p.until.Idle <= 0 || p.lastOutput.IsZero() {
		return 0, false
	}
	return p.lastOutput.Add(p.until.Idle).Sub(now), true
}
