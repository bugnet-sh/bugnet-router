package sessions

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-run/frame"

	"github.com/bugnet-sh/bugnet-router/internal/streams"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// jobStream writes frames to a fresh job's stream and returns its bus and id
func jobStream(t *testing.T, frames ...frame.Frame) (*streams.Bus, string) {
	t.Helper()
	vk, prefix := testenv.Valkey(t)
	const id = "j_replay"
	for _, f := range frames {
		raw, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		cmd := vk.B().Xadd().Key(prefix + "job:" + id + ":s").Id("*").FieldValue().
			FieldValue("f", string(raw)).Build()
		if err := vk.Do(t.Context(), cmd).Error(); err != nil {
			t.Fatal(err)
		}
	}
	return streams.NewBus(vk, prefix), id
}

func TestReplayStartPlacesEachOutputPastOnePage(t *testing.T) {
	frames := []frame.Frame{{Kind: frame.KindStderr, Data: []byte("warning: slow\n")}}
	line := []byte(strings.Repeat("o", 49) + "\n")
	for i := range 200 {
		frames = append(frames, frame.Frame{Kind: frame.KindStdout, Offset: int64(i * 50),
			Data: line})
	}
	bus, id := jobStream(t, frames...)
	// 100 entries replayed span two pages of 64, and stderr wrote only before them
	r, err := ReplayStart(t.Context(), bus, id, 5000)
	eq(t, err, nil)
	eq(t, r.Skip, true)
	eq(t, r.Cursor != "", true)
	eq(t, r.Out, int64(5000))
	eq(t, r.Err, int64(14))
	eq(t, r.MidOut, false)
	eq(t, r.MidErr, false)
	// A replay that holds every byte reads the stream from its start
	r, err = ReplayStart(t.Context(), bus, id, 1<<20)
	eq(t, err, nil)
	eq(t, r, Replay{})
}

func TestReplayStartInsideALine(t *testing.T) {
	bus, id := jobStream(t,
		frame.Frame{Kind: frame.KindStdout, Data: []byte("export T=bnt_")},
		frame.Frame{Kind: frame.KindStdout, Offset: 13,
			Data: []byte(strings.Repeat("Q", 30) + "\n")},
		frame.Frame{Kind: frame.KindStdout, Offset: 44, Data: []byte("done\n")})
	r, err := ReplayStart(t.Context(), bus, id, 30)
	eq(t, err, nil)
	eq(t, r.Skip, true)
	eq(t, r.Out, int64(13))
	eq(t, r.MidOut, true)
	eq(t, r.Err, int64(0))
	eq(t, r.MidErr, false)
	// The replay's text then drops the token's body with the rest of its line
	entries, err := bus.After(t.Context(), id, r.Cursor, 10)
	eq(t, err, nil)
	st := NewRedactor().Stream(Stdout)
	st.Resume()
	var got []Frame
	for _, e := range entries {
		got = append(got, st.Write(e.Frame.Data)...)
	}
	eq(t, joined(append(got, st.Flush()...)), "done\n")
}
