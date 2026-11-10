package store

import (
	"strings"
	"testing"
)

func TestTraceColumns(t *testing.T) {
	const good = "00-55a4dee648a7a4d0a70cd0a8b24c672f-69923b8439c47854-01"
	for _, c := range []struct {
		name, parent, state string
		wantParent, want    any
	}{
		{"valid", good, "k=v", good, "k=v"},
		{"no state", good, "", good, nil},
		{"long state", good, strings.Repeat("a", maxTracestate+1), good, nil},
		{"empty", "", "", nil, nil},
		{"upper case", strings.ToUpper(good), "k=v", nil, nil},
		{"short", "00-1-2-01", "k=v", nil, nil},
		{"version", "ff-55a4dee648a7a4d0a70cd0a8b24c672f-69923b8439c47854-01", "", nil, nil},
	} {
		gotParent, gotState := traceColumns(c.parent, c.state)
		if gotParent != c.wantParent || gotState != c.want {
			t.Errorf("%s: got %v, %v want %v, %v", c.name, gotParent, gotState, c.wantParent,
				c.want)
		}
	}
}
