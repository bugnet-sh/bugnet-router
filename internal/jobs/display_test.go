package jobs

import (
	"context"
	"slices"
	"testing"
)

func TestShownArgvPrefersTheDisplay(t *testing.T) {
	run := []string{"sh", "-c", "script"}
	if got := shownArgv(context.Background(), run); !slices.Equal(got, run) {
		t.Fatalf("no display: got %q", got)
	}
	ctx := WithDisplay(context.Background(), []string{"git", "clone", "x"})
	if got := shownArgv(ctx, run); !slices.Equal(got, []string{"git", "clone", "x"}) {
		t.Fatalf("display: got %q", got)
	}
}
