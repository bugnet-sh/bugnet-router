package bootstrap

import (
	"slices"
	"testing"
)

func TestCloneDisplayNamesTheRepositoryNotTheScript(t *testing.T) {
	got := cloneDisplay(sourceParts{owner: "bugnet-sh", repo: "example", ref: "main"})
	want := []string{"git", "clone", "https://github.com/bugnet-sh/example", "@main"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}
