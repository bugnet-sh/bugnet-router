package bootstrap

import (
	"slices"
	"testing"
)

func TestCloneDisplayNamesTheRepositoryNotTheScript(t *testing.T) {
	got := cloneDisplay(sourceParts{owner: "antfu", repo: "vitesse-lite", ref: "main"})
	want := []string{"git", "clone", "https://github.com/antfu/vitesse-lite", "@main"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}
