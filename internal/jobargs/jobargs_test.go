package jobargs_test

import (
	"reflect"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
)

// TestEveryArgsTypeHasTrace pins the trace field on every kind, tagged to leave the args of a
// call site without a span as they were
func TestEveryArgsTypeHasTrace(t *testing.T) {
	seen := map[string]bool{}
	all := jobargs.All()
	for _, a := range all {
		ty := reflect.TypeOf(a)
		f, ok := ty.FieldByName("Trace")
		if !ok || f.Type != reflect.TypeFor[jobargs.Trace]() {
			t.Errorf("%s has no Trace field", ty)
			continue
		}
		if f.Tag.Get("json") != "trace,omitzero" {
			t.Errorf("%s tags Trace %q", ty, f.Tag.Get("json"))
		}
		if seen[a.Kind()] {
			t.Errorf("kind %s is listed twice", a.Kind())
		}
		seen[a.Kind()] = true
	}
	if len(all) < 29 {
		t.Errorf("All lists %d kinds", len(all))
	}
}
