package config

import (
	"reflect"
	"slices"
	"strings"
	"testing"
)

var benchDefaults = Bench{ConfirmAboveMicros: 500000, MaxRuns: 200}

func TestBenchDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Bench, benchDefaults)
	eq(t, DefaultBench(), benchDefaults)
}

func TestBenchSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, "[bench]\nconfirm_above_micros = 0\nmax_runs = 50\n"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Bench, Bench{ConfirmAboveMicros: 0, MaxRuns: 50})
}

func TestBenchRefusals(t *testing.T) {
	for _, tc := range []struct{ toml, want string }{
		{"[bench]\nconfirm_above_micros = -1", "bench.confirm_above_micros must be 0 to"},
		{"[bench]\nmax_runs = 0", "bench.max_runs must be 1 to 10000"},
		{"[bench]\nmax_runs = 10001", "bench.max_runs must be 1 to 10000"},
		{"[bench]\nsurprise = 1", "surprise"},
	} {
		msg := problem(t, tc.toml+"\n")
		if !strings.Contains(msg, tc.want) {
			t.Errorf("%s gave %q, want %q", tc.toml, msg, tc.want)
		}
	}
}

func TestExampleTomlHoldsBenchDefaults(t *testing.T) {
	eq(t, loadExample(t).Bench, benchDefaults)
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	var written, keys []string
	for _, s := range doc {
		if len(s.path) == 2 && s.path[0] == "bench" {
			written = append(written, s.path[1])
		}
	}
	schema := reflect.TypeFor[benchFile]()
	for i := range schema.NumField() {
		keys = append(keys, schema.Field(i).Tag.Get("toml"))
	}
	slices.Sort(written)
	slices.Sort(keys)
	eq(t, written, keys)
}
