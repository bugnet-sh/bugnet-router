package rules

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/canon"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
)

var update = flag.Bool("update", false, "rewrite the golden outputs and testdata/version.txt")

// goldenCase is one corpus file, facts and a request in, the answer out
type goldenCase struct {
	Request        Request         `json:"request"`
	Facts          facts.Facts     `json:"facts"`
	Output         json.RawMessage `json:"output,omitempty"`
	Recommendation json.RawMessage `json:"recommendation,omitempty"`
}

type answer struct {
	Output         Output         `json:"output"`
	Recommendation Recommendation `json:"recommendation"`
}

// corpus reads every golden case by name
func corpus(t *testing.T) map[string]goldenCase {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("testdata", "golden", "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no golden cases: %v", err)
	}
	out := map[string]goldenCase{}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var c goldenCase
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		out[strings.TrimSuffix(filepath.Base(p), ".json")] = c
	}
	return out
}

// answerOf runs a case with every package available
func answerOf(t *testing.T, c goldenCase) answer {
	t.Helper()
	in := input(t, c.Request, c.Facts)
	out := Run(in)
	return answer{Output: out, Recommendation: Recommend(in, out, func(string) bool {
		return true
	})}
}

func mustCanon(t *testing.T, v any) []byte {
	t.Helper()
	b, err := canon.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// readVersionFile parses testdata/version.txt, the version then one hash and name a line
func readVersionFile(t *testing.T) (string, map[string]string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "version.txt"))
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Scan()
	version := strings.TrimSpace(sc.Text())
	hashes := map[string]string{}
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 2 {
			hashes[f[1]] = f[0]
		}
	}
	return version, hashes
}

// checkCorpus refuses changed or removed outputs recorded under the same version, and a stale
// record, while a new case needs no bump
func checkCorpus(recordedVersion string, recorded, current map[string]string) error {
	if recordedVersion != Version {
		return fmt.Errorf("version.txt holds %s, run go test -update to record %s",
			recordedVersion, Version)
	}
	var changed []string
	for name, sum := range current {
		if was, ok := recorded[name]; ok && was != sum {
			changed = append(changed, name)
		}
	}
	for name := range recorded {
		if _, ok := current[name]; !ok {
			changed = append(changed, name)
		}
	}
	if len(changed) > 0 {
		slices.Sort(changed)
		return fmt.Errorf("golden outputs %v changed while Version stayed %s, bump Version",
			changed, Version)
	}
	return nil
}

func TestGolden(t *testing.T) {
	if *update {
		writeCorpus(t)
	}
	for name, c := range corpus(t) {
		got := mustCanon(t, answerOf(t, c))
		want := mustCanon(t, map[string]json.RawMessage{"output": c.Output,
			"recommendation": c.Recommendation})
		if !bytes.Equal(got, want) {
			t.Errorf("%s: output differs from the golden file\ngot  %s\nwant %s", name, got,
				want)
		}
	}
}

func TestGoldenNeedsVersionBump(t *testing.T) {
	current := map[string]string{}
	for name, c := range corpus(t) {
		current[name] = canon.Sum(mustCanon(t, answerOf(t, c)))
	}
	version, recorded := readVersionFile(t)
	if err := checkCorpus(version, recorded, current); err != nil {
		t.Fatal(err)
	}
}

func TestCorpusGuard(t *testing.T) {
	rec := map[string]string{"a": "1", "b": "2"}
	eq(t, checkCorpus(Version, rec, map[string]string{"a": "1", "b": "2"}), nil)
	eq(t, checkCorpus(Version, rec, map[string]string{"a": "1", "b": "3"}) != nil, true)
	eq(t, checkCorpus(Version, rec, map[string]string{"a": "1"}) != nil, true)
	eq(t, checkCorpus(Version, rec, map[string]string{"a": "1", "b": "2", "c": "4"}), nil)
	eq(t, checkCorpus(Version, rec, map[string]string{"a": "1", "b": "3", "c": "4"}) != nil,
		true)
	eq(t, checkCorpus("est-2000.01.1", rec, rec) != nil, true)
}

// writeCorpus rewrites every golden output, refusing when one changed under the same Version
func writeCorpus(t *testing.T) {
	t.Helper()
	version, recorded := readVersionFileOrEmpty(t)
	current := map[string]string{}
	cases := corpus(t)
	names := make([]string, 0, len(cases))
	files := map[string][]byte{}
	for name, c := range cases {
		names = append(names, name)
		a := answerOf(t, c)
		current[name] = canon.Sum(mustCanon(t, a))
		c.Output = json.RawMessage(mustCanon(t, a.Output))
		c.Recommendation = json.RawMessage(mustCanon(t, a.Recommendation))
		b, err := json.MarshalIndent(c, "", "\t")
		if err != nil {
			t.Fatal(err)
		}
		files[name] = append(b, '\n')
	}
	if version == Version {
		if err := checkCorpus(version, recorded, current); err != nil {
			t.Fatal(err)
		}
	}
	for name, b := range files {
		path := filepath.Join("testdata", "golden", name+".json")
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(names)
	var buf strings.Builder
	buf.WriteString(Version + "\n")
	for _, name := range names {
		fmt.Fprintf(&buf, "%s  %s\n", current[name], name)
	}
	err := os.WriteFile(filepath.Join("testdata", "version.txt"), []byte(buf.String()), 0o644)
	if err != nil {
		t.Fatal(err)
	}
}

func readVersionFileOrEmpty(t *testing.T) (string, map[string]string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join("testdata", "version.txt")); err != nil {
		return "", nil
	}
	return readVersionFile(t)
}
