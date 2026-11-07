package live

import (
	"flag"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// row is one live row, run as the subtest <id>/<provider> on each provider it names
type row struct {
	id string
	// providers lists the providers the row runs on, every named provider when empty
	providers []string
	// vps means the row needs the dev front, so it fails at once in the local mode
	vps bool
	run func(t *testing.T, s *stack)
}

// dataplaneRows are DP 16.2's rows and the plan's 16.3 ids, each task adding its own
var dataplaneRows []row

func TestLiveDataplane(t *testing.T) {
	runRows(t, dataplaneRows)
}

// providerList is -providers as a list
func providerList() []string {
	var out []string
	for _, p := range strings.Split(*providersFlag, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// runRows runs each row selected by -run on each of its providers, one provider's stack at a time
func runRows(t *testing.T, rows []row) {
	providers := providerList()
	if len(providers) == 0 {
		t.Skip("live rows run through scripts/live-test.sh, which names -providers")
	}
	f, err := loadFront(filepath.Join(repo(t), "config", "dev-front.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPlan(providers, *frontFlag, f, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, p := range providers {
		var mine []row
		for _, r := range rows {
			if (len(r.providers) == 0 || slices.Contains(r.providers, p)) &&
				wanted(t.Name(), r.id, p) {
				mine = append(mine, r)
			}
		}
		if len(mine) == 0 {
			continue
		}
		var s *stack
		if needsStack(mine, *frontFlag) {
			s = startStack(t, p, *frontFlag, f)
		}
		for _, r := range mine {
			t.Run(r.id, func(t *testing.T) {
				t.Run(p, func(t *testing.T) {
					if r.vps && *frontFlag != "vps" {
						t.Fatalf("%s needs the VPS, run scripts/live-test.sh with --front vps",
							r.id)
					}
					r.run(t, s)
				})
			})
		}
		if s != nil {
			s.stop(t)
		}
	}
}

// needsStack is false when every row would fail at once for want of the dev front
func needsStack(rows []row, mode string) bool {
	return slices.ContainsFunc(rows, func(r row) bool { return !r.vps || mode == "vps" })
}

// wanted reports whether go test's -run pattern selects the subtest with these path names
func wanted(names ...string) bool {
	f := flag.Lookup("test.run")
	if f == nil || f.Value.String() == "" {
		return true
	}
	for _, alt := range split(f.Value.String(), '|') {
		if matches(split(alt, '/'), names) {
			return true
		}
	}
	return false
}

func matches(elems, names []string) bool {
	for i, e := range elems {
		if i >= len(names) {
			break
		}
		re, err := regexp.Compile(e)
		if err != nil || !re.MatchString(names[i]) {
			return false
		}
	}
	return true
}

// split cuts pattern at each sep outside parentheses and brackets, as go test reads -run
func split(pattern string, sep byte) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(pattern); i++ {
		switch c := pattern[i]; {
		case c == '\\':
			i++
		case c == '(' || c == '[':
			depth++
		case c == ')' || c == ']':
			depth--
		case c == sep && depth == 0:
			out = append(out, pattern[start:i])
			start = i + 1
		}
	}
	return append(out, pattern[start:])
}

func TestRunPatternSelection(t *testing.T) {
	for pattern, want := range map[string]bool{
		"":                                 true,
		"LiveDataplane":                    true,
		"LiveDataplane/(T[1-7]|BOAT5)":     true,
		"LiveDataplane/(T[1-7]|BOAT5)/e2b": true,
		"LiveDataplane/(T[1-7]|BOAT5)/boa": false,
		"LiveDataplane/BOAT5":              false,
		"LiveControl|LiveDataplane/T":      true,
		"LiveControl":                      false,
	} {
		elems := []string{"TestLiveDataplane", "T3", "e2b"}
		got := pattern == ""
		for _, alt := range split(pattern, '|') {
			got = got || matches(split(alt, '/'), elems)
		}
		if got != want {
			t.Errorf("-run %q selects T3 on e2b %v, want %v", pattern, got, want)
		}
	}
	eq(t, split(`a/(b|c)/d\/e|f`, '|'), []string{`a/(b|c)/d\/e`, "f"})
	eq(t, needsStack([]row{{vps: true}}, "local"), false)
	eq(t, needsStack([]row{{vps: true}, {}}, "local"), true)
}
