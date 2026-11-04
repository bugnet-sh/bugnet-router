// Package shell builds the one shell command line that providers taking a command string run
package shell

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/oynozan/bugnet-router/internal/provider"
)

var envKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Command quotes every word of spec, changes to its Cwd, sets its Env and closes stdin
func Command(spec provider.ExecSpec) (string, error) {
	if len(spec.Argv) == 0 {
		return "", invalid(errors.New("exec needs a command"))
	}
	var b strings.Builder
	if spec.Cwd != "" {
		b.WriteString("cd " + Quote(spec.Cwd) + " && ")
	}
	if len(spec.Env) > 0 {
		// env would read an argv0 holding = as one more variable and one starting with - as a flag
		if strings.Contains(spec.Argv[0], "=") || strings.HasPrefix(spec.Argv[0], "-") {
			return "", invalid(fmt.Errorf("command %q cannot follow env", spec.Argv[0]))
		}
		keys := make([]string, 0, len(spec.Env))
		for k := range spec.Env {
			if !envKey.MatchString(k) {
				return "", invalid(fmt.Errorf("environment variable name %q", k))
			}
			keys = append(keys, k)
		}
		slices.Sort(keys)
		b.WriteString("env")
		for _, k := range keys {
			b.WriteString(" " + Quote(k+"="+spec.Env[k]))
		}
		b.WriteString(" ")
	}
	for i, a := range spec.Argv {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(Quote(a))
	}
	b.WriteString(" </dev/null")
	line := b.String()
	if strings.ContainsRune(line, 0) || !utf8.ValidString(line) {
		return "", invalid(errors.New("commands go as UTF-8 text without NUL bytes"))
	}
	return line, nil
}

// Quote wraps s in single quotes, closing them around each single quote it holds
func Quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

func invalid(err error) error {
	return &provider.Error{Code: provider.CodeInvalid, Err: err}
}
