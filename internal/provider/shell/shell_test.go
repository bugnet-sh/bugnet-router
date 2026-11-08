package shell_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/shell"
)

func TestCommandRunsUnderShell(t *testing.T) {
	cwd := filepath.Join(t.TempDir(), "it's a dir")
	if err := os.Mkdir(cwd, 0o755); err != nil {
		t.Fatal(err)
	}
	greeting := "it's $HOME `id` \"q\" \\"
	line, err := shell.Command(provider.ExecSpec{
		Argv: []string{"sh", "-c", `printf '%s|' "$GREETING" "$1" "$2" "$(pwd)"; cat`, "argv0",
			"a b", "it's;$(echo no)"},
		Cwd: cwd,
		Env: map[string]string{"GREETING": greeting},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, "cd '") || !strings.Contains(line, " env ") ||
		!strings.HasSuffix(line, " </dev/null") {
		t.Fatalf("command %q does not cd, set env and close stdin", line)
	}
	cmd := exec.Command("sh", "-c", line)
	cmd.Stdin = strings.NewReader("stdin must stay unread")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	if want := greeting + "|a b|it's;$(echo no)|" + cwd + "|"; string(out) != want {
		t.Fatalf("shell ran %q, want %q", out, want)
	}
}

func TestCommandWithoutCwdOrEnv(t *testing.T) {
	line, err := shell.Command(provider.ExecSpec{Argv: []string{"echo", "a'b"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `'echo' 'a'"'"'b' </dev/null`; line != want {
		t.Fatalf("command %q, want %q", line, want)
	}
}

func TestCommandRejectsBadSpecs(t *testing.T) {
	bad := map[string]provider.ExecSpec{
		"no command":       {},
		"bad env key":      {Argv: []string{"true"}, Env: map[string]string{"A-B": "1"}},
		"nul byte":         {Argv: []string{"echo", "a\x00b"}},
		"invalid utf-8":    {Argv: []string{"echo", "\xff"}},
		"env reads argv0":  {Argv: []string{"A=B"}, Env: map[string]string{"X": "1"}},
		"env option argv0": {Argv: []string{"-i"}, Env: map[string]string{"X": "1"}},
	}
	for name, spec := range bad {
		_, err := shell.Command(spec)
		if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
			t.Errorf("%s gave %v, want invalid", name, err)
		}
	}
}

func TestQuote(t *testing.T) {
	for _, s := range []string{"", "plain", "it's", "'", "a b\nc", "$(x) `y` \\"} {
		out, err := exec.Command("sh", "-c", "printf %s "+shell.Quote(s)).Output()
		if err != nil || string(out) != s {
			t.Errorf("Quote(%q) read back as %q, %v", s, out, err)
		}
	}
}
