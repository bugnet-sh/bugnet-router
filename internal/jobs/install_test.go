package jobs_test

import (
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/jobs"
)

func TestInstallJobsMarked(t *testing.T) {
	for argv, want := range map[string]bool{
		"sudo sh -c 'pip install x'":   true,
		"pip install x":                true,
		"/usr/bin/npm ci":              true,
		"sudo -E -u root apt-get -y x": true,
		"bash -c cargo build":          true,
		"FOO=1 uv sync":                true,
		"sudo sudo go mod download":    true,
		"make":                         true,
		"python train.py":              false,
		"sh -c 'python train.py'":      false,
		"sudo":                         false,
		"sh -c":                        false,
		"echo pip install":             false,
		"sudo sh -c 'cd x && pip i'":   false,
		"":                             false,
		"sudo -u":                      false,
	} {
		var args []string
		switch argv {
		case "sudo sh -c 'pip install x'":
			args = []string{"sudo", "sh", "-c", "pip install x"}
		case "sh -c 'python train.py'":
			args = []string{"sh", "-c", "python train.py"}
		case "sudo sh -c 'cd x && pip i'":
			args = []string{"sudo", "sh", "-c", "cd x && pip i"}
		case "bash -c cargo build":
			args = []string{"bash", "-c", "cargo build"}
		default:
			args = fields(argv)
		}
		if got := jobs.IsInstall(args); got != want {
			t.Errorf("%q: got %v, want %v", argv, got, want)
		}
	}
}

func fields(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ' ' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
