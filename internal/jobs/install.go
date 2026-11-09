package jobs

import (
	"path"
	"slices"
	"strings"
)

// installers are the commands whose run counts as install time (CP 4.7)
var installers = []string{"pip", "uv", "npm", "pnpm", "yarn", "cargo", "go", "apt-get", "make"}

// shells are the programs whose -c script is looked through
var shells = []string{"sh", "bash", "dash", "zsh", "ash"}

// sudoValued are the sudo options that take a value
var sudoValued = []string{"-u", "-g", "-h", "-p", "-C", "-D", "-R", "-T", "-U", "-r", "-t"}

// maxLookThrough bounds how many sudo and shell layers IsInstall looks through
const maxLookThrough = 4

// IsInstall says whether argv runs a known installer as its first command, looking through sudo
// and sh -c
func IsInstall(argv []string) bool {
	for range maxLookThrough {
		argv = skipAssignments(argv)
		if len(argv) == 0 {
			return false
		}
		switch name := path.Base(argv[0]); {
		case name == "sudo":
			argv = afterSudo(argv[1:])
		case slices.Contains(shells, name):
			i := slices.Index(argv, "-c")
			if i < 0 || i+1 >= len(argv) {
				return false
			}
			argv = strings.Fields(argv[i+1])
		default:
			return slices.Contains(installers, name)
		}
	}
	return false
}

// skipAssignments drops leading NAME=value words
func skipAssignments(argv []string) []string {
	for len(argv) > 0 && isAssignment(argv[0]) {
		argv = argv[1:]
	}
	return argv
}

func isAssignment(w string) bool {
	name, _, ok := strings.Cut(w, "=")
	if !ok || name == "" {
		return false
	}
	for i, r := range name {
		letter := r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z'
		if !letter && (i == 0 || r < '0' || r > '9') {
			return false
		}
	}
	return true
}

// afterSudo drops sudo's own options
func afterSudo(argv []string) []string {
	for len(argv) > 0 && strings.HasPrefix(argv[0], "-") {
		if argv[0] == "--" {
			return argv[1:]
		}
		skip := 1
		if slices.Contains(sudoValued, argv[0]) {
			skip = 2
		}
		if skip > len(argv) {
			return nil
		}
		argv = argv[skip:]
	}
	return argv
}
