// Package leakscan searches the archive of a fresh sandbox for every secret of the router.
//
// The archive is a tar that collect.sh makes inside the sandbox. Needles are searched in raw
// bytes, base64, base64url, hex and URL-encoded form, shapes are searched by pattern, and a
// finding names a rule, a path, a pid and an offset, never a value.
package leakscan

import (
	"archive/tar"
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Collect is the script that runs as root in the sandbox and writes /tmp/leak/archive.tar
//
//go:embed collect.sh
var Collect string

// ArchivePath is where Collect leaves its archive
const ArchivePath = "/tmp/leak/archive.tar"

// MaxArchive is the size at which a run fails rather than pass on a partial scan
const MaxArchive = 64 << 20

// ErrTooLarge means the archive reached MaxArchive
var ErrTooLarge = errors.New("leakscan: the archive reached 64 MiB, so the scan is partial")

// Finding is one place a rule matched, and never holds the value
type Finding struct {
	Rule   string
	Path   string
	Pid    int
	Offset int64
}

// String prints a finding without any value
func (f Finding) String() string {
	pid := ""
	if f.Pid != 0 {
		pid = " pid " + strconv.Itoa(f.Pid)
	}
	return fmt.Sprintf("%s at %s%s offset %d", f.Rule, f.Path, pid, f.Offset)
}

// Result is a full scan
type Result struct {
	Findings []Finding
	// Own holds this sandbox's values from its own files, to search for in the other sandbox
	Own Needles
	// Names lists variables named like secrets that the allowlist does not expect
	Names []string
	// Vars lists the name of every environment variable in the archive
	Vars []string
}

type entry struct {
	path string
	data []byte
}

// Scan reports every finding in the archive that the allowlist does not cover
func Scan(archive io.Reader, n Needles, a Allowlist) ([]Finding, error) {
	r, err := ScanFull(archive, n, a)
	return r.Findings, err
}

// ScanFull is Scan with the sandbox's own values and the review list of variable names
func ScanFull(archive io.Reader, n Needles, a Allowlist) (Result, error) {
	entries, err := read(archive)
	if err != nil {
		return Result{}, err
	}
	var res Result
	var pats []pattern
	for _, x := range n {
		pats = append(pats, patterns("needle", x.Name, x.Value, "")...)
	}
	for _, o := range a.Own {
		for _, e := range entries {
			v := bytes.TrimSpace(e.data)
			if e.path == o.Path && len(v) >= minNeedle && !n.hasValue(v) {
				res.Own = append(res.Own, Needle{o.Name, v})
				pats = append(pats, patterns("own", o.Name, v, o.Path)...)
			}
		}
	}
	names, vars := map[string]bool{}, map[string]bool{}
	for _, e := range entries {
		res.Findings = append(res.Findings, scanEntry(e, pats, a, names, vars)...)
	}
	for name := range vars {
		res.Vars = append(res.Vars, name)
	}
	slices.Sort(res.Vars)
	for _, name := range a.Names {
		delete(names, name)
	}
	for name := range names {
		res.Names = append(res.Names, name)
	}
	slices.Sort(res.Names)
	slices.SortFunc(res.Findings, func(x, y Finding) int {
		return strings.Compare(x.String(), y.String())
	})
	return res, nil
}

// read unpacks the regular files of the archive, which may not reach MaxArchive
func read(archive io.Reader) ([]entry, error) {
	lr := &io.LimitedReader{R: archive, N: MaxArchive}
	tr := tar.NewReader(lr)
	var out []entry
	var rerr error
	for {
		h, err := tr.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				rerr = err
			}
			break
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			rerr = err
			break
		}
		out = append(out, entry{strings.TrimPrefix(path.Clean("/"+h.Name), "/"), data})
	}
	if rerr == nil {
		_, rerr = io.Copy(io.Discard, lr)
	}
	if lr.N == 0 {
		return nil, ErrTooLarge
	}
	if rerr != nil {
		return nil, fmt.Errorf("leakscan: read the archive: %w", rerr)
	}
	return out, nil
}

var (
	procPath = regexp.MustCompile(`^proc/([0-9]+)/`)
	// secretName matches the names of variables listed for review
	secretName = regexp.MustCompile(`(KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL)`)
	varName    = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)=$`)
)

func scanEntry(e entry, pats []pattern, a Allowlist, names, vars map[string]bool) []Finding {
	var out []Finding
	pid := 0
	if m := procPath.FindStringSubmatch(e.path); m != nil {
		pid, _ = strconv.Atoi(m[1])
	}
	add := func(rule string, off int) {
		out = append(out, Finding{Rule: rule, Path: e.path, Pid: pid, Offset: int64(off)})
	}
	if strings.HasPrefix(e.path, "metadata/") && !a.Metadata {
		add("metadata answered", 0)
	}
	for _, p := range pats {
		if p.skip == e.path {
			continue
		}
		for from := 0; ; {
			i := bytes.Index(e.data[from:], p.b)
			if i < 0 {
				break
			}
			add(p.rule, from+i)
			from += i + 1
		}
	}
	for _, s := range Shapes {
		for _, m := range s.Re.FindAllIndex(e.data, -1) {
			if m[0] > 0 && isWord(e.data[m[0]-1]) && s.Name != "private-key" {
				continue
			}
			if !a.allows(s.Name, e.path, envOf(e.data, m[0])) {
				add("shape "+s.Name, m[0])
			}
		}
	}
	if strings.HasSuffix(e.path, "/environ") {
		for _, kv := range bytes.Split(e.data, []byte{0}) {
			name, _, ok := strings.Cut(string(kv), "=")
			if !ok {
				continue
			}
			vars[name] = true
			if secretName.MatchString(name) {
				names[name] = true
			}
		}
	}
	return out
}

func isWord(b byte) bool {
	return b == '_' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

// envOf is the name of the variable whose value starts before off, or empty
func envOf(data []byte, off int) string {
	start := off
	for start > 0 && !bytes.ContainsRune([]byte(" \t\r\n\x00\"'"), rune(data[start-1])) {
		start--
	}
	if m := varName.FindSubmatch(data[start:off]); m != nil {
		return string(m[1])
	}
	return ""
}
