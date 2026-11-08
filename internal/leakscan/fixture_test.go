package leakscan

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// fixture builds the tar that collect.sh would leave
type fixture struct {
	t  *testing.T
	b  bytes.Buffer
	tw *tar.Writer
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t}
	f.tw = tar.NewWriter(&f.b)
	return f
}

// file adds a regular file
func (f *fixture) file(name string, data ...string) *fixture {
	f.t.Helper()
	body := strings.Join(data, "")
	err := f.tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)),
		Typeflag: tar.TypeReg})
	if err == nil {
		_, err = f.tw.Write([]byte(body))
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return f
}

// environ adds /proc/<pid>/environ from NAME=value pairs
func (f *fixture) environ(pid int, pairs ...string) *fixture {
	return f.file(fmt.Sprintf("proc/%d/environ", pid), strings.Join(pairs, "\x00")+"\x00")
}

func (f *fixture) archive() *bytes.Reader {
	f.t.Helper()
	if err := f.tw.Close(); err != nil {
		f.t.Fatal(err)
	}
	return bytes.NewReader(f.b.Bytes())
}

// values hold bytes that every encoding writes differently
func values() Needles {
	var n Needles
	for i := range 3 {
		n = append(n, Needle{fmt.Sprintf("N%d", i),
			[]byte(fmt.Sprintf("\xfb\xff\xbe needle %d with spaces/slash", i))})
	}
	return n
}

// inEachEncoding is v written in the five forms
func inEachEncoding(v []byte) map[string]string {
	return map[string]string{
		"raw":       string(v),
		"base64":    base64.StdEncoding.EncodeToString(v),
		"base64url": base64.URLEncoding.EncodeToString(v),
		"hex":       hex.EncodeToString(v),
		"url":       url.QueryEscape(string(v)),
	}
}

func fixtureWithEachNeedleInEachEncoding(t *testing.T) *bytes.Reader {
	f := newFixture(t)
	for _, n := range values() {
		for enc, text := range inEachEncoding(n.Value) {
			f.file("etc/"+n.Name+"-"+enc, "before ", text, " after")
		}
	}
	return f.archive()
}
