package x402

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"testing"

	"github.com/bugnet-sh/bugnet-proto/vectors"
)

// vector decodes one bugnet-proto x402 vector file strictly into v
func vector(t *testing.T, name string, v any) {
	t.Helper()
	raw, err := fs.ReadFile(vectors.X402, "x402/"+name)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func TestPathVectors(t *testing.T) {
	var file struct {
		V     int `json:"v"`
		Paths []struct {
			Path string `json:"path"`
			OK   bool   `json:"ok"`
		} `json:"paths"`
		Queries []struct {
			Query string `json:"query"`
			OK    bool   `json:"ok"`
		} `json:"queries"`
	}
	vector(t, "paths.json", &file)
	eq(t, file.V, 1)
	if len(file.Paths) < 30 {
		t.Fatalf("paths.json holds %d paths", len(file.Paths))
	}
	for _, c := range file.Paths {
		if PathOK(c.Path) != c.OK {
			t.Errorf("PathOK(%q) is %v, want %v", c.Path, !c.OK, c.OK)
		}
	}
}

func TestPathRules(t *testing.T) {
	for _, p := range []string{
		"", "/a/.", "/./a", "/..", "/a/%2E%2e/b", "/a%3B", "/a%5C", "/a%2F", "/a%7F",
		"/a%80", "/a%ff", "/a%FF", "/a%1F", "/a%0a", "/a%u0041", "/a%g0", "/a%0g",
		"/a\x00", "/a\x7f", "/a\tb", "/a\x80", "//", "/a/b//",
	} {
		if PathOK(p) {
			t.Errorf("PathOK(%q) is true", p)
		}
	}
	for _, p := range []string{
		"/", "/a", "/a/", "/a%20", "/a%7e", "/a%7E", "/a%2d", "/a%3a", "/a%3A", "/~a/",
		"/a.b/..c/c../", "/@x/!$&'()*+,=:",
	} {
		if !PathOK(p) {
			t.Errorf("PathOK(%q) is false", p)
		}
	}
}
