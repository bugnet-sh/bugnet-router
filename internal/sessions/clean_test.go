package sessions

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestCleanVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/sanitize-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ Name, In, Out string }
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 11 {
		t.Fatalf("read %d vectors, want the gateway's 11 or more", len(cases))
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			eq(t, Clean(c.In, 0), c.Out)
		})
	}
}

func TestCleanAddsWhatTheGatewayLacks(t *testing.T) {
	eq(t, Clean("a\u200bb\u200ec\U000E0041d", 80), "abcd")
	eq(t, Clean("é́́́́", 80), "é́́́")
}

func TestCleanCutsToCharacters(t *testing.T) {
	eq(t, Clean("日本語のファイル", 3), "日本語")
	eq(t, Clean("abc", 3), "abc")
	eq(t, Clean("abcd", 0), "abcd")
	eq(t, Clean("a\xffb", 10), "a�b")
}

func TestCleanRedacts(t *testing.T) {
	tok := "bnsec_" + strings.Repeat("a", 26)
	eq(t, Clean("env "+tok+" done", 0), "env [redacted token] done")
	// A token hidden by an invisible character is still caught, since cleaning comes first
	eq(t, Clean("bnsec\u200b_"+strings.Repeat("a", 26), 0), "[redacted token]")
}

func TestCutBytes(t *testing.T) {
	eq(t, CutBytes("héllo", 2), "h")
	eq(t, CutBytes("héllo", 3), "hé")
	eq(t, CutBytes("abc", 10), "abc")
}
