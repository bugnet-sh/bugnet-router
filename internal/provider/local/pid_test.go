package local

import "testing"

func TestParsePid(t *testing.T) {
	for in, want := range map[string]int{
		"42\n": 42, "2": 2, "007": 7,
		"": 0, "\n": 0, "1": 0, "0": 0, "00": 0, "-1": 0, "-42": 0, "+42": 0,
		"4 2": 0, "0x10": 0, "4.2": 0, "abc": 0, "99999999999": 0,
	} {
		got, ok := parsePid([]byte(in))
		if got != want || ok != (want != 0) {
			t.Errorf("parsePid(%q) gave %d, %v, want %d", in, got, ok, want)
		}
	}
}
