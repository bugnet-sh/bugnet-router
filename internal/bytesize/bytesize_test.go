package bytesize

import "testing"

func TestParse(t *testing.T) {
	sizes := map[string]int64{
		"1": 1, "8388608": 8 << 20, "8KiB": 8 << 10, "8MiB": 8 << 20, "2GiB": 2 << 30,
		"+2MiB": 2 << 20, "1048576MiB": 1 << 40, "1024GiB": 1 << 40,
	}
	for _, raw := range []string{
		"", "KiB", "MiB", "GiB", "0", "0MiB", "-1KiB", "8 KiB", "8kib", "8GB", "8TiB", "1e3",
		"0x10", "1048577MiB", "1025GiB", "1099511627777", "1.5GiB",
	} {
		sizes[raw] = 0
	}
	for raw, want := range sizes {
		got, ok := Parse(raw)
		if want == 0 {
			if ok {
				t.Errorf("%q gave %d, want it refused", raw, got)
			}
			continue
		}
		if !ok || got != want {
			t.Errorf("%q gave %d, %v, want %d", raw, got, ok, want)
		}
	}
}
