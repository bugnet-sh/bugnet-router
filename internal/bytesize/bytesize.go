// Package bytesize reads byte sizes written as a whole number with an optional binary unit
package bytesize

import (
	"strconv"
	"strings"
)

// Max is the largest size Parse accepts, 1 TiB, so sums of sizes never overflow
const Max = 1 << 40

// units are the suffixes a size may end in
var units = []struct {
	suffix string
	size   int64
}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}}

// Parse reads 1 byte to 1 TiB written as bytes, KiB, MiB or GiB, such as 65536, 64KiB or 2GiB
func Parse(raw string) (int64, bool) {
	digits, unit := raw, int64(1)
	for _, u := range units {
		if d, ok := strings.CutSuffix(raw, u.suffix); ok {
			digits, unit = d, u.size
			break
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n < 1 || n > Max/unit {
		return 0, false
	}
	return n * unit, true
}
