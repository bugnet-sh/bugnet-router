package x402

import "strings"

// PathOK applies XS 6.7's path rules to a path with no query
func PathOK(path string) bool {
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "//") {
		return false
	}
	for i := 0; i < len(path); i++ {
		switch c := path[i]; {
		case c < 0x21 || c > 0x7e, c == ';', c == '\\':
			return false
		case c == '%':
			if i+2 >= len(path) || !encodedOK(path[i+1], path[i+2]) {
				return false
			}
			i += 2
		}
	}
	for seg := range strings.SplitSeq(path, "/") {
		if seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// encodedOK says whether %hl is two hex digits for a byte XS 6.7 lets through
func encodedOK(h, l byte) bool {
	hi, okH := hex(h)
	lo, okL := hex(l)
	if !okH || !okL {
		return false
	}
	switch b := hi<<4 | lo; {
	case b < 0x20, b >= 0x7f, b == '/', b == '\\', b == '.', b == ';', b == '%':
		return false
	}
	return true
}

func hex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
