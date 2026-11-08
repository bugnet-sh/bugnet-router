package leakscan

import (
	"bytes"
	"strings"
)

// Place is one file or output of a sandbox, named for the report
type Place struct {
	Name    string
	Content []byte
}

// CheckPlaces compares the places of a sandbox with a secret to what CP 11.6 expects
//
// Each place in holds must show the placeholder and not the value. No place in clean may show
// the value, the placeholder, or a word of ours. The answers name places and never print them.
func CheckPlaces(holds, clean []Place, placeholder, value string) []string {
	var bad []string
	for _, p := range holds {
		if !bytes.Contains(p.Content, []byte(placeholder)) {
			bad = append(bad, p.Name+" lacks the placeholder")
		}
		if bytes.Contains(p.Content, []byte(value)) {
			bad = append(bad, p.Name+" holds the value")
		}
	}
	for _, p := range clean {
		low := strings.ToLower(string(p.Content))
		if bytes.Contains(p.Content, []byte(value)) {
			bad = append(bad, p.Name+" holds the value")
		}
		if bytes.Contains(p.Content, []byte(placeholder)) || strings.Contains(low, "bnsec_") ||
			strings.Contains(low, "bugnet") {
			bad = append(bad, p.Name+" holds something of ours")
		}
	}
	return bad
}
