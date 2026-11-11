package gpustatus_test

import (
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider/gpustatus"
)

// a Lium Reason matches whole words only, so FAILED is never read inside REBOOT_FAILED
func TestLiumReasonWords(t *testing.T) {
	reason := gpustatus.LiumReason(gpustatus.LiumStatusRebootFailed, gpustatus.LiumFlagNodeOutage)
	if reason != "REBOOT_FAILED node_outage" {
		t.Fatalf("reason %q", reason)
	}
	for word, want := range map[string]bool{
		gpustatus.LiumStatusRebootFailed: true, gpustatus.LiumFlagNodeOutage: true,
		gpustatus.LiumStatusFailed: false, gpustatus.LiumFlagNodeOffline: false, "node": false,
		"": false,
	} {
		if got := gpustatus.HasWord(reason, word); got != want {
			t.Errorf("HasWord(%q, %q) = %v, want %v", reason, word, got, want)
		}
	}
	if gpustatus.HasWord("", gpustatus.LiumMoved) {
		t.Error("an empty reason holds a word")
	}
}
