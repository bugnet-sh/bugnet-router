package x402

import (
	"testing"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
)

func TestCheckEventRefusesANegativeAllowanceLeft(t *testing.T) {
	ev := &routerv1.X402Event{Ref: "abcdefghijklmnop", Status: "paid", Host: "api.example.com",
		Method: "GET", Path: "/x", Count: 1}
	if err := CheckEvent(ev); err != nil {
		t.Fatalf("a valid event was refused: %v", err)
	}
	ev.AllowanceLeftMicros = -1
	if err := CheckEvent(ev); err == nil || err.Error() != "allowance_left_micros" {
		t.Fatalf("got %v, want allowance_left_micros", err)
	}
}
