package main

import (
	"fmt"

	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
)

// traceID prints the order trace id and root span id of a handle, derived offline (DP 20.2.1)
func (c *cli) traceID(args []string) error {
	fs := newFlags("trace-id")
	h := fs.String("handle", "", "the placement's handle")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	sum, err := handle.Hash(*h)
	if err != nil {
		return fmt.Errorf("%w: --handle must be a handle, sbx_ and 26 characters", errUsage)
	}
	tid, sid := telemetry.OrderIDs(sum)
	_, err = fmt.Fprintf(c.stdout, "trace_id=%s span_id=%s\n", tid, sid)
	return err
}
