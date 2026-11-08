package estimatortest

import (
	"context"
	"sync/atomic"

	"github.com/valkey-io/valkey-go"
)

// Flaky wraps a Valkey client and, while Down is set, answers every command with an error as a
// stopped server would
type Flaky struct {
	valkey.Client
	Down atomic.Bool
}

func dead(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	return ctx
}

// Do runs cmd unless Down is set
func (f *Flaky) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	if f.Down.Load() {
		ctx = dead(ctx)
	}
	return f.Client.Do(ctx, cmd)
}

// DoMulti runs cmds unless Down is set
func (f *Flaky) DoMulti(ctx context.Context, cmds ...valkey.Completed) []valkey.ValkeyResult {
	if f.Down.Load() {
		ctx = dead(ctx)
	}
	return f.Client.DoMulti(ctx, cmds...)
}
