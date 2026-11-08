package api

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// lateCtx is past its deadline before its own timer has ended it, as a socket deadline sees first
type lateCtx struct{ context.Context }

func (lateCtx) Deadline() (time.Time, bool) { return time.Now().Add(-time.Millisecond), true }

func TestSpendTakesAPassedDeadlineAsTheCallerGivingUp(t *testing.T) {
	_, prefix := testenv.Valkey(t)
	// A client of its own stays in valkey-go's synchronous mode, which sets the socket deadline
	c, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{testenv.ValkeyAddr()}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var logs bytes.Buffer
	s := &Server{
		d:   Deps{Log: slog.New(slog.NewTextHandler(&logs, nil))},
		ops: ratelimit.New(c, prefix, 1, 1),
	}
	err = s.spend(lateCtx{context.Background()}, store.Placement{ID: uuid.New()})
	if !errors.Is(err, context.DeadlineExceeded) || logs.Len() != 0 {
		t.Fatalf("spend past the caller's deadline gave %v and logged %q, want DeadlineExceeded "+
			"and no warning", err, logs.String())
	}
}
