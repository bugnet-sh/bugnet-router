package api

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/oynozan/bugnet-router/internal/jobs"
	"github.com/oynozan/bugnet-router/internal/streams"
	"github.com/oynozan/bugnet-router/internal/testenv"
)

func TestCallEndedByItsContextQuotesNoJob(t *testing.T) {
	c, prefix := testenv.Valkey(t)
	bus := streams.NewBus(c, prefix)
	const job = "j_quotedjobquotedjobquotedj"
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	late, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()

	s := &Server{d: Deps{Log: slog.New(slog.DiscardHandler)}}
	for _, tc := range []struct {
		ctx   context.Context
		cause error
		code  connect.Code
	}{
		{cancelled, context.Canceled, connect.CodeCanceled},
		{late, context.DeadlineExceeded, connect.CodeDeadlineExceeded},
	} {
		cause := bus.RequestCancel(tc.ctx, job, time.Now().Add(time.Minute))
		if !errors.Is(cause, tc.cause) || !strings.Contains(cause.Error(), job) {
			t.Fatalf("request cancel gave %v, want %v naming the job", cause, tc.cause)
		}
		err := s.status(cause)
		var ce *connect.Error
		if !errors.As(err, &ce) || ce.Code() != tc.code || !errors.Is(err, tc.cause) {
			t.Fatalf("status of %v is %v, want %v", cause, err, tc.code)
		}
		if strings.Contains(ce.Message(), job) {
			t.Errorf("%v answer %q quotes the job", tc.code, ce.Message())
		}
	}
}

func TestUnrecordedHoldIsUnavailableWithNoReason(t *testing.T) {
	s := &Server{d: Deps{Log: slog.New(slog.DiscardHandler)}}
	err := s.status(jobs.ErrNotRecorded)
	var ce *connect.Error
	if !errors.As(err, &ce) || ce.Code() != connect.CodeUnavailable {
		t.Fatalf("status of an unrecorded hold is %v, want Unavailable", err)
	}
	if len(ce.Details()) != 0 {
		t.Errorf("an unrecorded hold carries %d details, want none", len(ce.Details()))
	}
}
