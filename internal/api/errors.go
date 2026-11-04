package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/oynozan/bugnet-router/internal/jobs"
	"github.com/oynozan/bugnet-router/internal/placement"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/streams"
)

// errorDomain names the router in every google.rpc.ErrorInfo it sends
const errorDomain = "bugnet.router"

var (
	errNotReady     = errors.New("api: placement is not ready")
	errShuttingDown = errors.New("api: server is shutting down")
)

// codes maps service errors to status codes, the first match winning
var codes = []struct {
	err  error
	code connect.Code
}{
	{saga.ErrBadToken, connect.CodePermissionDenied},
	{saga.ErrUnderpaid, connect.CodePermissionDenied},
	{saga.ErrHandleMismatch, connect.CodePermissionDenied},
	{saga.ErrUnknownPackage, connect.CodeInvalidArgument},
	{saga.ErrNotExtension, connect.CodeInvalidArgument},
	{jobs.ErrInvalid, connect.CodeInvalidArgument},
	{streams.ErrBadCursor, connect.CodeInvalidArgument},
	{saga.ErrNotReady, connect.CodeFailedPrecondition},
	{saga.ErrExceedsLifetime, connect.CodeFailedPrecondition},
	{jobs.ErrNotReady, connect.CodeFailedPrecondition},
	{errNotReady, connect.CodeFailedPrecondition},
	{placement.ErrNoCandidate, connect.CodeResourceExhausted},
	{store.ErrNotFound, connect.CodeNotFound},
	{jobs.ErrNotFound, connect.CodeNotFound},
	{jobs.ErrShuttingDown, connect.CodeUnavailable},
	{errShuttingDown, connect.CodeUnavailable},
	{context.Canceled, connect.CodeCanceled},
	{context.DeadlineExceeded, connect.CodeDeadlineExceeded},
}

func invalid(format string, args ...any) error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(format, args...))
}

// withInfo attaches a google.rpc.ErrorInfo carrying reason to ce
func withInfo(ce *connect.Error, reason string) *connect.Error {
	d, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: reason, Domain: errorDomain})
	if err == nil {
		ce.AddDetail(d)
	}
	return ce
}

// status turns a service error into the Connect error the API answers with
func (s *Server) status(err error) error {
	return s.toConnect(err, false)
}

// fileStatus is status for a file operation, which reports a missing path as not found
func (s *Server) fileStatus(err error) error {
	return s.toConnect(err, true)
}

func (s *Server) toConnect(err error, fileOp bool) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		return err
	}
	for _, c := range codes {
		if errors.Is(err, c.err) {
			return connect.NewError(c.code, err)
		}
	}
	if e, ok := provider.AsError(err); ok {
		code := connect.CodeUnavailable
		switch {
		case e.Code == provider.CodeInvalid:
			code = connect.CodeInvalidArgument
		case e.Code == provider.CodeNotFound && fileOp:
			code = connect.CodeNotFound
		}
		return withInfo(connect.NewError(code, err), string(e.Code))
	}
	s.d.Log.Error("api: internal error", "err", err)
	return connect.NewError(connect.CodeInternal, errors.New("internal error"))
}
