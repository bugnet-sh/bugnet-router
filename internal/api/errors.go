package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/oynozan/bugnet-router/internal/jobs"
	"github.com/oynozan/bugnet-router/internal/placement"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/streams"
)

const (
	// errorDomain names the router in every google.rpc.ErrorInfo it sends
	errorDomain = "bugnet.router"
	// reasonRateLimited marks a sandbox operation refused by the placement's operation limit
	reasonRateLimited = "RATE_LIMITED"
	// reasonNotReady marks a call refused because the placement is placing, ending or ended
	reasonNotReady = "NOT_READY"
	// reasonExceedsLifetime marks an extension past the provider's maximum sandbox lifetime
	reasonExceedsLifetime = "EXCEEDS_LIFETIME"
)

// Codes a start package's unavailable_reason takes, never error text
const (
	unavailableNoCandidate = saga.ReasonNoCandidate
	unavailableFeature     = "feature_unavailable"
	unavailableInternal    = "internal"
)

var (
	errNotReady     = errors.New("api: placement is not ready")
	errShuttingDown = errors.New("api: server is shutting down")
)

// codes maps service errors to status codes and ErrorInfo reasons, the first match winning
var codes = []struct {
	err    error
	code   connect.Code
	reason string
}{
	{saga.ErrBadToken, connect.CodePermissionDenied, ""},
	{saga.ErrUnderpaid, connect.CodePermissionDenied, ""},
	{saga.ErrHandleMismatch, connect.CodePermissionDenied, ""},
	{saga.ErrUnknownPackage, connect.CodeInvalidArgument, ""},
	{saga.ErrNotExtension, connect.CodeInvalidArgument, ""},
	{jobs.ErrInvalid, connect.CodeInvalidArgument, ""},
	{streams.ErrBadCursor, connect.CodeInvalidArgument, ""},
	{saga.ErrNotReady, connect.CodeFailedPrecondition, reasonNotReady},
	{saga.ErrExceedsLifetime, connect.CodeFailedPrecondition, reasonExceedsLifetime},
	{jobs.ErrNotReady, connect.CodeFailedPrecondition, reasonNotReady},
	{errNotReady, connect.CodeFailedPrecondition, reasonNotReady},
	{placement.ErrNoCandidate, connect.CodeResourceExhausted, ""},
	{jobs.ErrTooManyJobs, connect.CodeResourceExhausted, ""},
	{store.ErrNotFound, connect.CodeNotFound, ""},
	{jobs.ErrNotFound, connect.CodeNotFound, ""},
	{jobs.ErrShuttingDown, connect.CodeUnavailable, ""},
	{errShuttingDown, connect.CodeUnavailable, ""},
	{context.Canceled, connect.CodeCanceled, ""},
	{context.DeadlineExceeded, connect.CodeDeadlineExceeded, ""},
}

// unavailable names an extension refusal as a PackageOption does, its ErrorInfo reason lowercased
func unavailable(refusal error) string {
	if refusal == nil {
		return ""
	}
	for _, c := range codes {
		if errors.Is(refusal, c.err) {
			return strings.ToLower(c.reason)
		}
	}
	return ""
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

// rateLimited refuses a sandbox operation, saying how long until the placement may make another
func rateLimited(wait time.Duration) error {
	ce := withInfo(connect.NewError(connect.CodeResourceExhausted,
		errors.New("too many sandbox operations, slow down")), reasonRateLimited)
	d, err := connect.NewErrorDetail(&errdetails.RetryInfo{RetryDelay: durationpb.New(wait)})
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

// fromBackend reports whether err's Connect error is a provider backend's, inside a provider error
func fromBackend(err error) bool {
	pe, ok := provider.AsError(err)
	var ce *connect.Error
	return ok && errors.As(pe.Err, &ce)
}

func (s *Server) toConnect(err error, fileOp bool) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if errors.As(err, &ce) && !fromBackend(err) {
		return err
	}
	for _, c := range codes {
		if !errors.Is(err, c.err) {
			continue
		}
		if c.reason != "" {
			return withInfo(connect.NewError(c.code, err), c.reason)
		}
		return connect.NewError(c.code, err)
	}
	if e, ok := provider.AsError(err); ok {
		code, msg := connect.CodeUnavailable, "sandbox provider unavailable"
		switch {
		case e.Code == provider.CodeInvalid:
			code, msg = connect.CodeInvalidArgument, "sandbox provider refused the request"
		case e.Code == provider.CodeNotFound && fileOp:
			code, msg = connect.CodeNotFound, "no such file or directory"
		}
		// The provider's text may name the provider or its sandbox, which the gateway never sees
		s.d.Log.Warn("api: provider error", "code", e.Code, "err", err)
		return withInfo(connect.NewError(code, errors.New(msg)), string(e.Code))
	}
	s.d.Log.Error("api: internal error", "err", err)
	return connect.NewError(connect.CodeInternal, errors.New("internal error"))
}
