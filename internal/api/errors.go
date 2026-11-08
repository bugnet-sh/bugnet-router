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

	"github.com/oynozan/bugnet-router/internal/edgeevents"
	"github.com/oynozan/bugnet-router/internal/jobs"
	"github.com/oynozan/bugnet-router/internal/placement"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/streams"
	"github.com/oynozan/bugnet-router/internal/transfer"
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
	// reasonHandleUnknown marks a handle the router never issued, or one that is malformed
	reasonHandleUnknown = "HANDLE_UNKNOWN"
	// reasonJobUnknown marks a job the handle's placement does not have
	reasonJobUnknown = "JOB_UNKNOWN"
	// reasonPackageUnknown marks a package the catalog lacks
	reasonPackageUnknown = "PACKAGE_UNKNOWN"
	// reasonNotStartPackage marks an extension package given where a start package is needed
	reasonNotStartPackage = "NOT_START_PACKAGE"
	// reasonFileNotFound marks a sandbox path that names no file
	reasonFileNotFound = "FILE_NOT_FOUND"
	// reasonNoSpace marks a write refused because the sandbox's disk is full
	reasonNoSpace = "NO_SPACE"
	// reasonFeatureUnavailable marks a call this router has no settings for
	reasonFeatureUnavailable = "FEATURE_UNAVAILABLE"
)

// Transfer reasons, named as DP 11.3 names them
const (
	reasonPathRefused      = "PATH_REFUSED"
	reasonSizeRequired     = "SIZE_REQUIRED"
	reasonSizeMismatch     = "SIZE_MISMATCH"
	reasonIsDirectory      = "IS_DIRECTORY"
	reasonNotRegular       = "NOT_REGULAR"
	reasonTokenUnknown     = "TOKEN_UNKNOWN"
	reasonTokenUsed        = "TOKEN_USED"
	reasonTokenExpired     = "TOKEN_EXPIRED"
	reasonInUse            = "IN_USE"
	reasonFileChanged      = "FILE_CHANGED"
	reasonOverCap          = "OVER_CAP"
	reasonRange            = "RANGE"
	reasonBudgetSpent      = "BUDGET_SPENT"
	reasonTooManyTransfers = "TOO_MANY_TRANSFERS"
	reasonChecksumMismatch = "CHECKSUM_MISMATCH"
)

// Preview reasons, named as DP 11.3 names them
const (
	reasonPortRefused     = "PORT_REFUSED"
	reasonPreviewUnknown  = "PREVIEW_UNKNOWN"
	reasonTokenInvalid    = "TOKEN_INVALID"
	reasonPreviewRevoked  = "PREVIEW_REVOKED"
	reasonTooManyPreviews = "TOO_MANY_PREVIEWS"
	reasonGateUnavailable = "GATE_UNAVAILABLE"
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
	// Transfer refusals that transfer's own errors do not cover
	errSizeRequired = errors.New("api: an upload needs size_bytes, 0 included")
	errSizeMismatch = errors.New("api: the upload's bytes differ from its declared size")
	errOverCap      = errors.New("api: the file is over the transfer cap")
	errRange        = errors.New("api: the range lies outside the file")
	errBudgetSpent  = errors.New("api: the placement spent its traffic budget")
	errChecksum     = errors.New("api: the upload's SHA-256 differs from the one given")
	errNoHost       = errors.New("api: this router serves no transfer host")
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
	{edgeevents.ErrBadCursor, connect.CodeInvalidArgument, ""},
	{saga.ErrNotReady, connect.CodeFailedPrecondition, reasonNotReady},
	{saga.ErrExceedsLifetime, connect.CodeFailedPrecondition, reasonExceedsLifetime},
	{jobs.ErrNotReady, connect.CodeFailedPrecondition, reasonNotReady},
	{errNotReady, connect.CodeFailedPrecondition, reasonNotReady},
	{placement.ErrNoCandidate, connect.CodeResourceExhausted, ""},
	{jobs.ErrTooManyJobs, connect.CodeResourceExhausted, ""},
	{transfer.ErrPathRefused, connect.CodeInvalidArgument, reasonPathRefused},
	{transfer.ErrIsDirectory, connect.CodeInvalidArgument, reasonIsDirectory},
	{transfer.ErrNotRegular, connect.CodeInvalidArgument, reasonNotRegular},
	{errSizeRequired, connect.CodeInvalidArgument, reasonSizeRequired},
	{errSizeMismatch, connect.CodeInvalidArgument, reasonSizeMismatch},
	{transfer.ErrUnknown, connect.CodeNotFound, reasonTokenUnknown},
	{transfer.ErrUsed, connect.CodeFailedPrecondition, reasonTokenUsed},
	{transfer.ErrClaimsSpent, connect.CodeFailedPrecondition, reasonTokenUsed},
	{transfer.ErrExpired, connect.CodeFailedPrecondition, reasonTokenExpired},
	{transfer.ErrInUse, connect.CodeFailedPrecondition, reasonInUse},
	{transfer.ErrFileChanged, connect.CodeFailedPrecondition, reasonFileChanged},
	{errNoHost, connect.CodeFailedPrecondition, reasonFeatureUnavailable},
	{errPortRefused, connect.CodeInvalidArgument, reasonPortRefused},
	{errPreviewUnknown, connect.CodeNotFound, reasonPreviewUnknown},
	{errTokenInvalid, connect.CodePermissionDenied, reasonTokenInvalid},
	{store.ErrPreviewRevoked, connect.CodeFailedPrecondition, reasonPreviewRevoked},
	{store.ErrTooManyPreviews, connect.CodeResourceExhausted, reasonTooManyPreviews},
	{errNoPreviews, connect.CodeFailedPrecondition, reasonFeatureUnavailable},
	{errNoFeature, connect.CodeFailedPrecondition, reasonFeatureUnavailable},
	{saga.ErrGateUnavailable, connect.CodeUnavailable, reasonGateUnavailable},
	{errOverCap, connect.CodeOutOfRange, reasonOverCap},
	{errRange, connect.CodeOutOfRange, reasonRange},
	{errBudgetSpent, connect.CodeResourceExhausted, reasonBudgetSpent},
	{transfer.ErrTooMany, connect.CodeResourceExhausted, reasonTooManyTransfers},
	{errChecksum, connect.CodeDataLoss, reasonChecksumMismatch},
	{errWrongWay, connect.CodeInvalidArgument, ""},
	{errFirstPart, connect.CodeInvalidArgument, ""},
	{errIdle, connect.CodeDeadlineExceeded, ""},
	{errStalled, connect.CodeDeadlineExceeded, ""},
	{errHardStop, connect.CodeDeadlineExceeded, ""},
	{store.ErrUnknownHandle, connect.CodeNotFound, reasonHandleUnknown},
	{jobs.ErrNotFound, connect.CodeNotFound, reasonJobUnknown},
	{store.ErrNotFound, connect.CodeNotFound, ""},
	{jobs.ErrShuttingDown, connect.CodeUnavailable, ""},
	{jobs.ErrNotRecorded, connect.CodeUnavailable, ""},
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

func invalid(format string, args ...any) *connect.Error {
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
		msg := err
		if c.code == connect.CodeCanceled || c.code == connect.CodeDeadlineExceeded ||
			c.err == saga.ErrGateUnavailable {
			// What wraps a context or gate error can name a job or the provider's words
			msg = c.err
		}
		if c.reason != "" {
			return withInfo(connect.NewError(c.code, msg), c.reason)
		}
		return connect.NewError(c.code, msg)
	}
	if e, ok := provider.AsError(err); ok {
		// The provider's text may name the provider or its sandbox, which the gateway never sees
		s.d.Log.Warn("api: provider error", "code", e.Code, "err", err)
		code, msg := connect.CodeUnavailable, "sandbox provider unavailable"
		switch {
		case e.Code == provider.CodeInvalid:
			code, msg = connect.CodeInvalidArgument, "sandbox provider refused the request"
		case e.Code == provider.CodeNotFound && fileOp:
			return withInfo(connect.NewError(connect.CodeNotFound,
				errors.New("no such file or directory")), reasonFileNotFound)
		case e.Code == provider.CodeNoSpace:
			return withInfo(connect.NewError(connect.CodeResourceExhausted,
				errors.New("the sandbox's disk is full")), reasonNoSpace)
		}
		return withInfo(connect.NewError(code, errors.New(msg)), string(e.Code))
	}
	s.d.Log.Error("api: internal error", "err", err)
	return connect.NewError(connect.CodeInternal, errors.New("internal error"))
}
