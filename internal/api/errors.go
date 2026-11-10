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

	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/jobs"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/streams"
	"github.com/bugnet-sh/bugnet-router/internal/transfer"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
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
	// reasonQuoteInvalid marks a quote id that is not 1 to 128 printable ASCII bytes
	reasonQuoteInvalid = "QUOTE_INVALID"
	// reasonNotStartPackage marks an extension package given where a start package is needed
	reasonNotStartPackage = "NOT_START_PACKAGE"
	// reasonFileNotFound marks a sandbox path that names no file
	reasonFileNotFound = "FILE_NOT_FOUND"
	// reasonNoSpace marks a write refused because the sandbox's disk is full
	reasonNoSpace = "NO_SPACE"
	// reasonFeatureUnavailable marks a call this router has no settings for
	reasonFeatureUnavailable = "FEATURE_UNAVAILABLE"
	// reasonHookUnknownProvider marks a webhook for a provider the router verifies none for
	reasonHookUnknownProvider = "HOOK_UNKNOWN_PROVIDER"
	// reasonHookBadSignature marks a webhook whose signature no current secret matches
	reasonHookBadSignature = "HOOK_BAD_SIGNATURE"
	// reasonSessionUnknown marks a session id with no record and a wrong key alike
	reasonSessionUnknown = "SESSION_UNKNOWN"
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

// Wallet reasons, named as CP 13.1 names them
const (
	reasonPayerInvalid       = "PAYER_INVALID"
	reasonPayerRequired      = "PAYER_REQUIRED"
	reasonWalletBlocked      = "WALLET_BLOCKED"
	reasonWalletBusy         = "WALLET_BUSY"
	reasonWalletRateLimited  = "WALLET_RATE_LIMITED"
	reasonFacilitatorAddress = "FACILITATOR_ADDRESS"
)

// Benchmark reasons, named as CP 13.1 names them
const (
	reasonCostUnconfirmed = "COST_UNCONFIRMED"
	reasonProviderBlocked = "PROVIDER_BLOCKED"
	reasonBenchUnknown    = "BENCH_UNKNOWN"
)

// Pool reasons, named as CP 13.1 names them
const reasonPoolUnknown = "POOL_UNKNOWN"

// Preview reasons, named as DP 11.3 names them
const (
	reasonPortRefused     = "PORT_REFUSED"
	reasonPreviewUnknown  = "PREVIEW_UNKNOWN"
	reasonTokenInvalid    = "TOKEN_INVALID"
	reasonPreviewRevoked  = "PREVIEW_REVOKED"
	reasonTooManyPreviews = "TOO_MANY_PREVIEWS"
	reasonGateUnavailable = "GATE_UNAVAILABLE"
)

// SSH reasons, named as DP 11.3 names them
const (
	reasonKeyInvalid         = "KEY_INVALID"
	reasonTooManyKeys        = "TOO_MANY_KEYS"
	reasonGrantUnknown       = "GRANT_UNKNOWN"
	reasonGrantInvalid       = "GRANT_INVALID"
	reasonTooManyConnections = "TOO_MANY_CONNECTIONS"
	reasonSSHPreparing       = "SSH_PREPARING"
)

// x402 reasons, named as XS 6.3 names them
const (
	reasonHostRefused      = "HOST_REFUSED"
	reasonPrefixRefused    = "PREFIX_REFUSED"
	reasonServiceDuplicate = "SERVICE_DUPLICATE"
	reasonMethodRefused    = "METHOD_REFUSED"
	reasonPriceRefused     = "PRICE_REFUSED"
	reasonPayToRefused     = "PAYTO_REFUSED"
	reasonX402Disabled     = "X402_DISABLED"
	reasonHostInUse        = "HOST_IN_USE"
	reasonHostNotAllowed   = "HOST_NOT_ALLOWED"
	reasonHostBlocked      = "HOST_BLOCKED"
	reasonTooManyServices  = "TOO_MANY_SERVICES"
	reasonTooManySecrets   = "TOO_MANY_SECRETS"
	reasonServiceUnknown   = "SERVICE_UNKNOWN"
	reasonOrderUnknown     = "ORDER_UNKNOWN"
)

// Codes a start package's unavailable_reason takes, never error text
const (
	unavailableNoCandidate = saga.ReasonNoCandidate
	unavailableFeature     = "feature_unavailable"
	unavailableInternal    = "internal"
)

var (
	errNotReady     = errors.New("api: placement is not ready")
	errOrderUnknown = errors.New("api: no placement has this start order")
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
	{saga.ErrPayerRequired, connect.CodePermissionDenied, reasonPayerRequired},
	{saga.ErrWalletBlocked, connect.CodePermissionDenied, reasonWalletBlocked},
	{saga.ErrWalletBusy, connect.CodeResourceExhausted, reasonWalletBusy},
	{wallets.ErrFacilitator, connect.CodeFailedPrecondition, reasonFacilitatorAddress},
	{wallet.ErrBadAddress, connect.CodeInvalidArgument, reasonPayerInvalid},
	{saga.ErrBadToken, connect.CodePermissionDenied, ""},
	{saga.ErrUnderpaid, connect.CodePermissionDenied, ""},
	{saga.ErrHandleMismatch, connect.CodePermissionDenied, ""},
	{saga.ErrUnknownPackage, connect.CodeInvalidArgument, ""},
	{saga.ErrQuoteInvalid, connect.CodeInvalidArgument, reasonQuoteInvalid},
	{saga.ErrNotExtension, connect.CodeInvalidArgument, ""},
	{jobs.ErrInvalid, connect.CodeInvalidArgument, ""},
	{streams.ErrBadCursor, connect.CodeInvalidArgument, ""},
	{edgeevents.ErrBadCursor, connect.CodeInvalidArgument, ""},
	{saga.ErrNotReady, connect.CodeFailedPrecondition, reasonNotReady},
	{saga.ErrAlreadyEnded, connect.CodeFailedPrecondition, reasonNotReady},
	{errTargetUnknown, connect.CodeNotFound, reasonTargetUnknown},
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
	{errNoSecrets, connect.CodeFailedPrecondition, reasonFeatureUnavailable},
	{errNoSecretFeature, connect.CodeFailedPrecondition, reasonFeatureUnavailable},
	{errX402Disabled, connect.CodeFailedPrecondition, reasonX402Disabled},
	{errNoX402Feature, connect.CodeFailedPrecondition, reasonFeatureUnavailable},
	{errTooManyServices, connect.CodeResourceExhausted, reasonTooManyServices},
	{errTooManyHosts, connect.CodeResourceExhausted, reasonTooManySecrets},
	{errTunnelUnknown, connect.CodeNotFound, reasonTokenUnknown},
	{errLinkUnknown, connect.CodeNotFound, reasonLinkUnknown},
	{errLinkLocked, connect.CodeFailedPrecondition, reasonLinkLocked},
	{errLinkUsed, connect.CodeFailedPrecondition, reasonTokenUsed},
	{errLinkExpired, connect.CodeFailedPrecondition, reasonTokenExpired},
	{errPutGrant, connect.CodePermissionDenied, reasonPutGrantInvalid},
	{saga.ErrGateUnavailable, connect.CodeUnavailable, reasonGateUnavailable},
	{saga.ErrSSHPreparing, connect.CodeUnavailable, reasonSSHPreparing},
	{sshkeys.ErrInvalid, connect.CodeInvalidArgument, reasonKeyInvalid},
	{errSSHPath, connect.CodeInvalidArgument, reasonPathRefused},
	{errFingerprint, connect.CodeInvalidArgument, ""},
	{errLeaseMalformed, connect.CodeInvalidArgument, ""},
	{store.ErrTooManyKeys, connect.CodeResourceExhausted, reasonTooManyKeys},
	{errNoSSH, connect.CodeFailedPrecondition, reasonFeatureUnavailable},
	{errNoSSHGateway, connect.CodeFailedPrecondition, reasonFeatureUnavailable},
	{errGrantUnknown, connect.CodeNotFound, reasonGrantUnknown},
	{errGrantInvalid, connect.CodePermissionDenied, reasonGrantInvalid},
	{errTooManyConns, connect.CodeResourceExhausted, reasonTooManyConnections},
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
	{errSessionUnknown, connect.CodeNotFound, reasonSessionUnknown},
	{store.ErrUnknownHandle, connect.CodeNotFound, reasonHandleUnknown},
	{errOrderUnknown, connect.CodeNotFound, reasonOrderUnknown},
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
	return withMeta(ce, reason, nil)
}

// withMeta attaches a google.rpc.ErrorInfo carrying reason and metadata to ce
func withMeta(ce *connect.Error, reason string, metadata map[string]string) *connect.Error {
	d, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: reason, Domain: errorDomain, Metadata: metadata,
	})
	if err == nil {
		ce.AddDetail(d)
	}
	return ce
}

// rateLimited refuses a sandbox operation, saying how long until the placement may make another
func rateLimited(wait time.Duration) error {
	return withRetry(withInfo(connect.NewError(connect.CodeResourceExhausted,
		errors.New("too many sandbox operations, slow down")), reasonRateLimited), wait)
}

// withRetry attaches a google.rpc.RetryInfo holding wait to ce
func withRetry(ce *connect.Error, wait time.Duration) *connect.Error {
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

// accountNoteTimeout bounds the insert of an account check, which never delays an answer longer
const accountNoteTimeout = 2 * time.Second

// noteAccount asks for an account check when a provider's answer says its account may be at fault
func (s *Server) noteAccount(e *provider.Error) {
	if s.d.River == nil || provider.AccountBlock(e.Code) == provider.BlockNone {
		return
	}
	if e.Provider == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), accountNoteTimeout)
	defer cancel()
	health.NoteAccount(ctx, s.d.River, e.Provider, e.Code)
}

func (s *Server) toConnect(err error, fileOp bool) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if errors.As(err, &ce) && !fromBackend(err) {
		return err
	}
	var refused *secrets.Refusal
	if errors.As(err, &refused) {
		return withInfo(connect.NewError(refused.Code, refused), refused.Reason)
	}
	if we, ok := errors.AsType[*saga.WaitError](err); ok {
		return withRetry(withInfo(connect.NewError(connect.CodeResourceExhausted,
			saga.ErrWalletRateLimited), reasonWalletRateLimited), we.Wait)
	}
	for _, c := range codes {
		if !errors.Is(err, c.err) {
			continue
		}
		msg := err
		if c.code == connect.CodeCanceled || c.code == connect.CodeDeadlineExceeded ||
			c.err == saga.ErrGateUnavailable || c.err == saga.ErrSSHPreparing {
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
		s.noteAccount(e)
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
