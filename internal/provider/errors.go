package provider

import (
	"errors"
	"time"
)

// Code classifies a provider failure so the router can retry, fail over or give up
type Code string

// Failure codes adapters map their backend errors to
const (
	CodeCapacity       Code = "capacity"
	CodeRateLimited    Code = "rate_limited"
	CodeQuota          Code = "quota"
	CodeUnfunded       Code = "unfunded"
	CodeAbuseLock      Code = "abuse_lock"
	CodeAuth           Code = "auth"
	CodeInvalid        Code = "invalid"
	CodeNotFound       Code = "not_found"
	CodeOutcomeUnknown Code = "outcome_unknown"
	CodeInternal       Code = "internal"
)

// Error is a classified provider failure
type Error struct {
	Code        Code
	RetryAfter  time.Duration
	CanFailOver bool
	Err         error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return "provider " + string(e.Code)
	}
	return "provider " + string(e.Code) + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// AsError finds an *Error in err's chain
func AsError(err error) (*Error, bool) {
	var e *Error
	ok := errors.As(err, &e)
	return e, ok
}
