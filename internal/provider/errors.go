package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
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
	CodeNoSpace        Code = "no_space"
)

// Error is a classified provider failure
type Error struct {
	Code        Code
	RetryAfter  time.Duration
	CanFailOver bool
	Err         error
	// Provider names the adapter on an account code, so any caller can ask for its account check
	Provider string
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

// ErrBadSignature marks a webhook whose signature, timestamp or body failed the check
var ErrBadSignature = errors.New("provider: the webhook signature is not valid")

// DecodeStrict reads one JSON value from body, allowing unknown fields and refusing trailing data
func DecodeStrict(body []byte, into any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(into); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("provider: data follows the JSON value")
	}
	return nil
}

// AccountBlock maps a code that says the account cannot work to its Block, BlockNone otherwise
func AccountBlock(c Code) Block {
	switch c {
	case CodeAbuseLock:
		return BlockBanned
	case CodeUnfunded:
		return BlockUnfunded
	case CodeAuth:
		return BlockAuth
	}
	return BlockNone
}

// AccountReading reads an error answer that names a block as the account, any other error stays
func AccountReading(err error) (Account, error) {
	if e, ok := AsError(err); ok {
		if b := AccountBlock(e.Code); b != BlockNone {
			return Account{Block: b, Detail: e.Error()}, nil
		}
	}
	return Account{}, err
}

// Named stamps the adapter's name on an error that carries an account code, and returns it
func Named(e *Error, name string) *Error {
	if AccountBlock(e.Code) != BlockNone {
		e.Provider = name
	}
	return e
}
