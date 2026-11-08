package estimator

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidArgument means a request field is out of range
	ErrInvalidArgument = errors.New("estimator: invalid argument")
	// ErrSourceInvalid is SOURCE_INVALID, a source that parses as nothing
	ErrSourceInvalid = errors.New("estimator: source invalid")
	// ErrSecretInArgument is SECRET_IN_ARGUMENT, a source or command holding a secret shape
	ErrSecretInArgument = errors.New("estimator: secret in argument")
	// ErrEstimateUnknown means no estimate has the id asked for
	ErrEstimateUnknown = errors.New("estimator: estimate unknown")
)

// RateLimitedError means one of the estimate buckets is empty
type RateLimitedError struct{ Wait time.Duration }

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("estimator: rate limited, try again in %s", e.Wait.Round(time.Second))
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}
