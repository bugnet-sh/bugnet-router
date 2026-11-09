package health

import (
	"context"
	"errors"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Outcome names the sample outcome of err and its provider code, empty without one. Only ok and
// failed count in a provider's health, as none of the others says the provider is broken.
func Outcome(err error) (outcome, code string) {
	if err == nil {
		return store.OutcomeOK, ""
	}
	e, ok := provider.AsError(err)
	if !ok {
		return store.OutcomeFailed, ""
	}
	switch e.Code {
	case provider.CodeCapacity:
		return store.OutcomeCapacity, string(e.Code)
	case provider.CodeRateLimited:
		return store.OutcomeRateLimited, string(e.Code)
	case provider.CodeQuota:
		return store.OutcomeQuota, string(e.Code)
	}
	return store.OutcomeFailed, string(e.Code)
}

// Unrecorded reports whether err says nothing about the provider, a cancelled call
func Unrecorded(err error) bool { return errors.Is(err, context.Canceled) }
