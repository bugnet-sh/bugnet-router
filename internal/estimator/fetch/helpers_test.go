package fetch_test

import (
	"errors"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/fetch"
)

func asLimited(err error, target **fetch.LimitedError) bool { return errors.As(err, target) }
