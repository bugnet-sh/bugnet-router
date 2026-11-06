package testenv

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestRetryOnTimeoutTriesOnceMore(t *testing.T) {
	timeout := fmt.Errorf("drop database: %w", context.DeadlineExceeded)
	refused := errors.New("permission denied")
	for name, c := range map[string]struct {
		errs  []error
		want  error
		calls int
	}{
		"first try works":       {errs: []error{nil}, calls: 1},
		"timeout then success":  {errs: []error{timeout, nil}, calls: 2},
		"two timeouts":          {errs: []error{timeout, timeout}, want: timeout, calls: 2},
		"other error":           {errs: []error{refused}, want: refused, calls: 1},
		"timeout then an error": {errs: []error{timeout, refused}, want: refused, calls: 2},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			err := retryOnTimeout(0, func() error {
				calls++
				return c.errs[calls-1]
			})
			if !errors.Is(err, c.want) {
				t.Fatalf("err %v, want %v", err, c.want)
			}
			if calls != c.calls {
				t.Fatalf("%d tries, want %d", calls, c.calls)
			}
		})
	}
}
