package reconcile

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

// a lifetime the router cannot move never drifts, so reconcile never calls SetLifetime on it
func TestDriftedSkipsLifetimesThatCannotMove(t *testing.T) {
	deadline := now.Add(time.Hour)
	far := deadline.Add(saga.LifetimeSlack + time.Hour)
	for prov, want := range map[string]bool{
		"e2b": true, "boat": true, "lium": true, "koyeb": false, "hf": false,
	} {
		if got := Drifted(prov, far, &deadline, 2*time.Minute); got != want {
			t.Errorf("%s drifted %v, want %v", prov, got, want)
		}
	}
	eq(t, Drifted("lium", deadline.Add(saga.LifetimeSlack), &deadline, 2*time.Minute), false)
}
