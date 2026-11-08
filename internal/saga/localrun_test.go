//go:build localrun

package saga_test

import (
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

func TestLocalrunSeams(t *testing.T) {
	ca := testCA(t)
	b := saga.BootSettings{CAPEM: ca, CAVersion: 2}
	spec := saga.SetupSpecFor(b, "local", provider.Ref{ID: "x"}, "key", false)
	eq(t, spec.CAPEM, string(ca))
	eq(t, spec.CAVersion, 2)
}
