package secrets

import (
	"bytes"
	"slices"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Matches says whether a stored secret holds exactly this put, so a retry is answered again
func Matches(row store.Secret, spec *routerv1.SecretSpec, value *routerv1.SealedValue) bool {
	return row.Name == spec.GetName() && slices.Equal(row.Hosts, spec.GetHosts()) &&
		slices.Equal(row.HeaderNames, spec.GetHeaderNames()) &&
		row.FetchOnly == spec.GetFetchOnly() && row.KeyID == value.GetKeyId() &&
		bytes.Equal(row.Sealed, value.GetBox())
}

var states = map[string]routerv1.SecretState{
	store.SecretApplying: routerv1.SecretState_SECRET_STATE_APPLYING,
	store.SecretActive:   routerv1.SecretState_SECRET_STATE_ACTIVE,
	store.SecretFailed:   routerv1.SecretState_SECRET_STATE_FAILED,
}

// Info shows a stored secret by its spec and state, never its value or placeholder
func Info(row store.Secret) *routerv1.SecretInfo {
	out := &routerv1.SecretInfo{
		Spec: &routerv1.SecretSpec{
			Name: row.Name, Hosts: slices.Clone(row.Hosts),
			HeaderNames: slices.Clone(row.HeaderNames), FetchOnly: row.FetchOnly,
		},
		State: states[row.State], UpdatedAt: timestamppb.New(row.UpdatedAt),
	}
	if row.State == store.SecretFailed {
		out.FailureReason = row.FailureReason
	}
	return out
}
