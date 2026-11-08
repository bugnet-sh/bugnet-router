package secrets

import (
	"testing"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func TestMatchesOnlyTheSamePut(t *testing.T) {
	spec := &routerv1.SecretSpec{Name: "HF_TOKEN", Hosts: []string{"huggingface.co"}}
	value := &routerv1.SealedValue{KeyId: "k1", Box: []byte("box")}
	row := store.Secret{
		Name: "HF_TOKEN", Hosts: []string{"huggingface.co"}, HeaderNames: []string{},
		Sealed: []byte("box"), KeyID: "k1",
	}
	eq(t, Matches(row, spec, value), true)
	for name, change := range map[string]func(*store.Secret){
		"box":       func(r *store.Secret) { r.Sealed = []byte("other") },
		"key":       func(r *store.Secret) { r.KeyID = "k2" },
		"hosts":     func(r *store.Secret) { r.Hosts = []string{"api.openai.com"} },
		"headers":   func(r *store.Secret) { r.HeaderNames = []string{"x-api-key"} },
		"fetchOnly": func(r *store.Secret) { r.FetchOnly = true },
		"name":      func(r *store.Secret) { r.Name = "GH_TOKEN" },
	} {
		r := row
		change(&r)
		if Matches(r, spec, value) {
			t.Errorf("a row with another %s matched", name)
		}
	}
}

func TestInfoNeverHoldsTheValue(t *testing.T) {
	at := time.Date(2026, 11, 5, 13, 58, 2, 0, time.UTC)
	row := store.Secret{
		Name: "HF_TOKEN", Hosts: []string{"huggingface.co"}, HeaderNames: []string{},
		Sealed: []byte("box"), KeyID: "k1", State: store.SecretFailed,
		FailureReason: "PORT_443_TAKEN", UpdatedAt: at, PlaceholderSHA256: []byte("sha"),
	}
	got := Info(row)
	want := &routerv1.SecretInfo{
		Spec: &routerv1.SecretSpec{
			Name: "HF_TOKEN", Hosts: []string{"huggingface.co"}, HeaderNames: []string{},
		},
		State: routerv1.SecretState_SECRET_STATE_FAILED, UpdatedAt: timestamppb.New(at),
		FailureReason: "PORT_443_TAKEN",
	}
	if !proto.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	row.State = store.SecretActive
	eq(t, Info(row).GetFailureReason(), "")
	eq(t, Info(row).GetState(), routerv1.SecretState_SECRET_STATE_ACTIVE)
	row.State = store.SecretApplying
	eq(t, Info(row).GetState(), routerv1.SecretState_SECRET_STATE_APPLYING)
}
