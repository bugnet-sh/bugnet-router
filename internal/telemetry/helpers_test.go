package telemetry_test

import (
	"fmt"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"reflect"
	"testing"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// at reads one exported attribute as text, empty when the span lacks it
func at(attrs map[string]any, key string) string {
	v, ok := attrs[key]
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}

func provRef(name, id string) provider.Ref { return provider.Ref{Provider: name, ID: id} }
