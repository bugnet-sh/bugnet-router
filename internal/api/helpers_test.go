package api_test

import (
	"reflect"
	"testing"

	"connectrpc.com/connect"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// mustCode fails the test unless err has code, and the ErrorInfo reason too when reason is set
func mustCode(t *testing.T, err error, code connect.Code, reason string) {
	t.Helper()
	wantCode(t, err, code)
	if reason != "" {
		wantInfo(t, err, reason)
	}
}
