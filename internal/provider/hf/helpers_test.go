package hf

import (
	"log/slog"
	"reflect"
	"testing"
	"time"
)

// fakeToken stands in for HF_JOBS_TOKEN, no real token is ever used
const fakeToken = "hf_FakeJobsTokenForTestsOnly0000000000"

// testNamespace stands in for hf.namespace
const testNamespace = "bugnet-test"

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// testOptions are design 4.12's settings with a pace no test waits on
func testOptions() Options {
	return Options{
		Token:        fakeToken,
		Namespace:    testNamespace,
		MaxLifetime:  2 * time.Hour,
		StartBilled:  3 * time.Minute,
		OpsPerSecond: 1000,
		Log:          slog.New(slog.DiscardHandler),
	}
}
