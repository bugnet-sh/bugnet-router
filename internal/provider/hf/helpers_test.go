package hf

import (
	"bytes"
	"log/slog"
	"reflect"
	"testing"
	"time"
)

// fakeToken stands in for HF_JOBS_TOKEN, no real token is ever used
const fakeToken = "hf_FakeJobsTokenForTestsOnly0000000000"

// testNamespace stands in for hf.namespace
const testNamespace = "bugnet-test"

const (
	// testImage stands in for images.hf.ref, design 4.10's digest
	testImage = "ghcr.io/bugnet-sh/bugnet-base@sha256:" +
		"2a471ceb0c0ca15563b40dd4421f27f81815765e5d5c4e44619da96ad176fb19"
	// testSBXSHA256 is design 4.10's sbx-server 0.7.0 digest
	testSBXSHA256 = "eb2b04f79fbe765195300aaf1582be2a2a9ea698ff067d0cab428171d5584932"
)

// testSeed stands in for HF_SANDBOX_SEED, the bytes 0 to 31
var testSeed = func() []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}()

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
		Token:           fakeToken,
		Namespace:       testNamespace,
		MaxLifetime:     2 * time.Hour,
		StartBilled:     3 * time.Minute,
		OpsPerSecond:    1000,
		SandboxSeed:     bytes.Clone(testSeed),
		Image:           testImage,
		ExtendRoom:      60 * time.Minute,
		TimeoutMargin:   10 * time.Minute,
		PollEvery:       time.Millisecond,
		SBXServerSHA256: testSBXSHA256,
		Log:             slog.New(slog.DiscardHandler),
	}
}
