package telemetry_test

import (
	"crypto/rand"
	"crypto/sha256"
	"math/big"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/telemetrytest"
)

const (
	handleA = "sbx_incf5d4j2g46mypgm6wajtyic5"
	handleB = "sbx_7777777777777777777777777z"
)

func sha(h string) []byte {
	s := sha256.Sum256([]byte(h))
	return s[:]
}

func sampled(t *testing.T, h string, ratio float64) bool {
	t.Helper()
	tel := telemetrytest.New(t, "router-api", ratio, nil)
	return tel.Order(sha(h)).IsSampled()
}

func TestOrderIDsVectors(t *testing.T) {
	tr, sp := telemetry.OrderIDs(sha(handleA))
	eq(t, tr.String(), "55a4dee648a7a4d0a70cd0a8b24c672f")
	eq(t, sp.String(), "69923b8439c47854")
	tr, sp = telemetry.OrderIDs(sha(handleB))
	eq(t, tr.String(), "2730084125185d2f596d4359ab9e35eb")
	eq(t, sp.String(), "7a5546d2e76201a9")
	eq(t, sampled(t, handleA, 1), true)
	eq(t, sampled(t, handleB, 1), true)
	eq(t, sampled(t, handleA, 0.5), false)
	eq(t, sampled(t, handleB, 0.5), true)
	eq(t, sampled(t, handleA, 0.25), false)
	eq(t, sampled(t, handleB, 0.25), false)
	eq(t, sampled(t, handleA, 0), false)
	// The log form of the handle's hash, which the gateway's logs use too
	eq(t, handleHash(handleA), "9a5bf94fe52b")
}

func TestOrderTraceparentVector(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, nil)
	sc := tel.Order(sha(handleA))
	eq(t, sc.IsRemote(), true)
	flags := sc.TraceFlags().String()
	eq(t, "00-"+sc.TraceID().String()+"-"+sc.SpanID().String()+"-"+flags,
		"00-55a4dee648a7a4d0a70cd0a8b24c672f-69923b8439c47854-01")
}

func TestOrderSampledMatchesSDK(t *testing.T) {
	for _, ratio := range []float64{0.1, 0.5} {
		tel := telemetrytest.New(t, "router-api", ratio, nil)
		sdk := sdktrace.TraceIDRatioBased(ratio)
		for range 10000 {
			h, sum := handle.New()
			_ = h
			sc := tel.Order(sum)
			res := sdk.ShouldSample(sdktrace.SamplingParameters{TraceID: sc.TraceID()})
			eq(t, sc.IsSampled(), res.Decision == sdktrace.RecordAndSample)
		}
	}
}

func TestOffOrderHasNoContext(t *testing.T) {
	eq(t, telemetry.Off().Order(sha(handleA)).IsValid(), false)
}

// randomTraceID returns a random valid trace id
func randomTraceID(t *testing.T) trace.TraceID {
	t.Helper()
	var id trace.TraceID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	id[0] |= 1
	return id
}

func randomInt(t *testing.T, n int64) int64 {
	t.Helper()
	v, err := rand.Int(rand.Reader, big.NewInt(n))
	if err != nil {
		t.Fatal(err)
	}
	return v.Int64()
}

// handleHash is the first 12 hex characters of a handle's SHA-256
func handleHash(h string) string {
	const digits = "0123456789abcdef"
	s := sha(h)
	out := make([]byte, 0, 12)
	for _, c := range s[:6] {
		out = append(out, digits[c>>4], digits[c&15])
	}
	return string(out)
}

func TestPlacementWithoutHandleHasNoOrder(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, nil)
	eq(t, tel.Order(nil).IsValid(), false)
	_, span := tel.InOrder(t.Context(), store.Placement{}, "command.takeover")
	eq(t, span.SpanContext().IsValid(), false)
	_, root := tel.StartOrderRoot(t.Context(), store.Placement{})
	eq(t, root.SpanContext().IsValid(), false)
}
