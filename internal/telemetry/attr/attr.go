// Package attr holds the only constructors of exported span attributes, one per allowlist key
package attr

import (
	"regexp"
	"slices"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

// KeyValue is one span attribute
type KeyValue = attribute.KeyValue

// String attribute keys of DP 20.6.1
const (
	keyRPCSystem    = "rpc.system.name"
	keyRPCMethod    = "rpc.method"
	keyRPCStatus    = "rpc.response.status_code"
	keyErrorType    = "error.type"
	keyPlacementID  = "bugnet.placement_id"
	keyEstimateID   = "bugnet.estimate_id"
	keyOrderHash    = "bugnet.order_hash"
	keyExtendHash   = "bugnet.extend_order_hash"
	keyHandleHash   = "bugnet.handle_hash"
	keySandboxHash  = "bugnet.sandbox_hash"
	keyTransferHash = "bugnet.transfer_hash"
	keyPreviewID    = "bugnet.preview_id"
	keyJobID        = "bugnet.job_id"
	keyPackageID    = "bugnet.package_id"
	keySize         = "bugnet.size"
	keyProvider     = "bugnet.provider"
	keyProviderCode = "bugnet.provider_code"
	keyState        = "bugnet.state"
	keyReason       = "bugnet.reason"
	keyEndCause     = "bugnet.end_cause"
	keyEventType    = "bugnet.event_type"
	keyDirection    = "bugnet.direction"
	keyTransport    = "bugnet.transport"
	keyLink         = "bugnet.link"
	keyEgressHost   = "bugnet.egress_host"
	keyJobKind      = "bugnet.job.kind"
	keyJobQueue     = "bugnet.job.queue"
	keyJobOutcome   = "bugnet.job.outcome"
	keyCanFailOver  = "bugnet.can_fail_over"
	keyTruncated    = "bugnet.truncated"
	keyRepaired     = "bugnet.repaired"
	keyHTTPStatus   = "http.response.status_code"
	keyAttempt      = "bugnet.attempt"
	keyExitCode     = "bugnet.exit_code"
	keyPort         = "bugnet.port"
	keyCount        = "bugnet.count"
	keyJobAttempt   = "bugnet.job.attempt"
	keyJobWaitMS    = "bugnet.job.wait_ms"
	keySizeBytes    = "bugnet.size_bytes"
	keyBytesIn      = "bugnet.bytes_in"
	keyBytesOut     = "bugnet.bytes_out"
	keyRequests     = "bugnet.requests"
	keyResponses4xx = "bugnet.responses_4xx"
	keyResponses5xx = "bugnet.responses_5xx"
	keyGateErrors   = "bugnet.gate_errors"
	keyWebsockets   = "bugnet.websockets"
	keySessions     = "bugnet.sessions"
	keyForwards     = "bugnet.forwards"
	keySwaps        = "bugnet.swaps"
	keyRefused      = "bugnet.refused"
	keyServiceName  = "service.name"
	keyServiceVer   = "service.version"
	keyServiceInst  = "service.instance.id"
	keyEnvironment  = "deployment.environment.name"
)

var (
	reErrorType = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)
	reUUID      = regexp.MustCompile(
		`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	reHex12      = regexp.MustCompile(`^[0-9a-f]{12}$`)
	reHex8       = regexp.MustCompile(`^[0-9a-f]{8}$`)
	rePreview    = regexp.MustCompile(`^[a-z2-7]{16}$`)
	reJobID      = regexp.MustCompile(`^j_[a-z2-7]{26}$`)
	rePackage    = regexp.MustCompile(`^[a-z0-9-]{1,64}$`)
	reProvider   = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)
	reLower      = regexp.MustCompile(`^[a-z_]{1,64}$`)
	reEvent      = regexp.MustCompile(`^[a-z_.]{1,64}$`)
	reHost       = regexp.MustCompile(`^[a-z0-9.-]{1,253}$`)
	reRPCStatus  = regexp.MustCompile(`^(OK|[A-Z][A-Z_]{0,31})$`)
	reRPCMethod  = regexp.MustCompile(`^bugnet\.router\.v1\.[A-Za-z0-9]{1,64}/[A-Za-z0-9]{1,64}$`)
	reSpanName   = regexp.MustCompile(`^[a-z][a-z0-9_.]{0,63}$`)
	reService    = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	reVersion    = regexp.MustCompile(`^([0-9a-f]{12}(\+dirty)?|unknown)$`)
	reInstance   = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	reEnvName    = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)
	secretShapes = []string{"sbx_", "bnp_", "bnx_", "bnsl_", "bnsp_", "bnt_", "bnf_", "bnsec_",
		"g1.", "bxd_", "e2b_", "boat_", "Bearer ", "Basic ", "-----BEGIN"}
)

// rule says whether a value is allowed for one key
type rule func(attribute.Value) bool

func str(re *regexp.Regexp) rule {
	return func(v attribute.Value) bool {
		return v.Type() == attribute.STRING && re.MatchString(v.AsString())
	}
}

func oneOf(words ...string) rule {
	return func(v attribute.Value) bool {
		if v.Type() != attribute.STRING {
			return false
		}
		for _, w := range words {
			if v.AsString() == w {
				return true
			}
		}
		return false
	}
}

func boolean(v attribute.Value) bool { return v.Type() == attribute.BOOL }

func count(v attribute.Value) bool { return v.Type() == attribute.INT64 && v.AsInt64() >= 0 }

func anyInt(v attribute.Value) bool { return v.Type() == attribute.INT64 }

func httpStatus(v attribute.Value) bool {
	return v.Type() == attribute.INT64 && v.AsInt64() >= 100 && v.AsInt64() <= 599
}

var rules = map[attribute.Key]rule{
	keyRPCSystem:    oneOf("connectrpc"),
	keyRPCMethod:    str(reRPCMethod),
	keyRPCStatus:    str(reRPCStatus),
	keyErrorType:    str(reErrorType),
	keyPlacementID:  str(reUUID),
	keyEstimateID:   str(reUUID),
	keyOrderHash:    str(reHex12),
	keyExtendHash:   str(reHex12),
	keyHandleHash:   str(reHex12),
	keySandboxHash:  str(reHex12),
	keyTransferHash: str(reHex8),
	keyPreviewID:    str(rePreview),
	keyJobID:        str(reJobID),
	keyPackageID:    str(rePackage),
	keySize:         str(rePackage),
	keyProvider:     str(reProvider),
	keyProviderCode: str(reLower),
	keyState:        str(reLower),
	keyReason:       str(reLower),
	keyEndCause:     str(reLower),
	keyEventType:    str(reEvent),
	keyDirection:    oneOf("upload", "download"),
	keyTransport:    oneOf("websocket", "tcp"),
	keyLink:         oneOf("order", "request", "job"),
	keyEgressHost:   str(reHost),
	keyJobKind:      str(reLower),
	keyJobQueue:     oneOf("default", "dataplane", "probes", "pools"),
	keyJobOutcome:   oneOf("ok", "error", "snoozed", "cancelled"),
	keyCanFailOver:  boolean,
	keyTruncated:    boolean,
	keyRepaired:     boolean,
	keyHTTPStatus:   httpStatus,
	keyAttempt:      count,
	keyExitCode:     anyInt,
	keyPort:         count,
	keyCount:        count,
	keyJobAttempt:   count,
	keyJobWaitMS:    count,
	keySizeBytes:    count,
	keyBytesIn:      count,
	keyBytesOut:     count,
	keyRequests:     count,
	keyResponses4xx: count,
	keyResponses5xx: count,
	keyGateErrors:   count,
	keyWebsockets:   count,
	keySessions:     count,
	keyForwards:     count,
	keySwaps:        count,
	keyRefused:      count,
}

// Allowed reports whether kv is on the allowlist with a value that matches its rule and holds
// no known secret shape
func Allowed(kv KeyValue) bool {
	r, ok := rules[kv.Key]
	if !ok || !r(kv.Value) {
		return false
	}
	return kv.Value.Type() != attribute.STRING || !HasSecretShape(kv.Value.AsString())
}

// HasSecretShape reports whether s holds a prefix of a token the router or a provider issues
func HasSecretShape(s string) bool {
	for _, p := range secretShapes {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

// Keys lists every allowlisted attribute key, sorted, for tests that pin the list
func Keys() []string {
	out := make([]string, 0, len(rules))
	for k := range rules {
		out = append(out, string(k))
	}
	slices.Sort(out)
	return out
}

// Filter returns the attributes of in that pass Allowed, and the keys of those that did not
func Filter(in []KeyValue) (kept []KeyValue, dropped []string) {
	for _, kv := range in {
		if Allowed(kv) {
			kept = append(kept, kv)
		} else {
			dropped = append(dropped, string(kv.Key))
		}
	}
	return kept, dropped
}

// ErrorTypeOK reports whether s may describe a status, the rule of error.type
func ErrorTypeOK(s string) bool { return reErrorType.MatchString(s) }

// SpanNameOK reports whether name is a span name of DP 20.6.1
func SpanNameOK(name string) bool {
	return reSpanName.MatchString(name) || reRPCMethod.MatchString(name)
}

// RPC attributes

// RPCSystem is connectrpc
func RPCSystem() KeyValue { return attribute.String(keyRPCSystem, "connectrpc") }

// RPCMethod is the procedure as bugnet.router.v1.Service/Method
func RPCMethod(v string) KeyValue { return attribute.String(keyRPCMethod, v) }

// RPCStatus is OK or an upper-case Connect code
func RPCStatus(v string) KeyValue { return attribute.String(keyRPCStatus, v) }

// ErrorType is the ErrorInfo reason of a failure, else the code
func ErrorType(v string) KeyValue { return attribute.String(keyErrorType, v) }

// Identity attributes

// PlacementID is the placement's canonical UUID
func PlacementID(v string) KeyValue { return attribute.String(keyPlacementID, v) }

// EstimateID is an estimate's canonical UUID
func EstimateID(v string) KeyValue { return attribute.String(keyEstimateID, v) }

// OrderHash is the keyed hash of a Payments order id
func OrderHash(v string) KeyValue { return attribute.String(keyOrderHash, v) }

// ExtendOrderHash is the keyed hash of an extension order id
func ExtendOrderHash(v string) KeyValue { return attribute.String(keyExtendHash, v) }

// HandleHash is the first 12 hex characters of the handle's SHA-256
func HandleHash(v string) KeyValue { return attribute.String(keyHandleHash, v) }

// SandboxHash is the keyed hash of a provider sandbox id
func SandboxHash(v string) KeyValue { return attribute.String(keySandboxHash, v) }

// TransferHash is 8 hex characters of a transfer token's SHA-256
func TransferHash(v string) KeyValue { return attribute.String(keyTransferHash, v) }

// PreviewID is a preview's host label
func PreviewID(v string) KeyValue { return attribute.String(keyPreviewID, v) }

// JobID is a command job's id
func JobID(v string) KeyValue { return attribute.String(keyJobID, v) }

// PackageID is a catalog package id
func PackageID(v string) KeyValue { return attribute.String(keyPackageID, v) }

// Size is a provider size name
func Size(v string) KeyValue { return attribute.String(keySize, v) }

// Provider is a configured provider name
func Provider(v string) KeyValue { return attribute.String(keyProvider, v) }

// ProviderCode is a provider.Code value
func ProviderCode(v string) KeyValue { return attribute.String(keyProviderCode, v) }

// State is a placement state
func State(v string) KeyValue { return attribute.String(keyState, v) }

// Reason is a stable reason word
func Reason(v string) KeyValue { return attribute.String(keyReason, v) }

// EndCause is why a connection or tunnel ended
func EndCause(v string) KeyValue { return attribute.String(keyEndCause, v) }

// EventType is an outbox event type
func EventType(v string) KeyValue { return attribute.String(keyEventType, v) }

// Direction is upload or download
func Direction(v string) KeyValue { return attribute.String(keyDirection, v) }

// Transport is websocket or tcp
func Transport(v string) KeyValue { return attribute.String(keyTransport, v) }

// Link is order, request or job, on links only
func Link(v string) KeyValue { return attribute.String(keyLink, v) }

// EgressHost is a host from the secret presets, else custom
func EgressHost(v string) KeyValue { return attribute.String(keyEgressHost, v) }

// JobKind is a River job kind
func JobKind(v string) KeyValue { return attribute.String(keyJobKind, v) }

// JobQueue is a River queue name
func JobQueue(v string) KeyValue { return attribute.String(keyJobQueue, v) }

// JobOutcome is ok, error, snoozed or cancelled
func JobOutcome(v string) KeyValue { return attribute.String(keyJobOutcome, v) }

// Booleans

// CanFailOver says a provider error lets the saga try another provider
func CanFailOver(v bool) KeyValue { return attribute.Bool(keyCanFailOver, v) }

// Truncated says a command's output was cut
func Truncated(v bool) KeyValue { return attribute.Bool(keyTruncated, v) }

// Repaired says a route was repaired
func Repaired(v bool) KeyValue { return attribute.Bool(keyRepaired, v) }

// Integers

// HTTPStatus is the response status of an HTTP call, 100 to 599
func HTTPStatus(v int) KeyValue { return attribute.Int(keyHTTPStatus, v) }

// Attempt is a saga attempt number
func Attempt(v int) KeyValue { return attribute.Int(keyAttempt, v) }

// ExitCode is a process exit code
func ExitCode(v int) KeyValue { return attribute.Int(keyExitCode, v) }

// Port is a sandbox port
func Port(v int) KeyValue { return attribute.Int(keyPort, v) }

// Count is a number of items
func Count(v int) KeyValue { return attribute.Int(keyCount, v) }

// JobAttempt is River's attempt number
func JobAttempt(v int) KeyValue { return attribute.Int(keyJobAttempt, v) }

// JobWaitMS is how long a job waited past its schedule, in milliseconds
func JobWaitMS(v int64) KeyValue { return attribute.Int64(keyJobWaitMS, v) }

// SizeBytes is the bytes a call moved
func SizeBytes(v int64) KeyValue { return attribute.Int64(keySizeBytes, v) }

// BytesIn is the bytes received
func BytesIn(v int64) KeyValue { return attribute.Int64(keyBytesIn, v) }

// BytesOut is the bytes sent
func BytesOut(v int64) KeyValue { return attribute.Int64(keyBytesOut, v) }

// Requests is a request count
func Requests(v int64) KeyValue { return attribute.Int64(keyRequests, v) }

// Responses4xx is a 4xx response count
func Responses4xx(v int64) KeyValue { return attribute.Int64(keyResponses4xx, v) }

// Responses5xx is a 5xx response count
func Responses5xx(v int64) KeyValue { return attribute.Int64(keyResponses5xx, v) }

// GateErrors is a count of gate failures
func GateErrors(v int64) KeyValue { return attribute.Int64(keyGateErrors, v) }

// Websockets is a WebSocket count
func Websockets(v int64) KeyValue { return attribute.Int64(keyWebsockets, v) }

// Sessions is an SSH session count
func Sessions(v int64) KeyValue { return attribute.Int64(keySessions, v) }

// Forwards is a port forward count
func Forwards(v int64) KeyValue { return attribute.Int64(keyForwards, v) }

// Swaps is a count of route swaps
func Swaps(v int64) KeyValue { return attribute.Int64(keySwaps, v) }

// Refused is a count of refused requests
func Refused(v int64) KeyValue { return attribute.Int64(keyRefused, v) }

// Resource returns the resource attributes of one process, which the exporter sets itself
func Resource(service, version, instance, environment string) []KeyValue {
	out := []KeyValue{
		attribute.String(keyServiceName, service),
		attribute.String(keyServiceVer, version),
		attribute.String(keyServiceInst, instance),
		attribute.String(keyEnvironment, environment),
	}
	return out
}

// ResourceAllowed reports whether service, version, instance and environment fit DP 20.6.1
func ResourceAllowed(service, version, instance, environment string) bool {
	return reService.MatchString(service) && reVersion.MatchString(version) &&
		reInstance.MatchString(instance) && reEnvName.MatchString(environment)
}
