package lium

import (
	"cmp"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"regexp"
	"slices"
	"strings"
)

const (
	// prefixOwned starts the name of a pod whose labels hold bugnet=1, prefixOther every other
	prefixOwned = "bn"
	prefixOther = "bx"
	labelBugnet = "bugnet"
	// labelDeployment names the deployment that made a pod
	labelDeployment = "bugnet_deployment"
	// codeHexKey starts the code of a label key that has no letter of its own
	codeHexKey = "x"
	// hexValue starts a value written in hex, which a plain value never starts with
	hexValue = "_"
)

// labelCodes are the one-letter codes of the label keys the router and its tests set
var labelCodes = map[string]string{
	"bugnet_deployment":  "d",
	"placement":          "p",
	"bugnet-contract":    "c",
	"bugnet-live":        "l",
	"bugnet_probe":       "q",
	"probe":              "r",
	"bugnet_image_check": "i",
}

var (
	codeLabels = func() map[string]string {
		out := map[string]string{}
		for k, c := range labelCodes {
			out[c] = k
		}
		return out
	}()
	keyHex     = regexp.MustCompile(`^[0-9a-f]{32}$`)
	plainValue = regexp.MustCompile(`^[a-z0-9-]+$`)
	lowerHex   = regexp.MustCompile(`^(?:[0-9a-f]{2})*$`)
	// customerRefWord is what Lium's X-Customer-Ref header takes
	customerRefWord = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)
)

// podKey is the 32 lowercase hex a pod's name, SSH key and Idempotency-Key derive from
func podKey(idempotencyKey string) string {
	if keyHex.MatchString(idempotencyKey) {
		return idempotencyKey
	}
	sum := sha256.Sum256([]byte(idempotencyKey))
	return hex.EncodeToString(sum[:])[:32]
}

// freshKey is 32 random lowercase hex for a create without an idempotency key
func freshKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// podName writes the key and labels into a pod name, one name for one label map
func podName(key string, labels map[string]string) string {
	prefix := prefixOther
	if labels[labelBugnet] == "1" {
		prefix = prefixOwned
	}
	parts := []string{prefix, key}
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		if k == labelBugnet && prefix == prefixOwned {
			continue
		}
		code, ok := labelCodes[k]
		if !ok {
			code = codeHexKey + hex.EncodeToString([]byte(k))
		}
		v := labels[k]
		if !plainValue.MatchString(v) {
			v = hexValue + hex.EncodeToString([]byte(v))
		}
		parts = append(parts, code, v)
	}
	return strings.Join(parts, ".")
}

// parsePodName reads a name podName wrote, false for any other name
func parsePodName(name string) (string, map[string]string, bool) {
	parts := strings.Split(name, ".")
	if len(parts) < 2 || len(parts)%2 != 0 || !keyHex.MatchString(parts[1]) {
		return "", nil, false
	}
	labels := map[string]string{}
	switch parts[0] {
	case prefixOwned:
		labels[labelBugnet] = "1"
	case prefixOther:
	default:
		return "", nil, false
	}
	var keys []string
	for i := 2; i < len(parts); i += 2 {
		k, ok := labelKey(parts[i])
		v, vok := labelValue(parts[i+1])
		if _, seen := labels[k]; !ok || !vok || seen {
			return "", nil, false
		}
		labels[k] = v
		keys = append(keys, k)
	}
	// Only the one name podName writes for these labels is ours
	if !slices.IsSorted(keys) || podName(parts[1], labels) != name {
		return "", nil, false
	}
	return parts[1], labels, true
}

// labelKey reads a key's code, refusing a known key written in hex
func labelKey(code string) (string, bool) {
	if k, ok := codeLabels[code]; ok {
		return k, true
	}
	raw, ok := strings.CutPrefix(code, codeHexKey)
	if !ok || !lowerHex.MatchString(raw) {
		return "", false
	}
	b, _ := hex.DecodeString(raw)
	_, known := labelCodes[string(b)]
	return string(b), !known
}

// labelValue reads a plain value or one written in hex
func labelValue(s string) (string, bool) {
	if plainValue.MatchString(s) {
		return s, true
	}
	raw, ok := strings.CutPrefix(s, hexValue)
	if !ok || !lowerHex.MatchString(raw) {
		return "", false
	}
	b, _ := hex.DecodeString(raw)
	return string(b), true
}

// customerRef tags a rent with the placement, else the deployment, else as an image check
func customerRef(labels map[string]string) string {
	return cmp.Or(fitRef(labels["placement"]), fitRef(prefixOwned, labels["bugnet_deployment"]),
		"bx.check")
}

// fitRef joins the words with dots when every one is set and the whole fits the header
func fitRef(words ...string) string {
	if slices.Contains(words, "") {
		return ""
	}
	ref := strings.Join(words, ".")
	if !customerRefWord.MatchString(ref) {
		return ""
	}
	return ref
}
