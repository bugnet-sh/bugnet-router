package lium

import (
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"testing"
)

const testKey = "3f2a9c0d1e4b5a6978c0d1e2f3a4b5c6"

// placementLabels are the labels the saga puts on a placement's sandbox
func placementLabels() map[string]string {
	return map[string]string{
		"bugnet": "1", "bugnet_deployment": "main",
		"placement": "0b8d1c2e-3f4a-4b5c-8d6e-7f8091a2b3c4",
	}
}

func TestLiumNamesRoundTrip(t *testing.T) {
	cases := map[string]map[string]string{
		"placement": placementLabels(),
		"bench probe": {
			"bugnet": "1", "bugnet_deployment": "main", "bugnet_probe": "bench",
			"probe": "6a0c2d1e-9f8b-4c7d-a6e5-4f3b2a1c0d9e",
		},
		"image check": {"bugnet_image_check": "1"},
		"contract":    {"bugnet-contract": "a1b2c3d4e5f60718"},
		"live suite":  {"bugnet": "1", "bugnet_deployment": "dev", "bugnet-live": "ab12"},
		"unknown key": {
			"bugnet": "1", "Team.Name": "Alpha Beta", "x": "", "_v": "_lead", "": "MiXed.case",
		},
		"bugnet other than 1": {"bugnet": "yes"},
		"no labels":           {},
	}
	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			pod := podName(testKey, labels)
			key, got, ok := parsePodName(pod)
			if !ok {
				t.Fatalf("%q does not parse", pod)
			}
			eq(t, key, testKey)
			if !maps.Equal(got, labels) {
				t.Fatalf("%q gave labels %v, want %v", pod, got, labels)
			}
			if podName(testKey, maps.Clone(labels)) != pod {
				t.Fatalf("one label map gave two names")
			}
		})
	}

	pod := podName(testKey, placementLabels())
	eq(t, pod, "bn."+testKey+".d.main.p.0b8d1c2e-3f4a-4b5c-8d6e-7f8091a2b3c4")
	eq(t, len(pod), 81)
	eq(t, podName(testKey, map[string]string{"bugnet_image_check": "1"}), "bx."+testKey+".i.1")
	eq(t, podName(testKey, map[string]string{"bugnet": "yes"}),
		"bx."+testKey+".x"+hex.EncodeToString([]byte("bugnet"))+".yes")
	eq(t, podName(testKey, map[string]string{"a.b": "C D"}),
		"bx."+testKey+".x"+hex.EncodeToString([]byte("a.b"))+"._"+
			hex.EncodeToString([]byte("C D")))
	eq(t, podName(testKey, map[string]string{"bugnet-contract": ""}), "bx."+testKey+".c._")
}

func TestLiumNamesRefuseOthers(t *testing.T) {
	k := testKey
	for _, name := range []string{
		"", "bn", "my-pod", "bn." + k[:31], "bn." + k + "0", "bn.3F2A9C0D1E4B5A6978C0D1E2F3A4B5C6",
		"bq." + k, "bn." + k + ".z.v", "bn." + k + ".d", "bn." + k + ".d.Main",
		"bn." + k + ".xzz.v", "bn." + k + ".x6.v", "bn." + k + ".d._zz", "bn." + k + ".d._6",
		"bn." + k + ".d.a.d.b", "bn." + k + ".p.a.d.b", "bn." + k + ".d._6d61696e",
		"bx." + k + ".x" + hex.EncodeToString([]byte("bugnet")) + ".1",
		"bx." + k + ".x" + hex.EncodeToString([]byte("placement")) + ".a",
		"bn." + k + ".d.",
	} {
		if _, _, ok := parsePodName(name); ok {
			t.Errorf("%q parsed as a Bugnet pod name", name)
		}
	}
}

func TestLiumPodKey(t *testing.T) {
	eq(t, podKey(testKey), testKey)
	sum := sha256.Sum256([]byte("probe-6a0c2d1e-9f8b-4c7d-a6e5-4f3b2a1c0d9e"))
	eq(t, podKey("probe-6a0c2d1e-9f8b-4c7d-a6e5-4f3b2a1c0d9e"), hex.EncodeToString(sum[:])[:32])
	upper := "3F2A9C0D1E4B5A6978C0D1E2F3A4B5C6"
	sum = sha256.Sum256([]byte(upper))
	eq(t, podKey(upper), hex.EncodeToString(sum[:])[:32])
	a, b := freshKey(), freshKey()
	if a == b || !keyHex.MatchString(a) || !keyHex.MatchString(b) {
		t.Fatalf("fresh keys %q and %q", a, b)
	}
}

func TestLiumCustomerRef(t *testing.T) {
	eq(t, customerRef(placementLabels()), "0b8d1c2e-3f4a-4b5c-8d6e-7f8091a2b3c4")
	eq(t, customerRef(map[string]string{"bugnet": "1", "bugnet_deployment": "main"}), "bn.main")
	eq(t, customerRef(map[string]string{"bugnet_image_check": "1"}), "bx.check")
	eq(t, customerRef(map[string]string{"bugnet-contract": "a1b2"}), "bx.check")
	// A value outside Lium's pattern falls to the next rule, never a malformed header
	eq(t, customerRef(map[string]string{"placement": "a b", "bugnet_deployment": "main"}),
		"bn.main")
	long := make([]byte, 70)
	for i := range long {
		long[i] = 'a'
	}
	eq(t, customerRef(map[string]string{"bugnet_deployment": string(long)}), "bx.check")
}
