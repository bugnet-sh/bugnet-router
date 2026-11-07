package dpkeys

import (
	"encoding/base64"
	"encoding/hex"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	p1  = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	p2  = uuid.MustParse("00000000-0000-0000-0000-000000000002")
	now = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
)

// twoRoots holds k2, the current root, and k1
func twoRoots(t *testing.T) Keys {
	t.Helper()
	return mustParse(t, "k2:"+b64(32, 2)+",k1:"+b64(32, 1))
}

func TestParse(t *testing.T) {
	k := mustParse(t, "k2:"+b64(32, 2)+",old-1:"+b64(32, 1))
	eq(t, k.Current().ID, "k2")
	eq(t, len(k.Roots()), 2)
	eq(t, k.Roots()[1].ID, "old-1")
	eq(t, k.Roots()[1].Key, []byte(strings.Repeat("\x01", 32)))
	k.Roots()[0].Key[0] = 9
	eq(t, k.Current().Key[0], byte(2))
}

func TestParseRefusesMalformedValues(t *testing.T) {
	good := b64(32, 7)
	for name, env := range map[string]string{
		"empty":            "",
		"no colon":         good,
		"empty id":         ":" + good,
		"upper case id":    "K1:" + good,
		"dotted id":        "k.1:" + good,
		"long id":          strings.Repeat("k", 33) + ":" + good,
		"not base64":       "k1:" + strings.Repeat("!", 44),
		"url base64":       "k1:" + base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		"31 bytes":         "k1:" + b64(31, 7),
		"33 bytes":         "k1:" + b64(33, 7),
		"duplicate id":     "k1:" + good + ",k1:" + b64(32, 8),
		"trailing comma":   "k1:" + good + ",",
		"space in entry":   "k1:" + good + ", k2:" + b64(32, 8),
		"second malformed": "k1:" + good + ",k2:" + b64(16, 8),
	} {
		_, err := Parse(env)
		if err == nil {
			t.Errorf("%s: parsed, want an error", name)
			continue
		}
		if strings.Contains(err.Error(), good) || strings.Contains(err.Error(), b64(32, 8)) {
			t.Errorf("%s: error quotes a key: %v", name, err)
		}
	}
}

func TestPreviewTokenShape(t *testing.T) {
	k := twoRoots(t)
	tok, err := k.PreviewToken("k1", "abcdefghijklmnop", 0)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(tok), 47)
	eq(t, strings.HasPrefix(tok, "bnp_"), true)
	eq(t, k.CheckPreviewToken("abcdefghijklmnop", 0, tok), true)
	eq(t, k.CheckPreviewToken("abcdefghijklmnop", 1, tok), false)
	eq(t, k.CheckPreviewToken("bcdefghijklmnopq", 0, tok), false)
	eq(t, k.CheckPreviewToken("abcdefghijklmnop", 0, tok[:46]), false)
	eq(t, k.CheckPreviewToken("abcdefghijklmnop", 0, ""), false)
	current, _ := k.PreviewToken("k2", "abcdefghijklmnop", 0)
	eq(t, current != tok && k.CheckPreviewToken("abcdefghijklmnop", 0, current), true)
	eq(t, mustParse(t, "k2:"+b64(32, 2)).CheckPreviewToken("abcdefghijklmnop", 0, tok), false)
}

func TestDerivedShapes(t *testing.T) {
	k := twoRoots(t)
	gate, _ := k.GateKey("k1", p1)
	eq(t, regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`).MatchString(gate), true)
	ph, _ := k.Placeholder("k1", p1, "HF_TOKEN")
	eq(t, regexp.MustCompile(`^bnsec_[a-z2-7]{26}$`).MatchString(ph), true)
	code, _ := k.PairingCode("k1", sha("bnsl_x"))
	eq(t, regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{6}$`).MatchString(code), true)
	x, _ := k.TransferToken("k1", p1, "call-1")
	y, _ := k.TransferToken("k1", p1, "call-1")
	eq(t, x == y && strings.HasPrefix(x, "bnx_"), true)
	eq(t, regexp.MustCompile(`^bnx_[A-Za-z0-9_-]{43}$`).MatchString(x), true)
	link, _ := k.LinkToken("k1", p1, "req-1")
	eq(t, regexp.MustCompile(`^bnsl_[A-Za-z0-9_-]{22}$`).MatchString(link), true)
	eq(t, regexp.MustCompile(`^bnsp_[A-Za-z0-9_-]{54}$`).MatchString(k.PutGrant(p1, now)), true)
	tunnel := NewTunnelToken()
	eq(t, regexp.MustCompile(`^bnt_[A-Za-z0-9_-]{43}$`).MatchString(tunnel), true)
	eq(t, tunnel != NewTunnelToken(), true)
	unkeyed := NewTransferToken()
	eq(t, regexp.MustCompile(`^bnx_[A-Za-z0-9_-]{43}$`).MatchString(unkeyed), true)
	eq(t, unkeyed != NewTransferToken(), true)
	eq(t, strings.HasPrefix(k.GrantID(p1, "SHA256:abc"), "g1."), true)
}

func TestDerivedValuesFollowEveryInput(t *testing.T) {
	k := twoRoots(t)
	differ := func(name string, a, b string) {
		t.Helper()
		if a == b {
			t.Errorf("%s: both inputs derive %s", name, a)
		}
	}
	g1, _ := k.GateKey("k1", p1)
	g2, _ := k.GateKey("k1", p2)
	g3, _ := k.GateKey("k2", p1)
	differ("gate key by placement", g1, g2)
	differ("gate key by root", g1, g3)
	ph1, _ := k.Placeholder("k1", p1, "HF_TOKEN")
	ph2, _ := k.Placeholder("k1", p1, "GH_TOKEN")
	ph3, _ := k.Placeholder("k1", p2, "HF_TOKEN")
	differ("placeholder by name", ph1, ph2)
	differ("placeholder by placement", ph1, ph3)
	x1, _ := k.TransferToken("k1", p1, "call-1")
	x2, _ := k.TransferToken("k1", p1, "call-2")
	x3, _ := k.TransferToken("k1", p2, "call-1")
	differ("transfer token by key", x1, x2)
	differ("transfer token by placement", x1, x3)
	l1, _ := k.LinkToken("k1", p1, "req-1")
	l2, _ := k.LinkToken("k1", p1, "req-2")
	differ("link token by request", l1, l2)
	differ("grant id by fingerprint", k.GrantID(p1, "SHA256:a"), k.GrantID(p1, "SHA256:b"))
	differ("put grant by placement", k.PutGrant(p1, now), k.PutGrant(p2, now))
	eq(t, Binding(p1) != Binding(p2), true)
}

// TestDerivedVectors pins every formula to values an independent HKDF and HMAC computed
func TestDerivedVectors(t *testing.T) {
	k := mustParse(t, "k1:"+b64(32, 1))
	eq(t, hex.EncodeToString(Derive(k.Current().Key, "gate")),
		"65d225fac5b7fd9c93b2aeedd6aec6623ed6491bdeb4c92aa54b9aed7d66066b")
	eq(t, hex.EncodeToString(Derive(k.Current().Key, "ref")),
		"b8f315a46542020c3349f40aad6006a9abc41a8527e30b8badcb63ebffad4652")
	must := func(v string, err error) string {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	eq(t, must(k.PreviewToken("k1", "abcdefghijklmnop", 3)),
		"bnp_bIP3uIX-MMggTqlrbZykTfYCCvBREiatjbZFkAcxOZU")
	eq(t, must(k.GateKey("k1", p1)), "fg4ST1bmLz1xRNoHo6chmZXk3L11xUjlHgMipxRXgb4")
	eq(t, k.GrantID(p1, "SHA256:abc"),
		"g1.AAAAAAAAAAAAAAAAAAAAAVNIQTI1NjphYmOrxmYWEo3vAJozVgA-gxpQ")
	eq(t, must(k.TransferToken("k1", p1, "call-1")),
		"bnx_GazhU4Jg-ys8Kb-RSJ-5mriEnAi6cIo67jI_f9O7uZg")
	eq(t, must(k.LinkToken("k1", p1, "req-1")), "bnsl_YVKdUQ95VJmeVPWg3Qr14A")
	eq(t, k.PutGrant(p1, time.Unix(1800000000, 0)),
		"bnsp_AAAAAAAAAAAAAAAAAAAAAQAAAABrSdIAXnl7Q3VoLY0Gs3NOeT2k8w")
	eq(t, must(k.PairingCode("k1", sha("bnsl_x"))), "B3WPQH")
	eq(t, must(k.Placeholder("k1", p1, "HF_TOKEN")), "bnsec_bq77yo65u3edmbw3nfqxxvg7wq")
}

func TestPurposesAreTheDerivedKeys(t *testing.T) {
	eq(t, Purposes, []string{"preview", "gate", "grant", "transfer", "link", "put", "pair",
		"placeholder", "ref"})
}

func TestUnknownKeyIDRefused(t *testing.T) {
	k := twoRoots(t)
	_, err1 := k.PreviewToken("k9", "abcdefghijklmnop", 0)
	_, err2 := k.GateKey("k9", p1)
	_, err3 := k.TransferToken("k9", p1, "call-1")
	_, err4 := k.LinkToken("k9", p1, "req-1")
	_, err5 := k.PairingCode("k9", sha("bnsl_x"))
	_, err6 := k.Placeholder("k9", p1, "HF_TOKEN")
	for i, err := range []error{err1, err2, err3, err4, err5, err6} {
		if err == nil {
			t.Errorf("call %d derived from an unknown root", i+1)
		}
	}
}

func TestEmptyInputsRefused(t *testing.T) {
	k := twoRoots(t)
	_, err1 := k.PreviewToken("k1", "", 0)
	_, err2 := k.TransferToken("k1", p1, "")
	_, err3 := k.LinkToken("k1", p1, "")
	_, err4 := k.Placeholder("k1", p1, "")
	_, err5 := k.PairingCode("k1", []byte("short"))
	for i, err := range []error{err1, err2, err3, err4, err5} {
		if err == nil {
			t.Errorf("call %d derived from an empty or short input", i+1)
		}
	}
}

func TestPutGrantExpiryAndRotation(t *testing.T) {
	k := twoRoots(t)
	g := k.PutGrant(p1, now.Add(10*time.Minute))
	got, err := k.CheckPutGrant(g, now)
	eq(t, err == nil && got == p1, true)
	_, err = k.CheckPutGrant(g, now.Add(11*time.Minute))
	eq(t, err, ErrExpired)
	_, err = k.CheckPutGrant(g, now.Add(10*time.Minute))
	eq(t, err, ErrExpired)
	_, err = mustParse(t, "k3:"+b64(32, 3)+",k2:"+b64(32, 2)+",k1:"+b64(32, 1)).
		CheckPutGrant(g, now)
	eq(t, err == nil, true)
	_, err = mustParse(t, "k3:"+b64(32, 3)+",k1:"+b64(32, 1)).CheckPutGrant(g, now)
	eq(t, err, ErrInvalid)
}

func TestPutGrantForgeriesRefused(t *testing.T) {
	k := twoRoots(t)
	g := k.PutGrant(p1, now.Add(10*time.Minute))
	raw, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(g, "bnsp_"))
	later := append([]byte{}, raw...)
	later[23]++
	other := append([]byte{}, raw...)
	other[15] = 2
	for name, grant := range map[string]string{
		"later expiry":    "bnsp_" + base64.RawURLEncoding.EncodeToString(later),
		"other placement": "bnsp_" + base64.RawURLEncoding.EncodeToString(other),
		"no prefix":       strings.TrimPrefix(g, "bnsp_"),
		"other prefix":    "bnx_" + strings.TrimPrefix(g, "bnsp_"),
		"short":           g[:len(g)-1],
		"padded":          g + "=",
		"empty":           "",
	} {
		if _, err := k.CheckPutGrant(grant, now); err != ErrInvalid {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
}

func TestGrantIDRoundTrip(t *testing.T) {
	k := twoRoots(t)
	id := k.GrantID(p1, "SHA256:abc/def+ghi")
	got, fp, err := k.ParseGrantID(id)
	eq(t, err, nil)
	eq(t, got, p1)
	eq(t, fp, "SHA256:abc/def+ghi")
	_, _, err = mustParse(t, "k3:"+b64(32, 3)+",k2:"+b64(32, 2)).ParseGrantID(id)
	eq(t, err, nil)
	_, _, err = mustParse(t, "k1:"+b64(32, 1)).ParseGrantID(id)
	eq(t, err, ErrInvalid)
	raw, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(id, "g1."))
	raw[17]++
	for name, bad := range map[string]string{
		"other fingerprint": "g1." + base64.RawURLEncoding.EncodeToString(raw),
		"no prefix":         strings.TrimPrefix(id, "g1."),
		"too short":         "g1." + base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		"empty":             "",
	} {
		if _, _, err := k.ParseGrantID(bad); err != ErrInvalid {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
}

func TestBindingGolden(t *testing.T) {
	eq(t, Binding(uuid.MustParse("00000000-0000-0000-0000-000000000001")),
		strings.TrimSpace(readFile(t, "testdata/binding.txt")))
	eq(t, regexp.MustCompile(`^[A-Z2-7]{52}$`).MatchString(Binding(p2)), true)
}

func TestSealRef(t *testing.T) {
	k := twoRoots(t)
	sealed := k.SealRef(p1, "envd-secret")
	eq(t, strings.HasPrefix(sealed, "v1.k2."), true)
	eq(t, strings.Contains(sealed, "envd-secret"), false)
	eq(t, sealed != k.SealRef(p1, "envd-secret"), true)
	got, err := k.OpenRef(p1, sealed)
	eq(t, err, nil)
	eq(t, got, "envd-secret")
	empty, err := k.OpenRef(p1, k.SealRef(p1, ""))
	eq(t, err == nil && empty == "", true)

	if _, err := k.OpenRef(p2, sealed); err == nil {
		t.Error("opened a value sealed for another placement")
	}
	if _, err := mustParse(t, "k1:"+b64(32, 1)).OpenRef(p1, sealed); err == nil {
		t.Error("opened a value without its root")
	}
	rotated := mustParse(t, "k3:"+b64(32, 3)+",k2:"+b64(32, 2))
	if got, err := rotated.OpenRef(p1, sealed); err != nil || got != "envd-secret" {
		t.Errorf("after rotation: %q, %v", got, err)
	}
	body := strings.TrimPrefix(sealed, "v1.k2.")
	raw, _ := base64.RawURLEncoding.DecodeString(body)
	raw[len(raw)-1]++
	for name, bad := range map[string]string{
		"tampered":     "v1.k2." + base64.RawURLEncoding.EncodeToString(raw),
		"other root":   "v1.k1." + body,
		"version 2":    "v2.k2." + body,
		"plaintext":    "envd-secret",
		"no body":      "v1.k2.",
		"short body":   "v1.k2.AAAA",
		"bad encoding": "v1.k2.!" + body,
	} {
		if _, err := k.OpenRef(p1, bad); err == nil {
			t.Errorf("%s: opened", name)
		}
	}
}

func TestNewEnvParses(t *testing.T) {
	a, b := NewEnv(), NewEnv()
	eq(t, a != b, true)
	k := mustParse(t, a)
	eq(t, k.Current().ID, "k1")
	eq(t, len(k.Roots()), 1)
}
