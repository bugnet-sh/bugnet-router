package lium

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// secretWords are the forms a secret could take in a request, raw, base64 and hex
func secretWords(secret []byte) []string {
	return []string{
		string(secret), base64.StdEncoding.EncodeToString(secret),
		base64.RawStdEncoding.EncodeToString(secret), base64.URLEncoding.EncodeToString(secret),
		base64.RawURLEncoding.EncodeToString(secret), hex.EncodeToString(secret),
		strings.ToUpper(hex.EncodeToString(secret)),
	}
}

func TestLiumSendsNoSecret(t *testing.T) {
	f, p := liveFake(t)
	ctx := context.Background()
	var refs []provider.Ref
	keyed := mustCreate(t, p, createReq(testKey))
	refs = append(refs, keyed)
	relist(t, p)
	f.inject(rentRoute, fault{drop: true, times: 1})
	check := createReq("")
	check.Labels = map[string]string{"bugnet_image_check": "1"}
	refs = append(refs, mustCreate(t, p, check))
	for _, ref := range refs {
		f.setPod(ref.ID, func(fp *fakePod) { fp.status = "RUNNING" })
		if _, err := p.ReadyMeta(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Inspect(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if err := p.SetLifetime(ctx, ref, time.Now().Add(20*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.ListOwned(ctx, placementLabels()); err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		if err := p.Destroy(ctx, ref); err != nil {
			t.Fatal(err)
		}
	}

	needles := append(secretWords([]byte(fakeKey)), secretWords(testSeed)...)
	for _, ref := range refs {
		priv := p.podPrivateKey(ref.Meta["pod_name"])
		needles = append(needles, secretWords(priv.Seed())...)
		needles = append(needles, secretWords(priv)...)
	}
	needles = append(needles, "PRIVATE KEY", "bnsec_", "BUGNET_", "LIUM_")

	f.mu.Lock()
	requests := append([]recorded(nil), f.requests...)
	f.mu.Unlock()
	if len(requests) < 15 {
		t.Fatalf("only %d requests recorded", len(requests))
	}
	for _, r := range requests {
		var sent bytes.Buffer
		sent.WriteString(r.method + " " + r.path + "?" + r.query.Encode() + "\n")
		sent.Write(r.body)
		for name, values := range r.header {
			if name == "X-Api-Key" {
				continue
			}
			sent.WriteString("\n" + name + ": " + strings.Join(values, ", "))
		}
		for _, n := range needles {
			if n != "" && bytes.Contains(sent.Bytes(), []byte(n)) {
				t.Errorf("%s %s carries a secret: %q", r.method, r.path, n)
			}
		}
		for _, h := range []string{"Authorization", "Cookie"} {
			if r.header.Get(h) != "" {
				t.Errorf("%s %s sent %s", r.method, r.path, h)
			}
		}
		if r.path == "/executors" && r.header.Get("X-Api-Key") != "" {
			t.Errorf("the keyless listing sent the key")
		}
	}
}
