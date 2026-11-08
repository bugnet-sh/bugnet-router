package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
)

// every caller token, the operator and payments tokens, no BUGNET_ORDER_PRIVATE_KEY
func TestKeysProductionSkipsOrderKey(t *testing.T) {
	out, stderr, code := runCLI(t, env(nil), "keys", "--production")
	if code != 0 {
		t.Fatalf("keys --production exited %d: %s", code, stderr)
	}
	for _, name := range []string{
		"BUGNET_INTERNAL_TOKEN", "BUGNET_HANDLE_KEY", "BUGNET_EVENT_SIGNING_KEY",
		"BUGNET_DATA_PLANE_KEYS", "BUGNET_EDGE_TOKEN", "BUGNET_SECRETS_TOKEN", "BUGNET_SSH_TOKEN",
		"BUGNET_EGRESS_TOKEN", "BUGNET_EGRESS_FETCH_TOKEN", "BUGNET_OPERATOR_TOKEN",
		"BUGNET_PAYMENTS_ROUTER_TOKEN", "BUGNET_SESSIONS_TOKEN", "BUGNET_HOOK_PATH_SECRET",
	} {
		if !strings.Contains(out, "\n"+name+"=") && !strings.HasPrefix(out, name+"=") {
			t.Errorf("keys --production lacks %s", name)
		}
	}
	if strings.Contains(out, "BUGNET_ORDER_PRIVATE_KEY") {
		t.Fatal("keys --production printed the order key, which Payments holds")
	}
}

func TestPubkeyEventPrintsOnlyThePublicHalf(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	seed := base64.StdEncoding.EncodeToString(priv.Seed())
	out, stderr, code := runCLI(t, env(map[string]string{"BUGNET_EVENT_SIGNING_KEY": seed}),
		"pubkey", "--event")
	if code != 0 {
		t.Fatalf("pubkey --event exited %d: %s", code, stderr)
	}
	eq(t, out, base64.StdEncoding.EncodeToString(pub)+"\n")
	if _, _, code := runCLI(t, env(nil), "pubkey", "--event"); code != 2 {
		t.Fatalf("pubkey --event with no key exited %d, want 2", code)
	}
}

// an unknown key fails, an empty list is allowed
func TestSiteGatewayUsers(t *testing.T) {
	base := "ipv4 = \"203.0.113.7\"\nadmin_ssh_port = 22\nssh_gateway_port = 2222\n"
	f := rendered(t, repoNames, writeTOML(t, "site.toml",
		base+"gateway_users = [\"bugnet-gateway\", \"bugnet-crawl\"]\n"), false)
	if !strings.Contains(f["deploy.env"], "\nGATEWAY_USERS=bugnet-gateway bugnet-crawl\n") ||
		!strings.Contains(f["deploy.env"], "\nSSH_GATEWAY_PORT=2222\n") {
		t.Fatalf("deploy.env:\n%s", f["deploy.env"])
	}
	// Answer (c): admin SSH stays on 22 and frontend ssh binds 2222
	ssh := "frontend ssh\n    bind :2222\n    bind :::2222 v6only\n"
	if !strings.Contains(f["haproxy.cfg"], ssh) {
		t.Fatalf("haproxy.cfg:\n%s", f["haproxy.cfg"])
	}
	if _, err := loadSite(writeTOML(t, "site.toml", base+"gateway_users = []\n")); err != nil {
		t.Fatal(err)
	}
	for _, doc := range []string{
		base + "gateway_users = [\"Bad User\"]\n",
		base + "gateway_users = [\"a\", \"a\"]\n",
		base + "gateway_user = [\"a\"]\n",
	} {
		if _, err := loadSite(writeTOML(t, "site.toml", doc)); err == nil {
			t.Errorf("%q loaded", doc)
		}
	}
}

func TestX402AllowedHosts(t *testing.T) {
	index := writeTOML(t, "x402-index.json", `{"v": 1, "services": [
		{"host": "a.workers.dev"}, {"host": "b.example.com"}, {"host": "a.workers.dev"}]}`)
	settings := writeTOML(t, "router.toml", "[x402]\nenabled = true\n")
	out, stderr, code := runCLI(t, env(nil), "x402", "allowed-hosts", "--index", index,
		"--config", settings)
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	eq(t, out, "allowed_shared_hosts = [\"a.workers.dev\"]\n")
}
