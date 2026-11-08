package vps

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestEgressUnitLoadsX402Credential(t *testing.T) {
	b, err := os.ReadFile("systemd/bugnet-egress@.service")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(b), "\n")
	for _, want := range []string{
		"LoadCredential=egress_x402_client_key:/etc/bugnet/keys/egress-x402-client.key",
		"LoadCredential=egress_ca_key:/etc/bugnet/keys/egress-ca.key",
		"ExecReload=/bin/kill -HUP $MAINPID",
	} {
		found := false
		for _, l := range lines {
			found = found || l == want
		}
		if !found {
			t.Errorf("the egress unit lacks %q", want)
		}
	}
}

func TestCheckComparesPaymentsTimeout(t *testing.T) {
	for _, tc := range []struct {
		egress, payments string
		pass             bool
	}{{"2s", "2s", false}, {"3s", "2s", true}} {
		dir := t.TempDir()
		write := func(name, body string) {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		write("egress.toml", "listen_addr = \"127.0.0.1:9443\"\n\n[x402]\n"+
			"payments_timeout = \""+tc.egress+"\"   # a comment\n")
		write("payments.toml", "[http]\npublic_listen = \"127.0.0.1:8450\"\n\n"+
			"[x402]                 # the signer\nsign_timeout = \""+tc.payments+"\"\n")
		cmd := exec.Command("bash", "check.sh", "timeouts")
		cmd.Env = append(os.Environ(), "BUGNET_ETC="+dir)
		out, err := cmd.CombinedOutput()
		if (err == nil) != tc.pass {
			t.Errorf("egress %s, Payments %s: passed %v, want %v:\n%s", tc.egress, tc.payments,
				err == nil, tc.pass, out)
		}
	}
}
