package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHardeningDefaults(t *testing.T) {
	empty, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]Config{"example.toml": loadExample(t), "empty": empty} {
		t.Run(name, func(t *testing.T) {
			eq(t, c.Hardening.MailPorts, []int{25, 465, 587})
			eq(t, c.Hardening.DenyRanges, []string{
				"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "fc00::/7",
				"169.254.0.0/16", "fe80::/10", "fd00:ec2::254/128",
			})
			eq(t, c.Hardening.PoolList, "/etc/bugnet/mining-pools.txt")
			eq(t, c.Hardening.Rules, map[string]HardenRule{
				"e2b":   {Required: true, Allow: []string{}},
				"boxd":  {Required: true, Allow: []string{}},
				"koyeb": {Required: true, Allow: []string{}},
				"boat":  {Required: true, Allow: []string{}},
				"lium":  {Required: true, Allow: []string{}, NoGateway: true},
				"hf":    {Allow: []string{}},
			})
		})
	}
}

func TestHardeningSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `[hardening]
mail_ports = [25]
deny_ranges = ["10.1.2.3/8", "169.254.169.254"]
pool_list = "pools.txt"

[boat]
harden_required = false
harden_allow = ["100.64.0.1", "10.0.0.0/24"]
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Hardening.MailPorts, []int{25})
	eq(t, c.Hardening.DenyRanges, []string{"10.0.0.0/8", "169.254.169.254/32"})
	abs, _ := filepath.Abs("pools.txt")
	eq(t, c.Hardening.PoolList, abs)
	eq(t, c.Hardening.Rules["boat"], HardenRule{
		Required: false, Allow: []string{"100.64.0.1/32", "10.0.0.0/24"},
	})
	eq(t, c.Hardening.Rules["e2b"].Required, true)
}

func TestHardeningValidation(t *testing.T) {
	many := strings.Repeat("25, ", 65) + "25"
	cases := []struct{ name, toml, want string }{
		{"mail port zero", "[hardening]\nmail_ports = [0]",
			"hardening.mail_ports entry 1 must be a port from 1 to 65535"},
		{"mail port too high", "[hardening]\nmail_ports = [25, 70000]",
			"hardening.mail_ports entry 2"},
		{"too many mail ports", "[hardening]\nmail_ports = [" + many + "]",
			"hardening.mail_ports holds more than 64 ports"},
		{"mail ports as strings", "[hardening]\nmail_ports = [\"25\"]",
			"hardening.mail_ports must be an array of whole numbers"},
		{"deny range as a name", "[hardening]\ndeny_ranges = [\"metadata.internal\"]",
			"hardening.deny_ranges entry 1 must be an IP address or a CIDR"},
		{"allow with a zone", "[koyeb]\nharden_allow = [\"fe80::1%eth0\"]",
			"koyeb.harden_allow entry 1"},
		{"required as a string", "[e2b]\nharden_required = \"yes\"",
			"e2b.harden_required must be true or false"},
	}
	for _, c := range cases {
		msg := problem(t, c.toml)
		if !strings.Contains(msg, c.want) {
			t.Errorf("%s: %q, want an error with %q", c.name, msg, c.want)
		}
		for _, v := range []string{"metadata.internal", "eth0", "70000"} {
			if strings.Contains(msg, v) {
				t.Errorf("%s: error quotes %q: %s", c.name, v, msg)
			}
		}
	}
}

func TestLoadPoolList(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	got, err := LoadPoolList(write("pools.txt",
		"# pools\n\n203.0.113.0/24\n  198.51.100.7  \n2001:db8::/32\n"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got, []string{"203.0.113.0/24", "198.51.100.7/32", "2001:db8::/32"})

	for _, path := range []string{"", filepath.Join(dir, "missing.txt"),
		write("empty.txt", "# nothing yet\n")} {
		got, err := LoadPoolList(path)
		if err != nil || len(got) != 0 {
			t.Fatalf("pool list %q: %v, %v, want an empty list", path, got, err)
		}
	}

	_, err = LoadPoolList(write("bad.txt", "203.0.113.0/24\npool.example.com\n"))
	if err == nil || !strings.Contains(err.Error(), "line 2 is not an IP address or a CIDR") ||
		strings.Contains(err.Error(), "pool.example.com") {
		t.Fatalf("a name in the pool list: %v", err)
	}

	var big strings.Builder
	for i := range MaxPools + 1 {
		fmt.Fprintf(&big, "10.%d.%d.0/24\n", i/256, i%256)
	}
	_, err = LoadPoolList(write("big.txt", big.String()))
	if err == nil || !strings.Contains(err.Error(), "more than 10000 entries") {
		t.Fatalf("a pool list past the cap: %v", err)
	}
}
